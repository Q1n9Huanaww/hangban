import asyncio
from dataclasses import asdict
import math
import time
from statistics import median
from typing import Any, Dict, List

from apify import Actor

from adsb_trace_fetch import (
    TracePoint,
    build_trace_url,
    create_session,
    fetch_json_with_retry,
    parse_trace,
    to_iso,
    warmup_page,
)


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r_km = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    km = r_km * c
    return km * 0.539956803


def _derive_flight_phase(latest: Dict[str, Any]) -> str:
    alt = latest.get("altitude_ft")
    gs = latest.get("gs_kt")
    vr = latest.get("vertical_rate_fpm")
    if isinstance(alt, (int, float)) and isinstance(gs, (int, float)):
        if alt < 1500 and gs < 90:
            return "ground"
    if isinstance(vr, (int, float)):
        if vr > 300:
            return "climb"
        if vr < -300:
            return "descent"
    return "cruise"


def _derive_anomaly(latest: Dict[str, Any], prev: Dict[str, Any] | None) -> Dict[str, Any]:
    anomalies: List[str] = []
    gs = latest.get("gs_kt")
    alt = latest.get("altitude_ft")
    vr = latest.get("vertical_rate_fpm")
    trk = latest.get("track")

    if isinstance(vr, (int, float)):
        if vr >= 3500:
            anomalies.append("rapid_climb")
        elif vr <= -3500:
            anomalies.append("rapid_descent")
    if isinstance(gs, (int, float)) and gs > 700:
        anomalies.append("high_groundspeed")
    if isinstance(gs, (int, float)) and isinstance(alt, (int, float)) and alt < 10000 and gs > 420:
        anomalies.append("low_alt_high_speed")

    if prev and isinstance(trk, (int, float)) and isinstance(prev.get("track"), (int, float)):
        dt = latest["ts"] - prev["ts"]
        if dt > 0 and dt <= 120:
            delta = abs(trk - prev["track"])
            delta = min(delta, 360 - delta)
            if delta >= 45:
                anomalies.append("abrupt_heading_change")

    return {
        "is_anomaly": len(anomalies) > 0,
        "anomaly_type": anomalies,
    }


def _derive_summary(points: List[TracePoint]) -> Dict[str, Any]:
    if not points:
        return {}

    alt_values = [p.altitude_ft for p in points if isinstance(p.altitude_ft, (int, float))]
    gs_values = [p.gs_kt for p in points if isinstance(p.gs_kt, (int, float))]
    first = points[0]
    last = points[-1]

    climb_gain = 0.0
    descent_loss = 0.0
    distance_nm = 0.0
    for i in range(1, len(points)):
        p0 = points[i - 1]
        p1 = points[i]
        if isinstance(p0.altitude_ft, (int, float)) and isinstance(p1.altitude_ft, (int, float)):
            d_alt = p1.altitude_ft - p0.altitude_ft
            if abs(d_alt) <= 5000:
                if d_alt > 0:
                    climb_gain += d_alt
                else:
                    descent_loss += -d_alt
        distance_nm += _haversine_nm(p0.lat, p0.lon, p1.lat, p1.lon)

    return {
        "duration_sec": round(max(0.0, last.ts - first.ts), 3),
        "distance_nm": round(distance_nm, 3),
        "min_altitude_ft": min(alt_values) if alt_values else None,
        "max_altitude_ft": max(alt_values) if alt_values else None,
        "avg_groundspeed_kt": round(sum(gs_values) / len(gs_values), 2) if gs_values else None,
        "total_climb_ft": round(climb_gain, 2),
        "total_descent_ft": round(descent_loss, 2),
    }


def _derive_confidence(latest: Dict[str, Any], points: List[TracePoint], now_ts: float) -> Dict[str, Any]:
    score = 0.5
    source = latest.get("source")
    freshness = max(0.0, now_ts - latest["ts"])
    if source == "adsb_icao":
        score += 0.25
    elif source:
        score += 0.1

    if freshness < 120:
        score += 0.15
    elif freshness < 600:
        score += 0.05
    else:
        score -= 0.05

    n = len(points)
    if n >= 200:
        score += 0.1
    elif n >= 30:
        score += 0.05

    if n >= 3:
        dts = [points[i].ts - points[i - 1].ts for i in range(1, n)]
        dts = [v for v in dts if v > 0]
        if dts:
            med = median(dts)
            if med <= 30:
                score += 0.1
            elif med <= 90:
                score += 0.05

    score = max(0.0, min(1.0, score))
    if score >= 0.75:
        level = "high"
    elif score >= 0.5:
        level = "medium"
    else:
        level = "low"
    return {
        "confidence": round(score, 3),
        "confidence_level": level,
        "freshness_sec": round(freshness, 3),
    }


def _sanitize_input(raw: Dict[str, Any]) -> Dict[str, Any]:
    icao = str(raw.get("icao", "")).strip().lower()
    if not icao:
        raise ValueError("input.icao is required")

    mode = str(raw.get("mode", "full")).strip().lower() or "full"
    if mode not in {"full", "recent"}:
        mode = "full"

    include_points = bool(raw.get("includePoints", False))
    warmup = bool(raw.get("warmup", True))

    max_points = raw.get("maxPoints")
    if isinstance(max_points, int) and max_points > 0:
        pass
    else:
        max_points = None

    return {
        "icao": icao,
        "mode": mode,
        "include_points": include_points,
        "max_points": max_points,
        "warmup": warmup,
    }


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        cfg = _sanitize_input(actor_input)

        icao = cfg["icao"]
        mode = cfg["mode"]
        include_points = cfg["include_points"]
        max_points = cfg["max_points"]
        do_warmup = cfg["warmup"]

        trace_url = build_trace_url(icao, mode=mode)
        Actor.log.info(f"trace_url={trace_url}")

        session = create_session(icao)
        if do_warmup:
            warmup_page(session, icao)

        payload = fetch_json_with_retry(session, trace_url)
        points = parse_trace(payload)

        if max_points is not None and len(points) > max_points:
            points = points[-max_points:]

        now_ts = time.time()
        latest = asdict(points[-1]) if points else None
        if latest:
            phase = _derive_flight_phase(latest)
            prev = asdict(points[-2]) if len(points) >= 2 else None
            anomaly = _derive_anomaly(latest, prev)
            conf = _derive_confidence(latest, points, now_ts)
            latest.update({"flight_phase": phase, **anomaly, **conf})

        summary = _derive_summary(points)
        result = {
            "meta": {
                "icao": payload.get("icao"),
                "registration": payload.get("r"),
                "type": payload.get("t"),
                "desc": payload.get("desc"),
                "base_timestamp": payload.get("timestamp"),
                "base_timestamp_iso": to_iso(float(payload.get("timestamp", 0.0))),
                "point_count": len(points),
                "mode": mode,
                "query_ts": round(now_ts, 3),
                "query_ts_iso": to_iso(now_ts),
            },
            "latest": latest,
            "summary": summary,
        }
        if include_points:
            result["points"] = [asdict(p) for p in points]

        await Actor.push_data(result)
        await Actor.set_value("OUTPUT", result)
        Actor.log.info(f"done icao={payload.get('icao')} points={len(points)}")


if __name__ == "__main__":
    asyncio.run(main())
