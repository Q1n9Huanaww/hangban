import asyncio
from dataclasses import asdict
import math
import time
from statistics import median
from typing import Any, Dict, List, Optional

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

WINDOW_SECONDS = {
    "15m": 15 * 60,
    "1h": 60 * 60,
    "6h": 6 * 60 * 60,
}


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
    return (r_km * c) * 0.539956803


def _initial_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dl)
    brng = math.degrees(math.atan2(x, y))
    return (brng + 360.0) % 360.0


def _bearing_to_corridor(b: float) -> str:
    bins = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = int(((b + 22.5) % 360) // 45)
    return bins[idx]


def _derive_flight_phase(alt: Optional[float], gs: Optional[float], vr: Optional[float]) -> str:
    if isinstance(alt, (int, float)) and isinstance(gs, (int, float)):
        if alt < 1500 and gs < 90:
            return "ground"
    if isinstance(vr, (int, float)):
        if vr > 300:
            return "climb"
        if vr < -300:
            return "descent"
    return "cruise"


def _derive_anomaly(current: Dict[str, Any], prev: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    anomalies: List[str] = []
    gs = current.get("gs_kt")
    alt = current.get("altitude_ft")
    vr = current.get("vertical_rate_fpm")
    trk = current.get("track")

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
        dt = current["ts"] - prev["ts"]
        if 0 < dt <= 120:
            delta = abs(trk - prev["track"])
            delta = min(delta, 360 - delta)
            if delta >= 45:
                anomalies.append("abrupt_heading_change")

    return {
        "is_anomaly": len(anomalies) > 0,
        "anomaly_type": anomalies,
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
    level = "high" if score >= 0.75 else ("medium" if score >= 0.5 else "low")
    return {
        "confidence": round(score, 3),
        "confidence_level": level,
        "freshness_sec": round(freshness, 3),
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


def _derive_status_change_events(points: List[TracePoint]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    if not points:
        return events

    prev_phase: Optional[str] = None
    prev_anomaly_active = False
    prev_anomaly_types: List[str] = []
    prev_point_map: Optional[Dict[str, Any]] = None

    for p in points:
        item = asdict(p)
        anomaly = _derive_anomaly(item, prev_point_map)
        phase = _derive_flight_phase(item.get("altitude_ft"), item.get("gs_kt"), item.get("vertical_rate_fpm"))
        ts = item["ts"]

        if prev_phase is None:
            prev_phase = phase
        elif phase != prev_phase:
            events.append(
                {
                    "type": "phase_change",
                    "from": prev_phase,
                    "to": phase,
                    "ts": round(ts, 3),
                    "ts_iso": to_iso(ts),
                }
            )
            prev_phase = phase

        active = anomaly["is_anomaly"]
        types = anomaly["anomaly_type"]
        if active and not prev_anomaly_active:
            events.append(
                {
                    "type": "anomaly_start",
                    "anomaly_type": types,
                    "ts": round(ts, 3),
                    "ts_iso": to_iso(ts),
                }
            )
        elif not active and prev_anomaly_active:
            events.append(
                {
                    "type": "anomaly_end",
                    "anomaly_type": prev_anomaly_types,
                    "ts": round(ts, 3),
                    "ts_iso": to_iso(ts),
                }
            )
        elif active and prev_anomaly_active and types != prev_anomaly_types:
            events.append(
                {
                    "type": "anomaly_update",
                    "from": prev_anomaly_types,
                    "to": types,
                    "ts": round(ts, 3),
                    "ts_iso": to_iso(ts),
                }
            )

        prev_anomaly_active = active
        prev_anomaly_types = types
        prev_point_map = item

    return events


def _window_subset(points: List[TracePoint], sec: int) -> List[TracePoint]:
    if not points:
        return []
    end_ts = points[-1].ts
    start_ts = end_ts - sec
    subset = [p for p in points if p.ts >= start_ts]
    return subset if subset else [points[-1]]


def _derive_window_trend(points: List[TracePoint], sec: int) -> Dict[str, Any]:
    subset = _window_subset(points, sec)
    if len(subset) < 2:
        return {"point_count": len(subset)}

    first = subset[0]
    last = subset[-1]
    dt = max(1.0, last.ts - first.ts)
    gs_first = first.gs_kt or 0.0
    gs_last = last.gs_kt or 0.0
    alt_first = first.altitude_ft or 0.0
    alt_last = last.altitude_ft or 0.0

    phase_counts = {"ground": 0, "climb": 0, "cruise": 0, "descent": 0}
    anomaly_points = 0
    prev_map: Optional[Dict[str, Any]] = None
    for p in subset:
        m = asdict(p)
        phase = _derive_flight_phase(m.get("altitude_ft"), m.get("gs_kt"), m.get("vertical_rate_fpm"))
        phase_counts[phase] = phase_counts.get(phase, 0) + 1
        if _derive_anomaly(m, prev_map)["is_anomaly"]:
            anomaly_points += 1
        prev_map = m

    return {
        "point_count": len(subset),
        "duration_sec": round(dt, 3),
        "avg_groundspeed_kt": round(sum((p.gs_kt or 0.0) for p in subset) / len(subset), 2),
        "avg_altitude_ft": round(sum((p.altitude_ft or 0.0) for p in subset) / len(subset), 2),
        "groundspeed_slope_kt_per_min": round((gs_last - gs_first) / (dt / 60.0), 3),
        "altitude_slope_ft_per_min": round((alt_last - alt_first) / (dt / 60.0), 3),
        "anomaly_density": round(anomaly_points / len(subset), 4),
        "phase_distribution": phase_counts,
    }


def _window_trends(points: List[TracePoint], windows: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for w in windows:
        sec = WINDOW_SECONDS.get(w)
        if sec:
            out[w] = _derive_window_trend(points, sec)
    return out


def _sanitize_input(raw: Dict[str, Any]) -> Dict[str, Any]:
    icaos: List[str] = []
    single = str(raw.get("icao", "")).strip().lower()
    if single:
        icaos.append(single)
    raw_list = raw.get("icaos", [])
    if isinstance(raw_list, list):
        for item in raw_list:
            v = str(item).strip().lower()
            if v:
                icaos.append(v)
    dedup_icaos = []
    seen = set()
    for v in icaos:
        if v not in seen:
            dedup_icaos.append(v)
            seen.add(v)
    if not dedup_icaos:
        raise ValueError("input.icao or input.icaos is required")

    mode = str(raw.get("mode", "full")).strip().lower() or "full"
    if mode not in {"full", "recent"}:
        mode = "full"
    include_points = bool(raw.get("includePoints", False))
    warmup = bool(raw.get("warmup", True))

    max_points = raw.get("maxPoints")
    if not isinstance(max_points, int) or max_points <= 0:
        max_points = None

    windows = raw.get("windows", ["15m", "1h", "6h"])
    if not isinstance(windows, list):
        windows = ["15m", "1h", "6h"]
    windows = [str(x) for x in windows if str(x) in WINDOW_SECONDS]
    if not windows:
        windows = ["15m", "1h", "6h"]

    concurrency = raw.get("concurrency", 5)
    if not isinstance(concurrency, int) or concurrency < 1:
        concurrency = 5
    concurrency = min(concurrency, 20)

    region = raw.get("region")
    if not isinstance(region, dict):
        region = None

    return {
        "icaos": dedup_icaos,
        "mode": mode,
        "include_points": include_points,
        "max_points": max_points,
        "warmup": warmup,
        "windows": windows,
        "concurrency": concurrency,
        "region": region,
    }


def _in_region(lat: float, lon: float, region: Dict[str, Any]) -> bool:
    try:
        min_lat = float(region["minLat"])
        max_lat = float(region["maxLat"])
        min_lon = float(region["minLon"])
        max_lon = float(region["maxLon"])
    except Exception:
        return True
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


def _derive_region_aggregate(flights: List[Dict[str, Any]], region: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    latest_rows = [f.get("latest") for f in flights if f.get("latest")]
    if region:
        latest_rows = [
            x
            for x in latest_rows
            if isinstance(x.get("lat"), (int, float))
            and isinstance(x.get("lon"), (int, float))
            and _in_region(x["lat"], x["lon"], region)
        ]

    if not latest_rows:
        return {
            "active_flights": 0,
            "avg_altitude_ft": None,
            "avg_groundspeed_kt": None,
            "congestion_index": 0.0,
            "dominant_corridors": {},
        }

    alt = [x["altitude_ft"] for x in latest_rows if isinstance(x.get("altitude_ft"), (int, float))]
    gs = [x["gs_kt"] for x in latest_rows if isinstance(x.get("gs_kt"), (int, float))]
    flights_n = len(latest_rows)

    # congestion index: simple normalized score using count and crowding around low altitude flights
    low_alt = len([x for x in latest_rows if isinstance(x.get("altitude_ft"), (int, float)) and x["altitude_ft"] < 10000])
    congestion_index = min(1.0, (flights_n / 120.0) * 0.75 + (low_alt / max(1, flights_n)) * 0.25)

    corridor_counts: Dict[str, int] = {}
    for f in flights:
        pts = f.get("_raw_points") or []
        if len(pts) < 2:
            continue
        p0 = pts[0]
        p1 = pts[-1]
        b = _initial_bearing_deg(p0.lat, p0.lon, p1.lat, p1.lon)
        c = _bearing_to_corridor(b)
        corridor_counts[c] = corridor_counts.get(c, 0) + 1

    return {
        "active_flights": flights_n,
        "avg_altitude_ft": round(sum(alt) / len(alt), 2) if alt else None,
        "avg_groundspeed_kt": round(sum(gs) / len(gs), 2) if gs else None,
        "congestion_index": round(congestion_index, 3),
        "dominant_corridors": dict(sorted(corridor_counts.items(), key=lambda x: x[1], reverse=True)),
    }


def _fetch_one(icao: str, mode: str, warmup: bool, max_points: Optional[int], windows: List[str], include_points: bool) -> Dict[str, Any]:
    trace_url = build_trace_url(icao, mode=mode)
    session = create_session(icao)
    if warmup:
        warmup_page(session, icao)

    payload = fetch_json_with_retry(session, trace_url)
    points = parse_trace(payload)
    if max_points is not None and len(points) > max_points:
        points = points[-max_points:]

    now_ts = time.time()
    latest = asdict(points[-1]) if points else None
    if latest:
        prev = asdict(points[-2]) if len(points) >= 2 else None
        anomaly = _derive_anomaly(latest, prev)
        phase = _derive_flight_phase(latest.get("altitude_ft"), latest.get("gs_kt"), latest.get("vertical_rate_fpm"))
        conf = _derive_confidence(latest, points, now_ts)
        latest.update({"flight_phase": phase, **anomaly, **conf})

    row = {
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
            "trace_url": trace_url,
        },
        "latest": latest,
        "summary": _derive_summary(points),
        "status_change_events": _derive_status_change_events(points),
        "window_trends": _window_trends(points, windows),
        "_raw_points": points,  # internal only
    }
    if include_points:
        row["points"] = [asdict(p) for p in points]
    return row


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        cfg = _sanitize_input(actor_input)

        icaos = cfg["icaos"]
        mode = cfg["mode"]
        include_points = cfg["include_points"]
        max_points = cfg["max_points"]
        do_warmup = cfg["warmup"]
        windows = cfg["windows"]
        concurrency = cfg["concurrency"]
        region = cfg["region"]

        Actor.log.info(f"batch_size={len(icaos)} mode={mode} concurrency={concurrency}")

        sem = asyncio.Semaphore(concurrency)

        async def run_one(icao: str) -> Dict[str, Any]:
            async with sem:
                try:
                    return await asyncio.to_thread(
                        _fetch_one,
                        icao,
                        mode,
                        do_warmup,
                        max_points,
                        windows,
                        include_points,
                    )
                except Exception as e:
                    return {
                        "meta": {
                            "icao": icao,
                            "mode": mode,
                            "error": str(e),
                            "query_ts": round(time.time(), 3),
                            "query_ts_iso": to_iso(time.time()),
                        },
                        "latest": None,
                        "summary": {},
                        "status_change_events": [],
                        "window_trends": {},
                        "_raw_points": [],
                    }

        flights = await asyncio.gather(*[run_one(x) for x in icaos])
        ok = len([f for f in flights if f.get("latest")])
        failed = len(flights) - ok

        aggregate = _derive_region_aggregate(flights, region)
        result = {
            "meta": {
                "batch_size": len(icaos),
                "success_count": ok,
                "failed_count": failed,
                "mode": mode,
                "windows": windows,
                "region_filter_applied": bool(region),
                "query_ts": round(time.time(), 3),
                "query_ts_iso": to_iso(time.time()),
            },
            "flights": [{k: v for k, v in f.items() if k != "_raw_points"} for f in flights],
            "region_aggregate": aggregate,
        }

        # backward compatibility: when single icao, expose top-level shortcuts
        if len(flights) == 1:
            one = flights[0]
            result["latest"] = one.get("latest")
            result["summary"] = one.get("summary")
            result["status_change_events"] = one.get("status_change_events")
            result["window_trends"] = one.get("window_trends")
            one_meta = one.get("meta", {})
            result["single_meta"] = one_meta

        await Actor.push_data(result)
        await Actor.set_value("OUTPUT", result)
        Actor.log.info(f"done success={ok} failed={failed}")


if __name__ == "__main__":
    asyncio.run(main())
