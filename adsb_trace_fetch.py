#!/usr/bin/env python3
import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests


LOG = logging.getLogger("adsb-trace")


@dataclass
class TracePoint:
    ts: float
    lat: float
    lon: float
    altitude_ft: Optional[float]
    gs_kt: Optional[float]
    track: Optional[float]
    vertical_rate_fpm: Optional[float]
    source: Optional[str]


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def shard_from_icao(icao: str) -> str:
    icao = icao.strip().lower()
    if len(icao) < 2:
        raise ValueError(f"invalid ICAO24: {icao!r}")
    return icao[-2:]


def build_trace_url(icao: str, mode: str = "full", base: str = "https://globe.adsbexchange.com") -> str:
    icao = icao.strip().lower()
    shard = shard_from_icao(icao)
    if mode not in {"full", "recent"}:
        raise ValueError("mode must be 'full' or 'recent'")
    return f"{base}/data/traces/{shard}/trace_{mode}_{icao}.json"


def create_session(icao: str) -> requests.Session:
    referer = f"https://globe.adsbexchange.com/?icao={icao.lower()}"
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer,
            "Origin": "https://globe.adsbexchange.com",
        }
    )
    return s


def warmup_page(session: requests.Session, icao: str, timeout: float = 15.0) -> None:
    url = f"https://globe.adsbexchange.com/?icao={icao.lower()}"
    try:
        resp = session.get(url, timeout=timeout)
        LOG.debug("warmup status=%s url=%s", resp.status_code, url)
    except requests.RequestException as exc:
        LOG.warning("warmup failed: %s", exc)


def fetch_json_with_retry(
    session: requests.Session,
    url: str,
    retries: int = 4,
    timeout: float = 20.0,
    backoff: float = 1.25,
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            LOG.warning("attempt=%d status=%d url=%s", attempt, resp.status_code, url)
            if resp.status_code in {403, 429, 500, 502, 503, 504}:
                time.sleep(backoff * attempt)
                continue
            resp.raise_for_status()
        except requests.RequestException as exc:
            last_exc = exc
            LOG.warning("attempt=%d request failed: %s", attempt, exc)
            time.sleep(backoff * attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"failed to fetch {url}")


def parse_trace(payload: Dict[str, Any]) -> List[TracePoint]:
    base_ts = float(payload.get("timestamp", 0.0) or 0.0)
    out: List[TracePoint] = []
    for row in payload.get("trace", []):
        if not isinstance(row, list) or len(row) < 10:
            continue
        try:
            ts = base_ts + float(row[0])
            lat = float(row[1])
            lon = float(row[2])
        except (TypeError, ValueError):
            continue
        out.append(
            TracePoint(
                ts=ts,
                lat=lat,
                lon=lon,
                altitude_ft=_safe_float(row[3]) if len(row) > 3 else None,
                gs_kt=_safe_float(row[4]) if len(row) > 4 else None,
                track=_safe_float(row[5]) if len(row) > 5 else None,
                vertical_rate_fpm=_extract_vertical_rate(row),
                source=str(row[9]) if len(row) > 9 and row[9] is not None else None,
            )
        )
    return out


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _extract_vertical_rate(row: List[Any]) -> Optional[float]:
    # Preferred: enriched object at row[8]
    details = row[8] if len(row) > 8 and isinstance(row[8], dict) else None
    if details:
        for key in ("baro_rate", "geom_rate", "vert_rate"):
            val = _safe_float(details.get(key))
            if val is not None:
                return val

    # Fallback: compact numeric slots often include rate values
    for idx in (7, 11):
        if len(row) > idx:
            val = _safe_float(row[idx])
            if val is not None:
                return val
    return None


def to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="ADS-B Exchange trace fetch demo")
    parser.add_argument("--icao", required=True, help="ICAO24 (e.g. 3c4598)")
    parser.add_argument("--mode", default="full", choices=["full", "recent"], help="trace mode")
    parser.add_argument("--no-warmup", action="store_true", help="skip landing-page warmup")
    parser.add_argument("--output", default="", help="output JSON file path")
    parser.add_argument("--verbose", action="store_true", help="enable debug logs")
    args = parser.parse_args()

    setup_logging(args.verbose)
    icao = args.icao.strip().lower()
    url = build_trace_url(icao, mode=args.mode)
    LOG.info("trace_url=%s", url)

    session = create_session(icao)
    if not args.no_warmup:
        warmup_page(session, icao)

    payload = fetch_json_with_retry(session, url)
    points = parse_trace(payload)
    LOG.info("icao=%s points=%d", payload.get("icao"), len(points))

    result = {
        "meta": {
            "icao": payload.get("icao"),
            "registration": payload.get("r"),
            "type": payload.get("t"),
            "desc": payload.get("desc"),
            "base_timestamp": payload.get("timestamp"),
            "base_timestamp_iso": to_iso(float(payload.get("timestamp", 0.0))),
            "point_count": len(points),
        },
        "latest": asdict(points[-1]) if points else None,
        "points": [asdict(p) for p in points],
    }

    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
        LOG.info("written %s", args.output)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
