import asyncio
from dataclasses import asdict
from typing import Any, Dict

from apify import Actor

from adsb_trace_fetch import (
    build_trace_url,
    create_session,
    fetch_json_with_retry,
    parse_trace,
    to_iso,
    warmup_page,
)


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

        latest = asdict(points[-1]) if points else None
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
            },
            "latest": latest,
        }
        if include_points:
            result["points"] = [asdict(p) for p in points]

        await Actor.push_data(result)
        await Actor.set_value("OUTPUT", result)
        Actor.log.info(f"done icao={payload.get('icao')} points={len(points)}")


if __name__ == "__main__":
    asyncio.run(main())
