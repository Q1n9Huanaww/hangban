# ADS-B Flight Intelligence API

## What does this Actor do?

ADS-B Flight Intelligence API fetches aircraft trace data by ICAO24 and returns structured flight intelligence, not just raw points.

It provides:

- Single or batch ICAO queries
- Latest flight state (speed, altitude, vertical rate, track)
- Flight phase classification (`ground`, `climb`, `cruise`, `descent`)
- Anomaly detection (rapid climb/descent, abrupt heading change, etc.)
- Status change events over time
- Window trends (`15m`, `1h`, `6h`)
- Regional aggregate metrics (active flights, avg altitude/speed, congestion index, corridors)

## What data can you get?

For each aircraft:

- ICAO, registration, type, description
- Latest state:
  - `lat`, `lon`
  - `altitude_ft`
  - `gs_kt`
  - `track`
  - `vertical_rate_fpm`
  - `flight_phase`
  - `is_anomaly`, `anomaly_type`
  - `freshness_sec`
  - `confidence`, `confidence_level`
- Summary:
  - duration, distance
  - min/max altitude
  - avg speed
  - total climb/descent
- `status_change_events`
- `window_trends`

For batch runs:

- `flights[]` for each ICAO
- `region_aggregate` across all matched flights

## How to use

1. Open the Actor input.
2. Provide `icao` (single) or `icaos` (batch).
3. Select mode (`full` / `recent`).
4. Click `Start`.
5. Read results in Dataset / API.

## Input

### Single-aircraft input

```json
{
  "icao": "3c4598",
  "mode": "full",
  "includePoints": false,
  "warmup": true
}
```

### Batch input

```json
{
  "icaos": ["3c4598", "a1d8bf", "4ca4f2"],
  "mode": "full",
  "includePoints": false,
  "maxPoints": 1200,
  "windows": ["15m", "1h", "6h"],
  "concurrency": 6,
  "region": {
    "minLat": 20,
    "maxLat": 50,
    "minLon": 100,
    "maxLon": 145
  },
  "warmup": true
}
```

## Output example (shortened)

```json
{
  "meta": {
    "batch_size": 2,
    "success_count": 2,
    "failed_count": 0,
    "mode": "full"
  },
  "flights": [
    {
      "meta": { "icao": "3c4598", "point_count": 1800 },
      "latest": {
        "lat": 34.88,
        "lon": 140.59,
        "altitude_ft": 31000,
        "gs_kt": 581.4,
        "vertical_rate_fpm": 0,
        "flight_phase": "cruise",
        "is_anomaly": false,
        "anomaly_type": [],
        "confidence": 0.9,
        "confidence_level": "high",
        "freshness_sec": 120.4
      },
      "summary": {},
      "status_change_events": [],
      "window_trends": {}
    }
  ],
  "region_aggregate": {
    "active_flights": 2,
    "avg_altitude_ft": 30500.0,
    "avg_groundspeed_kt": 560.2,
    "congestion_index": 0.12,
    "dominant_corridors": { "NE": 1, "E": 1 }
  }
}
```

## Access and integration

- Export via Dataset API
- Use with Python / Node.js SDK
- Trigger via webhooks
- Schedule via Tasks for periodic monitoring

## Notes

- Data freshness depends on upstream availability.
- This Actor is designed for operational analytics and monitoring workflows.
