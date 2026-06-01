# Design: Streaming Schema Design & Evolution

**Difficulty:** Easy–Medium
**Time:** ~30 minutes
**Focus areas:** Schema design, Kafka, backward/forward compatibility, schema evolution, Avro/Protobuf

---

## Problem Statement

A network of weather stations publishes sensor readings to a Kafka topic every 30 seconds. Each event contains:

- temperature
- barometric pressure
- humidity
- wind speed
- wind direction
- sun exposure

**Design a schema for these messages.**

---

## Part 1: Initial Schema Design

### Design Constraints to Consider
- How does the consumer know which schema version a message uses?
- How do you represent missing / invalid sensor values (nullable fields)?
- What format: JSON, Avro, Protobuf, Thrift?

### Option A: JSON (simple, schema-less)
```json
{
  "station_id": "NY-CATSKILLS-001",
  "event_ts": 1714500000,
  "temperature_c": 22.3,
  "barometric_pressure_hpa": 1013.25,
  "humidity_pct": 68.1,
  "wind_speed_kmh": 15.4,
  "wind_direction_deg": 270,
  "sun_exposure_wm2": 450.0
}
```

Pros: Human-readable, flexible.
Cons: No schema enforcement; consumers must handle missing fields ad-hoc; inefficient for high-throughput.

### Option B: Avro (recommended for Kafka at scale)
```json
{
  "type": "record",
  "name": "WeatherReading",
  "namespace": "com.weathernet.events",
  "fields": [
    {"name": "station_id",           "type": "string"},
    {"name": "event_ts",             "type": "long"},
    {"name": "temperature_c",        "type": ["null", "float"], "default": null},
    {"name": "barometric_pressure_hpa", "type": ["null", "float"], "default": null},
    {"name": "humidity_pct",         "type": ["null", "float"], "default": null},
    {"name": "wind_speed_kmh",       "type": ["null", "float"], "default": null},
    {"name": "wind_direction_deg",   "type": ["null", "int"],   "default": null},
    {"name": "sun_exposure_wm2",     "type": ["null", "float"], "default": null}
  ]
}
```

**Key decisions:**
- All sensor fields are **nullable** (null union) — hardware failures can produce null readings.
- `null` as default means old consumers can still read new messages that add fields.
- Schema is registered with Confluent Schema Registry; each message is prefixed with a 4-byte schema ID.

### Option C: Protobuf
```protobuf
syntax = "proto3";
message WeatherReading {
  string station_id = 1;
  int64  event_ts   = 2;
  float  temperature_c         = 3;  // 0 = missing by convention (or use oneof/wrapper)
  float  barometric_pressure_hpa = 4;
  float  humidity_pct          = 5;
  float  wind_speed_kmh        = 6;
  int32  wind_direction_deg    = 7;
  float  sun_exposure_wm2      = 8;
}
```

Pros: Binary, very efficient; backward compatible by default (unknown fields ignored).
Cons: Default values for missing data are ambiguous (0 vs. "missing").

---

## Part 2: Schema Evolution — Adding a New Field

**New requirement:** After 1 year, add `precipitation_rate_mmhr` to the schema. Rollout takes 3–6 months (not all stations support it immediately).

### Problem: Producer/Consumer Version Mismatch

During rollout:
- Some stations (new firmware) will include `precipitation_rate_mmhr`
- Some stations (old firmware) will NOT include it
- Consumers must handle both

### Solution: Backward-Compatible Schema Update

**In Avro:** Add the new field with a `null` default:
```json
{
  "name": "precipitation_rate_mmhr",
  "type": ["null", "float"],
  "default": null
}
```

**Compatibility rules:**
- **Backward compatible:** New schema can read old messages (field is missing → use default `null`).
- **Forward compatible:** Old schema can read new messages (unknown fields are ignored).
- **Full compatible:** Both — what you should aim for in a rolling deployment.

**Schema Registry enforcement:** Configure the subject with `FULL` compatibility mode. Any new schema version that breaks compatibility is rejected at registration time.

**Consumer handling:**
```python
# Consumers should always check for None before using the field
precipitation = event.get("precipitation_rate_mmhr")  # may be None for old stations
if precipitation is not None:
    process_precipitation(precipitation)
```

---

## Part 3: Outlier Annotations — New Event Type in the Same Stream

**New requirement:** Meteorologists can mark sensor readings as outliers (bad data) due to hardware issues (mountain lions eating wind sensors, etc.). An annotation event must reference one or more sensor readings in the same stream.

### Design Approach: Union Event Types in the Same Topic

Option 1: **Separate Avro schemas per event type** registered under the same subject with Schema Registry.

Option 2: **Wrapper schema** with a discriminated union field:

```json
{
  "type": "record",
  "name": "WeatherEvent",
  "fields": [
    {
      "name": "event_type",
      "type": {"type": "enum", "name": "EventType", "symbols": ["READING", "OUTLIER_ANNOTATION"]}
    },
    {
      "name": "reading",
      "type": ["null", "WeatherReading"],
      "default": null
    },
    {
      "name": "annotation",
      "type": ["null", "OutlierAnnotation"],
      "default": null
    }
  ]
}
```

```json
// OutlierAnnotation schema
{
  "type": "record",
  "name": "OutlierAnnotation",
  "fields": [
    {"name": "annotator_id",   "type": "string"},
    {"name": "annotation_ts",  "type": "long"},
    {"name": "station_id",     "type": "string"},
    {"name": "reading_ts",     "type": "long"},         // references the original reading
    {"name": "sensors_flagged", "type": {"type": "array", "items": "string"}},  // e.g. ["wind_speed_kmh"]
    {"name": "reason",         "type": "string"}
  ]
}
```

### Alternative: Topic-Per-Event-Type
Separate Kafka topics: `weather.readings` and `weather.annotations`. Simpler schemas per topic, but consumers that need both must read two topics and correlate by `(station_id, reading_ts)`.

---

## What to Cover in Your Answer

- [ ] Schema format choice (Avro/Protobuf/JSON) with trade-off reasoning
- [ ] Nullable fields for hardware failures (null union)
- [ ] Schema Registry: how schema ID is embedded in messages
- [ ] Backward / forward / full compatibility — definitions and which you're using
- [ ] How to add `precipitation_rate_mmhr` without breaking consumers
- [ ] How consumers handle the optional new field during rollout
- [ ] Outlier annotation: event type discrimination (wrapper schema or separate topic)

---

## Key Questions to Expect

1. "How does a consumer know which version of the schema was used to serialize a message?"
2. "If you add a non-nullable field without a default, what happens to consumers reading old messages?"
3. "What's the difference between backward and forward compatibility?"
4. "Why are nullable fields with `null` defaults a best practice in Avro event schemas?"
5. "If you have two event types in the same Kafka topic, how does the consumer distinguish them efficiently?"
