# Energy-Measurement Simulation

End-to-end validation pipeline for the **time-range-archive** concept
(`octo-construction-kit-engine/docs/concept-time-range-archives.md`). Generates 15-minute
`EnergyMeasurement` slots for a configurable mix of consumer and producer metering points,
writes them simultaneously into the runtime model (Lesart D — latest slot per
`(MeteringPoint, ObisCode)`) and the `TimeRangeArchive`, and demonstrates the chained
rollup hierarchy quarter-hour → hour → day → calendar-month.

## What the simulation produces

| Sink | What | Why |
|---|---|---|
| Mongo (RT model) | 1 `EnergyMeasurement` per `(MeteringPoint, ObisCode)` pair, carrying the most recent slot | Lesart D — RT holds *current* values, never overwritten by older back-fill |
| CrateDB `TimeRangeArchive` | One row per 15-min slot per pair, indefinitely retained | Long-term, idempotent time series (re-deliveries upsert + flip `was_updated`) |
| CrateDB `RollupArchive` (Hourly) | `BucketSize = PT1H`, `BucketAlignment = FixedSize` | Hourly aggregates from QH |
| CrateDB `RollupArchive` (Daily) | `BucketSize = P1D`, `BucketAlignment = CalendarDay` | Daily aggregates chained from Hourly |
| CrateDB `RollupArchive` (Monthly) | `BucketAlignment = CalendarMonth` | Calendar-month aggregates chained from Daily |

## Files

```
Samples.Simulator.Energy/
├── README.md                                       (this file)
├── blueprint.yaml                                  Samples.Simulator.Energy-1.0.0
└── seed-data/
    ├── master-data/facility.yaml                   1 OperatingFacility + 5 MeteringPoints
    ├── archives/time-range-and-rollups.yaml        1 TimeRangeArchive + 3 chained RollupArchives
    └── data-flows/energy-simulation.yaml           DataFlow + Pipeline definition
```

## Bootstrap

```bash
octo-cli -c EnableCommunication
octo-cli -c EnableStreamData
octo-cli -c InstallBlueprint -b Samples.Simulator.Energy-1.0.0

# Activate the archives in source-to-rollup order — activation provisions the
# per-archive CrateDB table, and a chained rollup needs its source activated first.
octo-cli -c ActivateArchive -id ab0000000000000000000015   # TimeRangeArchive (PT15M)
octo-cli -c ActivateArchive -id ab00000000000000000001a0   # Hourly Rollup
octo-cli -c ActivateArchive -id ab00000000000000000001b0   # Daily Rollup
octo-cli -c ActivateArchive -id ab00000000000000000001c0   # Monthly Rollup (CalendarMonth)

octo-cli -c DeployDataFlow -id cc00000000000000000000e1
octo-cli -c ExecutePipeline --identifier cc00000000000000000000e2
```

## Pipeline architecture

```
                         ┌─ SimulateEnergyMeasurements@1
                         │      generates 96 × numDays × Σ(OBIS per MP) slot candidates
                         │      (deterministic BDEW H0/G0/L0 + PV-curve math)
                         ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │ _candidates (List<EntityUpdateInfo<RtEntity>>) — all Inserts     │
   │ _candidateAssocs (List<AssociationUpdateInfo>) — ParentChild     │
   └──────────────────────────────────────────────────────────────────┘
                         │
                         ▼
                  UpdateRtEntityIfNewer@1
                  ├─ dedups per RtWellKnownName = "EM-{mpRtId}-{obisCode}"
                  ├─ assigns one canonical RtId per WKN (DB-existing wins)
                  └─ emits two streams:
                       _filteredEms (1 entry per WKN, latest slot, for Mongo)
                       _allEms (every slot with the canonical RtId, for archive)
                       _filteredAssocs (only the parent-assocs for genuine Inserts)
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
        ApplyChanges@2     SaveTimeRangeStreamDataInArchive@1
        → Mongo            → CrateDB TimeRangeArchive
        (11 entities)      (3168+ rows for 3-day default)
                                    │
                                    ▼
                  RollupOrchestratorHostedService (background, default 30s tick)
                  ├─ Hourly  ← QH         (FixedSize PT1H)
                  ├─ Daily   ← Hourly     (CalendarDay)
                  └─ Monthly ← Daily      (CalendarMonth)
```

The two new MeshAdapter nodes (`SimulateEnergyMeasurements@1`, `UpdateRtEntityIfNewer@1`)
live in `octo-mesh-adapter`. The PV / BDEW load-profile math lives in
`octo-sdk/.../Sdk.SimulationNodes/Generators/EnergyProfiles.cs`.

## Prerequisites

- OctoMesh running locally (or a remote tenant you can target):
  - **Asset Repository Services** (`https://localhost:5001`) — owns the archive + rollup orchestrator
  - **Communication Controller** (`https://localhost:5015`) — owns the pipeline trigger
  - **MeshAdapter** (`https://localhost:5020`) — runs the pipeline nodes
  - **MongoDB** + **CrateDB** containers
- `octo-cli` configured with the target tenant context (`octo-cli -c UseContext -n local_meshtest` or similar).
- The blueprint reachable from a catalog the tenant can read. For local testing publish it
  first: `octo-bpm -c publish -p src/Blueprints/Samples.Simulator.Energy --catalog LocalFileSystemBlueprintCatalog -f`
  (`octo-bpm -c config -lcp <workspace>/.octo/local-blueprint-catalog` beforehand — `publish`
  otherwise writes to its own global path and the asset service never sees it).

The Construction Kits come from the catalog via `ckModelDependencies`; no local CK build is
needed and there is no `ImportCk` step any more.

See the **Bootstrap** section above for the command sequence. Default sim window: `2026-01-01 00:00:00Z` + 3 days = **3168 QH slots** (5 metering points
× 96 slots/day × 3 days × mixed OBIS codes).

## Configuration knobs

Edit `seed-data/data-flows/energy-simulation.yaml` directly, inside the
`SaveTimeRangeStreamDataInArchive@1` block's `pipelineDefinition` attribute:

| Key | Default | What it controls |
|---|---|---|
| `startDate` | `2026-01-01T00:00:00Z` | UTC start of the simulation window |
| `numDays` | `3` | Number of full UTC days to simulate (= 96 × N slots per MP+OBIS) |
| `meteringPoints[].profileKind` | `Load:H0` / `Load:G0` / `PV` | BDEW H0 (household), G0 (commercial), L0 (agriculture) or PV (clipped-sine PV curve) |
| `meteringPoints[].profileParameter` | 15.0 / 5.0 / 10.0 | Daily energy in kWh (Load), or peak kWp (PV) |
| `meteringPoints[].obisCodes[]` | `["1-1:1.8.0", "1-1:1.8.1", "1-1:1.8.2"]` (consumer), `["1-1:2.8.0"]` (producer) | OBIS codes simulated per MP |
| `dataQuality` | `1` (= L1 / Measured) | `BasicEnergy/DataQuality` enum value |

To add metering points, append entries to both `meteringPoints[]` in the pipeline YAML
**and** to `seed-data/master-data/facility.yaml` (with a fresh `rtId` per MP).

## Verifying the result

### CrateDB direct (fastest)

```bash
# Substitute the port your CrateDB exposes (4301 on local docker-compose)
CRATE=http://localhost:4301/_sql

# Total counts
for tbl in archive_ab0000000000000000000015 archive_ab00000000000000000001a0 \
           archive_ab00000000000000000001b0 archive_ab00000000000000000001c0; do
  curl -s $CRATE -X POST -H "Content-Type: application/json" \
    -d "{\"stmt\":\"REFRESH TABLE meshtest.\\\"$tbl\\\"\"}" > /dev/null
  curl -s $CRATE -X POST -H "Content-Type: application/json" \
    -d "{\"stmt\":\"SELECT count(*) FROM meshtest.\\\"$tbl\\\"\"}"
done
```

Expected for the 3-day default (5 MPs × mixed OBIS = 11 pairs):

| Archive | Rows | Note |
|---|---|---|
| QH (`…0015`) | 3168 | 11 pairs × 96 × 3 d |
| Hourly (`…01a0`) | 792 | 11 × 24 × 3 d |
| Daily (`…01b0`) | 33 | 11 × 3 d |
| Monthly (`…01c0`) | 11 | 11 × 1 cal-month (data spans Jan only) |

### GraphQL — Lesart D snapshot

```graphql
{
  runtime {
    basicEnergyEnergyMeasurements {
      rows {
        edges {
          node {
            rtId
            rtWellKnownName
            timeRange { from to }
            amount { value unit }
            obisCode
          }
        }
      }
    }
  }
}
```

Expected: exactly 11 entities, each `timeRange.from` is the **latest** slot of the
simulation window (= `startDate + numDays - 15 min`).

## Idempotency & re-delivery (concept §5)

Re-trigger the same pipeline:

```bash
octo-cli -c ExecutePipeline --identifier cc00000000000000000000e2
```

Expected:

- QH archive: row count unchanged, all 3168 rows now `was_updated = TRUE` (ON CONFLICT upsert).
- Mongo `EnergyMeasurement`s: unchanged — the filterer's per-WKN dedup short-circuits older
  slots; the `_filteredEms` output stays empty.

## Watermark mechanics (read this before being surprised)

The `RollupOrchestrator` advances its watermark **monotonically forward**, even across
empty buckets. Two consequences:

1. **Late-arriving QH corrections do not propagate** automatically into already-aggregated
   hourly/daily/monthly buckets. The `MAX(was_updated)` rule in concept §5/§7 only fires
   when a bucket is (re-)aggregated.
2. **Back-filled historical QH data** likewise does not appear in the rollups: the
   watermark has already moved past the data range.

Recovery for both: **bottom-up rewind**, level by level:

```bash
# 1) Hourly first (re-aggregates from QH). Wait until count stabilises.
octo-cli -c RewindRollupWatermark -id ab00000000000000000001a0 -t 2026-01-01T00:00:00Z

# 2) Daily (re-aggregates from the now-up-to-date Hourly).
octo-cli -c RewindRollupWatermark -id ab00000000000000000001b0 -t 2026-01-01T00:00:00Z

# 3) Monthly (re-aggregates from the now-up-to-date Daily).
octo-cli -c RewindRollupWatermark -id ab00000000000000000001c0 -t 2026-01-01T00:00:00Z
```

A top-down rewind would re-read stale source rows and corrupt the rollup. Each tick
processes at most 60 buckets (= 30 min wall-clock for 30 d of daily buckets at the
default 30 s interval).

The full constraint is documented in
`octo-construction-kit-engine/docs/concept-time-range-archives.md` §5 (back-fill section).

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `Unknown discriminator 'SimulateEnergyMeasurements@1'` | MeshAdapter not rebuilt / restarted with the latest `MeshNodes.Sdk` + `MeshAdapter.Sdk`. The three sim nodes are registered in `MeshNodes.Sdk/Configuration/DataPipelineBuilderExtensions.cs` and `MeshAdapter.Sdk/Configuration/DependencyInjection/ServiceCollectionExtensions.cs`. |
| `Cannot activate rollup … source archive … in status Created` | Activate in source-to-rollup order: TimeRangeArchive → Hourly → Daily → Monthly. The bootstrap script does this. |
| `Mandatory attribute 'Archive.Columns' missing` (rollup activate) | Rollup `Columns` must be pre-populated because the seed import bypasses `RollupArchiveLifecycleService.CreateAsync`. The columns in `seed-data/archives/time-range-and-rollups.yaml` mirror what `RollupColumnGenerator` would produce. |
| `archive empty` after pipeline run, but log says 3168 inserted | CrateDB count materialises after `REFRESH TABLE` — `select count(*)` in the Crate REST UI is eventually consistent. |
| Rollup counts stuck at non-Σ values | Watermark hasn't moved past your data range yet → see "Watermark mechanics" above. |
| `Mandatory attribute 'Basic.Energy-…/EnergyMeasurement' missing` | The Basic.Energy CK isn't imported into the tenant. Re-run the bootstrap script — it does `ImportCk` for `Basic` and `Basic.Energy` first. |

## Engine fixes that landed alongside this simulation

The end-to-end validation surfaced and fixed five engine bugs that affected any user of
TimeRangeArchives, RollupArchives, or record-typed user columns:

1. `RtTypeWithAttributes.GetAttributeValue[OrDefault]` now coerces `TimeSpan` from
   String / Int64 / Int32 (octo-construction-kit-engine `Runtime.Contracts`).
2. Custom BSON `TimeSpanSerializer` (octo-construction-kit-engine-mongodb).
3. `MongoRollupArchiveRuntimeStore.GetAsync` and `MongoTimeRangeArchiveRuntimeStore.GetAsync`
   load via the abstract `RtArchive` base and type-check, so a speculative get against a
   sibling subtype no longer throws a Bson down-cast exception.
4. `MongoArchiveRuntimeStore.SetStatusAsync` + `ArchiveEntityAsync` use the entity's
   concrete `CkTypeId` instead of the abstract base — required since the
   `Archive → RawArchive` split where `Archive` became `isAbstract: true`.
5. `CrateDbStreamDataRepository.MapToTimeRangeDataPointDto` (and the equivalent for raw
   archives) flattens nested `RtRecord` attribute values into dotted-path keys so a
   `Basic/Amount` record with `Value: Double + Unit: Enum` ends up as `amount.value` /
   `amount.unit` columns rather than a single NULL `amount` column.

See the individual repo PRs for details.
