# Cron Materialized View Refresh with Multi-Coordinator Deduplication

## Overview

This branch adds **automatic cron-based refresh scheduling** for Iceberg materialized views and a **freshness-aware deduplication mechanism** that prevents redundant refreshes across multiple Trino coordinators, Stargate instances, or external orchestrators like Airflow.

Developers define a cron schedule directly in the MV DDL:

```sql
CREATE MATERIALIZED VIEW mv_daily_sales
  GRACE PERIOD INTERVAL '1' HOUR
  REFRESH SCHEDULE '0 */6 * * *'
AS SELECT region, sum(amount) AS total
   FROM sales
   GROUP BY region;
```

The coordinator's built-in scheduler scans for MVs with a `REFRESH SCHEDULE`, evaluates the cron expression, and dispatches `REFRESH MATERIALIZED VIEW` queries automatically. When multiple coordinators share the same metastore and storage, the freshness check ensures only one actually performs the work.

---

## Problem

In production Trino deployments, multiple coordinators (or mixed orchestration products) often share the same Hive Metastore and S3/HDFS storage. Without coordination, each coordinator independently decides an MV is due for refresh and dispatches its own `REFRESH MATERIALIZED VIEW` query. This causes:

- **Wasted compute** -- N coordinators running the same expensive aggregation query simultaneously
- **Iceberg commit conflicts** -- multiple writers race to update `metadata.json`, all but one fail with a CAS error
- **Log noise** -- repeated failures from conflicting commits obscure real issues

External solutions (distributed locks, ZooKeeper, a dedicated refresh service) add operational complexity and new infrastructure to maintain.

---

## Solution: Freshness Check Deduplication

The fix is a single freshness check added to the scheduler's `shouldRefresh()` method. After the cron expression confirms an MV is due, we call `metadata.getMaterializedViewFreshness()` before dispatching:

```
Cron says "time to refresh"
        |
        v
  Is the MV already FRESH?
   /              \
  YES              NO
  |                |
  Skip             Dispatch refresh
  (log it)         (proceed normally)
```

This works because Iceberg's freshness is determined by comparing the MV's storage table snapshots against the base tables' snapshots. Any writer -- whether this coordinator, another coordinator, Stargate, or an Airflow job -- that successfully commits a refresh will update the Iceberg metadata. Subsequent freshness checks by any reader see the MV as `FRESH` and skip.

### Why this is sufficient

| Scenario | What happens |
|----------|-------------|
| Coordinator A finishes before B checks | B sees FRESH, skips |
| A and B both start simultaneously | One succeeds, one fails at Iceberg commit (CAS on metadata.json) -- already handled gracefully by Iceberg |
| A crashes mid-refresh | Nothing committed, B picks it up next cycle |
| Airflow/Stargate refreshes the MV | Any Trino coordinator sees FRESH, skips |
| Freshness check itself fails | Fail-open: proceed with refresh (safe default) |

### What this does NOT require

- Zero new SPI methods
- Zero new table properties
- Zero distributed locking infrastructure
- Zero changes to the Iceberg connector

---

## Changes Made

### `MaterializedViewRefreshScheduler.java`
**Path:** `core/trino-main/src/main/java/io/trino/execution/scheduler/`

**1. Freshness check in `shouldRefresh()`** (core change)

After the cron timing check passes, we query the metadata for the MV's current freshness state. If `FRESH`, we skip the refresh and log it. The check is wrapped in a try/catch so failures fall through to dispatching (fail-open):

```java
if (!nextFireTime.isAfter(now)) {
    // Cron says it's due -- check if MV is already fresh
    try {
        MaterializedViewFreshness freshness =
            metadata.getMaterializedViewFreshness(session, mvName, false);
        if (freshness.getFreshness() == Freshness.FRESH) {
            log.info("Materialized view %s is already fresh, skipping refresh", mvName);
            return false;
        }
    }
    catch (Exception e) {
        log.warn(e, "Failed to check freshness for %s, proceeding with refresh", mvName);
    }
    // ... dispatch refresh
}
```

The freshness check is placed *after* the cron check intentionally -- this avoids calling `getMaterializedViewFreshness()` (which hits the metastore) for MVs that aren't yet due, keeping the scan cycle lightweight.

**2. `@VisibleForTesting` on `shouldRefresh()`**

Changed from `private` to package-private to enable direct unit testing without needing to construct a full `DispatchManager`.

**3. Test-only constructor**

Added a minimal constructor that only requires `Metadata` and `MaterializedViewRefreshConfig`, used by unit tests to avoid heavy Guice-injected dependencies.

### `TestMaterializedViewRefreshDeduplication.java` (new)
**Path:** `core/trino-main/src/test/java/io/trino/execution/scheduler/`

Unit test with 6 test methods exercising the freshness deduplication logic:

| Test | What it proves |
|------|----------------|
| `testStaleViewTriggersRefresh` | STALE MV + cron due dispatches refresh |
| `testFreshViewSkipsRefresh` | FRESH MV + cron due skips refresh |
| `testMultiCoordinatorDeduplication` | Two scheduler instances share metadata; after Coord A refreshes, Coord B sees FRESH and skips |
| `testFreshnessCheckFailureProceeds` | If `getMaterializedViewFreshness` throws, refresh proceeds anyway (fail-open) |
| `testNoScheduleSkipsRefresh` | MV without `REFRESH SCHEDULE` is never considered |
| `testCronNotDueSkipsRefresh` | Cron not yet due skips without checking freshness (avoids unnecessary metastore calls) |

Uses hand-written mock metadata (Trino style -- no mocking libraries) extending `AbstractMockMetadata`.

### `TestRefreshDeduplicationDemo.java` (new)
**Path:** `plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/`

End-to-end integration demo that boots a real Trino server with Iceberg and the refresh scheduler, creates random data, sets up an MV, and watches the scheduler deduplication in action.

---

## Test Results

### Unit tests (6/6 pass)

```
Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

### Integration demo (full lifecycle proven)

The demo starts a Trino server with `materialized-view-refresh.enabled=true` and walks through the complete lifecycle:

**Phase 1 -- MV is FRESH after manual refresh, scheduler skips:**
```
Materialized view iceberg.tpch.mv_event_summary is already fresh, skipping refresh
Refresh scan complete: scanned 1, dispatched 0
```

**Phase 2 -- INSERT makes MV STALE, scheduler detects and auto-refreshes:**
```
Materialized view iceberg.tpch.mv_event_summary is due for refresh
Dispatching refresh for iceberg.tpch.mv_event_summary
Refresh scan complete: scanned 1, dispatched 1
```

**Phase 3 -- After auto-refresh, back to skipping:**
```
Materialized view iceberg.tpch.mv_event_summary is already fresh, skipping refresh
Refresh scan complete: scanned 1, dispatched 0
```

**Phase 4 -- MV data includes new rows (auto-refresh worked):**
```
event_type=click,     count=227, avg=232.23, users=208
event_type=new_event, count=100, avg=999.99, users=1     <-- new data
event_type=purchase,  count=283, avg=244.78, users=247
```

---

## Developer Usage

### Enabling the scheduler

Add to `config.properties` on each coordinator:

```properties
materialized-view-refresh.enabled=true
materialized-view-refresh.scan-interval=60s
materialized-view-refresh.coordinator-id=coordinator-1
materialized-view-refresh.max-concurrent-refreshes=4
```

### Creating MVs with a refresh schedule

```sql
-- Every 15 minutes
CREATE MATERIALIZED VIEW mv_metrics
  REFRESH SCHEDULE '*/15 * * * *'
AS SELECT date_trunc('hour', ts) AS hour, count(*) AS cnt
   FROM events GROUP BY 1;

-- Daily at 2 AM UTC
CREATE MATERIALIZED VIEW mv_daily_report
  GRACE PERIOD INTERVAL '24' HOUR
  REFRESH SCHEDULE '0 2 * * *'
AS SELECT region, sum(revenue) FROM orders GROUP BY region;

-- Update schedule with OR REPLACE
CREATE OR REPLACE MATERIALIZED VIEW mv_metrics
  REFRESH SCHEDULE '*/5 * * * *'
AS SELECT date_trunc('hour', ts) AS hour, count(*) AS cnt
   FROM events GROUP BY 1;
```

### Multi-coordinator setup

No special configuration needed. Each coordinator runs its own scheduler instance. The freshness check naturally deduplicates: whichever coordinator refreshes first writes a new Iceberg snapshot, and all others see the MV as `FRESH` on their next scan cycle.

### Monitoring

Watch for these log messages:

| Log message | Meaning |
|------------|---------|
| `is due for refresh` | Cron triggered, MV is stale, dispatching |
| `is already fresh, skipping refresh` | Another coordinator already refreshed it |
| `Failed to check freshness, proceeding with refresh` | Metastore unreachable, refreshing anyway (safe) |
| `Dispatching refresh` | Refresh query submitted |
| `Refresh dispatched successfully` | Query accepted by execution engine |

### Running the tests

```bash
# Unit tests (fast, no Docker)
./mvnw test -pl core/trino-main \
  -Dtest=TestMaterializedViewRefreshDeduplication \
  -Dair.check.skip-all=true

# Integration demo (no Docker, ~2.5 min)
./mvnw test -pl plugin/trino-iceberg \
  -Dtest=TestRefreshDeduplicationDemo \
  -Dair.check.skip-all=true -nsu
```

---

## File Summary

| File | Type | Lines changed |
|------|------|---------------|
| `core/trino-main/.../MaterializedViewRefreshScheduler.java` | Modified | +19 |
| `core/trino-main/.../TestMaterializedViewRefreshDeduplication.java` | New | +295 |
| `plugin/trino-iceberg/.../TestRefreshDeduplicationDemo.java` | New | +155 |
