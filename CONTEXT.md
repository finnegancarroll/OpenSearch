# SearchStatsContributor — Extension Point for Plugin Search Stats

## Summary

This PR adds a server-side extension point (`SearchStatsContributor`) that allows plugins executing searches outside the standard Lucene path to contribute their query metrics into the existing `_nodes/stats` search stats output. The analytics engine plugin uses this to increment `query_total` for every analytics query, so existing monitoring tooling picks up these counts without modification.

## Motivation

Analytics engine queries bypass the standard `SearchService` → `SearchOperationListener` → `ShardSearchStats` path entirely. They go through `DefaultPlanExecutor` → `QueryScheduler` → fragment dispatch. Without this extension point, analytics engine queries are invisible to the standard `query_total` / `query_time_in_millis` counters in `_nodes/stats`.

On analytics-engine-enabled clusters, there are NO normal Lucene searches — all queries route through the analytics engine. Existing operational tooling reads `query_total` from `_nodes/stats` to monitor cluster health. This extension point ensures those tools work without modification.

## Architecture

```
Plugin (AnalyticsPlugin)
  implements SearchStatsContributor
  └── contributeSearchStats() → SearchStats(queryCount=N)

Node.java (startup)
  └── discovers SearchStatsContributor plugins
  └── passes list to IndicesService.setSearchStatsContributors()

IndicesService.stats() (on each _nodes/stats request)
  └── case Search:
      └── commonStats.search.add(oldShardsStats)     // existing
      └── for each contributor:
          └── commonStats.search.add(contributed)     // NEW — merges plugin stats
```

## Files Changed

### Server (core OpenSearch)

| File | Change |
|------|--------|
| `server/.../plugins/SearchStatsContributor.java` | New `@ExperimentalApi` interface |
| `server/.../indices/IndicesService.java` | Field + setter for contributors, merge in `stats()` |
| `server/.../node/Node.java` | Discovers contributors, passes to IndicesService |

### Analytics Engine Plugin

| File | Change |
|------|--------|
| `sandbox/.../analytics/AnalyticsPlugin.java` | Implements `SearchStatsContributor`, `AtomicLong` counter |
| `sandbox/.../analytics/exec/DefaultPlanExecutor.java` | Calls `incrementQueryCount()` on each query |

### Integration Test

| File | Purpose |
|------|---------|
| `sandbox/qa/.../SearchStatsContributorIT.java` | Verifies `query_total` increments after PPL queries |

## How to Extend

### Adding more fields (query_time, query_failed, etc.)

The `SearchStats.Stats.Builder` supports all standard fields. To add latency tracking:

```java
@Override
public SearchStats contributeSearchStats() {
    return new SearchStats(
        new SearchStats.Stats.Builder()
            .queryCount(queryCount.get())
            .queryTimeInMillis(totalQueryTimeMs.get())
            .queryFailedCount(failedCount.get())
            .build(),
        currentInFlight.get(),  // openContexts
        null                    // groupStats
    );
}
```

### Available SearchStats.Stats.Builder fields

- `queryCount(long)` → `query_total`
- `queryTimeInMillis(long)` → `query_time_in_millis`
- `queryCurrent(long)` → `query_current`
- `queryFailedCount(long)` → `query_failed_total`
- `fetchCount(long)` → `fetch_total` (could map to coordinator-reduce)
- `fetchTimeInMillis(long)` → `fetch_time_in_millis`
- `concurrentQueryCount(long)` → `concurrent_query_total`
- `concurrentQueryTimeInMillis(long)` → `concurrent_query_time_in_millis`

### Multiple contributors

The server iterates all `SearchStatsContributor` plugins and merges each one additively. If multiple plugins contribute, their stats sum together. Currently only the analytics-engine plugin implements this.

## Testing

### Run the integration test

```bash
./gradlew :sandbox:qa:analytics-engine-rest:integTest -Dsandbox.enabled=true \
  --tests "*SearchStatsContributorIT*" -x javadoc
```

### Manual verification

```bash
# Start cluster (see TESTING.md)
./start-node.sh

# Create index and run queries
curl -X PUT localhost:9200/test -H 'Content-Type: application/json' -d '{
  "settings": {"index.pluggable.dataformat.enabled": true, "index.pluggable.dataformat": "composite"},
  "mappings": {"properties": {"x": {"type": "integer"}}}
}'
curl -X POST localhost:9200/test/_bulk -H 'Content-Type: application/x-ndjson' -d '
{"index":{}}
{"x":1}
'
curl -X POST localhost:9200/test/_flush?force
curl -X POST localhost:9200/_analytics/ppl -H 'Content-Type: application/json' -d '{"query": "source = test"}'

# Check stats
curl -s localhost:9200/_nodes/stats/indices/search | python3 -m json.tool | grep query_total
```

## Known Limitations

- **Counter is cumulative, not resettable.** The `AtomicLong` grows monotonically. A node restart resets it. This matches the behavior of standard `ShardSearchStats` counters.
- **No per-index breakdown.** The contributed stats merge into the node-level total only. Per-index search stats (`_stats/search` on an index) won't include analytics engine queries. This is acceptable because analytics engine queries are always node-coordinated, not shard-local.
- **Intermittent IT flakiness.** The integration test occasionally fails due to a pre-existing gRPC streaming issue in the test cluster (`Failed to start streaming fragment`). This is unrelated to the stats contribution logic — retry resolves it.

## Future Work

- Add `queryTimeInMillis` and `queryFailedCount` once the detailed stats service (separate PR) is merged
- Consider adding `queryCurrent` (in-flight gauge) for backpressure monitoring
- Evaluate whether `fetchCount`/`fetchTimeInMillis` should map to coordinator-reduce phase
- Consider per-index breakdown if index-level stats tooling needs it
