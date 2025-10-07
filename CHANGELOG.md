# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Use Lucene `pack` method for `half_float` and `usigned_long` when using `ApproximatePointRangeQuery`.
- Expand fetch phase profiling to support inner hits and top hits aggregation phases  ([##18936](https://github.com/opensearch-project/OpenSearch/pull/18936))
- [Rule-based Auto-tagging] add the schema for security attributes ([##19345](https://github.com/opensearch-project/OpenSearch/pull/19345))
- Add temporal routing processors for time-based document routing ([#18920](https://github.com/opensearch-project/OpenSearch/issues/18920))
- Implement Query Rewriting Infrastructure ([#19060](https://github.com/opensearch-project/OpenSearch/pull/19060))
- The dynamic mapping parameter supports false_allow_templates ([#19065](https://github.com/opensearch-project/OpenSearch/pull/19065) ([#19097](https://github.com/opensearch-project/OpenSearch/pull/19097)))
- [Rule-based Auto-tagging] restructure the in-memory trie to store values as a set ([#19344](https://github.com/opensearch-project/OpenSearch/pull/19344))
- Add a toBuilder method in EngineConfig to support easy modification of configs([#19054](https://github.com/opensearch-project/OpenSearch/pull/19054))
- Add StoreFactory plugin interface for custom Store implementations([#19091](https://github.com/opensearch-project/OpenSearch/pull/19091))
- Use S3CrtClient for higher throughput while uploading files to S3 ([#18800](https://github.com/opensearch-project/OpenSearch/pull/18800))
- [Rule-based Auto-tagging] bug fix on Update Rule API with multiple attributes ([#19497](https://github.com/opensearch-project/OpenSearch/pull/19497))
- Add a dynamic setting to change skip_cache_factor and min_frequency for querycache ([#18351](https://github.com/opensearch-project/OpenSearch/issues/18351))
- Add overload constructor for Translog to accept Channel Factory as a parameter ([#18918](https://github.com/opensearch-project/OpenSearch/pull/18918))
- Addition of fileCache activeUsage guard rails to DiskThresholdMonitor ([#19071](https://github.com/opensearch-project/OpenSearch/pull/19071))
- Add subdirectory-aware store module with recovery support ([#19132](https://github.com/opensearch-project/OpenSearch/pull/19132))
- [Rule-based Auto-tagging] Modify get rule api to suit nested attributes ([#19429](https://github.com/opensearch-project/OpenSearch/pull/19429))
- [Rule-based Auto-tagging] Add autotagging label resolving logic for multiple attributes ([#19486](https://github.com/opensearch-project/OpenSearch/pull/19486))
- Field collapsing supports search_after ([#19261](https://github.com/opensearch-project/OpenSearch/pull/19261))
- Add a dynamic cluster setting to control the enablement of the merged segment warmer ([#18929](https://github.com/opensearch-project/OpenSearch/pull/18929))
- Publish transport-grpc-spi exposing QueryBuilderProtoConverter and QueryBuilderProtoConverterRegistry ([#18949](https://github.com/opensearch-project/OpenSearch/pull/18949))
- Add BindableServices extension point to transport-grpc-spi ([#19304](https://github.com/opensearch-project/OpenSearch/pull/19304))

### Changed
- Refactor to move prepareIndex and prepareDelete methods to Engine class ([#19551](https://github.com/opensearch-project/OpenSearch/pull/19551))

### Fixed

### Dependencies
- Bump `org.apache.zookeeper:zookeeper` from 3.9.3 to 3.9.4 ([#19535](https://github.com/opensearch-project/OpenSearch/pull/19535))

### Deprecated

### Removed

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.3...main
