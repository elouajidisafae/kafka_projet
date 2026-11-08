# Pinned stack comparison sources

Pins: kafka_exporter v1.9.0, Prometheus v3.5.0, Grafana 12.1.0,
Kafka 7.6.0, KHM local image tagged with the full source commit; Python base
3.11.13-slim-bookworm. Runtime image IDs/digests must be recorded by prepare.

- Exporter members are group-wide: [descriptor](https://github.com/danielqsj/kafka_exporter/blob/v1.9.0/kafka_exporter.go#L881-L885) and [len(group.Members) emission](https://github.com/danielqsj/kafka_exporter/blob/v1.9.0/kafka_exporter.go#L586-L588).
- No group-state descriptor is present in the inspected [complete metric declarations](https://github.com/danielqsj/kafka_exporter/blob/v1.9.0/kafka_exporter.go#L801-L885). This limits this stock input set, not PromQL's ability to query a state metric from another exporter.
- Topic partitions and per-topic lag/offset descriptors: [exporter declarations](https://github.com/danielqsj/kafka_exporter/blob/v1.9.0/kafka_exporter.go#L811-L879). Invalid commits are filtered in our recording rule. Offset visibility flag: [source](https://github.com/danielqsj/kafka_exporter/blob/v1.9.0/kafka_exporter.go#L755).
- Prometheus defaults: [scrape and evaluation 1 minute](https://github.com/prometheus/prometheus/blob/v3.5.0/config/config.go#L153-L157); [retention 15 days](https://github.com/prometheus/prometheus/blob/v3.5.0/cmd/prometheus/main.go#L127-L129). These are defaults in code, not example-config overrides.
- predict_linear and deriv use linear regression: [pinned function docs](https://github.com/prometheus/prometheus/blob/v3.5.0/docs/querying/functions.md#L606-L618), [deriv](https://github.com/prometheus/prometheus/blob/v3.5.0/docs/querying/functions.md#L145-L157). Our ETA uses the fitted value at t=0 and slope, avoiding an arbitrary finite look-ahead absent from KHM's scale rule.
- Alert activation/for timing: [pinned rule docs](https://github.com/prometheus/prometheus/blob/v3.5.0/docs/configuration/alerting_rules.md#L32-L43).
- Threshold audit limit: [last-successful-reload timestamp metric](https://github.com/prometheus/prometheus/blob/v3.5.0/cmd/prometheus/main.go#L118-L125), [reload handler ignores request identity and forwards a channel](https://github.com/prometheus/prometheus/blob/v3.5.0/web/web.go#L823-L829). This checked path is not a per-threshold editor/change history. Broader claims about all possible logging, GitOps, reverse proxies or Grafana audit editions are UNVERIFIED and not asserted.
- Grafana provisioning supports [data-source YAML](https://github.com/grafana/grafana/blob/v12.1.0/docs/sources/administration/provisioning/index.md#L63-L77) and [dashboard files](https://github.com/grafana/grafana/blob/v12.1.0/docs/sources/administration/provisioning/index.md#L292-L324).
- Docker stats memory differs from process RSS, including Linux cache subtraction: [CLI docs](https://docs.docker.com/reference/cli/docker/container/stats/). Docker image inspect logical sizes are not exclusive physical disk allocation; raw daemon disk-usage data is retained separately.

## Rule equivalence (inference from cited inputs and our expressions)

ProjectedCritical: approximate. Same 1h linear-fit idea, >=5 points, 0.05 deadband and R2>=0.5 gate; asynchronous scraping/recording and subquery time variance differ from KHM observation timestamps and confidence rounding.
Scaling: approximate. Group-wide members stand in for KHM per-topic assigned-member count; no state exclusion can be copied exactly.
Stranded: approximate. Zero group members is observable, but EMPTY/DEAD classification is unavailable.
Stalled: approximate. Positive members replaces the missing STABLE state; slope is scrape-based.
Stuck rebalance: not implementable from these stock metrics; no fake state alert is loaded.
These limits do not apply to every possible Prometheus deployment with custom collectors.
