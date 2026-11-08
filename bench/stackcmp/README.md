# Pinned equivalent-stack comparison

Read [SOURCES.md](SOURCES.md) for version-specific citations and rule limits.
No existing deployment/benchmark file is edited. All runs require committed source.
Each phase exclusively creates a folder under `bench/results/stack-comparison/`;
existing phase folders are never overwritten. Preserve failures and report them
instead of deleting evidence or repeatedly rerunning for favourable results.

Run from the repository root in PowerShell, on a quiet host:

```powershell
.\.venv-batch2\Scripts\python.exe bench/stackcmp_prepare.py
```

Prepare pulls exact versions and builds `khm-stackcmp:<full commit>` using the
existing distributed Dockerfile recipe with only an exact Python base pin.
Allow 5-20 minutes. It records image IDs, repository digests, logical image sizes
and Docker disk accounting in `prepare/`. Do not change source afterwards.

Functional check (about 7-12 minutes; no browser needed):

```powershell
.\.venv-batch2\Scripts\python.exe bench/stackcmp_functional.py
```

KHM and the exporter/Prometheus stack observe the same two three-partition topics.
Bottleneck uses one 10-msg/s consumer against 60-msg/s production, observed for
180 s. Stranded creates 12,000 messages, stops the consumer and observes 120 s.
Raw snapshots, first-observed firing times, errors and source/image provenance
are retained. Not firing within the fixed window is a result, not a reason to retry.

Before resources: close browser processes (including background Edge/Chrome),
and stop unrelated Docker workloads. The command refuses to run otherwise.
Inspect what is running before deciding what to stop:

```powershell
docker ps --format '{{.Names}}'
```

Resource comparison (minimum 135 timed minutes; allow about 2.5-3.5 hours overall):

```powershell
.\.venv-batch2\Scripts\python.exe bench/stackcmp_resources.py
```

This repeats the paper's 10/100/500 inactive committed-pair workload five times,
rotates size and setup order, warms each fresh setup for 60 s, and samples a fixed
120 s window. Every setup uses docker stats, summing its measured containers;
shared Kafka is excluded. No browser or Grafana panel query is opened during
sampling. Per-repetition time means are summarised as median [Q1,Q3] across five
repetitions. Raw per-container samples and startup manifests remain available.

Image totals are sums of inspect Size for distinct images (logical uncompressed
bytes), not unique physical host disk allocation. Raw `docker system df -v` JSON
is retained. Physical layer-deduplicated setup disk footprints remain unverified.
Configuration-effort counts explicitly include the whole shared compose source
for every setup plus only the setup's necessary configuration files. Generated
substitutions and runner/build code are excluded; both raw and noncomment line
counts are reported. This is a formatting-dependent count, not authoring time.

KHM is measured anew in the same run. Archived Table 6 PID-1 RSS is retained only
as a labelled historical reference: Docker stats is a different memory definition
and the source/build versions also differ. New numeric differences are written
without treating them as pure memory regressions.

All output folders have manifests; only a completed manifest and full schedule
qualify as measurements. No benchmark results have been collected merely by
adding these scripts/configuration files.
