#!/usr/bin/env python3
"""Sizing and cost model for the engineer-004 pipeline.

    python3 cost_model.py            # baseline, shards provisioned for a 10x spike
    python3 cost_model.py --spike 3  # shards provisioned for a smaller spike

Every input is a named constant with a source label, so a reviewer can
change one and re-derive the total. The arithmetic is deliberately plain.

Source labels follow SCORING.md:
  [Observed]    measured directly (here: from the fixture)
  [Benchmarked] published external source, named below
  [Estimated]   my own arithmetic from stated inputs
  [Assumed]     placeholder to make the plan concrete

Unit rates are us-east-1 on-demand list prices from the sources in
RATE_SOURCES. They are inputs, not findings: re-check against the AWS pricing
calculator before anyone commits budget. Anything not checked against a
published page is labelled [Assumed].
"""
from __future__ import annotations

import argparse
import math

HOURS_PER_MONTH = 730  # [Assumed] AWS billing convention
SECONDS_PER_MONTH = 30 * 86_400

# --- Workload ---------------------------------------------------------------
EVENTS_PER_DAY = 50_000_000          # [Assumed] from the brief ("~50M events/day")
MEAN_EVENT_BYTES = 229               # [Observed] mean of 25 events in the fixture
                                     #   (sha256 1aeb24b4...b24235). n=25, so treat
                                     #   as order-of-magnitude, not a precise mean.
ENVELOPE_OVERHEAD_BYTES = 380        # [Assumed] tenant_id, received_at, version +
                                     #   allowlisted headers (UA, XFF, Origin,
                                     #   Referer); Cookie is dropped at the edge
DIURNAL_PEAK_FACTOR = 3.0            # [Estimated] peak hour ~3x the daily mean.
                                     #   Not measured -- see "What breaks it".
SPIKE_MULTIPLIER = 10.0              # [Assumed] from the brief ("10x traffic spikes")
HOT_RETENTION_DAYS = 7               # [Assumed] brief implies last-7-days queries
COLD_RETENTION_DAYS = 90             # [Assumed] stated retention target

# --- Unit rates -------------------------------------------------------------
RATE_SOURCES = {
    "kinesis":  "https://aws.amazon.com/kinesis/data-streams/pricing/ [Benchmarked]",
    "dynamodb": "https://aws.amazon.com/dynamodb/pricing/on-demand/ (post Nov-2024) [Benchmarked]",
    "lambda":   "https://aws.amazon.com/lambda/pricing/ x86 [Benchmarked]",
    "firehose": "https://aws.amazon.com/firehose/pricing/ (5KB record rounding) [Benchmarked]",
    "s3":       "https://aws.amazon.com/s3/pricing/ Standard [Benchmarked]",
    "elb":      "https://aws.amazon.com/elasticloadbalancing/pricing/ [Benchmarked]",
    "apigw":    "https://aws.amazon.com/api-gateway/pricing/ [Benchmarked]",
    "assumed":  "Aurora / ElastiCache / RDS Proxy / WAF rates: not re-verified [Assumed]",
}
KINESIS_SHARD_HOUR = 0.015           # [Benchmarked] $/shard-hour
KINESIS_PUT_PER_MILLION = 0.014      # [Benchmarked] $/1M PUT payload units (25KB each)
SHARD_RECORDS_PER_SEC = 1_000        # [Benchmarked] hard per-shard write limit
SHARD_BYTES_PER_SEC = 1_000_000      # [Benchmarked] 1 MB/s per shard
SHARD_READ_BYTES_PER_SEC = 2_000_000 # [Benchmarked] 2 MB/s shared by standard consumers
STANDARD_CONSUMERS = 3               # [Estimated] Enrich, Firehose, Forwarder (W1-12)

DDB_WRITE_PER_MILLION = 0.625        # [Benchmarked] $/1M WRU, on-demand
DDB_READ_PER_MILLION = 0.125         # [Benchmarked] $/1M RRU, on-demand
# Every enriched event reads `tenant#anon -> user`. A write happens only when an
# event carries an anon->user link the graph does not already hold (identify,
# login, or any event with both ids), so writes are a small fraction. Each new
# link is stored in both directions, so an erasure can walk user -> every anon.
DDB_READS_PER_EVENT = 1              # [Estimated] one lookup per event
DDB_NEW_LINK_RATE = 0.05             # [Assumed] new links per event, unmeasured
DDB_ITEMS_PER_LINK = 2               # [Estimated] anon->user and user->anon

LAMBDA_PER_MILLION_REQ = 0.20        # [Benchmarked] $/1M invocations
LAMBDA_GB_SECOND = 0.0000166667      # [Benchmarked] $/GB-second (x86)

# Enrichment consumer: Kinesis event source mapping.
ENRICH_BATCH_MAX = 500               # [Estimated] max records per invocation
ENRICH_WINDOW_S = 1.0                # [Estimated] MaximumBatchingWindowInSeconds
ENRICH_PARALLELIZATION = 3           # [Estimated] concurrent batches per shard
                                     #   (Lambda keeps per-partition-key order)
ENRICH_OVERHEAD_S = 0.02             # [Assumed] fixed cost per invocation
ENRICH_PER_EVENT_S = 0.002           # [Assumed] Redis + DDB + batched Aurora insert
ENRICH_MEMORY_GB = 1.0               # [Assumed]

# Ingest edge: ALB -> Lambda, one invocation per beacon.
INGEST_LAMBDA_SECONDS = 0.05         # [Assumed] stamp, tokenize, PutRecord
INGEST_LAMBDA_MEMORY_GB = 0.128      # [Assumed]
LAMBDA_DEFAULT_CONCURRENCY = 1_000   # [Benchmarked] default regional quota
ALB_HOUR = 0.0225                    # [Benchmarked] $/ALB-hour
ALB_LCU_HOUR = 0.008                 # [Benchmarked] $/LCU-hour
ALB_NEW_CONN_PER_LCU = 25            # [Benchmarked] new connections/s per LCU
WAF_PER_MILLION = 0.60               # [Assumed] AWS WAF request charge
APIGW_REST_TIERS = ((333e6, 3.50), (667e6, 2.80), (19e9, 2.38))  # [Benchmarked] comparison only
APIGW_HTTP_FIRST_300M = 1.00         # [Benchmarked] comparison only
APIGW_HTTP_AFTER = 0.90              # [Benchmarked] comparison only

FIREHOSE_PER_GB = 0.029              # [Benchmarked] $/GB ingested
FIREHOSE_PARQUET_PER_GB = 0.018      # [Benchmarked] $/GB format conversion
FIREHOSE_ROUND_BYTES = 5 * 1024      # [Benchmarked] each record billed in 5KB steps
FIREHOSE_DP_PER_GB = 0.020           # [Benchmarked] dynamic partitioning, per GB processed
FIREHOSE_DP_PER_1K_OBJECTS = 0.005   # [Benchmarked] per 1,000 S3 objects delivered
FIREHOSE_JQ_HOUR = 0.07              # [Benchmarked] JQ parsing of the partition keys
FIREHOSE_BUFFER_S = 300              # [Assumed] one object per tenant per buffer flush
ACTIVE_TENANTS = 500                 # [Assumed] from the brief ("500+ customers")
S3_PER_GB_MONTH = 0.023              # [Benchmarked] $/GB-month, Standard
PARQUET_COMPRESSION = 5.0            # [Assumed] Parquet+snappy on event JSON

AURORA_WRITER_HOUR = 1.044           # [Assumed] db.r6g.2xlarge, Standard
AURORA_IO_OPTIMIZED_UPLIFT = 1.3     # [Assumed] I/O-Optimized: +30%, no I/O fees
AURORA_INSTANCES = 3                 # [Estimated] 1 writer + 2 readers
AURORA_VCPU_EACH = 8                 # [Benchmarked] r6g.2xlarge
AURORA_STORAGE_PER_GB = 0.225        # [Assumed] I/O-Optimized storage
AURORA_INDEX_OVERHEAD = 3.0          # [Assumed] rows + indexes vs raw bytes
RDS_PROXY_VCPU_HOUR = 0.015          # [Assumed] third-party sources only
REDIS_NODE_HOUR = 0.411              # [Assumed] cache.r7g.xlarge
REDIS_NODES = 3                      # [Estimated] primary + 2 replicas, multi-AZ
REDIS_NODE_GB = 26.32                # [Benchmarked] cache.r7g.xlarge memory
DEDUP_TTL_HOURS = 25                 # [Estimated] Kinesis 24h replay + 1h margin
DEDUP_BYTES_PER_KEY = 100            # [Assumed] tenant:event_id key + Redis overhead

OBSERVABILITY_MONTHLY = 1_500.0      # [Assumed] CloudWatch, traces, alerting

# Migration only (W1-12): a second Kinesis consumer replays each beacon to the
# legacy collector, so a slow legacy box backs up its own iterator, not the edge.
FORWARD_PER_EVENT_S = 0.005          # [Assumed] concurrent HTTP POSTs inside a batch
FORWARD_MEMORY_GB = 0.5              # [Assumed]

# Not modelled; the headroom has to absorb them.
NOT_MODELLED = ("S3 requests, KMS calls, Route 53, data egress, dashboard and "
                "Personalization API compute, NAT")
BUDGET_CEILING = 50_000.0            # [Assumed] from the brief


def rest_requests(n):
    cost, left = 0.0, n
    for size, rate in APIGW_REST_TIERS:
        take = min(left, size)
        cost += take / 1e6 * rate
        left -= take
    return cost


def model(spike: float):
    events_day = EVENTS_PER_DAY
    events_month = events_day * 30
    mean_rate = events_day / 86_400
    peak_rate = mean_rate * DIURNAL_PEAK_FACTOR
    spike_rate = peak_rate * spike

    gb_day = events_day * MEAN_EVENT_BYTES / 1e9
    gb_month = gb_day * 30
    record_bytes = MEAN_EVENT_BYTES + ENVELOPE_OVERHEAD_BYTES
    record_gb_month = events_month * record_bytes / 1e9

    shards_by_records = spike_rate / SHARD_RECORDS_PER_SEC
    shards_by_bytes = (spike_rate * record_bytes) / SHARD_BYTES_PER_SEC
    shards = math.ceil(max(shards_by_records, shards_by_bytes)) + 1

    lines = []
    add = lines.append

    kinesis = shards * KINESIS_SHARD_HOUR * HOURS_PER_MONTH
    kinesis += (events_month / 1e6) * KINESIS_PUT_PER_MILLION
    add(("Kinesis Data Streams", kinesis, f"{shards} shards + {events_month/1e6:,.0f}M PUT units"))

    # Edge: one Lambda invocation per beacon.
    ingest_gbs = events_month * INGEST_LAMBDA_SECONDS * INGEST_LAMBDA_MEMORY_GB
    ingest = (events_month / 1e6) * LAMBDA_PER_MILLION_REQ + ingest_gbs * LAMBDA_GB_SECOND
    add(("Lambda (ingest edge)", ingest, f"{events_month/1e6:,.0f}M invocations @ 50ms/128MB"))

    # Worst case: every beacon opens a new connection (no keep-alive). Billed at
    # the mean rate; spike hours cost more and are shown in the basis.
    alb_lcu = mean_rate / ALB_NEW_CONN_PER_LCU
    alb = (ALB_HOUR + ALB_LCU_HOUR * alb_lcu) * HOURS_PER_MONTH
    spike_lcu = spike_rate / ALB_NEW_CONN_PER_LCU
    add(("ALB", alb, f"{alb_lcu:,.0f} LCU avg; spike {spike_lcu:,.0f} LCU = "
                     f"${ALB_LCU_HOUR * spike_lcu:,.2f}/spike-hour"))

    waf = (events_month / 1e6) * WAF_PER_MILLION
    add(("WAF on ALB", waf, "per-IP rate limits + payload caps"))

    # Enrichment: with a 1s window every shard fires about once a second at mean
    # traffic, so invocation count is set by shards x seconds, not events / 500.
    enrich_inv = max(events_month / ENRICH_BATCH_MAX, shards * SECONDS_PER_MONTH / ENRICH_WINDOW_S)
    enrich_s = enrich_inv * ENRICH_OVERHEAD_S + events_month * ENRICH_PER_EVENT_S
    enrich = (enrich_inv / 1e6) * LAMBDA_PER_MILLION_REQ + enrich_s * ENRICH_MEMORY_GB * LAMBDA_GB_SECOND
    add(("Lambda (enrichment)", enrich, f"{enrich_inv/1e6:,.0f}M invocations, {enrich_s/1e6:,.1f}M s"))

    ddb_w = events_month * DDB_NEW_LINK_RATE * DDB_ITEMS_PER_LINK
    ddb_r = events_month * DDB_READS_PER_EVENT
    ddb = (ddb_w / 1e6) * DDB_WRITE_PER_MILLION + (ddb_r / 1e6) * DDB_READ_PER_MILLION
    add(("DynamoDB (identity graph)", ddb, f"{ddb_w/1e6:,.0f}M WRU + {ddb_r/1e6:,.0f}M RRU"))

    billed_bytes = math.ceil(record_bytes / FIREHOSE_ROUND_BYTES) * FIREHOSE_ROUND_BYTES
    firehose_gb = events_month * billed_bytes / 1e9
    firehose = firehose_gb * (FIREHOSE_PER_GB + FIREHOSE_PARQUET_PER_GB)
    add(("Firehose + Parquet", firehose, f"{firehose_gb:,.0f} GB billed (5KB rounding on {record_gb_month:,.0f} GB)"))

    objects = ACTIVE_TENANTS * (SECONDS_PER_MONTH / FIREHOSE_BUFFER_S)
    dp = (record_gb_month * FIREHOSE_DP_PER_GB + objects / 1e3 * FIREHOSE_DP_PER_1K_OBJECTS
          + HOURS_PER_MONTH * FIREHOSE_JQ_HOUR)
    add(("Firehose dynamic partitions", dp, f"{objects/1e6:,.1f}M objects, JQ all month"))

    s3_gb = record_gb_month / 30 * COLD_RETENTION_DAYS / PARQUET_COMPRESSION
    s3 = s3_gb * S3_PER_GB_MONTH
    add(("S3 (90-day lake)", s3, f"{s3_gb:,.0f} GB resident"))

    aurora_inst = AURORA_WRITER_HOUR * AURORA_IO_OPTIMIZED_UPLIFT * AURORA_INSTANCES * HOURS_PER_MONTH
    aurora_gb = gb_day * HOT_RETENTION_DAYS * AURORA_INDEX_OVERHEAD
    aurora = aurora_inst + aurora_gb * AURORA_STORAGE_PER_GB
    add(("Aurora PG, I/O-Optimized", aurora, f"{AURORA_INSTANCES} instances + {aurora_gb:,.0f} GB"))

    proxy = AURORA_INSTANCES * AURORA_VCPU_EACH * RDS_PROXY_VCPU_HOUR * HOURS_PER_MONTH
    add(("RDS Proxy", proxy, f"{AURORA_INSTANCES * AURORA_VCPU_EACH} vCPU"))

    redis = REDIS_NODE_HOUR * REDIS_NODES * HOURS_PER_MONTH
    add(("ElastiCache Redis", redis, f"{REDIS_NODES} nodes"))

    add(("Observability", OBSERVABILITY_MONTHLY, "CloudWatch, traces, alerting"))

    total = sum(c for _, c, _ in lines)

    # Capacity checks at the spike.
    per_shard_spike = spike_rate / shards
    read_mb_per_shard = per_shard_spike * record_bytes * STANDARD_CONSUMERS / 1e6
    batch_s = ENRICH_OVERHEAD_S + ENRICH_BATCH_MAX * ENRICH_PER_EVENT_S
    enrich_capacity = shards * ENRICH_PARALLELIZATION * ENRICH_BATCH_MAX / batch_s
    ingest_concurrency = spike_rate * INGEST_LAMBDA_SECONDS
    enrich_concurrency = shards * ENRICH_PARALLELIZATION

    forward_s = events_month * FORWARD_PER_EVENT_S
    forwarder = (enrich_inv / 1e6) * LAMBDA_PER_MILLION_REQ + forward_s * FORWARD_MEMORY_GB * LAMBDA_GB_SECOND
    dedup_gb = EVENTS_PER_DAY * DEDUP_TTL_HOURS / 24 * DEDUP_BYTES_PER_KEY / 1e9

    http_api = min(events_month, 300e6) / 1e6 * APIGW_HTTP_FIRST_300M
    http_api += max(events_month - 300e6, 0) / 1e6 * APIGW_HTTP_AFTER

    return {
        "spike": spike, "mean_rate": mean_rate, "peak_rate": peak_rate,
        "spike_rate": spike_rate, "gb_day": gb_day, "gb_month": gb_month,
        "shards": shards, "shards_by_records": shards_by_records,
        "shards_by_bytes": shards_by_bytes, "lines": lines, "total": total,
        "edge": ingest + alb + waf,
        "apigw_rest": rest_requests(events_month) + ingest + waf,
        # HTTP APIs cannot attach WAF at all, so this row has none.
        "apigw_http": http_api + ingest,
        "forwarder": forwarder, "dedup_gb": dedup_gb,
        "record_bytes": record_bytes, "read_mb_per_shard": read_mb_per_shard,
        "per_shard_spike": per_shard_spike, "enrich_capacity": enrich_capacity,
        "ingest_concurrency": ingest_concurrency, "enrich_concurrency": enrich_concurrency,
    }


def show(m):
    print("=" * 72)
    print("Sizing and cost model -- engineer-004")
    print("=" * 72)
    print(f"Events/day            : {EVENTS_PER_DAY:,} [Assumed, brief]")
    print(f"Mean event size       : {MEAN_EVENT_BYTES} B [Observed, fixture n=25]")
    print(f"Mean ingest rate      : {m['mean_rate']:,.0f} events/s [Estimated]")
    print(f"Peak hour ({DIURNAL_PEAK_FACTOR:.0f}x mean)   : {m['peak_rate']:,.0f} events/s [Estimated]")
    print(f"Spike ({m['spike']:.0f}x peak)      : {m['spike_rate']:,.0f} events/s [Estimated]")
    print(f"Volume                : {m['gb_day']:,.1f} GB/day, {m['gb_month']:,.0f} GB/mo [Estimated]")
    print()
    print(f"Kinesis record        : {m['record_bytes']} B = {MEAN_EVENT_BYTES} B event + "
          f"{ENVELOPE_OVERHEAD_BYTES} B envelope [Assumed]")
    print(f"Shard requirement     : {m['shards_by_records']:,.1f} by records/s, "
          f"{m['shards_by_bytes']:,.1f} by bytes/s")
    print(f"  -> records-bound, provision {m['shards']} shards (ceil + 1 headroom)")
    print()
    print(f"{'Component':<28}{'$/month':>10}   Basis")
    print("-" * 72)
    for name, cost, basis in m["lines"]:
        print(f"{name:<28}{cost:>10,.0f}   {basis}")
    print("-" * 72)
    print(f"{'TOTAL [Estimated]':<28}{m['total']:>10,.0f}")
    print(f"{'Budget ceiling [Assumed]':<28}{BUDGET_CEILING:>10,.0f}")
    print(f"{'Headroom':<28}{(1 - m['total'] / BUDGET_CEILING) * 100:>9,.0f}%")
    print(f"Not modelled: {NOT_MODELLED}.")
    print(f"Migration only (W1-12), on top: legacy forwarder ${m['forwarder']:,.0f}/mo")
    print()
    print("Ingest edge, same ingest Lambda behind each option [Estimated]:")
    print(f"  ALB + WAF           ${m['edge']:>8,.0f}/mo   (chosen)")
    print(f"  API Gateway HTTP    ${m['apigw_http']:>8,.0f}/mo   (no WAF: unsupported on HTTP APIs)")
    print(f"  API Gateway REST    ${m['apigw_rest']:>8,.0f}/mo   (+ WAF)")
    print()
    print("Capacity at the spike [Estimated]:")
    print(f"  Enrichment          {m['enrich_capacity']:>8,.0f} events/s capacity vs "
          f"{m['spike_rate']:,.0f} needed ({m['enrich_concurrency']} concurrent)")
    print(f"  Ingest Lambda       {m['ingest_concurrency']:>8,.0f} concurrent vs "
          f"{LAMBDA_DEFAULT_CONCURRENCY:,} default quota -> raise quota before launch")
    print(f"  Kinesis reads       {m['read_mb_per_shard']:>8,.2f} MB/s per shard "
          f"({STANDARD_CONSUMERS} standard consumers) vs "
          f"{SHARD_READ_BYTES_PER_SEC/1e6:.0f} MB/s limit")
    print(f"  Redis dedup keys    {m['dedup_gb']:>8,.1f} GB at a {DEDUP_TTL_HOURS}h TTL vs "
          f"{REDIS_NODE_GB} GB per node")
    print()
    print("Rate sources:")
    for k, v in RATE_SOURCES.items():
        print(f"  {k:<9} {v}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spike", type=float, default=SPIKE_MULTIPLIER,
                    help="spike multiplier over the peak hour used to size shards")
    args = ap.parse_args()
    m = model(args.spike)
    show(m)

    if args.spike == SPIKE_MULTIPLIER:
        print()
        print("Shard provisioning sensitivity (50M/day volume held fixed):")
        for s in (1, 3, 10):
            alt = model(s)
            print(f"  provision for {s:>2}x peak -> {alt['shards']:>3} shards, "
                  f"${alt['total']:,.0f}/mo")
        print("  This is shard headroom only. A sustained 10x in volume")
        print("  (500M events/day) scales the per-event lines linearly and is a")
        print("  different scenario from a spike.")


if __name__ == "__main__":
    main()
