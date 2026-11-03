# Cost model run

[Estimated] Unedited output of `python3 cost_model.py`; every input is labelled in the source.

```
========================================================================
Sizing and cost model -- engineer-004
========================================================================
Events/day            : 50,000,000 [Assumed, brief]
Mean event size       : 229 B [Observed, fixture n=25]
Mean ingest rate      : 579 events/s [Estimated]
Peak hour (3x mean)   : 1,736 events/s [Estimated]
Spike (10x peak)      : 17,361 events/s [Estimated]
Volume                : 11.4 GB/day, 344 GB/mo [Estimated]

Kinesis record        : 609 B = 229 B event + 380 B envelope [Assumed]
Shard requirement     : 17.4 by records/s, 10.6 by bytes/s
  -> records-bound, provision 19 shards (ceil + 1 headroom)

Component                      $/month   Basis
------------------------------------------------------------------------
Kinesis Data Streams               229   19 shards + 1,500M PUT units
Lambda (ingest edge)               460   1,500M invocations @ 50ms/128MB
ALB                                152   23 LCU avg; spike 694 LCU = $5.56/spike-hour
WAF on ALB                         900   per-IP rate limits + payload caps
Lambda (enrichment)                 76   49M invocations, 4.0M s
DynamoDB (identity graph)          281   150M WRU + 1,500M RRU
Firehose + Parquet                 361   7,680 GB billed (5KB rounding on 914 GB)
Firehose dynamic partitions         91   4.3M objects, JQ all month
S3 (90-day lake)                    13   548 GB resident
Aurora PG, I/O-Optimized         3,026   3 instances + 240 GB
RDS Proxy                          263   24 vCPU
ElastiCache Redis                  900   3 nodes
Observability                    1,500   CloudWatch, traces, alerting
------------------------------------------------------------------------
TOTAL [Estimated]                8,252
Budget ceiling [Assumed]        50,000
Headroom                           83%
Not modelled: S3 requests, KMS calls, Route 53, data egress, dashboard and Personalization API compute, NAT.
Migration only (W1-12), on top: legacy forwarder $72/mo

Ingest edge, same ingest Lambda behind each option [Estimated]:
  ALB + WAF           $   1,512/mo   (chosen)
  API Gateway HTTP    $   1,840/mo   (no WAF: unsupported on HTTP APIs)
  API Gateway REST    $   5,583/mo   (+ WAF)

Capacity at the spike [Estimated]:
  Enrichment            27,941 events/s capacity vs 17,361 needed (57 concurrent)
  Ingest Lambda            868 concurrent vs 1,000 default quota -> raise quota before launch
  Kinesis reads           1.67 MB/s per shard (3 standard consumers) vs 2 MB/s limit
  Redis dedup keys         5.2 GB at a 25h TTL vs 26.32 GB per node

Rate sources:
  kinesis   https://aws.amazon.com/kinesis/data-streams/pricing/ [Benchmarked]
  dynamodb  https://aws.amazon.com/dynamodb/pricing/on-demand/ (post Nov-2024) [Benchmarked]
  lambda    https://aws.amazon.com/lambda/pricing/ x86 [Benchmarked]
  firehose  https://aws.amazon.com/firehose/pricing/ (5KB record rounding) [Benchmarked]
  s3        https://aws.amazon.com/s3/pricing/ Standard [Benchmarked]
  elb       https://aws.amazon.com/elasticloadbalancing/pricing/ [Benchmarked]
  apigw     https://aws.amazon.com/api-gateway/pricing/ [Benchmarked]
  assumed   Aurora / ElastiCache / RDS Proxy / WAF rates: not re-verified [Assumed]

Shard provisioning sensitivity (50M/day volume held fixed):
  provision for  1x peak ->   3 shards, $8,055/mo
  provision for  3x peak ->   7 shards, $8,104/mo
  provision for 10x peak ->  19 shards, $8,252/mo
  This is shard headroom only. A sustained 10x in volume
  (500M events/day) scales the per-event lines linearly and is a
  different scenario from a spike.
```
