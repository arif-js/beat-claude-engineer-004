# Real-Time Analytics Pipeline — engineer-004

**Brief version: 2026-07.** Fixture: `fixtures/event_sample.jsonl`, `sha256
1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`, [Observed] 25 lines.

## Written answer

### The budget is not the binding constraint

At [Assumed] 50M events/day this costs [Estimated] **$8,252/mo against the [Assumed] $50K
ceiling — 83% headroom** (`cost_model.py`; S3 requests, KMS, egress and API compute
unmodelled). Sizing shards for a 10x spike adds only [Estimated] $197/mo. **Two engineers
for three months is the scarce resource**, so I spend infrastructure to buy engineering
time: managed services, over-provisioned shards, a narrow MVP.

### Architecture

```
JS SDK (unchanged) ──► Route 53 ──► ALB + WAF ──► Ingest Lambda (writes Kinesis, returns)
                       (weighted,   (per-IP rate   │ stamps received_at, schema_version;
                        60s TTL)     limits)       │ HMAC-tokenizes known PII keys;
                                                   │ allowlists headers (no Cookie);
                                                   │ throttled key → random key;
                                                   │ null tenant_id → quarantine
                                                   ▼
                         Kinesis Data Streams (19 shards, 24h replay)
      record = {tenant_id, received_at, headers, body (tokenized), legacy_pii (W1–12)}
                           partition key = tenant_id:anonymous_id
                                         │
       ┌──────────────────────┬──────────┴───────────┬────────────────────┐
       ▼                      ▼                      ▼                    ▼
 Enrich Lambda         Firehose (archive tee)   Forwarder Lambda     Quarantine / DLQ
 ≤500 / 1s window,     S3 Parquet without       restores legacy_pii, (alerted, never
 parallelization 3,    legacy_pii, tenant_id=/  replays the original  silently dropped)
 normalizes legacy     received_date=/,         beacon (W1–12)
 shape, resolves       90d, KMS                                     Compliance plane
 person via graph                                                   (deletion, human-
       │                                                             approved)
 ┌─────┼──────────────┐
 ▼     ▼              ▼
Redis  DynamoDB       RDS Proxy → Aurora PG writer (7d, RLS, daily partitions)
       identity graph               └─ reader endpoint ◄── Dashboard API
 └──► Personalization API (Redis: anon alias → person → counters)
```

| Component | Chosen | Rejected | Why ([Estimated], `cost_model.py`) |
|---|---|---|---|
| Ingest edge | ALB + WAF + Lambda | API Gateway HTTP / REST | $1,512 vs $1,840 (HTTP APIs cannot attach WAF at all) vs $5,583/mo |
| Transport | Kinesis | MSK / Kafka | Operator time we lack; 24h replay *is* the rollback |
| Compute | Lambda, 1s window | Flink | $76/mo at ~49M invocations; parallelization 3 gives 27,941/s capacity vs 17,361/s spike |
| Storage | Aurora 7d + S3 90d | ClickHouse, Redshift | Team runs Postgres; RLS isolates tenants; daily partitions dropped, never `DELETE`d |

**Event shape.** `ts` is the client's *claim*, never used for ordering, retention or
deletion; `received_at` is stamped at the edge. The body keeps its legacy shape but known
PII keys are tokenized; raw values ride in `legacy_pii` under a migration-only KMS key, which
the archive's Parquet schema omits and the Forwarder decrypts to replay what legacy saw. The
identity graph stores links **both ways**. Any event with both ids records one — [Observed]
`u-1077`'s only link to `anon-77a` is the erasure request itself — but only `identify` links
merge profiles; the rest widen deletion scope. Neither is authenticated.

**Personalization reads.** Enrich keeps Redis counters keyed `tenant:person` plus a
`tenant:anon → person` alias, folded together on `identify`. Given an `anonymous_id`, the
Personalization API reads alias then counters, never Aurora, so "viewed pricing 3x" spans
devices. Counters expire after [Assumed] 30 idle days.

**Compliance shapes the layout.** Every store is keyed by tenant first; KMS at rest;
CloudTrail plus the RLS audit log for SOC 2. HMAC tokens are pseudonymous personal data,
deterministic so an erasure by email finds them. An erasure walks the graph, deletes Aurora
rows, Redis keys and graph nodes, rewrites *closed* tenant-day Parquet files (~[Estimated]
61 MB raw each), purges old S3 versions, and writes a tombstone that outlives the [Assumed]
7-day backups and is re-applied on replay or restore — inside the [Benchmarked] one-month
(GDPR) and 45-day (CCPA) response windows. Until W12 `legacy_pii` lives only in
Kinesis's 24h retention; erasures also run against legacy, and the Forwarder skips tombstones.

### Scale, reliability, migration

**Sizing** ([Estimated], reproduce with `python3 cost_model.py`): 579 events/s mean, 1,736/s peak hour at 3x, 17,361/s at a 10x
spike. Shards are records-bound — 17.4 by records/s vs 10.6 by bytes/s for a 609 B record
([Observed] 229 B event + [Assumed] 380 B envelope) — so ceil + 1 = 19; three standard
consumers read 1.67 MB/s per shard of 2 MB/s; a throttled single-visitor key is re-keyed at
random. Launch tasks: raise Lambda concurrency (~868 at the spike vs a [Benchmarked] 1,000
default) and Firehose active partitions ([Benchmarked] 500 default). RDS Proxy fronts only
Enrich's writes; the dashboard reads the reader endpoint directly, as session `SET`s pin Proxy.

**Loss and double-count.** Goal: **no loss after the edge for outages under the 24h replay
window, no double-count**; unsent events are invisible without an SDK change. Duplicates stop
**at write**: one Redis Lua script runs `SET tenant:event_id <date> NX GET EX 25h`,
increments counters only if new, and returns the first-seen date; Enrich inserts into *that*
day's partition with `ON CONFLICT (tenant_id, received_date, event_id) DO NOTHING`. A retry
across midnight hits the same partition; a crash between steps retries safely. Redis is
`noeviction` ([Estimated] 5.2 GB of keys). Under backpressure **dashboards lag; capture never
stops**. Loss shows by reconciling ALB 2xx, Kinesis and Firehose counts plus DLQ depth.

**Migration — the SDK cannot change and nothing may break.** Weeks [Estimated]:

1. **W1–3 front door.** Route 53 shifts traffic [Assumed] 1% → 10% → 100% to the new ALB;
   the Forwarder feeds legacy from its own iterator, so a slow legacy box never stalls the
   edge. *Rollback: DNS weight, 60s TTL.*
2. **W4–7 shadow.** Dashboards read old; hourly per-tenant `event_id` set-diff. Exit is
   asymmetric since legacy drops ~3%: **`missing_in_new` < [Assumed] 0.01%**, every
   `extra_in_new` in a named bucket (legacy drop, dedup, bot skip).
3. **W8–10 read cutover per tenant**, smallest first, server-side flag. *Rollback: flip the
   flag in seconds; legacy is still being fed.*
4. **W11–12 stop forwarding.** Legacy stays read-only [Assumed] 30 days as a reference.

**Rollback triggers** (thresholds [Assumed]): `missing_in_new` >0.1% or unexplained
`extra_in_new` >0.5% for 2h; p99 >5s for 15 min; DLQ >1,000; any cross-tenant read. **"Accuracy verified"**: on a sampled tenant-hour, over `event_id`s both
received, per-type counts match within [Assumed] 0.1% and each visitor resolves identically.

### What is actually in the fixture

`detect_anomalies.py` finds [Observed] **12 findings across 12 classes in 25 events; 10 of
24 parsed events are clean** (`evidence/detector_run.md`). All ids and offsets below are [Observed]:

- **`evt-0020` truncated JSON** [Observed] → per-line parse, quarantine + alert; never costs the batch.
- **`evt-0007` PII in `properties`** (`contact_email`, `phone`) → tokenize at the edge; a
  schema contract is the real fix. `evt-0022` later links its `anon-3d0` to `u-7304`, so the
  token is in that person's deletion scope.
- **`evt-0017` GDPR erasure as a stream event** → compliance plane, never a consumer.
- **Clock skew, three faults:** `evt-0016` is a year ahead, yet the same visitor's `evt-0024`
  eight minutes later is correct — a year-field bug in app code, not the device clock; it
  would escape a `ts`-keyed retention rule. `evt-0006` is ahead 3,900.000s: 65 min is no
  timezone, and a millisecond-exact match survives no real network, so it looks computed —
  log it, alert only if it recurs. `evt-0005` (47.19s) is ordinary drift.
- **`evt-0009` legacy SDK** (`timestamp`, `page_path`, `pageview`, **no `received_at`**) →
  normalize permanently; stamp arrival at the edge. **`evt-0011` null `tenant_id`** →
  quarantine, never default.
- **`evt-0012`–`0015` bots** (4 navigations in 0.064s, `scanner.example-bot.net` referrer)
  → label `is_bot`, exclude from billing. **`evt-0002` duplicate**, second copy 7.43s later →
  dedupe at write. **`evt-0019` client aggregate** → recompute server-side.

**Three things I would not act on at face value** ([Observed]). (1) `evt-0002` is not two
clicks: identical payload, later arrival — redelivery that would inflate conversions. (2) The
bots are labelled, never *deleted*, or I could not explain why a customer's traffic fell. (3) `evt-0019` claims `count_today: 3` while the
stream holds **one** `/pricing` view for that visitor — the brief's "viewed pricing 3x"
segment, computed by a browser. Buggy counter or mid-session sample, 25 events cannot say;
a rule firing on a browser's arithmetic is indefensible to whoever it misfires on.

**The compound trap.** `evt-0017` erases `u-1077`. The same device (`anon-77a`) sent
`evt-0006` before linking, with `user_id: null`: **a `user_id`-scoped delete misses it and
reports success.** Being [Observed] +65 min skewed, it also escapes a `ts ≤ request_time`
bound (`15:11:03Z` vs `14:12:44Z`). Hence deletion walks the graph *and* bounds on
`received_at`, as retention does, so `evt-0016` cannot outlive 90 days.

**What I refused to conclude.** n=25 proves these classes exist, not their rates; the
brief's [Assumed] 3% loss is not visible. No `anonymous_id` is shared across tenants, but 25
events cannot call that clean. Nor can I count unique visitors: a cleared cookie mints a new
`anonymous_id`, so one person looks like two until they identify — and I would not merge
them on IP or user agent.

### Trade-offs and scope

**Optimizing for** operator time, then correctness, then latency; **sacrificing**
exactly-once, sub-second p99 ([Assumed] <5s is enough), and unit cost.

**MVP excludes**, each about a month of two engineers ([Estimated]), none blocking the
latency complaint: warehouse export, segmentation beyond server counters, a schema registry.
**Months 4–6** ship them (export from a deduplicated Parquet prefix), plus ClickHouse once
Aurora readers pass [Assumed] 60% CPU.

## Operating artifact

**`detect_anomalies.py`** (each finding with `event_id`s, disposition, reasoning; tenant-
scoped; exits 1 on critical) and **`cost_model.py`** (every rate a labelled constant).
Stdlib only, no network.

## Artifact access

Repository: https://github.com/arif-js/beat-claude-engineer-004 — public, no login. Python 3.9+, no dependencies.

```
python3 detect_anomalies.py <fixture>     # report; --json for machine form
python3 cost_model.py [--spike N]         # sizing, cost, capacity
```

Saved outputs are in `evidence/`. The fixture is not redistributed — point the script at your copy.

## Evidence log

| Claim | Tier | Checkable by |
|---|---|---|
| 12 anomaly classes at the cited ids; 10 of 24 clean; 229 B mean event; `user_id`-scoped delete misses `evt-0006` | **Tier 3** | `evidence/detector_run.md` + re-run on your copy |
| $8,252/mo; 19 shards; spike capacity; edge comparison | **Tier 2** | `cost_model.py` — inspectable model, rates linked in `RATE_SOURCES` |
| Aurora, Redis, RDS Proxy, WAF rates | **Tier 0** | Not re-verified, labelled `[Assumed]` in source |
| <5s latency; migration without breakage | **Tier 0** | Design targets — nothing was built |

## Number source labels

Labelled inline. **[Observed]**: the fixture at the stated checksum (`detect_anomalies.py
--json`). **[Benchmarked]**: AWS rates and limits (`RATE_SOURCES`), statutory deadlines.
**[Estimated]**: my arithmetic, including the $8,252 total (`cost_model.py`). **[Assumed]**:
brief placeholders (50M/day, $50K, 10x), unverified instance rates, my thresholds.

## AI usage disclosure

**Tools:** Claude (Claude Code); web search for AWS rates and limits. **Helped with:**
drafting both scripts and this document. **Checked:** several review rounds by separate AI
agents, each finding re-verified in code or AWS/PostgreSQL docs before fixing. **I decided:** team, not budget, binds;
ALB edge; `received_at` everywhere; graph-walk deletion; front-door-then-shadow
migration; the MVP cut; which anomalies are *not* actionable. **I changed:** classified clock
skew by magnitude and by the visitor's other events. **Weak spots:** unverified instance and
WAF rates; unmeasured diurnal factor, compression, per-event Lambda time.

## What breaks it

1. **3x diurnal is a guess.** At 10x it is ~58,000/s: ~59 shards ([Estimated] +$493/mo) and
   ~2,900 concurrent ingest Lambdas. Measure it in week 1.
2. **229 B comes from 25 synthetic events.** A 5 KB body plus envelope needs ~97 shards,
   overruns the shared 2 MB/s read (Enrich moves to enhanced fan-out, not costed), grows S3 and Aurora
   ~20x, and crosses Firehose's 5 KB step, doubling it to [Estimated] ~$722/mo.
3. **Aurora at the spike is unmeasured.** ~17K rows/s and the replica lag it causes are the
   least-proven link; if it falls behind, Kinesis buffers and dashboards lag. Load-test in W2.
4. **`tenant_id` is whatever the beacon says.** Unsigned without an SDK change; checking
   `Origin` against each tenant's domains stops browsers, not scripts. Likewise anyone can
   `identify` their `anonymous_id` as another's `user_id` and read that person's counters, so
   Enrich stops merging and alerts when an `anonymous_id` joins >[Assumed] 3 `user_id`s in 24h
   or a `user_id` gains >[Assumed] 5 aliases in an hour (the link stays, for deletion). Below
   them a forged merge still succeeds, the price of no signing; aliases record their
   `event_id`, so undoing one rebuilds both people's counters from S3 (Aurora keeps 7 days).
5. **PII under unknown keys reaches the archive** until the schema registry ships. Erasure
   drops it with the person's rows, but misses PII *about someone else* in another's event.
6. **"No breaking change" may reach past the SDK**: an unscoped webhook compatibility layer;
   and erasure cannot reach a customer's own Snowflake — a contractual answer.
7. **Hot partitions come from single visitors.** WAF limits per IP, not body fields; a
   many-IP flood on one `anonymous_id` is re-keyed on throttle, losing its order.
8. **Two engineers is tight.** If one is pulled onto incidents, I let the shadow window slip
   rather than cut the only proof the data is right.

## What stays human

1. **Executing a deletion request.** [Observed] `evt-0017` (`evidence/findings.json`) arrives on the unauthenticated
   beacon channel; a forged one would wipe a real customer's data. Verification stays human,
   including which *unverified* graph links fall in scope.
2. **Approving a new event schema** — it sets billing, retention and what PII enters the
   lake; `evt-0007` is the ungated case.
3. **Bot reclassification that changes an invoice**: detection automated, billing reviewed.
4. **Mid-incident rollback.** Triggers are automated; judging a 0.4% divergence is not.
5. **Declaring accuracy verified**: a human signs that divergences were *explained*, not small.
