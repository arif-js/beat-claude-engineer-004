# Beat Claude — engineer-004 artifact

Operating artifact for the Single Grain "Beat Claude" challenge (brief version 2026-07). Written answer: [`answer.md`](answer.md).

Pure Python standard library (3.9 or later). No dependencies, no credentials, no network calls.

## detect_anomalies.py

Reads a JSONL event stream and reports, class by class, what an ingest pipeline would
have to handle. Every finding names the `event_id`s it is based on, a disposition, and
the reasoning. Exits 1 if any finding is critical, so it can gate CI.

```
python3 detect_anomalies.py path/to/event_sample.jsonl
python3 detect_anomalies.py path/to/event_sample.jsonl --json
```

Notable: parsing is failure-isolated per line (one malformed or non-object record never
costs the batch); duplicates are dropped per tenant before any other check, so retries
never look like bots; ordering uses server `received_at`, never the client clock; and it
builds a tenant-scoped union-find identity graph over `anonymous_id`/`user_id` so a
deletion request's true scope includes pre-identify events carrying a null `user_id`.

## cost_model.py

Sizing, cost and spike-capacity model. Every unit rate is a declared constant with a
source label, so any input can be changed and the total re-derived.

```
python3 cost_model.py              # baseline, shards sized for a 10x spike
python3 cost_model.py --spike 3    # sensitivity
```

Aurora, ElastiCache, RDS Proxy and WAF rates are `[Assumed]` and unverified; they are
the softest inputs in the model and are labelled as such in the source.

## Reproducing the numbers in the written answer

```
shasum -a 256 path/to/event_sample.jsonl
# expected for the fixture analysed: 1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235

python3 detect_anomalies.py path/to/event_sample.jsonl   # 12 findings, 12 classes, 10 clean
python3 cost_model.py                                     # $8,252/mo, 19 shards

# mean event size (229 B):
python3 -c "import statistics,sys;print(round(statistics.mean(len(l.rstrip(chr(10)).encode()) for l in open(sys.argv[1]) if l.strip()),1))" path/to/event_sample.jsonl
```

Fixtures rotate between hiring rounds, so re-run `shasum` against your copy: if it
differs from the value above, the findings will differ too.

## evidence/

| File | What it is |
|---|---|
| `detector_run.md` | Unedited detector output against the stated checksum |
| `findings.json` | Same findings, machine-readable |
| `cost_model_run.md` | Unedited sizing, cost and capacity output |

The fixture itself is not redistributed here — point the scripts at your own copy. It ships
with the challenge repo, https://github.com/ericosiu/beat-claude, at:

```
challenges/engineer-004/fixtures/event_sample.jsonl
```
