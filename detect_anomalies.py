#!/usr/bin/env python3
"""Anomaly detector for the Beat Claude engineer-004 event fixture.

Usage:
    python3 detect_anomalies.py path/to/event_sample.jsonl [--json]

Reads a JSONL event stream and reports, class by class, what the ingest
pipeline would have to handle. Each finding names the event_ids it is
based on so the output can be checked against the data by hand.

Design notes that matter for the pipeline, not just this script:

* Parsing is per-line and failure-isolated. One malformed line must never
  cost us the other 24. A pipeline that json.loads() a whole batch inside
  one try block loses the batch.
* No finding is auto-remediated. Every class carries a disposition
  (drop / quarantine / flag / route-to-human) because several of these
  look like bad data and are not, and one of them is a GDPR control
  action that must not be executed by a stream consumer.
* Thresholds are declared as constants with a stated basis, not inlined
  as magic numbers, because they are the part a reviewer should argue
  with.

Pure stdlib. No network, no writes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# --- Thresholds -------------------------------------------------------------
# [Assumed] A browser SDK beacon normally lands within a few seconds of the
# action. We treat >5s as late rather than impossible, and anything received
# BEFORE its own ts as a clock problem (negative transit time is not physical).
MAX_PLAUSIBLE_TRANSIT = timedelta(seconds=5)

# [Assumed] Four *navigations* from one anonymous_id inside this window averages
# 500ms per page, which is below the time to render and read anything. Applied
# to page_view only: rapid clicks on one page are ordinary human behaviour, and
# a false positive here is cheap because the disposition is a label, not a drop.
# This is the threshold a reviewer should argue with first.
BURST_WINDOW = timedelta(seconds=2)
BURST_MIN_EVENTS = 4
BURST_TYPES = {"page_view"}

# Legacy SDK spellings -> current spelling. An event carrying the legacy key is
# an older SDK we cannot force to upgrade (brief constraint), so these mappings
# are permanent, not a migration aid.
LEGACY_TOP_LEVEL_KEYS = {"timestamp": "ts"}
LEGACY_PROPERTY_KEYS = {"page_path": "path", "ref": "referrer"}
# Legacy/misspelled type values seen in the wild -> canonical.
TYPE_ALIASES = {"pageview": "page_view"}

# Property keys that carry personal data if a customer puts them there. Not
# exhaustive by design: see the note in report().
PII_KEY_PATTERN = re.compile(
    r"email|phone|ssn|first_?name|last_?name|full_?name|address|dob|birth", re.I
)
EMAIL_VALUE_PATTERN = re.compile(r"[^@\s]+@[^@\s]+\.[a-z]{2,}", re.I)

# Referrer hosts that self-identify as automated traffic.
BOT_REFERRER_PATTERN = re.compile(r"bot|crawl|scanner|spider|headless", re.I)


def parse_ts(value):
    """Lenient ISO-8601 parse. Returns None rather than raising.

    A timestamp with no zone is treated as unusable rather than guessed at:
    assuming UTC would invent a skew, and mixing naive and aware datetimes
    raises on subtraction.
    """
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def load(path):
    """Parse line by line, isolating failures. Returns (events, parse_errors)."""
    events, errors = [], []
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                if not isinstance(obj, dict):
                    # Valid JSON but not an event: a bare list or number would
                    # otherwise crash every check that expects a mapping.
                    raise json.JSONDecodeError(
                        f"not a JSON object ({type(obj).__name__})", raw, 0
                    )
                events.append((lineno, obj))
            except json.JSONDecodeError as exc:
                # Recover the id if we can, so the event is traceable even
                # though the record is unusable.
                hint = re.search(r'"event_id"\s*:\s*"([^"]+)"', raw)
                errors.append(
                    {
                        "line": lineno,
                        "event_id": hint.group(1) if hint else None,
                        "error": str(exc),
                        "bytes": len(raw),
                    }
                )
    return events, errors


def normalize(event):
    """Map legacy SDK spellings onto the canonical shape.

    Returns (normalized_event, list_of_legacy_keys_found). We normalize for
    analysis but report the drift, because the brief forbids making customers
    upgrade the SDK -- meaning both shapes are permanent, not transitional.
    """
    found = []
    out = dict(event)
    for legacy, canonical in LEGACY_TOP_LEVEL_KEYS.items():
        if legacy in out:
            found.append(legacy)
            out.setdefault(canonical, out[legacy])
    props = out.get("properties")
    if props is not None and not isinstance(props, dict):
        found.append("properties:not-an-object")
        props = None
    props = dict(props or {})
    for legacy, canonical in LEGACY_PROPERTY_KEYS.items():
        if legacy in props:
            found.append(f"properties.{legacy}")
            props.setdefault(canonical, props[legacy])
    out["properties"] = props
    if out.get("type") in TYPE_ALIASES:
        found.append(f"type={out['type']}")
        out["type"] = TYPE_ALIASES[out["type"]]
    return out, found


def build_identity_graph(norm):
    """Union anonymous_id and user_id into one component per person.

    An identify event links a device to a person, which means a person's data
    is spread across every anonymous_id they ever used -- including events that
    arrived BEFORE the identify and therefore carry a null user_id. Resolving
    by user_id alone silently misses those, which is the difference between a
    complete erasure and a compliance incident, so the graph is built once and
    used by every check that has to reason about a person rather than a device.
    """
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # Scope all identity keys by tenant so a rotated fixture that reuses
    # anonymous_id/user_id across tenants never merges identities across them.
    for _, ev, _ in norm:
        t = ev.get("tenant_id") or "t:_null"
        anon, uid = ev.get("anonymous_id"), ev.get("user_id")
        if anon:
            find(f"{t}|anon:{anon}")
        if uid:
            find(f"{t}|user:{uid}")
        if anon and uid:
            union(f"{t}|anon:{anon}", f"{t}|user:{uid}")

    def component_of(ev):
        t = ev.get("tenant_id") or "t:_null"
        anon, uid = ev.get("anonymous_id"), ev.get("user_id")
        key = f"{t}|anon:{anon}" if anon else (f"{t}|user:{uid}" if uid else None)
        return find(key) if key else None

    members = defaultdict(set)
    for _, ev, _ in norm:
        root = component_of(ev)
        if root:
            for node in (ev.get("anonymous_id"), ev.get("user_id")):
                if node:
                    prefix = "anon" if node == ev.get("anonymous_id") else "user"
                    members[root].add(f"{prefix}:{node}")
    return component_of, members


def detect(events, parse_errors):
    """Return an ordered list of findings. Each finding is a dict."""
    findings = []
    add = findings.append

    # --- Class 1: unparseable records --------------------------------------
    if parse_errors:
        add(
            {
                "cls": "malformed_json",
                "severity": "high",
                "ids": [e["event_id"] for e in parse_errors],
                "lines": [e["line"] for e in parse_errors],
                "detail": "; ".join(
                    f"line {e['line']} ({e['event_id'] or 'id unrecoverable'}): {e['error']}"
                    for e in parse_errors
                ),
                "disposition": "quarantine to DLQ, alert, never drop silently",
                "why": "A truncated record is the shape of partial-write or "
                       "transport truncation. The count is the signal: if this "
                       "rate moves, something upstream is cutting payloads.",
            }
        )

    norm = []
    legacy_hits = defaultdict(list)
    drift_lines = set()
    for lineno, raw in events:
        ev, legacy = normalize(raw)
        norm.append((lineno, ev, raw))
        for key in legacy:
            legacy_hits[key].append(ev.get("event_id"))
            drift_lines.add(lineno)

    # Every finding records the input lines it covers. event_id alone is not
    # enough: the same id can legitimately appear in two tenants.
    line_of = {id(ev): ln for ln, ev, _ in norm}

    def lines(evs):
        return sorted({line_of[id(e)] for e in evs})

    # --- Class 2: schema drift (older SDK) ---------------------------------
    if legacy_hits:
        # key=str: an event with a null event_id must not crash the sort.
        ids = sorted({i for v in legacy_hits.values() for i in v}, key=str)
        add(
            {
                "cls": "schema_drift_legacy_sdk",
                "severity": "high",
                "ids": ids,
                "lines": sorted(drift_lines),
                "detail": "; ".join(f"{k} -> {sorted(set(v), key=str)}" for k, v in legacy_hits.items()),
                "disposition": "normalize in the pipeline; version every event; "
                               "keep both shapes supported indefinitely",
                "why": "The brief forbids requiring an SDK update, so this is "
                       "not a migration state that ends. Normalization has to "
                       "live in the pipeline permanently, and the legacy shape "
                       "has no received_at at all, so server-side arrival time "
                       "must be stamped on receipt or ordering is unrecoverable.",
            }
        )

    component_of, members = build_identity_graph(norm)

    # --- Class 3: duplicate event_id ---------------------------------------
    by_id = defaultdict(list)
    missing_id = []
    for lineno, ev, _ in norm:
        eid = ev.get("event_id")
        if not eid:
            # Two events that both lack an id are not duplicates of each other.
            # Grouping them on a None key invents a duplicate that is not there.
            missing_id.append(lineno)
            continue
        # Scoped by tenant: the same id in two tenants is two events, and
        # treating it as a duplicate would drop one customer's data.
        by_id[(ev.get("tenant_id"), eid)].append((lineno, ev))
    if missing_id:
        add(
            {
                "cls": "missing_event_id",
                "severity": "high",
                "ids": [f"line {ln}" for ln in missing_id],
                "lines": missing_id,
                "detail": f"{len(missing_id)} event(s) carry no event_id",
                "disposition": "quarantine; synthesise an id only for storage, "
                               "never for deduplication",
                "why": "event_id is the only thing that makes delivery "
                       "idempotent. Without it a retry is indistinguishable "
                       "from a real second action, so these events cannot be "
                       "safely deduplicated and must not be counted as though "
                       "they were.",
            }
        )
    dropped_lines = set()
    for (_, eid), group in by_id.items():
        if len(group) < 2:
            continue
        payloads = {
            json.dumps(
                {k: v for k, v in ev.items() if k != "received_at"}, sort_keys=True
            )
            for _, ev in group
        }
        arrivals = [ev.get("received_at") for _, ev in group]
        identical = len(payloads) == 1
        # Keep the earliest arrival, exactly as the disposition says, and run
        # every later check on the deduplicated stream: otherwise retries of
        # one event look like a bot burst or a late arrival.
        earliest = min(
            group,
            key=lambda g: (parse_ts(g[1].get("received_at")) is None,
                           parse_ts(g[1].get("received_at")) or datetime.max, g[0]),
        )
        if not identical:
            # Same id, different content: two real events sharing an id. Both
            # stay in the stream, or whatever the second one carries (PII, an
            # erasure request) would never be checked.
            add(
                {
                    "cls": "event_id_reuse",
                    "severity": "high",
                    "ids": [eid],
                    "lines": [ln for ln, _ in group],
                    "detail": f"{len(group)} different payloads share this id on "
                              f"lines {[ln for ln, _ in group]}",
                    "disposition": "keep every copy; quarantine for review; never "
                                   "dedupe on this id",
                    "why": "Deduplicating would silently delete a real event. "
                           "This is an id generator bug in the client, and it "
                           "breaks idempotency for every event that hits it.",
                }
            )
            continue
        dropped_lines.update(ln for ln, _ in group if ln != earliest[0])
        add(
            {
                "cls": "duplicate_event_id",
                "severity": "medium",
                "ids": [eid],
                "lines": [ln for ln, _ in group],
                "detail": f"{len(group)} copies on lines "
                          f"{[ln for ln, _ in group]}; payload identical="
                          f"{identical}; received_at={arrivals}; kept line "
                          f"{earliest[0]}",
                "disposition": "dedupe on event_id, keep earliest received_at",
                "why": "Identical payload with a later arrival is at-least-once "
                       "redelivery (SDK retry or ingest replay), not a second "
                       "user action. Counting it as two inflates the metric the "
                       "customer is watching. Payloads are compared, not assumed: "
                       "differing ones are id reuse and reported separately.",
            }
        )

    stream = [row for row in norm if row[0] not in dropped_lines]

    # --- Class 4: clock problems -------------------------------------------
    skews = []
    for lineno, ev, _ in stream:
        ts, ra = parse_ts(ev.get("ts")), parse_ts(ev.get("received_at"))
        if ts is None or ra is None:
            continue
        delta = ra - ts
        if delta < timedelta(0) or delta > MAX_PLAUSIBLE_TRANSIT:
            skews.append((ev, delta))

    def plausible(other):
        ts, ra = parse_ts(other.get("ts")), parse_ts(other.get("received_at"))
        return ts is not None and ra is not None and \
            timedelta(0) <= ra - ts <= MAX_PLAUSIBLE_TRANSIT

    for ev, delta in skews:
        eid = ev.get("event_id")
        secs = delta.total_seconds()
        if secs < 0:
            ahead = abs(secs)
            exact = ahead % 1 == 0
            # Distance to the nearest whole minute. Real transit adds 0.1-0.3s
            # of jitter, so an exact-millisecond test alone would misread a
            # misset clock as drift; a tolerance of a couple of seconds does not.
            off_minute = min(ahead % 60, 60 - ahead % 60)
            # Same person, same window, a sane clock: then the device clock is
            # fine and the bad value came from the code that built this event.
            root = component_of(ev)
            sane_siblings = [
                o.get("event_id") for _, o, _ in stream
                if o is not ev and component_of(o) == root and plausible(o)
            ]
            if ahead > 86_400 * 300:
                shape = "year_scale"
                cause = (
                    f"The same person's {', '.join(sane_siblings)} carries a "
                    "correct ts, so the device clock is fine: this is a date "
                    "bug in the code path that built this event (a year field), "
                    "not an unset system clock. "
                    if sane_siblings else
                    "A date typo or an unset system clock, not drift. "
                )
                why = (
                    cause
                    + "This is the most dangerous of the clock cases and the "
                    "reason partitioning on client ts is unsafe: the record "
                    "lands in a partition far outside the retention window, "
                    "where a 90-day lifecycle rule and a deletion sweep scoped "
                    "to recent partitions will both walk straight past it. That "
                    "turns a clock bug into an undeletable record, which is a "
                    "compliance problem rather than a data-quality one."
                )
                disp = ("partition and retain on received_at; clamp ts into the "
                        "retention window and keep the raw value in a side field")
            elif ahead >= 300 and off_minute <= 2:
                shape = "systematic_offset"
                why = (
                    "Minutes ahead and within seconds of a whole minute is a "
                    "set offset, not drift, which is ragged. 65 minutes is not "
                    "a timezone, so it is a misset clock or a code path adding "
                    "an offset. "
                    + ("Matching received_at to the millisecond is implausible "
                       "for a real client, since transit always adds jitter, "
                       "which points at a computed value rather than a device. "
                       if exact else "")
                    + "One event cannot say which, or how many sources share "
                    "it. In the pipeline it would be logged per source and "
                    "alert only if the same offset recurs across a tenant's "
                    "traffic; this script sees one sample, so it only flags it."
                )
                disp = ("keep ts as reported, order on received_at; flag, do not "
                        "alert on a single event")
            else:
                shape = "device_drift"
                why = (
                    "Seconds to minutes and not near a whole minute: ordinary "
                    "unsynchronised device clock. Not worth an alert on its "
                    "own. It still cannot be trusted for ordering, so the event "
                    "is sequenced on arrival time while the reported ts is "
                    "preserved for the customer."
                )
                disp = "keep ts as reported, order on received_at"
            add(
                {
                    "cls": f"clock_skew_future_ts:{shape}",
                    "severity": "critical" if shape == "year_scale" else "high",
                    "ids": [eid],
                    "lines": lines([ev]),
                    "detail": f"received_at - ts = {secs:+.3f}s; client clock "
                              f"ahead by {ahead:,.3f}s "
                              f"({'exact to the ms' if exact else 'non-integer'}; "
                              f"{off_minute:.3f}s from a whole minute)",
                    "disposition": disp,
                    "why": why,
                }
            )
        else:
            add(
                {
                    "cls": "late_arrival",
                    "severity": "low",
                    "ids": [eid],
                    "lines": lines([ev]),
                    "detail": f"transit {secs:+.3f}s, over the {MAX_PLAUSIBLE_TRANSIT.total_seconds()}s threshold",
                    "disposition": "accept; count against the late-arrival budget",
                    "why": "Expected on mobile and flaky networks. Matters only "
                           "because a <5s dashboard promise means a late event "
                           "changes a number a customer already looked at.",
                }
            )

    # --- Class 5: missing received_at --------------------------------------
    missing_ra_evs = [ev for _, ev, _ in stream if not ev.get("received_at")]
    missing_ra = [ev.get("event_id") for ev in missing_ra_evs]
    if missing_ra:
        add(
            {
                "cls": "missing_received_at",
                "severity": "high",
                "ids": missing_ra,
                "lines": lines(missing_ra_evs),
                "detail": "no server arrival timestamp on the record",
                "disposition": "stamp received_at at the edge before anything else",
                "why": "Without a server-trusted time there is no defence against "
                       "a wrong client clock and no basis for retention or "
                       "deletion windows. This is the same records as the legacy "
                       "SDK, which is why edge stamping cannot be optional.",
            }
        )

    # --- Class 5b: missing or unusable client ts ---------------------------
    bad_ts_evs = [ev for _, ev, _ in stream if parse_ts(ev.get("ts")) is None]
    if bad_ts_evs:
        add(
            {
                "cls": "missing_or_invalid_ts",
                "severity": "medium",
                "ids": [ev.get("event_id") for ev in bad_ts_evs],
                "lines": lines(bad_ts_evs),
                "detail": "ts absent, unparseable, or without a timezone: "
                          + ", ".join(sorted({repr(ev.get("ts"))[:40] for ev in bad_ts_evs})),
                "disposition": "accept; order, retain and delete on received_at; store ts as null",
                "why": "The client clock is only ever a claim, so a broken one costs "
                       "nothing downstream as long as nothing keys on it. Guessing a "
                       "zone would turn a visible gap into a silent error.",
            }
        )

    # --- Class 6: unroutable tenant ----------------------------------------
    no_tenant_evs = [ev for _, ev, _ in stream if not ev.get("tenant_id")]
    no_tenant = [ev.get("event_id") for ev in no_tenant_evs]
    if no_tenant:
        add(
            {
                "cls": "null_tenant_id",
                "severity": "high",
                "ids": no_tenant,
                "lines": lines(no_tenant_evs),
                "detail": "tenant_id is null",
                "disposition": "quarantine; never assign a default tenant",
                "why": "In a 500-tenant system an unattributed event has no safe "
                       "home. Guessing a tenant is how one customer's data shows "
                       "up in another customer's dashboard, which is the "
                       "compliance failure that ends the contract. It also cannot "
                       "be served by a deletion request, because no tenant owns it.",
            }
        )

    # --- Class 7: PII in free-form properties ------------------------------
    def scan_pii(obj, path=""):
        """Walk nested properties. PII does not stay at the top level."""
        hits = []
        if isinstance(obj, dict):
            for key, value in obj.items():
                here = f"{path}.{key}" if path else str(key)
                if PII_KEY_PATTERN.search(str(key)):
                    hits.append(here)
                else:
                    hits.extend(scan_pii(value, here))
        elif isinstance(obj, list):
            for idx, value in enumerate(obj):
                hits.extend(scan_pii(value, f"{path}[{idx}]"))
        elif isinstance(obj, str) and EMAIL_VALUE_PATTERN.search(obj):
            hits.append(f"{path} (value looks like an email)")
        return hits

    pii = []
    for _, ev, _ in stream:
        hits = scan_pii(ev.get("properties") or {})
        if hits:
            pii.append((ev.get("event_id"), ev.get("tenant_id"), hits, ev))
    if pii:
        add(
            {
                "cls": "pii_in_properties",
                "severity": "critical",
                "ids": [i for i, _, _, _ in pii],
                "lines": lines([e for _, _, _, e in pii]),
                "detail": "; ".join(f"{i} [{t}]: {h}" for i, t, h, _ in pii),
                "disposition": "tokenize or hash at the edge; keep raw PII out of "
                               "the analytics store and out of warehouse exports",
                "why": "A customer put direct identifiers in an arbitrary "
                       "properties blob, which the SDK lets them do. Once PII is "
                       "in a free-form field it is in the data lake, the Parquet "
                       "files and every warehouse export, and an Article 17 "
                       "deletion can no longer be satisfied by deleting rows keyed "
                       "on user_id. Key-name matching finds this one; it will not "
                       "find PII in a field called `notes`, which is why this "
                       "needs a schema contract, not only a detector.",
            }
        )

    # --- Class 8: automated traffic ----------------------------------------
    bot_ref, bursts, bot_lines = [], [], set()
    for _, ev, _ in stream:
        ref = (ev.get("properties") or {}).get("referrer") or ""
        if isinstance(ref, str) and BOT_REFERRER_PATTERN.search(ref):
            bot_ref.append((ev.get("event_id"), ref))
            bot_lines.add(line_of[id(ev)])
    seq = defaultdict(list)
    for _, ev, _ in stream:
        # Order on server arrival, not client ts. A bot with a skewed or
        # spoofed clock would otherwise scatter across the timeline and never
        # register as a burst -- and this detector argues elsewhere that client
        # ts cannot be trusted for ordering, so it must not rely on it here.
        if ev.get("type") not in BURST_TYPES:
            continue
        when = parse_ts(ev.get("received_at")) or parse_ts(ev.get("ts"))
        if when and ev.get("anonymous_id"):
            seq[(ev.get("tenant_id"), ev["anonymous_id"])].append(
                (when, line_of[id(ev)], ev.get("event_id")))
    for anon, items in seq.items():
        items.sort()
        # Sliding window: any BURST_MIN_EVENTS consecutive events inside the
        # window, rather than requiring the whole session to be that short. A
        # fixed total span misses a crawler that simply paces itself.
        for start in range(len(items) - BURST_MIN_EVENTS + 1):
            end = start + BURST_MIN_EVENTS - 1
            span = items[end][0] - items[start][0]
            if span <= BURST_WINDOW:
                window = items[start : end + 1]
                bursts.append(
                    (anon[1], [i for _, _, i in window], span.total_seconds())
                )
                bot_lines.update(ln for _, ln, _ in window)
                break
    if bot_ref or bursts:
        ids = sorted({i for i, _ in bot_ref} | {i for _, lst, _ in bursts for i in lst}, key=str)
        detail = []
        if bot_ref:
            detail.append("self-identifying referrer: " + "; ".join(f"{i} <- {r}" for i, r in bot_ref))
        if bursts:
            detail.append(
                "navigation burst: "
                + "; ".join(f"{a} {lst} in {s:.3f}s" for a, lst, s in bursts)
            )
        add(
            {
                "cls": "automated_traffic",
                "severity": "medium",
                "ids": ids,
                "lines": sorted(bot_lines),
                "detail": " | ".join(detail),
                "disposition": "label is_bot=true, exclude from billed and "
                               "customer-facing metrics, retain the raw events",
                "why": "Two independent signals agree here, which is the only "
                       "reason to act: a referrer that says scanner, and four "
                       "navigations in well under a second that no human "
                       "produces. Delete it and we cannot show a customer why "
                       "their traffic number dropped, so this is a label, not a "
                       "filter at ingest.",
            }
        )

    # --- Class 9: client-computed aggregate contradicted by the stream -----
    for _, ev, _ in stream:
        props = ev.get("properties") or {}
        claimed = props.get("count_today")
        if not isinstance(claimed, int):
            continue
        # Derive what the counter is supposed to be counting from the event
        # name rather than assuming /pricing: `viewed_pricing` -> /pricing.
        name = str(props.get("name") or "")
        match = re.fullmatch(r"viewed_(\w+)", name)
        target_path = f"/{match.group(1)}" if match else None
        # Count over the whole person, not one device: the visitor may have
        # browsed anonymously before identifying, and those events are theirs.
        root = component_of(ev)
        basis = (
            f"page_view on {target_path}" if target_path else "page_view (any path)"
        )
        observed = sum(
            1
            for _, other, _ in stream
            if component_of(other) == root
            and other.get("type") == "page_view"
            and (
                target_path is None
                or (other.get("properties") or {}).get("path") == target_path
            )
        )
        if observed == claimed:
            continue  # agrees with the stream; nothing to report
        scope = sorted(members.get(root, set())) or [str(ev.get("anonymous_id"))]
        add(
            {
                "cls": "untrusted_client_aggregate",
                "severity": "high",
                "ids": [ev.get("event_id")],
                "lines": lines([ev]),
                "detail": f"properties.count_today={claimed} for event "
                          f"'{name or 'unnamed'}'; this sample holds {observed} "
                          f"matching {basis} across {scope}",
                "disposition": "never read a client-supplied count; recompute "
                               "server-side from the event stream",
                "why": "This is exactly the segment the brief asks for -- viewed "
                       "pricing 3x -- arriving as a number the browser computed "
                       "and we cannot audit. It disagrees with the events we "
                       "actually hold. The gap may be a real client-side bug or "
                       "just a sample that starts mid-session; either way a "
                       "personalization rule firing on a browser's own arithmetic "
                       "is a rule we cannot defend to the customer it misfires on.",
            }
        )

    # --- Class 10: deletion request arriving as a stream event -------------
    for _, ev, _ in stream:
        if ev.get("type") != "privacy_request":
            continue
        uid, tenant = ev.get("user_id"), ev.get("tenant_id")
        root = component_of(ev)
        # Everything this person produced that a user_id-scoped delete misses:
        # events from any of their devices that predate the identify call.
        orphans = [
            other.get("event_id")
            for _, other, _ in stream
            if component_of(other) == root
            and not other.get("user_id")
            and other.get("event_id") != ev.get("event_id")
        ]
        scope = sorted(members.get(root, set()))
        add(
            {
                "cls": "privacy_request_in_stream",
                "severity": "critical",
                "ids": [ev.get("event_id")],
                "lines": lines([ev]),
                "detail": f"{ev['properties'].get('regulation')} "
                          f"{ev['properties'].get('request')} for {uid} [{tenant}]; "
                          f"identity scope {scope}; events a user_id-scoped "
                          f"delete would miss: {orphans or 'none'}",
                "disposition": "route to the compliance control plane for human "
                               "verification; never execute from a stream consumer",
                "why": "Two separate traps. First, a delete that resolves only "
                       "user_id misses this device's pre-identify events, which "
                       "carry a null user_id and are still that person's data -- "
                       "so deletion has to walk the identity graph, which means "
                       "the anonymous-to-user mapping must itself survive until "
                       "the delete completes. Second, an erasure that an "
                       "unauthenticated SDK can trigger is an abuse vector: a "
                       "forged beacon would wipe a real customer's records.",
            }
        )

    # --- Class 11: one anonymous_id claimed by several tenants --------------
    tenants_of = defaultdict(set)
    for _, ev, _ in stream:
        if ev.get("anonymous_id") and ev.get("tenant_id"):
            tenants_of[ev["anonymous_id"]].add(ev["tenant_id"])
    shared = {a: sorted(t) for a, t in tenants_of.items() if len(t) > 1}
    if shared:
        add(
            {
                "cls": "anonymous_id_across_tenants",
                "severity": "high",
                "ids": sorted(
                    (ev.get("event_id") for _, ev, _ in stream
                     if ev.get("anonymous_id") in shared),
                    key=str,
                ),
                "lines": lines(
                    ev for _, ev, _ in stream if ev.get("anonymous_id") in shared
                ),
                "detail": "; ".join(f"{a} in {t}" for a, t in shared.items()),
                "disposition": "keep identities tenant-scoped; investigate the "
                               "id generator before trusting either tenant's graph",
                "why": "Anonymous ids are minted per site, so the same one under "
                       "two tenants is a collision, a copied cookie or a forged "
                       "beacon. Merging on it would join two customers' visitors.",
            }
        )

    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    findings.sort(key=lambda f: (order.get(f["severity"], 9), f["cls"]))
    return findings, norm


def report(path, findings, norm, parse_errors, total_lines):
    digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
    print("Beat Claude engineer-004 -- fixture anomaly report")
    print(f"file     : {path}")
    print(f"sha256   : {digest}")
    print(f"lines    : {total_lines} ({len(norm)} parsed, {len(parse_errors)} unparseable)")
    print(f"findings : {len(findings)} across {len({f['cls'] for f in findings})} classes")
    print()
    for i, f in enumerate(findings, 1):
        print(f"[{i}] {f['cls']}  ({f['severity']})")
        print(f"    events      : {', '.join(str(x) for x in f['ids'])}")
        print(f"    observed    : {f['detail']}")
        print(f"    disposition : {f['disposition']}")
        print(f"    reasoning   : {f['why']}")
        print()
    # Count by input line, not by event_id: the same id can be a different
    # event in another tenant, and duplicate copies are each their own row.
    flagged_lines = {ln for f in findings for ln in f.get("lines", [])}
    clean = sum(
        1 for lineno, ev, _ in norm
        if lineno not in flagged_lines and ev.get("event_id")
    )
    print(f"Events with no finding: {clean} of {len(norm)} parsed.")
    print(
        f"n={total_lines} is too small to support any rate claim. These are "
        "existence proofs of anomaly classes the pipeline must handle, not "
        "frequencies."
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("path", help="path to event_sample.jsonl")
    ap.add_argument("--json", action="store_true", help="emit findings as JSON")
    args = ap.parse_args()

    if not Path(args.path).is_file():
        print(f"error: no such file: {args.path}", file=sys.stderr)
        return 2

    total_lines = sum(1 for l in open(args.path, encoding="utf-8") if l.strip())
    events, parse_errors = load(args.path)
    findings, norm = detect(events, parse_errors)

    if args.json:
        json.dump(
            {
                "file": args.path,
                "sha256": hashlib.sha256(Path(args.path).read_bytes()).hexdigest(),
                "lines": total_lines,
                "parsed": len(norm),
                "unparseable": len(parse_errors),
                "findings": findings,
            },
            sys.stdout,
            indent=2,
        )
        print()
    else:
        report(args.path, findings, norm, parse_errors, total_lines)

    # Non-zero when anything critical is present, so this can gate a CI step.
    return 1 if any(f["severity"] == "critical" for f in findings) else 0


if __name__ == "__main__":
    sys.exit(main())
