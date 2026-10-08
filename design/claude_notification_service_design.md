# Notification Service — Architecture & Design

**Status:** Proposed (design within two fixed, accepted decisions)
**Author:** Lead Architecture (Claude)
**Audience:** Domain-service teams (nameservice first), the Notification Service team, the external Digital integration team, platform, security/privacy, and operations.

---

## Executive summary

This design introduces a dedicated **Notification Service** as its own microservice and bounded context: domain services stop building and publishing Digital email messages entirely (fixed decision 1). Instead, each domain publishes **template-agnostic business events** in its own ubiquitous language — stating what happened and who is involved — and never references Digital template codes, property names, or the Digital envelope; the Notification Service alone owns the mapping from those events to Digital templates and properties (fixed decision 2). The integration is event-driven over Kafka: domain services use a **transactional outbox** so a state change and its event commit atomically, and the Notification Service uses an **inbox + delivery-intent + audit** pattern so that every event is either durably accepted and auditable or explicitly quarantined before its offset is acknowledged. The service decides *how* to notify (which template, format, channel) but never *whether* a business fact occurred or *whether* a person should be contacted — those remain in the producing domain. Digital's fixed, quirky contract (`languagePerference`, `eMAilAddress`, the `header/feature/profile/contact/notice` envelope, and `noticePropertyList` scalar/`Object` properties) is quarantined inside a single outbound Anti-Corruption Layer, and the event→template→property mapping is **configuration-driven** so that adding a template for an existing event needs no domain-service code change and ideally no Notification Service code change. We assume **at-least-once** delivery on every hop and design idempotency explicitly with a stable delivery key; the one residual risk the design cannot fully remove is a duplicate email after an ambiguous publish to Digital, which is mitigated but depends on Digital supporting a dedup key. Recipient names and email addresses are PII; every hop that moves or stores them states its control (encryption, minimization, access, retention).

---

## Assumptions (placeholders were unfilled in the inputs)

Every `{{...}}` placeholder in the brief arrived empty, and no real sample messages or template inventory were supplied. The following assumptions are used throughout; **items marked (blocking)** change the design materially if wrong and are repeated as open questions at the end.

| Input (placeholder) | Assumption made for this design | Blocking? |
|---|---|---|
| `LIST_OF_DOMAIN_SERVICES` | `nameservice` (migrated first), then `addressservice`, `advisorservice`. | No |
| `TECH_STACK` | Java 21, Spring Boot 3.3, Spring Kafka, Confluent/Apicurio **Schema Registry** with **Avro** (Protobuf acceptable), PostgreSQL 16, Kubernetes, OpenTelemetry. | No |
| `NFRS` — volume | Peak ~100 events/s aggregate at maturity (~5M notices/day); nameservice ~20/s. Sizing is illustrative only. | **Yes** |
| `NFRS` — latency | Target P99 *event-published → accepted-by-Digital-broker* < 5 s, excluding deliberate retry backoff. | **Yes** |
| `NFRS` — retention | Audit ledger retained 7 years (financial-comms record-keeping); encrypted quarantine 30 days; terminal delivery intents purged 90 days. | **Yes** |
| `NFRS` — availability | 99.9% service availability; RPO = 0 for *accepted* events (durable outbox/inbox); RTO < 15 min. | **Yes** |
| `NFRS` — compliance | Regulated financial services: recipient name + email are PII under regional privacy law; audit must be immutable and queryable. | No |
| `SAMPLE_MESSAGES` | **None supplied.** `Sample.txt` in the repo is unrelated and is ignored. Task 9 reviews a *reconstructed* sample built only from the contract description in the brief; it cannot assert defects in messages that were never provided — it lists the defect **classes** the described contract already reveals, plus the questions those raise. | **Yes** |
| `TEMPLATE_TO_EVENT_LIST` | Assumed nameservice inventory to make the design concrete: `NAME_CHANGE_SUBMITTED`, `NAME_CHANGE_COMPLETED`, `NAME_CHANGE_REJECTED`, `NAME_CHANGE_REVIEW_ASSIGNED`. Treated as placeholders pending the real list. | **Yes** |

Where the brief was ambiguous (e.g., multi-valued `objValue` representation, request-ID reuse) the ambiguity is called out at the point it matters and an explicit assumption is stated rather than silently resolved.

---

## Task 1 — The Notification Service bounded context

### Responsibilities (what it owns)

The Notification Service owns the **transformation and delivery of a notification given a valid business event**. Concretely:

- Consuming business events from domain-owned topics and validating them against their registered schema.
- Deciding **which template(s)**, **format**, and **channel** apply to an event, via the configuration-driven catalog (Task 5).
- Mapping event facts to the Digital contract's properties, including nested `Object` properties, constants, and per-environment values.
- Constructing a durable **delivery intent** per (event, rule, recipient, channel), persisting an **audit** record, and dispatching to Digital.
- Owning idempotency, retry, dead-letter/quarantine, replay, and all operational visibility for notifications.
- Owning its own inbound-event tolerance and the outbound Digital contract **adapter and version** (the wire mapping), but not the contract itself.

### Non-responsibilities (what it must never do)

These are stated as hard exclusions because each is a realistic way the service would rot into a second, shadow domain:

- It does **not** decide whether a business event occurred. It trusts the event as the authoritative statement of fact.
- It does **not** decide whether a person should be contacted (consent, eligibility, suppression, frequency capping are business/compliance decisions owned elsewhere). *If a suppression/consent capability is later required, it is modelled as its own policy owner and consulted explicitly — it is not quietly embedded in mapping config.*
- It does **not** select recipients by applying business rules. The producer supplies the intended recipient(s); the service validates their shape, not their eligibility.
- It does **not** own or persist domain state, and it does **not** call back into a domain service to "enrich" a missing fact. Missing facts become an **explicit, audited rejection** — never a hidden synchronous lookup that recreates coupling.
- It does **not** own the Digital contract. Digital owns the destination schema and downstream processing.

### Scope guardrail (the rule that stops scope creep)

> **It decides HOW to notify, never WHETHER.**

Operational litmus test for any proposed change: *does implementing it require knowing a business rule — who is eligible, under what policy, at what moment it is appropriate to contact someone?* If yes, it belongs in the producing domain or a named policy owner, **not** here. The structural enforcement of this guardrail is that **adding a template for an existing event is a catalog/config change only** (Task 5) and **requires no domain-service release**; if a "notification change" ever forces a domain code change, the guardrail has been crossed and should be treated as a design defect.

**Why / cost:** this boundary keeps the Digital contract's blast radius to one service and keeps domains ignorant of Digital. The cost is that the Notification Service must be *strict* — it rejects incomplete events loudly rather than being "helpful", which pushes data-quality work back onto producers (correctly).

---

## Task 2 — The business event contract

### 2a. Canonical event envelope (shared by all domain services)

Events are serialized with a registry schema (Avro assumed). The envelope is identical across domains; only `data` differs. JSON shown for readability:

```json
{
  "eventId":        "0192f0b2-7e3a-7c21-9a7b-0a1b2c3d4e5f",
  "eventType":      "nameservice.name-change.completed",
  "schemaVersion":  1,
  "source":         "nameservice",
  "occurredAt":     "2026-10-09T17:59:00.142Z",
  "publishedAt":    "2026-10-09T17:59:02.004Z",
  "subject": {
    "type":    "NameChange",
    "id":      "nc-48291",
    "version": 7
  },
  "correlationId":  "case-19302",
  "causationId":    "cmd-82013",
  "traceparent":    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
  "locale":         "en-IN",
  "recipients": [
    {
      "recipientId":    "party-771",
      "role":           "customer",
      "contact":        { "kind": "EMAIL", "valueRef": "tok_2b9f...e1", "value": null },
      "locale":         "en-IN"
    }
  ],
  "data": { "...": "domain-specific business facts, see 2b" }
}
```

| Field | Purpose and rationale | Cost / caveat |
|---|---|---|
| `eventId` | Globally unique, stable **idempotency/dedup identity**. UUIDv7 so it is time-ordered for index locality. Same value is kept across producer/relay retries. | Consumers must dedup on it; it must never be reused for a different event. |
| `eventType` | Fully-qualified domain meaning (`<context>.<aggregate>.<fact>`), independent of any template. Drives catalog routing. | Renaming is a breaking change. |
| `schemaVersion` | Schema identity for the `data` payload, decoupled from `eventType`. | Must be paired with registry compatibility rules (2c). |
| `source` | Producing service = schema namespace owner, ACL subject, support attribution. | — |
| `occurredAt` / `publishedAt` | Business time vs broker-publication time; both are needed to diagnose latency and to order audit without conflating the two. | Clocks must be NTP-synced for the gap to be meaningful. |
| `subject` `{type,id,version}` | Aggregate identity + **monotonic version** → partition ordering and stale/gap detection (Task 3). | `version` must be monotonic per aggregate or ordering diagnostics are useless. |
| `correlationId` | Ties all events/commands in one business interaction for tracing and audit. | Does **not** replace `eventId` for dedup. |
| `causationId` | The command/event that caused this one — causality chain. | Optional; present where known. |
| `traceparent` | W3C trace context so a notification trace stitches to the originating request. | Must carry no PII. |
| `locale` | Default rendering locale; a recipient's own `locale` overrides it. | — |
| `recipients[]` | **Producer-supplied** intended recipients: stable `recipientId`, business `role`, a `contact` (tokenized by default), and optional per-recipient `locale`. This is how audience and contact details are supplied. | PII — see 2b and PII controls. The service validates presence/shape, **not** eligibility. |
| `data` | Immutable business facts (2b). | Must be self-sufficient for rendering without callbacks. |

**Idempotency** is carried by `eventId` end to end. **Ordering** is carried by `subject` + partition key (Task 3). **Audience/recipients** are carried by `recipients[]`. **Locale** is two-tier (event default + per-recipient override). This keeps the Notification Service free of any recipient-selection logic.

### 2b. Rules for `data` (so new templates rarely need new fields)

The design intent is that most new templates for an existing event are satisfiable from facts already present, so a template addition is config-only.

1. **Carry facts, not just identifiers.** `NameChangeCompleted` carries `previousName`, `newName`, and `completedAt` — not merely `nameChangeId`. A new "confirmation with before/after" template then needs no schema change.
2. **Carry before/after, effective time, and status** for any state transition, since these are the fields templates most often add later.
3. **Carry display-safe values** the producer already knows (e.g., a formatted case reference, an approved deep-link), so the Notification Service never reconstructs domain-specific presentation.
4. **Minimize.** Do not embed an entire customer profile. Every PII field must have a stated purpose, classification, and retention. Facts that no plausible template needs are omitted.
5. **Recipients and contact details** travel in `recipients[]`, not in `data`. Each recipient carries a `contact` that is **tokenized by default** (`valueRef` to a token the Notification Service or a contact service can resolve under policy); a raw `value` is allowed only where the producer is the authoritative holder and policy permits, and that choice is a documented control.
6. **No Digital vocabulary.** `data` never contains template codes, Digital property names, the Digital envelope, or vendor formatting instructions.
7. **No hidden lookups.** If a required fact is absent, the event is rejected and audited; the service never silently calls the domain to fill the gap.

**Why / cost:** fact-rich events are the mechanism that keeps domains out of the notification-change loop. The cost is larger events and more PII in transit, which is why minimization and tokenization are mandatory, not optional.

### 2c. Naming, ownership, registration, validation

- **Naming:** `<context>.<aggregate>.<past-tense-fact>`, lower-kebab per segment, e.g. `nameservice.name-change.completed`. Past tense because an event is a completed fact. Names describe the **business fact**, never a notification action (never `send-name-change-email`).
- **Ownership:** the **producing domain team owns and versions** its event schemas (`data` shape and meaning). A lightweight **event-governance guild** reviews the shared *envelope*, PII classification, naming collisions, and compatibility — it does not take ownership of domain meaning.
- **Registration:** each `eventType` is a subject in the Schema Registry under the owning service's namespace. **Backward-transitive** compatibility is the baseline. Optional fields may be added; existing field meanings must never be repurposed. A meaning-breaking change requires a new `eventType` or a major `schemaVersion` and a migration plan.
- **Validation:** producers validate against the registry **in CI** (contract tests) and at publish; the Notification Service validates **at ingress** and tolerates unknown *optional* fields but fails safely on unknown *types* or incompatible *required* values. The catalog's source paths are checked against registered schemas in CI and at startup (Task 5), so a mapping can never reference a field the schema does not define.

### 2d. Example events (one per assumed template)

```json
// NAME_CHANGE_SUBMITTED  →  nameservice.name-change.submitted  (customer acknowledgement)
{
  "eventId": "0192f0b2-0001-7000-8000-000000000001",
  "eventType": "nameservice.name-change.submitted",
  "schemaVersion": 1, "source": "nameservice",
  "occurredAt": "2026-10-09T09:00:00Z", "publishedAt": "2026-10-09T09:00:01Z",
  "subject": { "type": "NameChange", "id": "nc-48291", "version": 1 },
  "correlationId": "case-19302", "locale": "en-IN",
  "recipients": [{ "recipientId": "party-771", "role": "customer",
                   "contact": { "kind": "EMAIL", "valueRef": "tok_2b9f", "value": null }, "locale": "en-IN" }],
  "data": {
    "nameChangeId": "nc-48291",
    "submittedAt": "2026-10-09T08:59:30Z",
    "currentName": { "given": "Asha", "family": "Rao" },
    "requestedName": { "given": "Asha", "family": "Iyer" },
    "caseReference": "NC-2026-48291"
  }
}
```

```json
// NAME_CHANGE_COMPLETED  →  nameservice.name-change.completed  (customer confirmation)
{
  "eventId": "0192f0b2-0003-7000-8000-000000000003",
  "eventType": "nameservice.name-change.completed",
  "schemaVersion": 1, "source": "nameservice",
  "occurredAt": "2026-10-09T18:19:00Z", "publishedAt": "2026-10-09T18:19:01Z",
  "subject": { "type": "NameChange", "id": "nc-48291", "version": 7 },
  "correlationId": "case-19302", "locale": "en-IN",
  "recipients": [{ "recipientId": "party-771", "role": "customer",
                   "contact": { "kind": "EMAIL", "valueRef": "tok_2b9f", "value": null }, "locale": "en-IN" }],
  "data": {
    "nameChangeId": "nc-48291",
    "completedAt": "2026-10-09T18:18:30Z",
    "previousName": { "given": "Asha", "family": "Rao" },
    "newName": { "given": "Asha", "family": "Iyer" },
    "caseReference": "NC-2026-48291"
  }
}
```

```json
// NAME_CHANGE_REJECTED  →  nameservice.name-change.rejected  (customer, with reason)
{
  "eventId": "0192f0b2-0002-7000-8000-000000000002",
  "eventType": "nameservice.name-change.rejected",
  "schemaVersion": 1, "source": "nameservice",
  "occurredAt": "2026-10-09T14:05:00Z", "publishedAt": "2026-10-09T14:05:01Z",
  "subject": { "type": "NameChange", "id": "nc-48293", "version": 4 },
  "correlationId": "case-19304", "locale": "en-IN",
  "recipients": [{ "recipientId": "party-802", "role": "customer",
                   "contact": { "kind": "EMAIL", "valueRef": "tok_9aa1", "value": null }, "locale": "en-IN" }],
  "data": {
    "nameChangeId": "nc-48293",
    "rejectedAt": "2026-10-09T14:04:30Z",
    "requestedName": { "given": "John", "family": "O'Brien" },
    "rejectionReasonCode": "DOCUMENT_MISMATCH",
    "rejectionReasonText": "Submitted proof did not match the requested legal name.",
    "caseReference": "NC-2026-48293"
  }
}
```

```json
// NAME_CHANGE_REVIEW_ASSIGNED  →  nameservice.name-change.review-assigned  (internal reviewer)
{
  "eventId": "0192f0b2-0004-7000-8000-000000000004",
  "eventType": "nameservice.name-change.review-assigned",
  "schemaVersion": 1, "source": "nameservice",
  "occurredAt": "2026-10-09T10:15:00Z", "publishedAt": "2026-10-09T10:15:01Z",
  "subject": { "type": "NameChange", "id": "nc-48291", "version": 3 },
  "correlationId": "case-19302", "locale": "en-IN",
  "recipients": [{ "recipientId": "employee-52", "role": "assignedReviewer",
                   "contact": { "kind": "EMAIL", "valueRef": "tok_emp52", "value": null }, "locale": "en-IN" }],
  "data": {
    "nameChangeId": "nc-48291",
    "taskId": "task-7702",
    "assignedAt": "2026-10-09T10:14:30Z",
    "assigneeDisplayName": "Ravi Shah",
    "customerDisplayName": "Asha Rao",
    "caseReference": "NC-2026-48291",
    "reviewLink": "https://approved.internal.example/cases/nc-48291"
  }
}
```

Note the deliberate absence of any `noticeType`, `languagePerference`, or `noticePropertyList` — those are Digital's vocabulary and appear only in the outbound adapter.

---

## Task 3 — High-level architecture

### ASCII architecture diagram

```
   DOMAIN SERVICES (producers)                     NOTIFICATION SERVICE                         EXTERNAL
 ┌───────────────────────────┐              ┌──────────────────────────────────────┐       ┌──────────────┐
 │ nameservice               │              │  Inbound Kafka adapter                │       │  Digital team│
 │  ┌─────────────────────┐  │  business    │   decode + schema-validate            │       │              │
 │  │ domain txn:         │  │  event        │            │                          │       │  Digital     │
 │  │ state + OUTBOX row  │  │  topics       │            v                          │       │  Kafka topic │
 │  │ (one local commit)  │──┼──►(per ctx)──►│  Application core (framework-free)    │       │  (fixed      │
 │  └─────────┬───────────┘  │              │   - validate facts/recipients         │       │   contract)  │
 │            │ relay/CDC    │              │   - plan intents via CATALOG          │       │              │
 │            ▼              │              │            │                          │       └──────▲───────┘
 │     publish (keep eventId)│              │            v                          │              │
 └───────────────────────────┘              │  Postgres: INBOX (source,eventId)     │              │
                                            │           + AUDIT ledger              │              │
 ┌───────────────────────────┐              │           + DELIVERY-INTENT outbox    │              │
 │ addressservice (later)    │──►(topics)──►│            │                          │              │
 │ advisorservice (later)    │              │            v                          │              │
 └───────────────────────────┘              │  Dispatcher (reads due intents)       │              │
                                            │            │                          │              │
                                            │            v                          │              │
                                            │  Digital OUTBOUND ADAPTER (ACL)  ──────┼──────────────┘
                                            │   DTOs w/ languagePerference,         │  publish w/ delivery key
                                            │   eMAilAddress, noticePropertyList    │
                                            └───────────────┬──────────────────────┘
                                                            │ invalid input / permanent failure
                                                            ▼
                                            ┌──────────────────────────────────────┐
                                            │ QUARANTINE (restricted, encrypted)    │
                                            │ + alerting + replay tooling           │
                                            └──────────────────────────────────────┘
```

### Topic layout and ownership

- **One topic per bounded context (or stable event family), not one per event type.** `nameservice.events`, `addressservice.events`, `advisorservice.events`. This keeps per-aggregate ordering simple and avoids a topic explosion; the trade-off is that consumers filter by `eventType`, which is cheap. Split a context's topic only when access boundaries or wildly different retention/volume demand it.
- **Ownership:** each domain owns its topic, schema subjects, producer ACLs, retention classification, and event contract. The Notification Service owns its **consumer groups**, retry/quarantine topics, and delivery-intent store. **Digital owns the outbound topic** and its schema.

### Partition key, ordering, and its limits

- **Partition key = `source | subject.type | subject.id`.** This gives **ordering within a partition for one aggregate**, provided the producer publishes that aggregate's records in order.
- **Limits (stated plainly):** there is **no** global ordering, **no** cross-topic ordering, and ordering is **not** preserved across a partition-count change (rehashing moves keys). `subject.version` is included so the consumer can **detect** staleness/gaps; it does not reorder. Where strict per-aggregate notification ordering is genuinely required, later versions are held until the gap resolves or routed to an operational exception path — this is opt-in because it adds latency and complexity.

### Transactional outbox (in each domain service)

Each domain commits its **business state change and an outbox row in one local transaction**. A relay (polling publisher or Debezium CDC on the outbox table) publishes the row to the domain topic and marks it published after broker ack. If the relay crashes after publish but before marking, it **republishes with the same `eventId`** — which is exactly why consumers dedup on `eventId`. This removes the dual-write failure (state committed, event lost). **Cost:** outbox table, a relay to run and monitor, and relay lag as a new SLI.

### No lost and no unaudited notices (the inbox + intent + audit pattern)

For each inbound record the Notification Service, in one Postgres transaction:

1. Inserts the inbox identity `(source, eventId)` with a **unique constraint** (dedup).
2. Validates schema + facts + recipient shape, and resolves the catalog rule(s).
3. Writes the **audit** outcome and the durable **delivery intents** (one per rule×recipient×channel).
4. Commits the DB transaction.
5. **Only then** acknowledges the Kafka offset.

If the record is redelivered after commit, the unique inbox row makes the second attempt a no-op. If validation/mapping fails, the failure is persisted and quarantined **before** the offset is acknowledged. If persistence fails, the offset is **not** acknowledged and the record is redelivered. This is how "no lost notices" (durable before ack) and "no unaudited notices" (audit committed in the same transaction as acceptance) are both guaranteed.

### Idempotency and the delivery key

A **stable delivery key** makes retries and redeliveries safe end to end:

```
deliveryKey = SHA-256( source | eventId | recipientId | ruleId | channel )
```

It distinguishes multiple templates/recipients/channels for one event and is sent to Digital as a dedup key **if the contract supports one** (an open question for Digital — see Task 9). The catalog version used to plan an intent is **pinned** to that intent, so a retry never silently re-renders with a newer catalog.

### Retry, dead-letter, and error classification

| Class | Examples | Handling |
|---|---|---|
| **Retryable — transient** | Broker unavailable, timeout, throttling, DB failover | Bounded exponential backoff + jitter; preserve intent and delivery key; alert on attempt count/age. |
| **Non-retryable — bad event** | Unknown `eventType`/version, malformed event, missing required fact, invalid recipient shape | Persist safe failure code; quarantine; do **not** retry unchanged data. Fix is a producer/schema correction. |
| **Non-retryable — bad config** | Missing mapping, incompatible path type, unresolved env value, unknown formatter | **Fail startup/CI** for static defects. Unknown runtime event types are audited + quarantined + alerted. |
| **Non-retryable — Digital rejection** | Digital rejects payload as invalid | Stop blind retries; keep safe response metadata; fix mapping/contract; replay with explicit audit. |
| **Ambiguous** | Timeout after publish may have reached Digital | Retry with **same delivery key** only where Digital dedups; otherwise expose as a reconcilable duplicate-risk item. Never silently re-send without the key. |

Undecodable input is written to quarantine with at least `topic/partition/offset`, a payload **hash**, the decoder/schema failure code, and timestamp **before** the offset is acknowledged. Raw payloads and exception text never go to ordinary logs.

### Audit trail and replay

The **audit ledger** records: event identity, source, schema version, subject identity+version, correlation ID, catalog version, rule ID, recipient reference, delivery key, state transitions, attempt timestamps, outcome, and a **safe** failure code. It avoids duplicating raw PII; where a raw/rendered payload must be kept for replay it is encrypted, access-restricted, and expiry-bound.

**Replay** is an audited operator action carrying identity, reason, original-delivery link, catalog-version choice, and a dry-run option. It passes through normal validation and idempotency — replaying an event with its original identity does **not** authorize a second email; a deliberate resend is a new attempt linked to the original.

### Observability

Propagate `eventId`, `correlationId`, `causationId`, W3C trace context, Kafka coordinates, and `deliveryKey` through structured logs and OpenTelemetry traces. **Never** attach names, addresses, or raw payloads to logs, traces, or metric labels; use low-cardinality labels (`source`, `eventType`, `ruleId`, `failureClass`, delivery `state`). Minimum signals: consumer lag and oldest-unprocessed age; oldest pending intent age; counts/rates by delivery state and failure class; retry attempt/age distribution; catalog/config failures and quarantine growth; Digital publish failures and ambiguous outcomes; event→broker-accept latency; audit DB health. **Alert thresholds depend on the (missing) NFRs** and cannot be set responsibly until volume/latency targets are confirmed.

### PII handling across the lifecycle

| Lifecycle point | Control |
|---|---|
| Domain outbox + event topic | Minimize facts/recipients; tokenize contact by default; service-identity ACLs; TLS; encryption at rest; classified retention. |
| Consumer + logs/traces | No raw payload/address logging; safe failure codes; trace context carries no PII. |
| Notification DB | Encrypt storage + backups; minimize recipient snapshot; restrict + audit read access; retention/deletion policy; separate operational roles. |
| Quarantine + replay | Restricted access; encrypt raw payload only when replay justifies it; reasoned/audited replay; explicit expiry. |
| Digital outbound | TLS + approved producer identity; no payload copies in generic logs; access/contract controls co-owned with Digital. |

### Schema evolution (inbound and outbound)

- **Inbound:** registry subjects per `eventType`, backward-transitive compatibility baseline, additive optional fields, no meaning changes; breaking changes get a new type/major version + migration. Tolerant reader on the consumer.
- **Outbound (Digital):** the wire contract is pinned and versioned **inside the outbound adapter**, with approved fixtures and contract tests. Breaking Digital changes are coordinated with Digital and rolled via an adapter/catalog version; catalog and contract versions are recorded in audit. Digital's names must never leak back into event schemas or domain code.

---

## Task 4 — Low-level structure (Hexagonal / Ports & Adapters)

### Module and package layout

The hexagon is enforced by **separate build modules**, not just packages — the compiler then makes an illegal dependency impossible, which is stronger than naming convention.

```text
notification-service/                      (Gradle/Maven multi-module)
├── notification-domain/                   depends on: JDK only
│   └── com.example.notification.domain
│       ├── event/        BusinessEvent, EventId, EventType, Subject, Recipient, Contact, Locale
│       ├── delivery/     NotificationIntent, DeliveryKey, DeliveryState, Channel, Attempt
│       └── shared/       DomainException, NonRetryableEventException
├── notification-application/              depends on: domain (+ JDK)
│   └── com.example.notification.application
│       ├── port.in/      HandleBusinessEvent, DispatchDueDeliveries, ReplayDelivery
│       ├── port.out/     EventInbox, IntentStore, AuditLog, TemplateCatalog,
│       │                 NotificationGateway, Clock, IdGenerator
│       └── service/      HandleBusinessEventService, DispatchService, ReplayService
├── notification-adapter-kafka-in/         depends on: application (+ Spring Kafka)
│   └── ...adapter.in.kafka/   BusinessEventListener, EventDecoder
├── notification-adapter-persistence/      depends on: application (+ Spring Data/JPA)
│   └── ...adapter.out.persistence/        InboxJpaAdapter, IntentJpaAdapter, AuditJpaAdapter
├── notification-adapter-catalog/          depends on: application (+ YAML binding)
│   └── ...adapter.out.catalog/            YamlTemplateCatalog, CatalogValidator, mapping strategies
├── notification-adapter-digital/          depends on: application (+ Spring Kafka) — THE ACL
│   └── ...adapter.out.digital/            DigitalGatewayAdapter, DigitalPayloadMapper, Digital*Dto
└── notification-boot/                     depends on: all adapters — Spring wiring, config
    └── ...bootstrap/                      SpringBootApplication, @Configuration beans
```

### Domain model (aggregates, value objects, invariants)

- **Aggregate `NotificationIntent`** — the unit of durable work and the consistency boundary: identified by `DeliveryKey`; holds the pinned `ruleId` + `catalogVersion`, the `recipientId`, `channel`, `DeliveryState`, and attempt history. Invariant: state transitions are restricted (`PLANNED → RETRY_PENDING → PLANNED`, terminal `ACCEPTED_BY_BROKER` / `PERMANENT_FAILURE`, explicit `AMBIGUOUS`); an intent cannot leave `PLANNED` without an `Attempt` recorded.
- **`BusinessEvent`** (entity-like, immutable) — the validated inbound fact: `EventId`, `EventType`, `schemaVersion`, `Subject`, `occurredAt`, `recipients`, and immutable `data`. Invariant: required envelope fields present; `data`/`recipients` are defensively copied and unmodifiable.
- **Value objects:** `EventId(source, value)` (both non-blank, stable across redelivery), `DeliveryKey` (non-blank; the SHA-256 identity), `Recipient(recipientId, role, Contact, locale)` (identity + role + contact presence validated; eligibility **not** judged), `Contact(kind, valueRef, value)` (at least one of ref/value present), `EventType`, `Subject(type,id,version)`.
- **Invariants live in constructors** (records with compact canonical constructors) so an invalid object cannot exist — the application layer never has to defensively re-check.

### Ports

- **Inbound (driving):** `HandleBusinessEvent`, `DispatchDueDeliveries`, `ReplayDelivery`.
- **Outbound (driven):** `EventInbox` (dedup + persist), `IntentStore` (persist/query intents), `AuditLog`, `TemplateCatalog` (plan intents from an event), `NotificationGateway` (send to Digital), plus `Clock` and `IdGenerator` injected for deterministic tests.

### Adapters and where the ACL lives

- **Inbound Kafka adapter** decodes + schema-validates records and translates Kafka metadata into domain types, then calls `HandleBusinessEvent`. It owns Spring Kafka and acknowledgement timing.
- **Persistence adapter** implements `EventInbox`, `IntentStore`, `AuditLog` on Postgres/JPA.
- **Catalog adapter** implements `TemplateCatalog` from YAML (Task 5) — it knows event field paths and target property names but still produces a *neutral* rendered result keyed by logical property names.
- **Digital outbound adapter is the Anti-Corruption Layer.** It, and only it, knows Digital's envelope, the misspellings (`languagePerference`, `eMAilAddress`), `noticeType`, `noticePropertyList`, the scalar/`Object` split, and the Digital Kafka producer. The DTOs, the mapper, and the wire serialization live here. The rest of the system speaks the ubiquitous language; this adapter is the single translation membrane.

### Enforcing dependency direction (build modules + ArchUnit)

Separate modules give compile-time enforcement: `notification-domain` has no Spring/Kafka/JPA/Jackson on its classpath at all, so a framework import does not compile. ArchUnit tests then guard the finer rules that modules alone can't:

```java
@AnalyzeClasses(packages = "com.example.notification")
class ArchitectureRules {

  @ArchTest static final ArchRule domain_is_pure =
      noClasses().that().resideInAPackage("..domain..")
        .should().dependOnClassesThat().resideInAnyPackage(
            "org.springframework..", "org.apache.kafka..",
            "jakarta.persistence..", "com.fasterxml.jackson..");

  @ArchTest static final ArchRule application_has_no_frameworks =
      noClasses().that().resideInAPackage("..application..")
        .should().dependOnClassesThat().resideInAnyPackage(
            "org.springframework..", "org.apache.kafka..", "jakarta.persistence..");

  @ArchTest static final ArchRule digital_names_are_contained =
      noClasses().that().resideOutsideOfPackage("..adapter.out.digital..")
        .should().dependOnClassesThat().haveSimpleNameContaining("Digital");

  @ArchTest static final ArchRule hexagon_points_inward =
      layeredArchitecture().consideringOnlyDependenciesInLayers()
        .layer("domain").definedBy("..domain..")
        .layer("application").definedBy("..application..")
        .layer("adapters").definedBy("..adapter..")
        .whereLayer("adapters").mayNotBeAccessedByAnyLayer()
        .whereLayer("application").mayOnlyBeAccessedByLayers("adapters");
}
```

**Why / cost:** multi-module + ArchUnit prevents the classic rot where a Kafka or Digital DTO leaks into the core and an external contract change becomes a domain change. The cost is more build configuration and a slower first build.

---

## Task 5 — Configuration-driven template catalog

The catalog is **data, not code**, owned by the Notification Service and loaded by the catalog adapter. It maps one `eventType` to one or more templates, and each template's logical properties to event field paths / constants / per-environment values, with formatting and required-field rules, including nested `Object` properties.

```yaml
# notification-catalog.yaml   (validated at CI and at startup; version pinned per intent)
catalogVersion: "2026-10-09.1"

environment:
  clientApplication: ${DIGITAL_CLIENT_APP_ID}     # resolved from env/secret manager, never inlined
  featureName: "CustomerComms"

formatters:            # allowlisted, non-executable — no expression language
  - localDate
  - personNameObject
  - approvedHttpsUrl
  - upperCase

rules:

  - id: name-change-submitted-v1
    eventType: nameservice.name-change.submitted
    templates:
      - template: NAME_CHANGE_SUBMITTED         # Digital noticeType (lives only here)
        audienceRole: customer                  # which recipient role this template targets
        properties:
          customerName:   { from: data.currentName, required: true, format: personNameObject }
          requestedName:  { from: data.requestedName, required: true, format: personNameObject }
          caseReference:  { from: data.caseReference, required: true }
          submittedDate:  { from: data.submittedAt, required: true, format: localDate }

  - id: name-change-completed-v1
    eventType: nameservice.name-change.completed
    templates:
      - template: NAME_CHANGE_COMPLETED
        audienceRole: customer
        properties:
          previousName:  { from: data.previousName, required: true, format: personNameObject }  # nested Object
          newName:       { from: data.newName, required: true, format: personNameObject }        # nested Object
          completedDate: { from: data.completedAt, required: true, format: localDate }
          caseReference: { from: data.caseReference, required: true }
          helpLine:      { constant: "Contact us on 1800-000-000", required: true }

  - id: name-change-rejected-v1
    eventType: nameservice.name-change.rejected
    templates:
      - template: NAME_CHANGE_REJECTED
        audienceRole: customer
        properties:
          requestedName: { from: data.requestedName, required: true, format: personNameObject }
          reasonText:    { from: data.rejectionReasonText, required: true }
          caseReference: { from: data.caseReference, required: true }

  - id: name-change-review-assigned-v1
    eventType: nameservice.name-change.review-assigned
    templates:
      - template: NAME_CHANGE_REVIEW_ASSIGNED
        audienceRole: assignedReviewer
        properties:
          reviewerName: { from: data.assigneeDisplayName, required: true }
          customerName: { from: data.customerDisplayName, required: true }
          taskId:       { from: data.taskId, required: true }
          reviewLink:   { from: data.reviewLink, required: true, format: approvedHttpsUrl }
```

**Nested `Object` properties** (`previousName`, `newName`) are produced as structured maps by the `personNameObject` formatter; the Digital adapter decides they become `datatype: Object` with `objValue` (Task 7). The catalog never mentions `objValue` — that is Digital vocabulary.

**One event → many templates / recipients:** a rule may list several `templates`, and each `template` targets an `audienceRole`; the planner produces one intent per matching recipient × template, each with its own delivery key. This is how "add a template for an existing event" stays config-only.

### Startup and CI validation rules (fail-fast)

Startup and CI both reject the catalog if any holds:

- Invalid YAML, missing `catalogVersion`, duplicate `rule.id`, or duplicate logical property within a template.
- An `eventType` or a `from:` path that is **absent from the registered schema** for that event.
- A source type **incompatible** with its declared `format` (e.g., `localDate` on a non-temporal field) or with the target nested shape.
- A `required: true` property with neither `from` nor `constant`, or one that resolves to null/blank under its policy, or one mapped more than once.
- A `format` not in the `formatters` allowlist.
- An unresolved `${...}` environment placeholder, or a secret inlined in the file.
- An enabled `eventType` with no rule and no explicit ignore policy.

**Why / cost:** fail-fast turns "wrong template config" from a production mis-send into a deployment that never starts. The cost is that the catalog loader must hold a copy of the registered schemas to check paths — handled by a CI step and a startup registry fetch.

---

## Task 6 — SOLID mapped to concrete classes (and the specific mistake each prevents here)

| Principle | Where it lives in this design | The specific mistake it prevents in *this* system |
|---|---|---|
| **Single Responsibility** | `BusinessEventListener` only decodes/acks; `HandleBusinessEventService` only orchestrates accept+plan; `YamlTemplateCatalog` only resolves rules; `DigitalPayloadMapper` only builds the wire payload; `DispatchService` only drives retries. | A single Kafka listener that decodes, maps templates, writes audit, *and* publishes the Digital payload — so a Digital field rename forces a change in the consumer and risks the acknowledgement/audit logic. |
| **Open/Closed** | New templates/mappings are added by editing `notification-catalog.yaml`; mapping behavior extends via `Mapping`/`Formatter` strategy types. | Every new template editing a Java `switch(eventType)` in the core — and worse, forcing a domain-service release to add an email variant. |
| **Liskov Substitution** | `NotificationGateway` contract: `deliver()` returns only after *durable broker acceptance* or throws a classified failure; the in-memory test fake and the Digital adapter obey the identical contract. | A test fake (or a future second channel) reporting "sent" before durable acceptance, so retries/idempotency pass in tests but drop or duplicate messages in production. |
| **Interface Segregation** | Separate `EventInbox`, `IntentStore`, `AuditLog`, `TemplateCatalog`, `NotificationGateway` ports instead of one `NotificationRepository`. | A god-gateway that couples mapping code to SQL *and* Kafka, so every unit test mocks a kitchen-sink interface and the mapper can't be tested without a DB. |
| **Dependency Inversion** | `application` depends on `port.out` interfaces; adapters implement them; `notification-boot` wires concretions. | Spring/JPA/Kafka and Digital DTOs creeping into the core, which makes the external Digital contract a *domain* concern and defeats fixed decision 2. |

---

## Task 7 — Java code skeletons

> Domain and application modules below use **JDK types only** — no Spring, Kafka, JPA, or Jackson imports — which is what the zero-framework-core constraint means in practice.

### Domain model

```java
package com.example.notification.domain.event;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record EventId(String source, String value) {
    public EventId {
        if (isBlank(source) || isBlank(value))
            throw new IllegalArgumentException("source and eventId are required");
    }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}

public record Subject(String type, String id, long version) {
    public Subject {
        if (type == null || type.isBlank() || id == null || id.isBlank())
            throw new IllegalArgumentException("subject type and id are required");
    }
}

public record Contact(String kind, String valueRef, String value) {
    public Contact {
        if (kind == null || kind.isBlank())
            throw new IllegalArgumentException("contact kind is required");
        if ((valueRef == null || valueRef.isBlank()) && (value == null || value.isBlank()))
            throw new IllegalArgumentException("contact needs a token ref or a value");
    }
}

public record Recipient(String recipientId, String role, Contact contact, String locale) {
    public Recipient {
        if (recipientId == null || recipientId.isBlank()
                || role == null || role.isBlank())
            throw new IllegalArgumentException("recipient id and role are required");
        Objects.requireNonNull(contact, "contact is required");
        // NOTE: eligibility/consent is deliberately NOT judged here.
    }
}

public record BusinessEvent(
        EventId id, String eventType, int schemaVersion, Instant occurredAt,
        Subject subject, String correlationId,
        List<Recipient> recipients, Map<String, Object> data) {
    public BusinessEvent {
        Objects.requireNonNull(id); Objects.requireNonNull(occurredAt); Objects.requireNonNull(subject);
        if (eventType == null || eventType.isBlank() || schemaVersion < 1)
            throw new IllegalArgumentException("eventType and positive schemaVersion are required");
        if (recipients == null || recipients.isEmpty())
            throw new IllegalArgumentException("at least one recipient is required");
        recipients = List.copyOf(recipients);
        data = Map.copyOf(data);
    }
}
```

```java
package com.example.notification.domain.delivery;

import com.example.notification.domain.event.EventId;
import java.util.Objects;

public record DeliveryKey(String value) {
    public DeliveryKey { if (value == null || value.isBlank())
        throw new IllegalArgumentException("delivery key is required"); }
}

public enum DeliveryState { PLANNED, RETRY_PENDING, ACCEPTED_BY_BROKER, PERMANENT_FAILURE, AMBIGUOUS }

public record NotificationIntent(
        DeliveryKey key, EventId eventId, String ruleId, String templateId,
        String catalogVersion, String recipientId, String channel, DeliveryState state) {
    public NotificationIntent {
        Objects.requireNonNull(key); Objects.requireNonNull(eventId); Objects.requireNonNull(state);
        if (ruleId == null || ruleId.isBlank() || templateId == null || templateId.isBlank()
                || catalogVersion == null || catalogVersion.isBlank())
            throw new IllegalArgumentException("rule, template and catalog version are required");
    }
    public NotificationIntent withState(DeliveryState next) {
        return new NotificationIntent(key, eventId, ruleId, templateId, catalogVersion, recipientId, channel, next);
    }
}
```

### Ports

```java
package com.example.notification.application.port.in;

import com.example.notification.domain.event.BusinessEvent;

public interface HandleBusinessEvent {
    void handle(BusinessEvent event, InboundPosition position);
    record InboundPosition(String topic, int partition, long offset) {}
}
```

```java
package com.example.notification.application.port.out;

import com.example.notification.domain.event.BusinessEvent;
import com.example.notification.domain.delivery.NotificationIntent;
import java.util.List;

public interface TemplateCatalog {
    /** Pure planning: event facts -> rendered intents + logical properties. No I/O, no Digital vocabulary. */
    List<PlannedNotification> plan(BusinessEvent event);
    String catalogVersion();
}

public interface EventInbox {
    /** Atomic: dedup on (source,eventId), persist audit + intents, in ONE transaction. Idempotent on redelivery. */
    void acceptOnceAndStore(BusinessEvent event, List<NotificationIntent> intents);
}

public interface AuditLog { void recordRejected(BusinessEvent event, String safeFailureCode); }

public interface NotificationGateway {
    /** Returns only after durable broker acceptance; throws a classified exception otherwise. */
    void deliver(RenderedNotification rendered, DeliveryMetadata metadata);
}
```

### Application service

```java
package com.example.notification.application.service;

import com.example.notification.application.port.in.HandleBusinessEvent;
import com.example.notification.application.port.out.*;
import com.example.notification.domain.event.BusinessEvent;
import com.example.notification.domain.shared.NonRetryableEventException;

public final class HandleBusinessEventService implements HandleBusinessEvent {
    private final TemplateCatalog catalog;
    private final EventInbox inbox;
    private final AuditLog audit;

    public HandleBusinessEventService(TemplateCatalog catalog, EventInbox inbox, AuditLog audit) {
        this.catalog = catalog; this.inbox = inbox; this.audit = audit;
    }

    @Override
    public void handle(BusinessEvent event, InboundPosition position) {
        try {
            var planned = catalog.plan(event);                 // may throw NonRetryableEventException
            var intents = planned.stream().map(PlannedNotification::toIntent).toList();
            inbox.acceptOnceAndStore(event, intents);          // dedup + audit + intents, one txn
        } catch (NonRetryableEventException bad) {
            audit.recordRejected(event, bad.safeCode());       // quarantine path; safe code carries no PII
        }
        // Transient/persistence failures are NOT caught here: they propagate so the
        // inbound adapter does not acknowledge the Kafka offset, and the record redelivers.
    }
}
```

### Nested-object mapping strategy (the `Object`/`objValue` case)

```java
package com.example.notification.adapter.out.catalog;

import java.util.LinkedHashMap;
import java.util.Map;

public sealed interface Mapping permits PathMapping, ConstantMapping {}
public record PathMapping(String sourcePath, boolean required, String format) implements Mapping {}
public record ConstantMapping(Object value, boolean required) implements Mapping {}

/** Resolves one logical property, which may itself be a nested object (becomes datatype:Object downstream). */
public final class NestedObjectMapping {
    private final Map<String, Mapping> fields;
    private final Formatters formatters;

    public NestedObjectMapping(Map<String, Mapping> fields, Formatters formatters) {
        this.fields = Map.copyOf(fields); this.formatters = formatters;
    }

    public Object map(Map<String, Object> eventData) {
        var out = new LinkedHashMap<String, Object>();
        fields.forEach((name, mapping) -> {
            Object raw = switch (mapping) {
                case PathMapping p  -> formatters.apply(p.format(), readPath(eventData, p.sourcePath()));
                case ConstantMapping c -> c.value();
            };
            boolean required = switch (mapping) {
                case PathMapping p -> p.required();
                case ConstantMapping c -> c.required();
            };
            if (raw == null && required)
                throw new CatalogMappingException("required property missing"); // no field value in message
            if (raw != null) out.put(name, raw);
        });
        return Map.copyOf(out); // a Map => the Digital adapter emits datatype:Object with objValue
    }

    private static Object readPath(Map<String, Object> root, String dotted) {
        Object cur = root;
        for (String seg : dotted.split("\\.")) {
            if (!(cur instanceof Map<?, ?> m)) return null;
            cur = m.get(seg);
        }
        return cur;
    }
}
```

### Inbound Kafka adapter

```java
package com.example.notification.adapter.in.kafka;

import com.example.notification.application.port.in.HandleBusinessEvent;
import com.example.notification.application.port.in.HandleBusinessEvent.InboundPosition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

public final class BusinessEventListener {
    private final EventDecoder decoder;          // schema-registry aware; validates + maps to domain
    private final HandleBusinessEvent useCase;

    public BusinessEventListener(EventDecoder decoder, HandleBusinessEvent useCase) {
        this.decoder = decoder; this.useCase = useCase;
    }

    @KafkaListener(topics = "#{'${notification.inbound.topics}'.split(',')}",
                   groupId = "${notification.inbound.group-id}")
    public void onMessage(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        var event = decoder.decodeAndValidate(record.value(), record.headers()); // bad schema -> quarantine+throw
        useCase.handle(event, new InboundPosition(record.topic(), record.partition(), record.offset()));
        ack.acknowledge(); // MANUAL ack: only after durable accept/audit inside handle(...)
    }
}
```

### Digital outbound adapter — DTOs + mapper (the ACL; the only place Digital's names appear)

```java
package com.example.notification.adapter.out.digital;

import java.util.List;

// Digital's fixed, quirky field names/spellings are quarantined to this file ONLY.
public record DigitalEnvelopeDto(
        DigitalHeaderDto header, DigitalFeatureDto feature, DigitalProfileDto profile,
        DigitalContactDto contact, DigitalNoticeDto notice) {}

public record DigitalHeaderDto(String requestId, String requestDate, String clientApplication) {}
public record DigitalFeatureDto(String featureName) {}
public record DigitalProfileDto(String languagePerference, String profileCategory) {} // sic
public record DigitalContactDto(String eMAilAddress) {}                                 // sic
public record DigitalNoticeDto(String noticeType, List<DigitalPropertyDto> noticePropertyList) {}
public record DigitalPropertyDto(String name, String datatype, String stringValue, Object objValue) {}
```

```java
package com.example.notification.adapter.out.digital;

import java.util.List;
import java.util.Map;

public final class DigitalPayloadMapper {

    public DigitalEnvelopeDto map(RenderedNotification n, DeliveryMetadata meta) {
        List<DigitalPropertyDto> props = n.properties().entrySet().stream()
                .map(e -> toProperty(e.getKey(), e.getValue()))
                .toList();
        return new DigitalEnvelopeDto(
                new DigitalHeaderDto(meta.deliveryKey(), meta.requestDate(), meta.clientApplication()),
                new DigitalFeatureDto(meta.featureName()),
                new DigitalProfileDto(n.locale(), meta.profileCategory()),  // our locale -> their "languagePerference"
                new DigitalContactDto(n.emailAddress()),                    // -> their "eMAilAddress"
                new DigitalNoticeDto(n.templateId(), props));               // template -> noticeType
    }

    // Scalar -> stringValue; Map/List -> datatype:Object with objValue. The scalar/Object split lives here.
    private DigitalPropertyDto toProperty(String name, Object value) {
        if (value instanceof Map<?, ?> || value instanceof List<?>)
            return new DigitalPropertyDto(name, "Object", null, value);
        return new DigitalPropertyDto(name, "String", value == null ? null : value.toString(), null);
    }
}
```

```java
package com.example.notification.adapter.out.digital;

import com.example.notification.application.port.out.*;

public final class DigitalGatewayAdapter implements NotificationGateway {
    private final DigitalPayloadMapper mapper;
    private final DigitalKafkaProducer producer; // wraps Spring Kafka; distinguishes reject vs ambiguous timeout

    public DigitalGatewayAdapter(DigitalPayloadMapper mapper, DigitalKafkaProducer producer) {
        this.mapper = mapper; this.producer = producer;
    }

    @Override
    public void deliver(RenderedNotification rendered, DeliveryMetadata metadata) {
        var payload = mapper.map(rendered, metadata);
        // delivery key used as Kafka key AND as Digital dedup key IF the contract supports one.
        producer.publishSync(payload, metadata.deliveryKey()); // throws classified transient/ambiguous/permanent
    }
}
```

---

## Task 8 — Phased migration from the current nameservice

The principle: **one real sender at a time**. Shadow and cutover are per-template so a problem is contained to one template, and rollback is a routing switch at a defined boundary, not a blind replay.

| Phase | Work | Exit gate |
|---|---|---|
| **0. Inventory & contract** | Trace every nameservice point that builds/publishes a Digital message. For each: the business trigger, the template (`noticeType`), the required properties, the recipient authority, the facts used, and current failure behavior. Capture **real** Digital payload fixtures. Define + register the event schema at each real trigger. | Every trigger has an owner, an event meaning + registered schema, a recipient policy, a real Digital fixture, and a required-property spec. |
| **1. Transactional event production** | Add the nameservice **outbox**, committed atomically with each relevant state transition; publish to `nameservice.events`. **Legacy direct sends stay enabled** — no behavior change for customers yet. | Outbox↔state atomicity proven by integration tests; relay lag and errors observable. |
| **2. Shadow mode (compare, do not deliver)** | Notification Service consumes events, persists audit/intents, renders to a **non-delivery sink**, and compares normalized output against the approved current-output fixtures. No shadow traffic reaches customers; avoid long-lived PII copies. | All template fixtures + trigger cases compared; every diff explained and resolved; zero customer-facing sends from shadow. |
| **3. Per-template cutover** | Enable **one** template. At the boundary, disable the matching legacy sender and enable Notification Service dispatch for that template only. Watch delivery state, lag, failures, duplicate reports, and the business workflow. | Agreed observation window meets SLOs; audit completeness verified; business-owner sign-off recorded. |
| **4. Rollback rehearsal** | For a cut-over template, rehearse: stop new dispatch, preserve pending intents, reconcile ambiguous sends, restore legacy routing at a precise time/event boundary. | Rollback demonstrated with **no** uncontrolled dual-send and **no** lost events. |
| **5. Decommission direct publishing** | After all nameservice templates are cut over and backlog reconciled, remove the Digital payload builders, producer code, and credentials from nameservice. | Repo/deploy evidence shows no direct Digital publishing remains; runbooks + ownership accepted. |
| **6. Onboard other domains** | Repeat 0–5 for `addressservice`, then `advisorservice`: event + outbox + schema + catalog rules + contract fixtures + shadow + cutover + rollback. | Each domain clears the same schema, privacy, shadow-compare, cutover, and rollback gates. |

**Hard rule:** never run legacy and new senders *delivering* the same template concurrently. The only concurrent running allowed is Phase 2's explicitly non-delivering comparison sink.

---

## Task 9 — Review of sample messages (defects, ambiguities, questions)

**No real sample messages were supplied, so no defect can be *asserted* against a real payload.** `Sample.txt` is unrelated and ignored. What follows reviews a **reconstructed** sample built solely from the contract *described* in the brief — it is honest about that. The brief's own description already exposes several defect **classes** and ambiguities worth raising with Digital now; a full line-by-line review must be redone against real fixtures (Phase 0).

Reconstructed sample (illustrative, from the described contract):

```json
{
  "header":  { "requestId": "REQ-1001", "requestDate": "2026-10-09", "clientApplication": "CustomerComms" },
  "feature": { "featureName": "NameChange" },
  "profile": { "languagePerference": "en-IN", "profileCategory": "RETAIL" },
  "contact": { "eMAilAddress": "asha.iyer@example.com" },
  "notice":  {
    "noticeType": "NAME_CHANGE_COMPLETED",
    "noticePropertyList": [
      { "name": "completedDate", "datatype": "String", "stringValue": "2026-10-09" },
      { "name": "newName", "datatype": "Object",
        "objValue": { "given": "Asha", "family": "Iyer" } }
    ]
  }
}
```

| Severity | Defect / ambiguity | Question for the Digital team | What it blocks |
|---|---|---|---|
| **High** | **`requestId` scope / reuse.** It is unclear whether `requestId` must be unique per request or may repeat across retries, and whether Digital dedups on it. | Is `requestId` an idempotency/dedup key? Must retries reuse it, or must it be new each publish? Is there a separate dedup key we can set to our `deliveryKey`? | Our entire idempotency story and the ambiguous-timeout handling (Task 3). |
| **High** | **Empty / null / whitespace required values.** `stringValue` may be present-but-blank; behavior is unspecified. | For each template, how is a required property represented when the value is absent vs null vs empty vs whitespace? Does Digital reject or silently send a blank email? | Required-field validation rules in the catalog (Task 5). |
| **High** | **Multi-valued / nested `Object` representation.** The brief says properties can be `Object` with `objValue`, but not whether `objValue` may be an array, how a repeated property is expressed, or the exact nested schema. | What is the precise shape, cardinality, and nullability of each nested property? Is a list expressed as an `objValue` array, repeated `name` entries, or a delimited string? | The nested-object mapping strategy and formatter output (Tasks 5, 7). |
| **High** | **Ambiguous publish outcome.** A producer timeout after Digital may have accepted the message risks a duplicate email. | On timeout, is there a dedup window, a status lookup, a callback, or a reconciliation API? | Duplicate-suppression and the `AMBIGUOUS` state handling (Task 3). |
| **Medium** | **Field-name quirks are error-prone** (`languagePerference`, `eMAilAddress`). These are easy to misspell "correctly to the spec" and break silently. | Please confirm the exact, authoritative spelling/casing of **every** field, in a versioned schema, so our ACL fixtures lock them. | Contract fixtures and the outbound DTOs (Task 7). |
| **Medium** | **Date / timezone / locale / URL formats unspecified.** `requestDate` is a bare date; `completedDate` format and timezone are unstated; `languagePerference` value set is unknown. | What are the canonical date, timezone, locale-code, and URL constraints per template? | `localDate`/`approvedHttpsUrl` formatter definitions (Task 5). |
| **Medium** | **One event → many notices / ordering.** Can one business event legitimately produce multiple templates or recipients, and does Digital treat order as significant? | Is multi-template / multi-recipient supported, and is message order meaningful to Digital? | Planner fan-out and per-aggregate ordering decisions (Tasks 3, 5). |
| **Medium** | **Authoritative envelope schema + version policy.** The envelope is described prose-only; no schema or change-notification policy is given. | Where is the authoritative envelope schema, which fields are required, and how are contract changes versioned and announced? | Outbound schema evolution and contract tests (Task 3). |
| **Low** | **Undocumented / environment-specific fields.** `profileCategory`, `clientApplication`, `featureName` meanings and allowed values are undocumented. | Who owns these fields, what are allowed values, and are any environment-specific? Any field we must send that is undocumented? | Per-environment catalog values (Task 5). |
| **Low** | **Datatype vocabulary.** Only `String` and `Object` are implied; numeric/boolean/date datatypes are unconfirmed. | What is the full `datatype` enumeration and the `stringValue` conventions for non-string scalars? | Mapper scalar handling (Task 7). |

---

## (a) Open questions for stakeholders

| Open question | Who must answer | What it blocks |
|---|---|---|
| Complete domain-service list and **every** nameservice Digital-publish trigger point? | Domain owners (nameservice) | Event inventory + migration scope (Phase 0). |
| Authoritative Digital fixtures, full template inventory, and required-property schemas? | Digital + product owner | Verified catalog, outbound DTOs, and the real Task 9 review. |
| Is `requestId` (or another key) a Digital dedup key, and what is the timeout/reconciliation behavior? | Digital | Idempotency + ambiguous-send handling. |
| Who is authoritative for recipient selection, contact address, locale, and consent/suppression? | Domain + profile + compliance owners | Recipient schema, PII data flow, and the scope guardrail. |
| Volume, latency, retention, availability, RPO/RTO targets? | Product + SRE + compliance | Partition sizing, alert thresholds, and all SLOs. |
| Schema-registry format + compatibility policy; mandatory PII controls (encryption, tokenization, retention)? | Platform + security/privacy | Production readiness and the PII control table. |

## (b) Key risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Provisional mapping ≠ real Digital contract | High (until fixtures arrive) | High — wrong or failed emails | Obtain signed-off schema/fixtures; fail CI/startup on mismatch; shadow-compare before any cutover. |
| Duplicate email after an ambiguous publish to Digital | Medium | High — customer-visible | Stable `deliveryKey`; request Digital-side dedup/status lookup; audited reconciliation of `AMBIGUOUS`. |
| PII leaked to logs or over-retained | Medium | High — regulatory | No payload logging; encryption + least-privilege ACLs; minimize/classify/expire copies; audit access. |
| Event facts insufficient for current/future templates | Medium | Medium–High — re-introduces domain coupling | Fact-rich `data` rules; catalog path checks in CI; schema review; shadow comparison. |
| Outbox / intent backlog grows undetected | Medium | High — delayed notices | Alert on oldest-item age + consumer lag; capacity test; on-call ownership + runbooks. |
| Partition-count change silently reorders per-aggregate notices | Low | Medium | Treat repartitioning as an ordering-impacting change event; use `subject.version` gap detection. |

## (c) Architecture Decision Record

**Title:** Business-event-driven Notification Service
**Status:** Accepted

**Context.** Today nameservice builds and publishes externally-owned Digital email messages from multiple workflow points. This couples every domain to a fixed external contract (including its quirks), spreads delivery mechanics across bounded contexts, and makes a template change a domain release.

**Decision.**
1. Customer notifications are handled by a **dedicated Notification Service**, deployed as its own microservice and modelled as its own bounded context; domain services no longer build or publish Digital messages.
2. The integration is **event-driven**: domain services publish template-agnostic **business events** stating what happened and who is involved, and never reference Digital template codes, property names, or the envelope; the Notification Service owns the mapping from business events to Digital templates and properties.

**Consequences.** Domains must publish complete, minimized facts + intended recipients via transactional outboxes. The Notification Service owns mapping config, durable intent, dispatch, retries, audit, and replay. Template changes are isolated from domain deployments (config-only). PII now flows through additional storage/transport and requires strict encryption, minimization, access, retention, and audit controls. At-least-once delivery can still yield a duplicate at the Digital boundary unless Digital provides idempotency.

**Alternatives considered.** These two decisions are fixed inputs and are not re-evaluated here; alternatives (in-domain publishing, a shared library, synchronous notification API) were considered out of scope for this design.

## (d) Challenges to fixed decisions

**None.** No concrete evidence in the available inputs shows either fixed decision failing for this use case. The one real residual concern — a duplicate email after an ambiguous publish to Digital — is a *boundary-delivery* limitation of at-least-once semantics, not a flaw in either decision; it is mitigated by a stable delivery key, Digital-side deduplication or status lookup where available, and audited reconciliation of ambiguous outcomes. If Digital confirms it offers **no** dedup key and **no** status/callback API (open question above), that would not overturn the decisions but would raise the duplicate risk from "mitigated" to "accepted residual", which must then be signed off by the product/compliance owner.
