# Notification Service: Architecture Overview

## 1. Executive Summary
The Notification Service is a dedicated bounded context responsible for standardizing and routing customer communications in a highly regulated financial environment. It operates strictly on an event-driven contract: upstream domain microservices (e.g., Name Service, Address Service) publish template-agnostic business events. The Notification Service translates these "fat" domain events into platform-specific digital templates via a configuration-driven catalog, ensuring that domains remain entirely decoupled from delivery mechanics, channels, and third-party contracts. 

## 2. Bounded Context Definition

*   **Primary Responsibility:** Interpreting cross-domain business events, resolving applicable communication templates, mapping payload facts to target schemas, securely routing payloads, and maintaining a strict, non-repudiable audit log of all outbound communications.
*   **Explicit Non-Responsibilities:** Deciding *if* a business event occurred, maintaining customer opt-in/opt-out preferences (handled upstream), and the actual physical transmission of emails/SMS (delegated to the Digital platform).
*   **Guardrails:** Zero business conditional logic resides here. If a template is configured for an event, the notification is mapped and forwarded. Suppressions must be signaled in the upstream event envelope.

## 3. High-Level Architecture Diagram

The system relies on asynchronous messaging via Apache Kafka and guarantees at-least-once delivery using the Transactional Outbox pattern on both the publishing and consuming sides.

```mermaid
flowchart TD
    subgraph Domain Bounded Contexts
        NS[Name Service]
        AS[Address Service]
        AcS[Account Service]
    end

    subgraph Event Broker
        K1[[Kafka Topics: domain.*.events\n(Avro, Field-Encrypted)]]
    end

    subgraph Notification Service Bounded Context
        NSC[Notification Service App]
        DB[(PostgreSQL)]
        Outbox[Outbox / Audit Table]
        Idempotency[Idempotency Table]
        
        DB --- Outbox
        DB --- Idempotency
        NSC <-->|Local DB Tx| DB
    end

    subgraph Digital Delivery Platform
        K2[[Kafka Topic: digital.outbound\n(JSON, Digital Contract)]]
        DP[Digital Platform Microservices]
    end

    NS -->|Transactional Outbox| K1
    AS -->|Transactional Outbox| K1
    AcS -->|Transactional Outbox| K1
    
    K1 -->|Consumes Events| NSC
    NSC -->|Relays Mapped DTOs| K2
    K2 -->|Consumes Directives| DP
```

## 4. Transactional Integrity & Delivery Guarantees

To meet strict financial auditing and resilience constraints, the architecture leverages the **Transactional Outbox Pattern**:

1.  **Domain Side:** The domain service writes its state change (e.g., updating a name in the DB) and inserts the `CustomerNameChanged` event into a local `outbox` table within the same SQL transaction. A relay publishes this to Kafka.
2.  **Notification Side (Exactly-Once Processing locally):** 
    *   Consumes the event from `domain.*.events`.
    *   Extracts the `eventId` and attempts an insert into the `processed_messages` (Idempotency) table.
    *   Maps the payload to the Digital template.
    *   Writes the resulting payload to its own `outbox` (which doubles as the 7-year audit trail) in the **same transaction**.
    *   Kafka offsets are committed only after the DB transaction succeeds.
3.  **Outbound Relay:** A background process polls the Notification Service's outbox and pushes to `digital.notifications.outbound`.

## 5. Security & PII Handling

Due to regulatory requirements (GDPR/PCI), Personally Identifiable Information (PII) is strictly controlled:
*   **Field-Level Encryption:** Domain services encrypt PII fields (like `contactDetails.email` or `newName`) within the Avro payload using envelope encryption (KMS). 
*   **Kafka:** Brokers only store ciphertext.
*   **Notification Service:** Holds the symmetric keys in memory to transiently decrypt the payload during mapping, reformats it, and re-encrypts it using the Digital Platform's public key (or relies on mTLS if transport security is deemed sufficient by InfoSec).
*   **Audit Log:** The database audit log encrypts PII at rest.

## 6. Migration Strategy

Migrating existing direct-publishers (e.g., `nameservice`) will follow a 4-phase approach to ensure zero downtime and zero lost notices:

1.  **Event Generation (Shadow Mode):** Domain services begin publishing events to the new Kafka topics. Existing direct Digital publishing remains active.
2.  **Shadow Mapping:** Notification Service consumes events, maps them, and logs the output (no publishing to Digital). Automated comparisons verify the payloads match the legacy system.
3.  **Per-Template Cutover:** Toggle feature flags in the domain to disable legacy publishing. Notification Service begins publishing to the Digital topic for that specific template.
4.  **Decommission:** Remove legacy Digital SDKs/DTOs from the domain services entirely.