# Notification Service: Detailed Design

## 1. Hexagonal Architecture (Ports and Adapters)

The internal structure strictly adheres to Hexagonal Architecture to isolate the pure domain mapping logic from framework dependencies (Spring) and external quirks (Digital's messy JSON contract).

```mermaid
flowchart LR
    subgraph Infrastructure Layer (Adapters)
        InK[Kafka Listener\nAdapter]
        OutD[Digital Kafka\nPublisher Adapter]
        OutDB[Postgres Audit\nRepository Adapter]
    end

    subgraph Application Layer (Ports)
        InP((Inbound Port\nUse Case))
        OutP1((Outbound Port\nPublisher))
        OutP2((Outbound Port\nAudit Repo))
    end

    subgraph Domain Layer (Zero Dependencies)
        DM[Domain Entities\nNotice, Template]
        DS[Domain Service\nTemplateMapper]
    end

    InK -->|Domain Event| InP
    InP --> DS
    DS --> DM
    InP -->|Save Audit| OutP2
    InP -->|Send Notice| OutP1
    
    OutP1 -->|Implements| OutD
    OutP2 -->|Implements| OutDB
    
    style Domain Layer fill:#f9f,stroke:#333,stroke-width:2px
```

*   **Anti-Corruption Layer (ACL):** External misspellings (`languagePerference`, `eMAilAddress`) exist **only** within `Digital Kafka Publisher Adapter`. The core domain uses clean, standard naming conventions.

## 2. Event Contract (Upstream)

Domain services must emit "Fat Events" (Event-Carried State Transfer). The Notification Service does not make synchronous calls back to domain APIs to gather missing data.

**Standard Envelope Definition:**
```json
{
  "eventId": "a1b2c3d4-...",
  "correlationId": "req-998877",
  "timestamp": 1718293847566,
  "eventType": "CustomerNameChanged",
  "schemaVersion": 1,
  "locale": "en-CA",
  "audience": "RETAIL",
  "data": {
    "customerId": "CUST-8899",
    "contactDetails": {
      "email": "jane.doe@example.com"
    },
    "previousName": { "firstName": "Jane", "lastName": "Smith" },
    "newName": { "firstName": "Jane", "lastName": "Doe" },
    "reasonCode": "MARRIAGE"
  }
}
```

## 3. Configuration-Driven Template Catalog

To adhere to the Open-Closed Principle (OCP), adding a new notification template requires zero code changes. Mapping rules are defined in a YAML catalog evaluated via JSONPath.

**Sample `template-catalog.yml`:**
```yaml
templates:
  - eventType: CustomerNameChanged
    digitalTemplateName: NAME_CHANGE_CONFIRMATION
    audience: RETAIL
    properties:
      - name: previousFirstName
        sourcePath: "$.data.previousName.firstName"
        required: true
        type: SCALAR
      # Handling nested Digital quirks
      - name: updatedNameRecord
        required: false
        type: OBJECT
        nestedProperties:
          - name: objValue
            sourcePath: "$.data.newName"
            type: SCALAR
      # Static injection
      - name: noticeReason
        staticValue: "CLIENT_REQUEST"
        type: SCALAR
```

*Startup Validation:* A Spring `ApplicationRunner` compiles all `sourcePath` JSONPath expressions on startup. If any expression is syntactically invalid, the application fails to start (`fail-fast`).

## 4. SOLID Principles Implementation

| Principle | Implementation Context | Mistake Prevented |
| :--- | :--- | :--- |
| **SRP** (Single Responsibility) | `TemplateMappingDomainService` vs. `DigitalKafkaAdapter`. | Prevents JSONPath mapping evaluation from being tangled up with mapping Jackson DTOs for the Digital platform. |
| **OCP** (Open-Closed) | `TemplateCatalogEngine` via YAML configuration. | Modifying core Java code just because the Marketing team created a new email variant. |
| **LSP** (Liskov Substitution) | `DigitalPublisherPort` implementations. | Allows seamless swapping of the Kafka outbound adapter with an in-memory mock during integration testing without altering use-case logic. |
| **ISP** (Interface Segregation) | Separate `ProcessEventUseCase` and `AuditReplayUseCase` interfaces. | Forces the Kafka listener to only depend on the methods it needs, preventing unintended access to sensitive audit replay functionalities. |
| **DIP** (Dependency Inversion) | `AuditRepositoryPort` defined in Application layer, implemented in Infrastructure. | Domain core remains pristine; it does not import `org.springframework.data` or know about SQL/Hibernate dialects. |

## 5. Error Classification & DLQ Strategy

*   **Retryable Errors (Transient):** Database connection timeouts, brief Kafka broker unavailability. 
    *   *Action:* Indefinite backoff and retry. The partition processing is blocked to ensure strictly ordered delivery per customer.
*   **Non-Retryable Errors (Terminal):** Event missing mandatory catalog fields, schema validation failure, invalid JSON.
    *   *Action:* Sent immediately to `notification.dlq` topic. Kafka offsets are committed to unblock the partition. DLQ retains field-level encryption to protect PII while awaiting manual operational review.