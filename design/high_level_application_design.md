# High Level Application Design (HLD)
**System:** Alert / Notification Gateway Service
**Architecture Pattern:** Hexagonal Architecture (Ports and Adapters)

## 1. Purpose and Scope
The Notification Gateway Service is a dedicated microservice responsible for consuming business domain events (e.g., Name Change, Address Change) and transforming them into standardized notification payloads required by the Digital Team's Kafka infrastructure. It acts as an Anti-Corruption Layer (ACL) between internal domain services and external enterprise notification systems.

## 2. Application Core & Hexagonal Structure
The application strictly adheres to Hexagonal Architecture to isolate business rules from infrastructure (Kafka, Databases).

### 2.1. The Domain Core (Center)
The Core contains pure business logic with zero framework dependencies.
*   **Domain Models:** `NotificationCommand`, `NoticeMetadata`, `RecipientProfile`.
*   **Inbound Ports:** Interfaces defining what the application can do (e.g., `ProcessNotificationUseCase`).
*   **Outbound Ports:** Interfaces defining what the application needs from the outside world (e.g., `DigitalNotificationPublisherPort`, `NotificationAuditPort`).
*   **Domain Services:** 
    *   `NotificationStrategyEngine`: Determines which template/mapping logic to apply based on the incoming domain event type.
    *   `PayloadBuilderService`: Enforces business rules (e.g., ensuring mandatory fields like `X-CLIENT_APP_ID` or `requestId` are populated).

### 2.2. Inbound Adapters (Driving)
These adapters translate external stimuli into core domain commands.
*   **DomainEventKafkaConsumer:** Listens to internal topics (e.g., `domain.nameservice.events`, `domain.addressservice.events`). Deserializes the specific domain JSON into a generic `NotificationCommand` and invokes the `ProcessNotificationUseCase`.
*   **AdminRestController (Optional):** Exposes HTTP endpoints for manual notification triggers or operational health checks.

### 2.3. Outbound Adapters (Driven)
These adapters implement the Core's outbound ports to interact with external systems.
*   **DigitalKafkaProducerAdapter:** Implements `DigitalNotificationPublisherPort`. Responsible for mapping the internal `NotificationCommand` into the highly specific Digital JSON structure (containing `header`, `feature`, `profile`, `contact`, and `noticePropertyList`).
*   **AuditDatabaseAdapter:** Implements `NotificationAuditPort` to log successful/failed notification dispatches into a local relational database for tracking.

## 3. Data Flow & Transformation Strategy

1.  **Ingestion:** `DomainEventKafkaConsumer` receives a `NameChangeTaskAssigned` event.
2.  **Delegation:** Consumer calls `ProcessNotificationUseCase.process(command)`.
3.  **Strategy Resolution:** The Core identifies the event type and selects the appropriate mapping strategy (e.g., `NameChangeTaskStrategy`).
4.  **Transformation:** The strategy maps domain-specific fields (like `firstName`, `browserViewLink`) into the generic `noticePropertyList` Key-Value array.
5.  **Dispatch:** The Core calls `DigitalNotificationPublisherPort.publish(formattedAlert)`.
6.  **Translation to External Schema:** The Outbound adapter constructs the final JSON payload (injecting `X-CLIENT_APP_ID`, `X-REQUEST-DATE`) and publishes it to the Digital Team's Kafka topic.

## 4. Error Handling and Resiliency
*   **Validation Errors:** Invalid payloads are logged and discarded (or sent to an internal Dead Letter Queue) without attempting publication to the Digital team.
*   **Kafka Unavailable:** If the Digital Kafka cluster is unreachable, the Outbound Adapter throws an infrastructure exception. The framework (e.g., Spring Kafka) will utilize a back-off retry policy.
*   **Dead Letter Queue (DLQ):** Messages that fail transformation or delivery after maximum retries are routed to a generic `notification-dlq` topic for manual operational review.