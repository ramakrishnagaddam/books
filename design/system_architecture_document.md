# System Architecture Document
**System:** Alert / Notification Gateway Service

## 1. Executive Summary
To support a Domain-Driven Design (DDD) ecosystem, notification management is centralized into a dedicated Gateway Service. This prevents domain services (Name, Address, Advisor) from becoming tightly coupled to the Digital Team's proprietary notification schemas and Kafka infrastructure.

## 2. Context View (System Landscape)

*   **Upstream Systems (Producers):** Name Service, Address Service, Advisor Service. These publish lightweight, schema-independent domain events to internal Kafka topics.
*   **Core System:** Notification Gateway Service. Acts as the aggregator, transformer, and dispatcher.
*   **Downstream System (Consumer):** Digital Team Notification Kafka. Receives standardized JSON payloads containing headers, profiles, contacts, and dynamic `noticePropertyList` arrays.

## 3. Container Architecture

```text
+-------------------+      +-------------------+      +-------------------+
|   Name Service    |      |  Address Service  |      |  Advisor Service  |
+---------+---------+      +---------+---------+      +---------+---------+
          |                          |                          |
          v                          v                          v
=============================================================================
                          INTERNAL KAFKA CLUSTER
             (Topics: domain.events.name, domain.events.address)
=============================================================================
                                     |
                                     v
+---------------------------------------------------------------------------+
|                    NOTIFICATION GATEWAY SERVICE (Hexagonal)               |
|                                                                           |
|  [ Inbound Kafka Adapters ] ---> [ Core Mapping/Strategy ] ---> [ DB ]    |
|                                            |                              |
|                                            v                              |
|                               [ Outbound Kafka Adapter ]                  |
+---------------------------------------------------------------------------+
                                     |
                                     v
=============================================================================
                       DIGITAL TEAM KAFKA CLUSTER
                      (Topic: digital.notifications)
=============================================================================
```

## 4. Key Architectural Decisions (ADRs)

### ADR 1: Centralized Notification Gateway
*   **Status:** Accepted
*   **Context:** Multiple domain microservices need to send emails via a unified Digital Team API/Kafka topic.
*   **Decision:** Build a separate service to handle all notification transformations instead of embedding digital-specific logic in domain services.
*   **Consequences:** Improved separation of concerns, easier updates to email templates, and centralized monitoring of outgoing communications.

### ADR 2: Hexagonal Architecture (Ports and Adapters)
*   **Status:** Accepted
*   **Context:** The Digital Team's contract (`noticePropertyList`, custom headers) is complex and subject to change.
*   **Decision:** Implement Hexagonal Architecture.
*   **Consequences:** The core transformation logic remains completely unaware of Kafka specifics. Adapters map the core models to the strict JSON required by the Digital team.

### ADR 3: Strategy Pattern for Payload Mapping
*   **Status:** Accepted
*   **Context:** Different notifications (e.g., `NAME_CHG_TASK_ASSIGNED` vs `NAME_CHANGE_COMPLETED`) require different key-value pairs in the `noticePropertyList`.
*   **Decision:** Use the Strategy Design Pattern within the Application Core. Each notice type will have a dedicated mapping class implementation.
*   **Consequences:** High cohesion and adherence to the Open-Closed Principle. Adding a new notification template only requires adding a new strategy class.

## 5. Security and Observability
*   **Distributed Tracing:** The Gateway will capture `X-REQUEST-ID` and `X_ACTIVITY_SOURCE_ID` from upstream domain events and inject them into the Digital Kafka headers and payload. This ensures end-to-end trace correlation across Kibana/Splunk.
*   **PII Data Masking:** Since the payloads contain Personally Identifiable Information (PII) such as `firstName`, `lastName`, and `emailAddress`, application logs will mask these fields before writing to output streams.
*   **Metrics:** Prometheus metrics will track `notifications_processed_total`, `notifications_failed_total`, and processing latency, categorized by `noticeType`.