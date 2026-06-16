# Wave Message Context Structure

## Overview

Wave messages support a structured context system that includes DDD aggregate tracking,
event subscription metadata, and standard cross-cutting concerns.
The context is mapped to CloudEvents `extensions` field according to the CloudEvents v1.0 specification.

---

## Context Structure

### Base Context

All message types (events, commands, queries) share a base context structure:

```typescript
interface WaveBaseContext {
  // DDD Aggregate Tracking
  aggregateId?: string;           // ID of the aggregate (e.g., "user-123", "order-456")
  
  // Event Subscription Metadata
  events?: string[];              // List of event names sender is interested in
  
  // Cross-Cutting Concerns
  correlationId?: string;         // Correlation ID for request tracing
  causationId?: string;           // ID of the message that caused this one
  userId?: string;                // User who initiated the action
  
  // Extensibility
  [key: string]: any;             // Allow custom fields
}
```

### Message-Specific Context Extensions

#### Event Context

```typescript
interface WaveEventContext extends WaveBaseContext {
  eventVersion?: string;          // Event schema version
  occurredAt?: string;            // When the domain event occurred (ISO 8601)
}
```

#### Command Context

```typescript
interface WaveCommandContext extends WaveBaseContext {
  replyQueue?: string;            // Internal: reply queue for RPC
  timeoutMs?: number;             // Command execution timeout
}
```

#### Query Context

```typescript
interface WaveQueryContext extends WaveBaseContext {
  cacheKey?: string;              // Optional cache key for query results
  cacheTtl?: number;              // Cache TTL in seconds
}
```

---

## CloudEvents Mapping

### Mapping Strategy

The context is mapped to CloudEvents as follows:

- **Payload** → `data` field
- **Context** → `extensions` field (flat structure)
- **Trace propagation** → Merged into `extensions`

### Example CloudEvent

```json
{
  "specversion": "1.0",
  "id": "uuid-123",
  "source": "wave://Billing",
  "type": "wave.event.Billing.InvoiceCreated",
  "subject": "Billing.InvoiceCreated",
  "time": "2025-05-19T10:00:00Z",
  "datacontenttype": "application/json",
  "data": {
    "invoiceId": "INV-001",
    "amount": 100
  },
  "extensions": {
    "aggregateId": "invoice-123",
    "events": ["InvoiceCreated", "PaymentProcessed"],
    "correlationId": "corr-456",
    "userId": "user-789",
    "traceparent": "00-trace-id-span-id-01"
  }
}
```

---

## Usage Examples

### Sending an Event with Context

```typescript
import createBus from '@wave/bus-rabbitmq';

const bus = createBus();

await bus.sendEvent({
  kind: 'event',
  namespace: 'Billing.Invoice',
  name: 'InvoiceCreated',
  payload: {
    invoiceId: 'INV-001',
    amount: 100,
    currency: 'USD'
  },
  context: {
    aggregateId: 'invoice-123',
    userId: 'user-456',
    correlationId: 'corr-789',
    events: ['PaymentProcessed', 'InvoiceEmailed']
  }
});
```

### Sending a Command with Context

```typescript
const result = await bus.sendCommand({
  kind: 'command',
  namespace: 'Billing.Invoice',
  name: 'ProcessPayment',
  payload: {
    invoiceId: 'INV-001',
    paymentMethod: 'credit_card'
  },
  awaitResponse: true,
  context: {
    aggregateId: 'invoice-123',
    userId: 'user-456',
    correlationId: 'corr-789'
  }
});
```

### Sending a Query with Context

```typescript
const invoice = await bus.sendQuery({
  kind: 'query',
  namespace: 'Billing.Invoice',
  name: 'GetInvoice',
  payload: {
    invoiceId: 'INV-001'
  },
  context: {
    aggregateId: 'invoice-123',
    userId: 'user-456',
    cacheKey: 'invoice:INV-001',
    cacheTtl: 300
  }
});
```

### Handling Messages with Context

```typescript
await bus.addEventListener(
  'Billing.Invoice',
  'InvoiceCreated',
  async (message, executionContext) => {
    // Access context fields
    const { aggregateId, userId } = message.context || {};
    
    console.log(`Invoice ${aggregateId} created by user ${userId}`);
    
    // Your business logic here
    await processInvoice(message, { userId });
  }
);
```

---

## Field Descriptions

### aggregateId

**Type:** `string`  
**Purpose:** Identifies the specific aggregate instance this message relates to.

**Examples:**
- `"user-123"`
- `"order-456"`
- `"invoice-789"`
- `"a6f50c11-3eb8-4771-8a6d-636921135014"`

**Best Practices:**
- Use consistent ID format across your domain
- Include aggregate type prefix for clarity (e.g., `"user-123"` vs `"123"`)
- Keep IDs immutable and globally unique

---

### events

**Type:** `string[]`  
**Purpose:** List of event names the sender is interested in.
This is metadata for the sender's internal use—the bus does not implement special subscription logic based on this field.

**Examples:**
```typescript
events: ["InvoiceCreated", "PaymentProcessed", "InvoiceEmailed"]
```

**Use Cases:**
- Document which events a service expects to handle
- Audit trail of event dependencies

**Best Practices:**
- Use clear, past-tense event names (e.g., `"InvoiceCreated"` not `"CreateInvoice"`)
- Limit to relevant events
- Keep event names consistent with your domain language

---

### correlationId

**Type:** `string`  
**Purpose:** Correlation ID for tracing related messages across service boundaries.

**Examples:**
- `"corr-123e4567-e89b-12d3-a456-426614174000"`
- `"request-abc123"`

**Best Practices:**
- Generate once at the entry point of a request
- Propagate through all related messages
- Use UUIDs for uniqueness
- Include in logs for traceability

---

### causationId

**Type:** `string`  
**Purpose:** ID of the message that directly caused this message to be sent.

**Examples:**
- `"msg-123e4567-e89b-12d3-a456-426614174000"`

**Best Practices:**
- Set to the ID of the triggering message
- Enables causality tracking in event sourcing
- Useful for debugging message chains

---

### userId

**Type:** `string`  
**Purpose:** Identifies the user who initiated the action.

**Examples:**
- `"user-123"`
- `"admin-456"`

**Best Practices:**
- Use consistent user ID format
- Include for audit trails
- Required for authorization checks

---

## Best Practices

### 1. Always Include Correlation ID

```typescript
const correlationId = randomUUID();

await bus.sendEvent({
  // ...
  context: { correlationId }
});
```

### 2. Use Aggregate Tracking for DDD

```typescript
await bus.sendEvent({
  // ...
  context: {
    aggregateId: 'invoice-123',
  }
});
```

### 3. Document Event Dependencies

```typescript
await bus.sendCommand({
  // ...
  context: {
    events: ['PaymentProcessed', 'InvoiceEmailed']
  }
});
```

### 4. Use Consistent Naming

- **aggregateId**: `{type}-{id}` (e.g., `"invoice-123"`)
- **events**: PastTense (e.g., `"InvoiceCreated"`)

---

## Related Documentation

- [CloudEvents Specification](https://github.com/cloudevents/spec)
- [Wave Transport API](../README.md)
- [DDD Aggregate Pattern](https://martinfowler.com/bliki/DDD_Aggregate.html)
- [Correlation ID Pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CorrelationIdentifier.html)
