# @moj/bus-rabbitmq

A lightweight, TypeScript-first distributed messaging bus built on top of RabbitMQ, implementing the CloudEvent specification and Wave transport pattern for microservices communication.

## Overview

`@moj/bus-rabbitmq` provides a high-level abstraction for building distributed systems with RabbitMQ. It supports three fundamental message types:

- **Events** - Fire-and-forget notifications (e.g., `UserRegistered`)
- **Commands** - Request-response operations with optional acknowledgments (e.g., `CreateInvoice`)
- **Queries** - Synchronous data retrieval operations (e.g., `GetUserById`)

The package integrates with OpenTelemetry for distributed tracing, follows CloudEvent specifications, and provides automatic retry logic with dead-letter queue (DLQ) support.

## Features

- 🚀 **Type-safe messaging** - Full TypeScript support with generic type parameters
- 🔗 **CloudEvent compliance** - Standardized event format for cross-service communication
- 📊 **Distributed tracing** - OpenTelemetry integration for end-to-end visibility
- 🔄 **Retry & DLQ** - Configurable retry policies with dead-letter queue support
- ⚡ **RPC-style commands** - Request-response pattern with configurable timeouts
- 🎯 **Message acknowledgment** - Manual or automatic ACK/NACK control
- 🔌 **Wave transport pattern** - Decoupled service communication

## Installation

```bash
npm install @moj/bus-rabbitmq
# or
yarn add @moj/bus-rabbitmq
# or
bun add @moj/bus-rabbitmq
```

**Requirements:**
- Node.js >= 18
- RabbitMQ server (available locally or via cloud provider)

## Quick Start

### Basic Setup

```typescript
import createBus from '@moj/bus-rabbitmq';

// Create a bus instance (uses default RabbitMQ connection)
const bus = createBus();

// Or customize configuration
const bus = createBus({
  url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
  prefetchCount: 10,
});
```

## Usage

### 1. Sending Events

Events are fire-and-forget notifications. They don't expect any response.

```typescript
import createBus from '@moj/bus-rabbitmq';

const bus = createBus();

// Send an event
await bus.sendEvent({
  kind: 'event',
  namespace: 'UserManagement',
  name: 'UserCreated',
  payload: {
    userId: 'usr-123',
    email: 'john@example.com',
    createdAt: new Date().toISOString(),
  },
});

console.log('User created event sent');
```

### 2. Handling Events

```typescript
import createBus from '@moj/bus-rabbitmq';

const bus = createBus();

// Register an event listener
await bus.addEventListener('UserManagement', 'UserCreated', async (event, context) => {
  console.log('User created:', event);
  
  // Your business logic here
  await sendWelcomeEmail(event.email);
  
  // Auto-ack is enabled by default, but you can manually acknowledge:
  // await context.ack();
}, {
  autoAck: true,
  maxRetries: 3,
  retryDelayMs: 1000,
});

console.log('Event listener registered');
```

### 3. Sending Commands (RPC Style)

Commands support request-response pattern with optional timeouts.

```typescript
import createBus from '@moj/bus-rabbitmq';

interface CreateInvoiceCommand {
  customerId: string;
  items: Array<{ productId: string; quantity: number; price: number }>;
  currency: string;
}

interface InvoiceResponse {
  invoiceId: string;
  status: 'created' | 'failed';
  totalAmount: number;
}

const bus = createBus();

// Send command and wait for response
const response = await bus.sendCommand<CreateInvoiceCommand, InvoiceResponse>(
  {
    kind: 'command',
    namespace: 'Billing',
    name: 'CreateInvoice',
    payload: {
      customerId: 'cust-456',
      items: [
        { productId: 'prod-1', quantity: 2, price: 29.99 },
        { productId: 'prod-2', quantity: 1, price: 49.99 },
      ],
      currency: 'USD',
    },
    awaitResponse: true, // Enable RPC response
  },
  {
    timeoutMs: 10000, // 10 second timeout
  }
);

console.log('Invoice created:', response);
```

### 4. Handling Commands

```typescript
import createBus from '@moj/bus-rabbitmq';

interface CreateInvoiceCommand {
  customerId: string;
  items: Array<{ productId: string; quantity: number; price: number }>;
}

interface InvoiceResponse {
  invoiceId: string;
  status: string;
}

const bus = createBus();

// Register a command listener
const unsubscribe = await bus.addCommandListener<CreateInvoiceCommand, InvoiceResponse>(
  'Billing',
  'CreateInvoice',
  async (command, context) => {
    console.log('Processing invoice creation:', command);
    
    // Business logic
    const invoiceId = `INV-${Date.now()}`;
    
    // Return the response - automatically sent back to caller
    return {
      invoiceId,
      status: 'created',
    };
  },
  {
    autoAck: true,
    maxRetries: 3,
    retryDelayMs: 1000,
  }
);

// Later, to stop listening:
// await unsubscribe();
```

### 5. Sending Queries

Queries are synchronous data retrieval operations.

```typescript
import createBus from '@moj/bus-rabbitmq';

interface GetUserQuery {
  userId: string;
}

interface UserResponse {
  id: string;
  name: string;
  email: string;
}

const bus = createBus();

// Send a query
const user = await bus.sendQuery<GetUserQuery, UserResponse>(
  {
    kind: 'query',
    namespace: 'UserManagement',
    name: 'GetUser',
    payload: {
      userId: 'usr-123',
    },
  },
  {
    timeoutMs: 5000,
  }
);

console.log('User:', user);
```

### 6. Handling Queries

```typescript
import createBus from '@moj/bus-rabbitmq';

interface GetUserQuery {
  userId: string;
}

interface UserResponse {
  id: string;
  name: string;
  email: string;
}

const bus = createBus();

// Register a query listener
const unsubscribe = await bus.addQueryListener<GetUserQuery, UserResponse>(
  'UserManagement',
  'GetUser',
  async (query, context) => {
    console.log('Fetching user:', query.userId);
    
    // Your query logic
    const user = await getUserFromDatabase(query.userId);
    
    return user;
  },
  {
    autoAck: true,
  }
);

// To stop listening:
// await unsubscribe();
```

### 7. Manual Acknowledgment

For more control over message processing lifecycle:

```typescript
const bus = createBus();

await bus.addEventListener('Notifications', 'EmailSent', async (event, context) => {
  try {
    // Process the event
    await processEmailEvent(event);
    
    // Manual acknowledgment
    await context.ack();
  } catch (error) {
    // Negative acknowledgment - message goes to DLQ
    await context.nack(false);
  }
}, {
  autoAck: false, // Disable auto-ack
  maxRetries: 3,
});
```

### 8. Graceful Shutdown

```typescript
import createBus from '@moj/bus-rabbitmq';

const bus = createBus();

// Register listeners...
await bus.addEventListener('UserManagement', 'UserCreated', handler);

// Handle shutdown signals
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  
  // Unsubscribe from all listeners (if you kept references)
  // await unsubscribe();
  
  // Close the connection
  await bus.shutdown();
  
  process.exit(0);
});
```

## Advanced Configuration

### Custom Tracer, Logger, and Propagator

```typescript
import createBus, { WaveTracer, WaveLogger, WavePropagator } from '@moj/bus-rabbitmq';

class CustomLogger implements WaveLogger {
  info(key: string, data: any) {
    console.log(`[INFO] ${key}:`, data);
  }
  
  error(key: string, data: any) {
    console.error(`[ERROR] ${key}:`, data);
  }
}

class CustomTracer implements WaveTracer {
  trace(name: string, fn: () => Promise<any>) {
    console.log(`[TRACE] Starting: ${name}`);
    return fn().finally(() => console.log(`[TRACE] Finished: ${name}`));
  }
}

const bus = createBus(
  { url: 'amqp://localhost:5672' },
  new CustomTracer(),
  new CustomLogger(),
  new CustomPropagator()
);
```

### Connection Configuration

```typescript
const bus = createBus({
  url: process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',
  prefetchCount: 10,  // Number of messages to prefetch
  reconnect: true,    // Enable automatic reconnection
});
```

## Message Types Reference

### WaveBaseMessage

All message types share these common fields:

```typescript
interface WaveBaseMessage<TPayload = unknown, TContext = unknown> {
  namespace: string;      // Bounded context, e.g., "Billing"
  name: string;           // Logical name, e.g., "CreateInvoice"
  payload: TPayload;      // Business data
  context?: TContext;     // Out-of-band metadata
}
```

### Event Message

```typescript
interface WaveEventMessage<TPayload> {
  kind: 'event';
  namespace: string;
  name: string;
  payload: TPayload;
  context?: Record<string, any>;
}
```

### Command Message

```typescript
interface WaveCommandMessage<TPayload, TResponse, TContext> {
  kind: 'command';
  namespace: string;
  name: string;
  payload: TPayload;
  awaitResponse?: boolean;  // Enable RPC-style response
  context?: TContext;
}
```

### Query Message

```typescript
interface WaveQueryMessage<TPayload, TResponse, TContext> {
  kind: 'query';
  namespace: string;
  name: string;
  payload: TPayload;
  context?: TContext;
}
```

## Best Practices

1. **Use meaningful namespaces** - Organize by bounded context (e.g., `Billing`, `UserManagement`)
2. **Version your messages** - Consider adding version to the message name for breaking changes
3. **Set appropriate timeouts** - Always configure `timeoutMs` for commands and queries
4. **Handle errors gracefully** - Use try-catch and implement proper retry logic
5. **Implement graceful shutdown** - Always clean up resources on application termination
6. **Monitor your bus** - Use the built-in logging and OpenTelemetry integration

## Examples

See the `examples/` directory for complete working examples:

- `examples/rpc/command-rpc-example.ts` - Command RPC with response handling
- `examples/rpc/advanced-rpc-example.ts` - Advanced RPC scenarios

## API Reference

### Transport Methods

| Method | Description |
|--------|-------------|
| `sendEvent<T>(message)` | Publish a fire-and-forget event |
| `sendCommand<T, R>(message, options?)` | Send a command, optionally waiting for response |
| `sendQuery<T, R>(message, options?)` | Send a query and await response |
| `addEventListener(ns, name, handler, options?)` | Register event listener |
| `addCommandListener(ns, name, handler, options?)` | Register command listener |
| `addQueryListener(ns, name, handler, options?)` | Register query listener |
| `shutdown()` | Gracefully close the connection |

### Listener Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `autoAck` | boolean | `true` | Auto-acknowledge after successful processing |
| `maxRetries` | number | `0` | Maximum retry attempts |
| `retryDelayMs` | number | `1000` | Base delay between retries (exponential backoff) |

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting pull requests.

## License

This project is proprietary and confidential.

## Support

For issues and questions, please open an issue on the repository.
## Context Structure

Wave messages support a structured context system for DDD aggregate tracking and cross-cutting concerns.

### Base Context Fields

```typescript
interface WaveBaseContext {
  // DDD Aggregate Tracking
  aggregateId?: string;           // ID of the aggregate (e.g., "invoice-123")
  aggregateType?: string;         // Type of aggregate (e.g., "Invoice")
  
  // Event Subscription Metadata
  events?: string[];              // List of event names sender is interested in
  
  // Cross-Cutting Concerns
  correlationId?: string;         // Correlation ID for request tracing
  causationId?: string;           // ID of the message that caused this one
  userId?: string;                // User who initiated the action
  tenantId?: string;              // Multi-tenant isolation identifier
  
  // Extensibility
  [key: string]: any;             // Allow custom fields
}
```

### Using Context

```typescript
import createBus from '@moj/bus-rabbitmq';

const bus = createBus();

// Send event with context
await bus.sendEvent({
  kind: 'event',
  namespace: 'Billing.Invoice',
  name: 'InvoiceCreated',
  payload: {
    invoiceId: 'INV-001',
    amount: 100
  },
  context: {
    aggregateId: 'invoice-123',
    aggregateType: 'Invoice',
    events: ['PaymentProcessed', 'InvoiceEmailed'],
    correlationId: 'corr-456',
    userId: 'user-001',
    tenantId: 'tenant-001'
  }
});
```

Context fields are mapped to CloudEvents `extensions`:

```json
{
  "specversion": "1.0",
  "id": "uuid-123",
  "source": "wave://Billing",
  "type": "wave.event.Billing.InvoiceCreated",
  "data": { "invoiceId": "INV-001", "amount": 100 },
  "extensions": {
    "aggregateId": "invoice-123",
    "aggregateType": "Invoice",
    "events": ["PaymentProcessed", "InvoiceEmailed"],
    "correlationId": "corr-456",
    "userId": "user-001",
    "tenantId": "tenant-001",
    "traceparent": "00-trace-id-span-id-01"
  }
}
```

For complete documentation, see [Context Structure Guide](./docs/CONTEXT_STRUCTURE.md).
