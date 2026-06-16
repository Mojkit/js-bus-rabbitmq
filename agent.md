# CLAUDE.md - RabbitMQ Bus Package for Wave Framework

## Project Overview

This is the RabbitMQ driver package for the **Wave** framework's bus system. Wave is a large-scale microservice framework built on **DDD** (Domain-Driven Design) and **CQRS** (Command-Query Responsibility Segregation) principles. Each service in the Wave ecosystem connects to other services via this RabbitMQ-based bus.

**Package:** `@wave/bus-rabbitmq`  
**Language:** TypeScript  
**Purpose:** Message bus implementation using RabbitMQ for distributed microservice communication

## Architecture

### Core Concepts

1. **Wave Transport Pattern**: A transport abstraction layer that provides a consistent interface for message sending and receiving, decoupled from the underlying messaging infrastructure.

2. **Message Types**:
   - **Events**: Fire-and-forget notifications (e.g., `userRegistered`)
   - **Commands**: Request-response operations with optional acknowledgments (e.g., `createInvoice`)
   - **Queries**: Synchronous data retrieval operations (e.g., `getUserById`)

3. **CloudEvents**: All messages follow the CloudEvents specification for standardized event format across services.

4. **OpenTelemetry Integration**: Built-in distributed tracing support for end-to-end visibility across services.

5. **NameSpace**: The bounded contexts and aggreage. some examples:

- UserManagement.User: "UserManagement" is bounded context and "User" is aggregate.
- UserManagement.Communication.Email: "Communication" is a bounded context inside "UserManagement" bounded context, and "Email" is aggregate.

### Key Files and Structure

```
src/
├── index.ts                    # Main export - createBus factory function
├── WaveTransport.ts            # Core transport interface definition
├── RabbitMQBase.ts             # Base class for RabbitMQ connection management
├── RabbitMQWaveTransport.ts    # Main implementation of WaveTransport using RabbitMQ
├── Logging.ts                  # Wave logging abstraction interface
├── Propagation.ts              # Trace context propagation abstraction
└── Tracing.ts                  # OpenTelemetry tracing abstraction
```

### Module Dependencies

- **rabbitmq-client**: Primary RabbitMQ client library for connection and message handling
- **@opentelemetry/api**: OpenTelemetry API for distributed tracing
- **typescript**: Type checking and compilation

## Core Interfaces

### WaveTransport
The main interface that all bus implementations must follow:

```typescript
interface WaveTransport {
  sendEvent<TPayload>(message: WaveEventMessage<TPayload>): Promise<void>;
  sendCommand<TPayload, TResponse>(
    message: WaveCommandMessage<TPayload, TResponse>,
    options?: WaveSendOptions
  ): Promise<TResponse | void>;
  sendQuery<TPayload, TResponse>(
    message: WaveQueryMessage<TPayload, TResponse>,
    options?: WaveSendOptions
  ): Promise<TResponse>;
  addEventListener<TPayload>(
    namespace: string,
    name: string,
    handler: WaveHandler<WaveEventMessage<TPayload>>,
    options?: ListenerOptions
  ): Promise<Unsubscribe>;
  addCommandListener<TPayload, TResponse>(
    namespace: string,
    name: string,
    handler: WaveHandler<WaveCommandMessage<TPayload, TResponse>>,
    options?: ListenerOptions
  ): Promise<Unsubscribe>;
  addQueryListener<TPayload, TResponse>(
    namespace: string,
    name: string,
    handler: WaveHandler<WaveQueryMessage<TPayload, TResponse>>,
    options?: ListenerOptions
  ): Promise<Unsubscribe>;
  shutdown(): Promise<void>;
}
```

### Message Types

```typescript
interface WaveBaseMessage<TPayload, TContext> {
  namespace: string;      // Bounded context (e.g., "Billing", "UserManagement")
  name: string;           // Message name (e.g., "CreateInvoice")
  payload: TPayload;      // Business data
  context?: TContext;     // Metadata (user, correlation IDs, etc.)
}

interface WaveEventMessage<TPayload> extends WaveBaseMessage<TPayload> {
  kind: 'event';
}

interface WaveCommandMessage<TPayload, TResponse> extends WaveBaseMessage<TPayload> {
  kind: 'command';
  awaitResponse?: boolean;  // Enable RPC-style response
}

interface WaveQueryMessage<TPayload, TResponse> extends WaveBaseMessage<TPayload> {
  kind: 'query';
}
```

### Execution Context

```typescript
interface ExecutionContext {
  ack(): Promise<void>;          // Acknowledge message processing
  nack(requeue?: boolean): Promise<void>;  // Negative acknowledge
  message: Record<string, any>;  // Raw message metadata
}
```

## Configuration

### Basic Setup

```typescript
import createBus from '@wave/bus-rabbitmq';

// Default configuration (uses RABBITMQ_URL env or localhost)
const bus = createBus();

// Custom configuration
const bus = createBus({
  url: process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',
  prefetchCount: 10,
  reconnect: true,
});
```

### Advanced Configuration with Tracing and Logging

```typescript
import createBus, {
  WaveTracer,
  WaveLogger,
  WavePropagator
} from '@moj/bus-rabbitmq';

const bus = createBus(
  { url: 'amqp://localhost:5672' },
  new CustomTracer(),    // OpenTelemetry-backed tracer
  new CustomLogger(),    // Your logging implementation
  new CustomPropagator() // Trace context propagator
);
```

## Usage Patterns

### 1. Sending Events

```typescript
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
```

### 2. Handling Events

```typescript
await bus.addEventListener('UserManagement', 'userCreated', async (event, context) => {
  console.log('User created:', event);
  
  // Your business logic here
  await sendWelcomeEmail(event.email);
  
  // Manual acknowledgment (if autoAck: false)
  // await context.ack();
}, {
  autoAck: true,
  maxRetries: 3,
  retryDelayMs: 1000,
});
```

### 3. Sending Commands (RPC Style)

```typescript
interface CreateInvoiceCommand {
  customerId: string;
  items: Array<{ productId: string; quantity: number; price: number }>;
}

interface InvoiceResponse {
  invoiceId: string;
  status: 'created' | 'failed';
}

const response = await bus.sendCommand<CreateInvoiceCommand, InvoiceResponse>(
  {
    kind: 'command',
    namespace: 'Billing',
    name: 'CreateInvoice',
    payload: {
      customerId: 'cust-456',
      items: [{ productId: 'prod-1', quantity: 2, price: 29.99 }],
    },
    awaitResponse: true,  // Enable RPC response
  },
  {
    timeoutMs: 10000,  // 10 second timeout
  }
);
```

### 4. Handling Commands

```typescript
await bus.addCommandListener('Billing', 'createInvoice', async (command, context) => {
  console.log('Processing invoice:', command);
  
  const invoiceId = `INV-${Date.now()}`;
  
  // Return response - automatically sent back to caller
  return {
    invoiceId,
    status: 'created',
  };
}, {
  autoAck: true,
  maxRetries: 3,
});
```

### 5. Sending Queries

```typescript
interface GetUserQuery {
  userId: string;
}

interface UserResponse {
  id: string;
  name: string;
  email: string;
}

const user = await bus.sendQuery<GetUserQuery, UserResponse>(
  {
    kind: 'query',
    namespace: 'UserManagement',
    name: 'getUser',
    payload: { userId: 'usr-123' },
  },
  {
    timeoutMs: 5000,
  }
);
```

### 6. Graceful Shutdown

```typescript
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await bus.shutdown();
  process.exit(0);
});
```

## Connection Management

The package uses the `RabbitMQBase` class which handles:
- Automatic reconnection
- Connection status tracking
- Graceful shutdown
- Connection error handling

```typescript
const bus = createBus();

// Check connection status
if (bus.isConnected()) {
  console.log('Connected to:', bus.getConnectionUrl());
}

// Force reconnection
await bus.ensureConnected();

// Graceful shutdown
await bus.shutdown();
```

## RabbitMQ Configuration

The package configures the following RabbitMQ entities:

### Exchanges

Each first level namespace has it's own exchange. ex: "wave.UserManagement". If the namespace is not defined, the exchange can be one these:

- **eventPublisher**: For publishing events (topic exchange)
- **commandPublisher**: For publishing commands (topic exchange)
- **queryClient**: For query responses (direct exchange)

### Queues
- Event listener queues (auto-declared, unique per listener)
- Command listener queues (auto-declared, unique per listener)
- Query listener queues (auto-declared, unique per listener)
- Temporary reply queues for RPC responses

### Message Routing
Messages are routed using `namespace.name` as the routing key, allowing flexible subscription patterns.

## Testing

### Running Tests

```bash
# Run all tests
bun test

# Run in watch mode
bun test --watch

# Run specific test file
bun test __tests__/unit/ListenerRegistration.test.ts

# Run integration tests with Docker
docker-compose -f docker-compose.test.yml up -d
bun test __tests__/integration/RabbitMQIntegration.test.ts
docker-compose -f docker-compose.test.yml down
```

### Test Structure

```
__tests__/
├── mocks/           # Mock utilities (logger, tracer, propagator)
├── unit/            # Unit tests for message types, utilities
├── integration/     # Integration tests with real RabbitMQ
└── QUICK_REFERENCE.md  # Testing guide
```

## Best Practices

1. **Use meaningful namespaces**: Organize by bounded context (e.g., `Billing`, `UserManagement`)
2. **Set appropriate timeouts**: Always configure `timeoutMs` for commands and queries
3. **Implement graceful shutdown**: Always clean up resources on application termination
4. **Monitor your bus**: Use the built-in logging and OpenTelemetry integration
5. **Handle errors gracefully**: Use try-catch and implement proper retry logic
6. **Version your messages**: Consider adding version to message names for breaking changes

## Common Patterns

### Manual Acknowledgment

```typescript
await bus.addEventListener('Notifications', 'EmailSent', async (event, context) => {
  try {
    await processEmailEvent(event);
    await context.ack();  // Manual ACK
  } catch (error) {
    await context.nack(false);  // Send to DLQ
  }
}, {
  autoAck: false,
  maxRetries: 3,
});
```

### Custom Logger

```typescript
class CustomLogger implements WaveLogger {
  info(message: string, meta?: Record<string, unknown>) {
    console.log(`[INFO] ${message}:`, meta);
  }
  
  error(message: string, meta?: Record<string, unknown>) {
    console.error(`[ERROR] ${message}:`, meta);
  }
}
```

### Distributed Tracing

```typescript
class CustomTracer implements WaveTracer {
  async startActiveSpan<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const span = tracer.startSpan(name);
    try {
      return await fn();
    } finally {
      span.end();
    }
  }
}
```

## Dependencies

### External Dependencies
- **rabbitmq-client** (^5.0.8): RabbitMQ connection and messaging library
- **@opentelemetry/api** (^1.9.0): OpenTelemetry API for tracing

### Development Dependencies
- **bun**: Test runner and runtime
- **typescript**: Type checking
- **tsup**: Bundling and build
- **eslint** & **prettier**: Code qualmojity

## Related Packages

This is part of the Wave framework ecosystem:
- **Wave core framework**: DDD/CQRS microservice framework
- **Other bus drivers**: Additional transport implementations may exist
- **Wave packages**: Core framework packages that provide logging, tracing, and propagator implementations

## Troubleshooting

### Connection Issues
- Verify RabbitMQ is running: `docker-compose -f docker-compose.test.yml ps`
- Check connection URL: `amqp://guest:guest@localhost:5672`
- Review logs for connection error details

### Message Not Delivered
- Check exchange and queue declarations
- Verify routing keys match subscription patterns
- Ensure DLQ is configured for retry scenarios

### Performance Issues
- Adjust `prefetchCount` based on throughput requirements
- Consider manual acknowledgment for heavy processing
- Monitor RabbitMQ queue depths and message rates

## Contributing

When adding new features:
1. Follow existing patterns in `RabbitMQWaveTransport.ts`
2. Add comprehensive tests in `__tests__/`
3. Update documentation as needed
4. Ensure backward compatibility

---

*For detailed API documentation, see README.md*  
*For testing details, see __tests__/QUICK_REFERENCE.md*
