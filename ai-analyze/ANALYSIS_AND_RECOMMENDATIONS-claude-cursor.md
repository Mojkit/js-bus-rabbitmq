# Wave RabbitMQ Framework - Analysis & Enhancement Recommendations

## Executive Summary

Your Wave framework demonstrates a **solid architectural foundation** for enterprise-grade microservices communication.
The design shows good understanding of CQRS, event-driven architecture, and cloud-native patterns.
Below is a comprehensive analysis with prioritized recommendations.

---

## ✅ Strengths

### 1. **Clean Abstraction Layers**
- Clear separation between `WaveTransport` (interface), `RabbitMQWaveTransport` (implementation), and `RabbitMQBase` (infrastructure)
- Well-designed abstraction for Logging, Propagation, and Tracing - allows swappable implementations
- No vendor lock-in at the Wave protocol level

### 2. **CloudEvents Adoption**
- Choosing CloudEvents v1.0 is excellent for interoperability and standardization
- Proper use of extensions for custom metadata and trace context
- Good separation of concerns in `CloudEvent.ts`

### 3. **CQRS Pattern Clarity**
- Distinct handling of Commands (RPC-style), Events (pub/sub), and Queries (RPC)
- Type-safe generics for payload and response types
- Message context isolation through CloudEvents

### 4. **Telemetry-Ready Design**
- OpenTelemetry hooks through `WaveTracer` interface
- Trace propagation abstraction with `WavePropagator`
- Non-invasive instrumentation points

---

## 🎯 Priority 1: Critical Gaps (Enterprise-Grade Requirements)

### 1.1 **Listener/Handler Registration Missing**
**Issue**: No way to register command/event/query handlers to consume messages.
- `sendCommand`, `sendEvent`, `sendQuery` exist but no corresponding `addCommandListener`, `addEventListener`, `addQueryListener`
- According to diagrams, these should exist but aren't implemented

**Impact**: Framework cannot function as a message broker - only sends, never receives.

**Recommendation**:
```typescript
// Add to WaveTransport interface
interface WaveTransport {
  // Existing...
  
  // New: Listener registration
  addCommandListener<TPayload, TResponse>(
    namespace: string,
    name: string,
    handler: (message: WaveCommandMessage<TPayload>, context: ExecutionContext) => Promise<TResponse | void>,
    options?: ListenerOptions
  ): Promise<() => Promise<void>>; // Returns cancel function

  addEventListener<TPayload>(
    namespace: string,
    name: string,
    handler: (message: WaveEventMessage<TPayload>, context: ExecutionContext) => Promise<void>,
    options?: ListenerOptions
  ): Promise<() => Promise<void>>;

  addQueryListener<TPayload, TResponse>(
    namespace: string,
    name: string,
    handler: (message: WaveQueryMessage<TPayload>, context: ExecutionContext) => Promise<TResponse>,
    options?: ListenerOptions
  ): Promise<() => Promise<void>>;
}
```

### 1.2 **RPC Response Handling Incomplete**
**Issue**: `sendCommand` with `awaitResponse=true` currently returns `void`.

**Current Code**:
```typescript
public async sendCommand<TPayload, TResponse>(
  message: WaveCommandMessage<TPayload, TResponse>,
  _options?: WaveSendOptions,
): Promise<TResponse | void> {
  // ... setup ...
  await this.commandPublisher!.send(...);
  return undefined as TResponse | void; // ❌ Always undefined!
}
```

**Recommendation**: Implement proper RPC handling using reply queues or correlation IDs:
- Use `rabbitmq-client`'s RPC capabilities or implement correlation ID pattern
- Support timeout with proper cleanup
- Handle response routing back to caller

### 1.3 **No Error/Rejection Response Handling**
**Issue**: Command handlers can reject, but no way to return error responses.

**Recommendation**:
```typescript
interface WaveCommandResponse<TResponse> {
  success: true;
  data: TResponse;
}

interface WaveCommandError {
  success: false;
  error: {
    code: string;
    message: string;
    details?: Record<string, unknown>;
  };
}

type CommandResult<TResponse> = WaveCommandResponse<TResponse> | WaveCommandError;
```

---

## 🎯 Priority 2: Important Enhancements

### 2.1 **Connection Lifecycle Issues**
**Issue**: 
- `init()` method doesn't actually connect (just sets `initialized = true`)
- No wait for connection establishment before using publishers
- Race conditions possible between connection and first message

**Recommendation**:
```typescript
public async init(): Promise<void> {
  if (this.initialized) return;
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error('RabbitMQ connection timeout')), 
      this.config.connectionTimeoutMs ?? 30000
    );
    
    const onConnection = () => {
      clearTimeout(timeout);
      this.connection.off('connection', onConnection);
      this.connection.off('error', onError);
      this.initialized = true;
      resolve();
    };
    
    const onError = (err: Error) => {
      clearTimeout(timeout);
      this.connection.off('connection', onConnection);
      this.connection.off('error', onError);
      reject(err);
    };
    
    this.connection.on('connection', onConnection);
    this.connection.on('error', onError);
  });
}
```

### 2.2 **Dead Letter Queue (DLQ) Strategy**
**Issue**: No handling of failed messages (retries, poison pill handling)

**Recommendation**:
- Add DLQ for each exchange (events, commands)
- Implement retry logic with exponential backoff
- Add metrics/logs for DLQ messages
- Configuration for DLQ retention/TTL

```typescript
interface RabbitMQConfig {
  // ... existing ...
  dlq?: {
    enabled: boolean;
    maxRetries: number;
    backoffMs: number;
    backoffMultiplier: number;
  };
}
```

### 2.3 **Message Acknowledgment Strategy**
**Issue**: No explicit ACK/NACK handling configuration

**Recommendation**:
- Auto-ACK vs Manual ACK modes
- Negative acknowledgment (NACK) with requeue option
- Per-listener configuration

```typescript
interface ListenerOptions {
  autoAck?: boolean; // Default: false (manual ack required)
  maxRetries?: number;
  retryDelayMs?: number;
}

interface ExecutionContext {
  ack(): Promise<void>;
  nack(requeue?: boolean): Promise<void>;
}
```

### 2.4 **Event Subscriber Validation**
**Issue**: In pub/sub pattern, event is published even if no subscribers exist

**Current behavior**: Message goes to queue but nobody's listening

**Recommendation**:
- Add optional `validateSubscribers` mode
- Track active listeners per namespace.name
- Could warn if publishing to topic with no subscribers

---

## 🎯 Priority 3: Enterprise Features

### 3.1 **Message Ordering Guarantees**
- Implement per-partition ordering (using routing key as partition key)
- Document ordering guarantees for each message type
- Consider adding `partitionKey` to message context

### 3.2 **Dead Letter Exchange (DLX) Pattern**
- Implement automatic DLX binding
- Separate queues for errors vs timeouts vs processing failures
- Monitoring/alerting hooks for DLQ messages

### 3.3 **Distributed Tracing Enhancement**
- Support for OpenTelemetry baggage (not just trace context)
- Automatic context propagation across async boundaries
- Span link support for fan-out scenarios

### 3.4 **Message Encryption & Signing**
- Consider built-in support for message encryption
- Digital signature verification for sensitive operations
- Hook for custom encryption providers

### 3.5 **Schema Registry Integration**
- Optional integration with schema registry (Confluent/AWS/etc)
- Payload validation before processing
- Schema versioning support

---

## 🔧 Priority 4: Developer Experience

### 4.1 **Type-Safe Handler Registration**
**Current**: Handlers are just functions with loose typing

**Recommendation**: Provide typed decorators (if using classes):
```typescript
@CommandHandler('Billing', 'CreateInvoice')
class CreateInvoiceHandler {
  async handle(
    cmd: WaveCommandMessage<CreateInvoicePayload>
  ): Promise<InvoiceCreatedEvent> {
    // Handler implementation
  }
}
```

### 4.2 **Comprehensive Testing Utilities**
- In-memory bus implementation for unit tests
- Test fixtures and mocks
- Message capture/assertion helpers
- Performance testing tools

### 4.3 **Better Documentation**
- Add examples to README (currently empty)
- Per-use-case documentation
- Migration guides for future versions
- Troubleshooting guide

### 4.4 **Metrics & Observability**
- Message count/latency metrics
- Error rate tracking
- Dead letter monitoring
- Queue depth tracking

---

## 🐛 Priority 5: Code Quality Issues

### 5.1 **TypeScript Strictness**
- Enable `strict: true` if not already
- Fix `@typescript-eslint/no-unused-vars` false positive in `NoopWaveTracer`
- Add proper error types instead of generic `Error`

### 5.2 **Error Handling**
- Create custom error hierarchy:
  ```typescript
  class WaveTransportError extends Error {
    constructor(public code: string, message: string) { ... }
  }
  class CommandTimeoutError extends WaveTransportError { ... }
  class ConnectionError extends WaveTransportError { ... }
  ```

### 5.3 **Resource Cleanup**
- Ensure publishers/RPC clients are properly cleaned up
- Add `.catch(() => undefined)` pattern consistently
- Consider using AbortController for timeout handling

### 5.4 **Configuration Validation**
- Validate `RabbitMQConfig` on instantiation
- Provide sensible defaults
- Add `prefetchCount` validation (should be > 0)

---

## 📊 Recommended File Structure

```
src/
  index.ts                    // Current
  WaveTransport.ts            // Current
  
  transport/
    WaveTransport.ts          // Move interface + types
    RabbitMQWaveTransport.ts  // Current
    RabbitMQBase.ts           // Current
    
  core/
    CloudEvent.ts             // Current
    Message.ts                // New: Message types
    Response.ts               // New: Response handling
    
  handlers/
    Listener.ts               // New: Listener interface
    CommandListener.ts        // New: Command handler
    EventListener.ts          // New: Event handler
    QueryListener.ts          // New: Query handler
    
  telemetry/
    Logging.ts                // Current
    Tracing.ts                // Current
    Propagation.ts            // Current
    Metrics.ts                // New: Metrics collection
    
  errors/
    WaveError.ts              // New: Error hierarchy
    
  utils/
    RetryPolicy.ts            // New: Retry logic
    CircuitBreaker.ts         // New: Circuit breaker pattern
```

---

## 🚀 Phased Implementation Roadmap

### Phase 1: Foundation (v0.1.0)
- [ ] Implement listener registration (Priority 1.1)
- [ ] Fix RPC response handling (Priority 1.2)
- [ ] Add error response support (Priority 1.3)
- [ ] Fix connection lifecycle (Priority 2.1)

### Phase 2: Reliability (v0.2.0)
- [ ] DLQ implementation (Priority 2.2)
- [ ] ACK/NACK handling (Priority 2.3)
- [ ] Comprehensive error handling (Priority 5.2)
- [ ] Timeout and retry logic

### Phase 3: Enterprise (v0.3.0)
- [ ] Message ordering guarantees (Priority 3.1)
- [ ] Message encryption (Priority 3.4)
- [ ] Metrics collection (Priority 4.4)
- [ ] Schema validation (Priority 3.5)

### Phase 4: Polish (v1.0.0)
- [ ] Complete documentation (Priority 4.3)
- [ ] Testing utilities (Priority 4.2)
- [ ] Performance optimization
- [ ] Changelog + migration guides

---

## 💡 Key Architectural Decisions to Consider

1. **Event Sourcing Integration**: How will this integrate with event store? Consider adding event envelope types.

2. **Saga Pattern**: For distributed transactions, consider adding saga orchestration helpers.

3. **Circuit Breaker**: Should be built-in for resilience given enterprise focus.

4. **Outbox Pattern**: For guaranteed message delivery, consider built-in outbox support.

5. **Polyglot Support**: Current design is TypeScript-only. Consider protocol compatibility for other languages.

---

## 📋 Quick Start Implementation Plan

Start with these 3 items to make the framework actually functional:

```typescript
// 1. Add minimal listener support
interface ListenerHandler<T> {
  (message: T, context: { ack(): void; nack(): void }): Promise<void>;
}

// 2. Add proper command response handling
async sendCommand<TPayload, TResponse>(
  message: WaveCommandMessage<TPayload, TResponse>,
  options?: WaveSendOptions,
): Promise<TResponse> {
  // Implement correlation ID + reply queue pattern
}

// 3. Fix connection initialization
async init(): Promise<void> {
  // Wait for actual connection before resolving
}
```

---

## Conclusion

Your Wave framework has **excellent conceptual foundations**. The main gap is **lack of message consumption/handler registration** - which is essential for a message broker framework.

**Next steps**:
1. Implement listener registration (blocking issue)
2. Fix RPC/response handling
3. Add proper connection lifecycle
4. Build comprehensive tests
5. Document with examples

The framework is on track to be a solid enterprise solution. Focus on Phase 1 to make it functionally complete, then phase in enterprise features.

---

*Generated: 2026-02-27*
