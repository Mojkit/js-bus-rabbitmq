# Error Handling in RPC Pattern

This document describes how errors are propagated across service boundaries in the Wave RPC pattern.

## Overview

The Wave bus implements a two-layer error handling approach:

1. **Transport Layer** (`RabbitMQWaveTransport`): Serializes exceptions into CloudEvent-compatible wire format
2. **Core Layer** (`core/bus/listeners`): Transforms domain errors into serializable format with business context

This design ensures:
- Protocol-level consistency for all error responses
- Rich domain context in error messages
- Transport-agnostic error representation
- Minimal coupling between layers

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Service A (Handler)                                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Domain Handler                                             │
│  └─> throws ValidationError (extends SerializableError)    │
│                                                             │
│  Core Layer (listeners.ts)                                 │
│  └─> Catches error                                          │
│  └─> Adds domain context (namespace, command, userId)      │
│  └─> Re-throws SerializableError                           │
│                                                             │
│  Transport Layer (RabbitMQWaveTransport)                   │
│  └─> Catches error                                          │
│  └─> Serializes to wire format:                            │
│      {                                                      │
│        kind: 'command-response',                            │
│        correlationId: '...',                                │
│        error: {                                             │
│          type: 'ValidationError',                           │
│          message: 'Invalid email format',                   │
│          code: 'VALIDATION_ERROR',                          │
│          context: { field: 'email', ... }                   │
│        }                                                    │
│      }                                                      │
│  └─> Sends error response via reply queue                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ RabbitMQ
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ Service B (Caller)                                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Transport Layer (RabbitMQWaveTransport)                   │
│  └─> Receives error response                               │
│  └─> Detects error field in response                       │
│  └─> Throws RemoteServiceError with error data             │
│                                                             │
│  Application Code                                           │
│  └─> Catches RemoteServiceError                            │
│  └─> Handles based on error.code                           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Error Types

### SerializableError (Service A - Domain Layer)

Base class for domain errors that can be serialized across service boundaries.

```typescript
import { SerializableError } from '@wave/core/bus';

class ValidationError extends SerializableError {
  constructor(message: string, field: string) {
    super(message, 'VALIDATION_ERROR', { field });
  }
}

// Usage in handler
async function createUserHandler(message, context) {
  if (!message.email.includes('@')) {
    throw new ValidationError('Invalid email format', 'email');
  }
  // ... business logic
}
```

### RemoteServiceError (Service B - Client Side)

Error thrown when a remote service returns an error response.

```typescript
try {
  const result = await app.UserManagement.CreateUser({ email: 'invalid' });
} catch (error) {
  if (error instanceof RemoteServiceError) {
    console.error('Remote error:', {
      type: error.type,        // 'ValidationError'
      message: error.message,  // 'Invalid email format'
      code: error.code,        // 'VALIDATION_ERROR'
      context: error.context,  // { field: 'email', domain: 'UserManagement', ... }
      remoteStack: error.remoteStack // Stack trace (dev mode only)
    });
  }
}
```

## Wire Format

Errors are serialized into the following structure:

```typescript
interface WaveErrorResponse {
  type: string;           // Error class name (e.g., 'ValidationError')
  message: string;        // Human-readable error message
  code?: string;          // Machine-readable error code (e.g., 'VALIDATION_ERROR')
  context?: Record<string, any>;  // Additional error context
  stack?: string;         // Stack trace (only in development mode)
}
```

Example error response:

```json
{
  "kind": "command-response",
  "correlationId": "abc-123",
  "error": {
    "type": "ValidationError",
    "message": "Invalid email format",
    "code": "VALIDATION_ERROR",
    "context": {
      "field": "email",
      "domain": "UserManagement",
      "command": "CreateUser",
      "userId": "user-456"
    },
    "stack": "Error: Invalid email format\n    at ..."
  },
  "timestamp": "2025-05-21T10:00:00.000Z"
}
```

## Usage Examples

### 1. Define Domain Errors

```typescript
// domains/UserManagement/errors.ts
import { SerializableError } from '@wave/core/bus';

export class ValidationError extends SerializableError {
  constructor(message: string, field: string, value?: any) {
    super(message, 'VALIDATION_ERROR', { field, value });
  }
}

export class NotFoundError extends SerializableError {
  constructor(resourceType: string, resourceId: string) {
    super(
      `${resourceType} with id ${resourceId} not found`,
      'NOT_FOUND',
      { resourceType, resourceId }
    );
  }
}

export class UnauthorizedError extends SerializableError {
  constructor(action: string, resource: string) {
    super(
      `Not authorized to ${action} ${resource}`,
      'UNAUTHORIZED',
      { action, resource }
    );
  }
}
```

### 2. Throw Errors in Handlers

```typescript
// domains/UserManagement/commands/CreateUser.ts
import { ValidationError, UnauthorizedError } from '../errors';

export async function createUserHandler(message, context) {
  const { email, name } = message;

  // Validation
  if (!email?.includes('@')) {
    throw new ValidationError('Invalid email format', 'email', email);
  }

  // Authorization
  if (!context.busMessage.userId) {
    throw new UnauthorizedError('create', 'user');
  }

  // Business logic
  return await createUser({ email, name });
}
```

### 3. Handle Errors on Client Side

```typescript
// Service B calling Service A
import { RemoteServiceError } from '@wave/core/bus';

async function registerUser(email: string, name: string) {
  try {
    const user = await app.UserManagement.CreateUser({ email, name });
    return { success: true, user };
  } catch (error) {
    if (error instanceof RemoteServiceError) {
      switch (error.code) {
        case 'VALIDATION_ERROR':
          return {
            success: false,
            error: 'Invalid input',
            field: error.context?.field,
          };

        case 'UNAUTHORIZED':
          return {
            success: false,
            error: 'Please log in to continue',
          };

        case 'NOT_FOUND':
          return {
            success: false,
            error: 'Resource not found',
          };

        default:
          return {
            success: false,
            error: 'An unexpected error occurred',
          };
      }
    }
    
    // Handle local errors
    throw error;
  }
}
```

## Best Practices

### 1. Always Extend SerializableError for Domain Errors

```typescript
// ✅ Good
class ValidationError extends SerializableError {
  constructor(message: string, field: string) {
    super(message, 'VALIDATION_ERROR', { field });
  }
}

// ❌ Bad - won't serialize properly
class ValidationError extends Error {
  constructor(message: string) {
    super(message);
  }
}
```

### 2. Use Specific Error Codes

```typescript
// ✅ Good - enables programmatic handling
throw new SerializableError('Invalid email', 'VALIDATION_ERROR', { field: 'email' });

// ❌ Bad - client can't distinguish error types
throw new Error('Invalid email');
```

### 3. Include Relevant Context

```typescript
// ✅ Good - provides debugging information
throw new NotFoundError('User', userId);

// ❌ Bad - missing context
throw new Error('Not found');
```

### 4. Handle Errors Gracefully on Client Side

```typescript
// ✅ Good - specific error handling
try {
  await app.Service.Command(payload);
} catch (error) {
  if (error instanceof RemoteServiceError) {
    if (error.code === 'NOT_FOUND') {
      // Fallback logic
      return defaultValue;
    }
  }
  throw error;
}

// ❌ Bad - swallowing all errors
try {
  await app.Service.Command(payload);
} catch (error) {
  console.error(error);
  return null;
}
```

### 5. Don't Expose Sensitive Information

```typescript
// ✅ Good - no sensitive data
throw new UnauthorizedError('access', 'user-profile');

// ❌ Bad - exposes internal details
throw new Error(`Database connection failed: ${dbPassword}`);
```

## Error Transparency vs Coupling

### Error Transparency

The two-layer approach provides **maximum transparency**:

- **Protocol metadata** (from transport layer): Ensures consistent wire format
- **Business context** (from core layer): Provides domain-specific information
- **Stack traces** (conditional): Included in development mode for debugging

Service B receives structured errors it can programmatically handle without knowing Service A's internal implementation.

### Architectural Coupling

The design maintains **minimal coupling**:

- Transport layer only knows generic error structure (type, message, code, context)
- Core layer transforms domain errors into transport-compatible format
- No direct dependency between domain error types and transport implementation
- Future protocol changes (NATS, gRPC) only require updating transport layer

## Testing

See `__tests__/integration/ErrorPropagation.test.ts` for comprehensive test examples.

```typescript
test('command handler error is propagated to caller', async () => {
  await transport.addCommandListener('Test', 'FailingCommand', async () => {
    throw new ValidationError('Invalid input', 'email');
  });

  try {
    await transport.sendCommand({
      kind: 'command',
      namespace: 'Test',
      name: 'FailingCommand',
      payload: {},
      awaitResponse: true,
    });
    expect.unreachable('Should have thrown');
  } catch (error: any) {
    expect(error.code).toBe('VALIDATION_ERROR');
    expect(error.context.field).toBe('email');
  }
});
```

## Troubleshooting

### Error not being propagated

**Problem**: Error thrown in handler but not received by caller.

**Solution**: Ensure handler is registered with `autoAck: false` or explicitly handles ack/nack.

### Missing error context

**Problem**: Error context is undefined on client side.

**Solution**: Ensure error extends `SerializableError` and context is passed to constructor.

### Stack trace not included

**Problem**: `remoteStack` is undefined.

**Solution**: Stack traces are only included when `NODE_ENV=development`. Set environment variable for debugging.

## Migration Guide

If you have existing error handling code:

1. **Update domain errors** to extend `SerializableError`
2. **Update error handling** to check for `RemoteServiceError`
3. **Add error codes** to enable programmatic handling
4. **Test error propagation** across service boundaries

See `core/examples/error-handling-example.ts` for a complete migration example.
