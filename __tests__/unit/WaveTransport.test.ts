/**
 * @file WaveTransport.test.ts - Unit tests for WaveTransport types, interfaces, and contracts.
 *
 * Tests the core message types, interfaces, and handler types defined in WaveTransport.ts
 * without requiring a RabbitMQ connection. These are pure TypeScript type and structure tests.
 */

import { test, expect, describe } from 'bun:test';
import type {
  WaveBaseMessage,
  WaveCommandMessage,
  WaveEventMessage,
  WaveQueryMessage,
  WaveSendOptions,
  ListenerOptions,
  ExecutionContext,
  WaveHandler,
  Unsubscribe,
  BaseMessageHandler,
} from '../../src/WaveTransport';

describe('WaveTransport - Message Type Definitions', () => {
  describe('WaveBaseMessage', () => {
    test('base message requires namespace, name, kind, and payload', () => {
      const message: WaveBaseMessage<{ value: string }> = {
        namespace: 'Billing.Invoice',
        name: 'InvoiceCreated',
        kind: 'event',
        payload: { value: 'test' },
      };

      expect(message.namespace).toBe('Billing.Invoice');
      expect(message.name).toBe('InvoiceCreated');
      expect(message.kind).toBe('event');
      expect(message.payload).toEqual({ value: 'test' });
    });

    test('base message with context', () => {
      const message: WaveBaseMessage<
        { id: string },
        { userId: string; tenantId: string }
      > = {
        namespace: 'Users.Auth',
        name: 'LoginAttempt',
        kind: 'event',
        payload: { id: 'evt-123' },
        context: {
          userId: 'user-456',
          tenantId: 'tenant-789',
        },
      };

      expect(message.context?.userId).toBe('user-456');
      expect(message.context?.tenantId).toBe('tenant-789');
    });

    test('base message without context is valid', () => {
      const message: WaveBaseMessage = {
        namespace: 'System',
        name: 'HealthCheck',
        kind: 'event',
        payload: { status: 'ok' },
      };

      expect(message.context).toBeUndefined();
    });

    test('base message supports generic payload types', () => {
      const message: WaveBaseMessage<{ items: string[]; total: number }> = {
        namespace: 'Inventory',
        name: 'StockUpdate',
        kind: 'event',
        payload: { items: ['item1', 'item2'], total: 2 },
      };

      expect(message.payload.items).toHaveLength(2);
      expect(message.payload.total).toBe(2);
    });

    test('base message with empty strings for namespace/name', () => {
      const message: WaveBaseMessage = {
        namespace: '',
        name: '',
        kind: 'event',
        payload: {},
      };

      expect(message.namespace).toBe('');
      expect(message.name).toBe('');
    });
  });

  describe('WaveCommandMessage', () => {
    test('command message has kind "command"', () => {
      const message: WaveCommandMessage = {
        kind: 'command',
        namespace: 'Billing',
        name: 'ProcessPayment',
        payload: { amount: 100 },
      };

      expect(message.kind).toBe('command');
    });

    test('command with awaitResponse true expects response', () => {
      const message: WaveCommandMessage<
        { invoiceId: string },
        { success: boolean }
      > = {
        kind: 'command',
        namespace: 'Billing',
        name: 'ProcessPayment',
        payload: { invoiceId: 'inv-123' },
        awaitResponse: true,
      };

      expect(message.awaitResponse).toBe(true);
    });

    test('command with awaitResponse false is fire-and-forget', () => {
      const message: WaveCommandMessage = {
        kind: 'command',
        namespace: 'Notifications',
        name: 'SendEmail',
        payload: { to: 'user@example.com' },
        awaitResponse: false,
      };

      expect(message.awaitResponse).toBe(false);
    });

    test('command without awaitResponse property', () => {
      const message: WaveCommandMessage = {
        kind: 'command',
        namespace: 'Billing',
        name: 'RecordTransaction',
        payload: { transactionId: 'txn-456' },
      };

      expect(message.awaitResponse).toBeUndefined();
    });

    test('command extends base message fields', () => {
      const message: WaveCommandMessage<{ data: string }, { result: string }> =
        {
          kind: 'command',
          namespace: 'QueryService',
          name: 'GetUser',
          payload: { data: 'user-123' },
          awaitResponse: true,
          context: { correlationId: 'corr-789' },
        };

      expect(message.namespace).toBe('QueryService');
      expect(message.name).toBe('GetUser');
      expect(message.context?.correlationId).toBe('corr-789');
    });
  });

  describe('WaveEventMessage', () => {
    test('event message has kind "event"', () => {
      const message: WaveEventMessage = {
        kind: 'event',
        namespace: 'Orders',
        name: 'OrderCreated',
        payload: { orderId: 'ord-123' },
      };

      expect(message.kind).toBe('event');
    });

    test('event with payload', () => {
      const message: WaveEventMessage<{ orderId: string; total: number }> = {
        kind: 'event',
        namespace: 'Orders',
        name: 'OrderCreated',
        payload: { orderId: 'ord-123', total: 250.0 },
      };

      expect(message.payload.orderId).toBe('ord-123');
      expect(message.payload.total).toBe(250.0);
    });

    test('event with context metadata', () => {
      const message: WaveEventMessage<
        { userId: string },
        { tenantId: string; sessionId: string }
      > = {
        kind: 'event',
        namespace: 'Users',
        name: 'UserRegistered',
        payload: { userId: 'user-123' },
        context: {
          tenantId: 'tenant-456',
          sessionId: 'session-789',
        },
      };

      expect(message.context?.tenantId).toBe('tenant-456');
      expect(message.context?.sessionId).toBe('session-789');
    });

    test('event is fire-and-forget (no awaitResponse)', () => {
      const message: WaveEventMessage = {
        kind: 'event',
        namespace: 'Analytics',
        name: 'PageView',
        payload: { page: '/home' },
      };

      // Events should not have awaitResponse
      expect((message as any).awaitResponse).toBeUndefined();
    });

    test('event with complex nested payload', () => {
      const message: WaveEventMessage<{
        user: { id: string; name: string };
        action: string;
        metadata: Record<string, unknown>;
      }> = {
        kind: 'event',
        namespace: 'Activity',
        name: 'UserAction',
        payload: {
          user: { id: 'user-123', name: 'John' },
          action: 'login',
          metadata: { ip: '127.0.0.1', userAgent: 'TestBrowser' },
        },
      };

      expect(message.payload.user.name).toBe('John');
      expect(message.payload.action).toBe('login');
    });
  });

  describe('WaveQueryMessage', () => {
    test('query message has kind "query"', () => {
      const message: WaveQueryMessage = {
        kind: 'query',
        namespace: 'Catalog',
        name: 'GetProduct',
        payload: { id: 'prod-123' },
      };

      expect(message.kind).toBe('query');
    });

    test('query with request/response types', () => {
      const message: WaveQueryMessage<
        { id: string },
        { name: string; price: number }
      > = {
        kind: 'query',
        namespace: 'Catalog',
        name: 'GetProduct',
        payload: { id: 'prod-123' },
      };

      expect(message.payload.id).toBe('prod-123');
    });

    test('query with filter criteria payload', () => {
      const message: WaveQueryMessage<{
        filter: string;
        limit: number;
        offset: number;
      }, { items: unknown[]; total: number }> = {
        kind: 'query',
        namespace: 'Search',
        name: 'FindProducts',
        payload: { filter: 'laptop', limit: 10, offset: 0 },
      };

      expect(message.payload.filter).toBe('laptop');
      expect(message.payload.limit).toBe(10);
    });

    test('query extends base message fields', () => {
      const message: WaveQueryMessage<
        { userId: string },
        { name: string },
        { tenantId: string }
      > = {
        kind: 'query',
        namespace: 'Users',
        name: 'GetProfile',
        payload: { userId: 'user-123' },
        context: { tenantId: 'tenant-456' },
      };

      expect(message.context?.tenantId).toBe('tenant-456');
    });
  });

  describe('Message Kind Discrimination', () => {
    test('command kind is distinct from event and query', () => {
      const msg: WaveBaseMessage = {
        namespace: 'Test',
        name: 'TestMessage',
        kind: 'command',
        payload: {},
      };

      if (msg.kind === 'command') {
        expect(msg).toHaveProperty('awaitResponse');
      } else if (msg.kind === 'event') {
        // Events don't have awaitResponse
      } else if (msg.kind === 'query') {
        // Queries don't have awaitResponse
      }
    });

    test('event kind is distinct', () => {
      const msg: WaveBaseMessage = {
        namespace: 'Test',
        name: 'TestEvent',
        kind: 'event',
        payload: {},
      };

      expect(msg.kind).toBe('event');
    });

    test('query kind is distinct', () => {
      const msg: WaveBaseMessage = {
        namespace: 'Test',
        name: 'TestQuery',
        kind: 'query',
        payload: {},
      };

      expect(msg.kind).toBe('query');
    });
  });
});

describe('WaveTransport - Handler Type Definitions', () => {
  describe('WaveHandler', () => {
    test('handler receives message and context', async () => {
      const handler: WaveHandler<
        WaveEventMessage<{ value: string }>
      > = async (message, context) => {
        expect(message.payload.value).toBe('test');
        expect(context).toHaveProperty('ack');
        expect(context).toHaveProperty('nack');
        expect(context).toHaveProperty('message');
      };

      const message: WaveEventMessage<{ value: string }> = {
        kind: 'event',
        namespace: 'Test',
        name: 'TestEvent',
        payload: { value: 'test' },
      };

      const mockContext: ExecutionContext = {
        ack: () => 1,
        nack: () => 0,
        message: {},
      };

      await handler(message, mockContext);
    });

    test('handler with generic types', async () => {
      interface CustomPayload {
        id: string;
        data: Record<string, unknown>;
      }

      const handler: WaveHandler<
        WaveCommandMessage<CustomPayload, string>,
        string
      > = async (message) => {
        return message.payload.id;
      };

      const message: WaveCommandMessage<CustomPayload, string> = {
        kind: 'command',
        namespace: 'Test',
        name: 'TestCommand',
        payload: { id: 'cmd-123', data: { key: 'value' } },
        awaitResponse: true,
      };

      const mockContext: ExecutionContext = {
        ack: () => 1,
        nack: () => 0,
        message: {},
      };

      const result = await handler(message, mockContext);
      expect(result).toBe('cmd-123');
    });

    test('handler can return a value for commands', async () => {
      const handler: WaveHandler<
        WaveCommandMessage<{}, { result: string }>,
        { result: string }
      > = async (_message) => {
        return { result: 'success' };
      };

      const message: WaveCommandMessage<{}, { result: string }> = {
        kind: 'command',
        namespace: 'Test',
        name: 'TestCommand',
        payload: {},
        awaitResponse: true,
      };

      const mockContext: ExecutionContext = {
        ack: () => 1,
        nack: () => 0,
        message: {},
      };

      const result = await handler(message, mockContext);
      expect(result).toEqual({ result: 'success' });
    });

    test('handler returns void for events by default', async () => {
      const handler: WaveHandler<WaveEventMessage> = async (_message) => {
        // No return value
      };

      const message: WaveEventMessage = {
        kind: 'event',
        namespace: 'Test',
        name: 'TestEvent',
        payload: {},
      };

      const mockContext: ExecutionContext = {
        ack: () => 1,
        nack: () => 0,
        message: {},
      };

      const result = await handler(message, mockContext);
      expect(result).toBeUndefined();
    });

  });
  });

  describe('BaseMessageHandler', () => {
    test('base handler receives raw data object', async () => {
      const handler: BaseMessageHandler = async (data: Record<string, unknown>) => {
        expect(data).toHaveProperty('value');
      };

      const data = { value: 'test-data' };
      const mockContext: ExecutionContext = {
        ack: () => 1,
        nack: () => 0,
        message: {},
      };

      await handler(data, mockContext);
    });

    test('base handler can return a value', async () => {
      const handler: BaseMessageHandler = async (data: Record<string, unknown>) => {
        return { processed: true, data };
      };

      const data = { id: '123' };
      const mockContext: ExecutionContext = {
        ack: () => 1,
        nack: () => 0,
        message: {},
      };

      const result = await handler(data, mockContext);
      expect(result).toEqual({ processed: true, data });
    });

    test('base handler can return void', async () => {
      const handler: BaseMessageHandler = async (data: Record<string, unknown>) => {
        // Process but don't return
        void data;
      };

      const data = { id: '456' };
      const mockContext: ExecutionContext = {
        ack: () => 1,
        nack: () => 0,
        message: {},
      };

      const result = await handler(data, mockContext);
      expect(result).toBeUndefined();
    });
  });

  describe('Unsubscribe Type', () => {
    test('unsubscribe is an async function', async () => {
      let called = false;
      const unsubscribe: Unsubscribe = async () => {
        called = true;
      };

      await unsubscribe();
      expect(called).toBe(true);
    });

    test('unsubscribe can be called multiple times safely', async () => {
      let callCount = 0;
      const unsubscribe: Unsubscribe = async () => {
        callCount++;
      };

      await unsubscribe();
      await unsubscribe();
      await unsubscribe();

      expect(callCount).toBe(3);
    });

    test('unsubscribe returns void promise', async () => {
      const unsubscribe: Unsubscribe = async () => {
        // No return
      };

      const result = await unsubscribe();
      expect(result).toBeUndefined();
    });
  });
});

describe('WaveTransport - Execution Context', () => {
  describe('ExecutionContext', () => {
    test('ack returns a number', () => {
      const context: ExecutionContext = {
        ack: () => {
          return 1; // ConsumerStatus.ACK
        },
        nack: (requeue?: boolean) => {
          return requeue ? 2 : 0; // ConsumerStatus.REQUEUE or DROP
        },
        message: { routingKey: 'test' },
      };

      const result = context.ack();
      expect(typeof result).toBe('number');
    });

    test('nack with requeue returns a number', () => {
      const context: ExecutionContext = {
        ack: () => 1,
        nack: (requeue?: boolean) => {
          return requeue ? 2 : 0;
        },
        message: {},
      };

      const result = context.nack(true);
      expect(typeof result).toBe('number');
    });

    test('nack without requeue returns a number', () => {
      const context: ExecutionContext = {
        ack: () => 1,
        nack: (requeue?: boolean) => {
          return requeue ? 2 : 0;
        },
        message: {},
      };

      const result = context.nack(false);
      expect(typeof result).toBe('number');
    });

    test('nack defaults to no requeue', () => {
      let requeueValue: boolean | undefined;
      const context: ExecutionContext = {
        ack: () => 1,
        nack: (requeue?: boolean) => {
          requeueValue = requeue;
          return 0;
        },
        message: {},
      };

      context.nack();
      expect(requeueValue).toBe(false);
    });

    test('context.message contains raw message metadata', () => {
      const rawMessage = {
        routingKey: 'test.routing.key',
        headers: { 'x-correlation-id': 'corr-123' },
        properties: { contentType: 'application/json' },
      };

      const context: ExecutionContext = {
        ack: () => 1,
        nack: () => 0,
        message: rawMessage,
      };

      expect(context.message.routingKey).toBe('test.routing.key');
      expect(context.message.headers['x-correlation-id']).toBe('corr-123');
    });

    test('context can be used for explicit ack/nack control', () => {
      let ackCalled = false;
      let nackCalled = false;

      const context: ExecutionContext = {
        ack: () => {
          ackCalled = true;
          return 1;
        },
        nack: () => {
          nackCalled = true;
          return 0;
        },
        message: {},
      };

      // Simulate handler explicitly acking
      context.ack();
      expect(ackCalled).toBe(true);
      expect(nackCalled).toBe(false);

      // Simulate handler explicitly nacking
      nackCalled = false;
      context.nack(false);
      expect(nackCalled).toBe(true);
      expect(ackCalled).toBe(true);
    });
  });
});

describe('WaveTransport - Listener Options', () => {
  describe('ListenerOptions', () => {
    test('default options have no autoAck', () => {
      const options: ListenerOptions = {};
      expect(options.autoAck).toBeUndefined();
    });

    test('autoAck can be set to true', () => {
      const options: ListenerOptions = {
        autoAck: true,
      };
      expect(options.autoAck).toBe(true);
    });

    test('autoAck can be set to false', () => {
      const options: ListenerOptions = {
        autoAck: false,
      };
      expect(options.autoAck).toBe(false);
    });
  });
});

describe('WaveTransport - Send Options', () => {
  describe('WaveSendOptions', () => {
    test('default options have no timeout', () => {
      const options: WaveSendOptions = {};
      expect(options.timeoutMs).toBeUndefined();
    });

    test('timeoutMs can be specified', () => {
      const options: WaveSendOptions = {
        timeoutMs: 5000,
      };
      expect(options.timeoutMs).toBe(5000);
    });

    test('timeoutMs is a number', () => {
      const options: WaveSendOptions = {
        timeoutMs: 100,
      };
      expect(typeof options.timeoutMs).toBe('number');
      expect(options.timeoutMs).toBeGreaterThan(0);
    });
  });
});

describe('WaveTransport - Type Safety', () => {
  test('payload type is preserved through message construction', () => {
    interface CustomPayload {
      id: string;
      name: string;
      metadata: Record<string, unknown>;
    }

    const message: WaveEventMessage<CustomPayload> = {
      kind: 'event',
      namespace: 'Products',
      name: 'ProductCreated',
      payload: {
        id: 'prod-123',
        name: 'Widget',
        metadata: { sku: 'WID-001' },
      },
    };

    // TypeScript would catch type errors at compile time
    // Here we verify runtime structure
    expect(message.payload.id).toBe('prod-123');
    expect(message.payload.name).toBe('Widget');
    expect(message.payload.metadata.sku).toBe('WID-001');
  });

  test('context type is preserved through message construction', () => {
    interface AppContext {
      userId: string;
      tenantId: string;
      requestId: string;
    }

    const message: WaveBaseMessage<
      { data: string },
      AppContext
    > = {
      namespace: 'MultiTenant',
      name: 'TenantEvent',
      kind: 'event',
      payload: { data: 'test' },
      context: {
        userId: 'user-123',
        tenantId: 'tenant-456',
        requestId: 'req-789',
      },
    };

    expect(message.context?.userId).toBe('user-123');
    expect(message.context?.tenantId).toBe('tenant-456');
    expect(message.context?.requestId).toBe('req-789');
  });

  test('multiple message types can coexist', () => {
    const event: WaveEventMessage = {
      kind: 'event',
      namespace: 'Test',
      name: 'TestEvent',
      payload: { type: 'event' },
    };

    const command: WaveCommandMessage = {
      kind: 'command',
      namespace: 'Test',
      name: 'TestCommand',
      payload: { type: 'command' },
      awaitResponse: true,
    };

    const query: WaveQueryMessage = {
      kind: 'query',
      namespace: 'Test',
      name: 'TestQuery',
      payload: { type: 'query' },
    };

    expect(event.kind).toBe('event');
    expect(command.kind).toBe('command');
    expect(query.kind).toBe('query');
  });
});

describe('WaveTransport - Edge Cases', () => {
  test('message with null-like payload', () => {
    // TypeScript allows unknown payload, so null/undefined should work
    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Test',
      name: 'NullPayloadEvent',
      payload: null as any,
    };

    expect(message.payload).toBeNull();
  });

  test('message with empty string values', () => {
    const message: WaveEventMessage = {
      kind: 'event',
      namespace: '',
      name: '',
      payload: '',
    };

    expect(message.namespace).toBe('');
    expect(message.name).toBe('');
    expect(message.payload).toBe('');
  });

  test('message with deeply nested context', () => {
    const message: WaveEventMessage<
      { level: number },
      {
        level1: {
          level2: {
            level3: { value: string };
          };
        };
      }
    > = {
      kind: 'event',
      namespace: 'Deep',
      name: 'NestedContext',
      payload: { level: 3 },
      context: {
        level1: {
          level2: {
            level3: { value: 'deep' },
          },
        },
      },
    };

    expect(message.context?.level1.level2.level3.value).toBe('deep');
  });

  test('message with array payload', () => {
    const message: WaveEventMessage<string[]> = {
      kind: 'event',
      namespace: 'Bulk',
      name: 'BatchEvent',
      payload: ['item1', 'item2', 'item3'],
    };

    expect(message.payload).toEqual(['item1', 'item2', 'item3']);
  });

  test('message with numeric payload', () => {
    const message: WaveEventMessage<number> = {
      kind: 'event',
      namespace: 'Metrics',
      name: 'CounterUpdate',
      payload: 42,
    };

    expect(message.payload).toBe(42);
  });

  test('message with boolean payload', () => {
    const message: WaveEventMessage<boolean> = {
      kind: 'event',
      namespace: 'Flags',
      name: 'FeatureToggle',
      payload: true,
    };

    expect(message.payload).toBe(true);
  });
});

describe('WaveTransport - Handler Implementation Patterns', () => {
  test('handler with explicit ack', async () => {
    let acked = false;

    const handler: BaseMessageHandler = async (_data: Record<string, unknown>, context) => {
      // Process message...
      context.ack();
      acked = true;
    };

    const data = { action: 'process' };
    const mockContext: ExecutionContext = {
      ack: () => {
        acked = true;
        return 1;
      },
      nack: () => 0,
      message: {},
    };

    await handler(data, mockContext);
    expect(acked).toBe(true);
  });

  test('handler with explicit nack', async () => {
    let nacked = false;

    const handler: BaseMessageHandler = async (_data: Record<string, unknown>, context) => {
      // Error occurred...
      context.nack(true); // Requeue for retry
      nacked = true;
    };

    const data = { action: 'process' };
    const mockContext: ExecutionContext = {
      ack: () => 1,
      nack: (requeue?: boolean) => {
        expect(requeue).toBe(true);
        nacked = true;
        return 2;
      },
      message: {},
    };

    await handler(data, mockContext);
    expect(nacked).toBe(true);
  });

  test('handler can return value and ack in sequence', async () => {
    const handler: BaseMessageHandler = async (_data: Record<string, unknown>, context) => {
      const result = { success: true };
      context.ack();
      return result;
    };

    const data = { action: 'process' };
    const mockContext: ExecutionContext = {
      ack: () => 1,
      nack: () => 0,
      message: {},
    };

    const result = await handler(data, mockContext);
    expect(result).toEqual({ success: true });
  });
});
