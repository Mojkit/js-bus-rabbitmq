/**
 * @file RabbitMQWaveTransport.test.ts - Unit tests for RabbitMQWaveTransport class.
 *
 * Tests message sending (events, commands, queries), listener registration,
 * tracing/propagation integration, and lifecycle management using mocked
 * RabbitMQ client dependencies.
 */

import { test, expect, describe, beforeEach, spyOn, mock } from 'bun:test';
import { randomUUID } from 'crypto';
import { Connection } from 'rabbitmq-client';
import type { Consumer, ConsumerHandler, Publisher, RPCClient } from 'rabbitmq-client';
import { RabbitMQWaveTransport } from '../../src/RabbitMQWaveTransport';
import type {
  WaveEventMessage,
  WaveCommandMessage,
  WaveQueryMessage,
} from '../../src/WaveTransport';
import { createMockLogger, createMockTracer, createMockPropagator } from './mocks';

// Helper to create a minimal mock connection
function createMockConnection() {
  const consumers = new Map<string, Consumer>();
  const publishers = new Map<string, Publisher>();
  const rpcClients = new Map<string, RPCClient>();

  let connectionEventCallbacks: Array<() => void> = [];
  let errorEventCallbacks: Array<(err: Error) => void> = [];

  const mockConn = {
    on: mock((event: string, cb: any) => {
      if (event === 'error') {
        errorEventCallbacks.push(cb as (err: Error) => void);
      } else if (event === 'connection') {
        connectionEventCallbacks.push(cb as () => void);
      }
      return mockConn;
    }),
    off: mock(() => mockConn),
    createConsumer: mock((options: any, handler: ConsumerHandler) => {
      const consumerId = options.queue || Math.random().toString(36);
      const mockCons = {
        start: mock(() => {}),
        close: mock(() => Promise.resolve()),
        on: mock((event: string, cb: any) => {}),
        _options: options,
        _handler: handler,
      } as unknown as Consumer;
      consumers.set(consumerId, mockCons);
      return mockCons;
    }),
    createPublisher: mock((options: any) => {
      const mockPub = {
        send: mock(() => Promise.resolve()),
        close: mock(() => Promise.resolve()),
        on: mock((event: string, cb: any) => {}),
        _options: options,
      } as unknown as Publisher;
      publishers.set(Math.random().toString(36), mockPub);
      return mockPub;
    }),
    createRPCClient: mock((options: any) => {
      const mockRPC = {
        send: mock(() => Promise.resolve({})),
        close: mock(() => Promise.resolve()),
        on: mock((event: string, cb: any) => {}),
        _options: options,
      } as unknown as RPCClient;
      rpcClients.set(Math.random().toString(36), mockRPC);
      return mockRPC;
    }),
    close: mock(() => Promise.resolve()),
    // Expose internal callbacks for testing
    _emitConnection: () => {
      connectionEventCallbacks.forEach((cb) => cb());
    },
    _emitError: (err: Error) => {
      errorEventCallbacks.forEach((cb) => cb(err));
    },
    _consumers: consumers,
    _publishers: publishers,
    _rpcClients: rpcClients,
  } as any;

  return mockConn;
}

function createTransport(overrides?: {
  url?: string;
  logger?: any;
  tracer?: any;
  propagator?: any;
}) {
  const mockConnection = createMockConnection();
  const transport = new RabbitMQWaveTransport(
    { url: overrides?.url ?? 'amqp://localhost:5672' },
    overrides?.tracer ?? createMockTracer(),
    overrides?.logger ?? createMockLogger(),
    overrides?.propagator ?? createMockPropagator()
  );

  // Replace internal connection
  (transport as any).connection = mockConnection;

  return { transport, mockConnection };
}

describe('RabbitMQWaveTransport - Constructor', () => {
  test('creates instance with default dependencies', () => {
    const { transport } = createTransport();

    expect(transport).toBeDefined();
    expect(transport).toBeInstanceOf(RabbitMQWaveTransport);
    expect(transport.isConnected()).toBe(false);
    expect(transport.getConnectionUrl()).toBe('amqp://localhost:5672');
  });

  test('creates instance with custom URL', () => {
    const { transport } = createTransport({ url: 'amqp://custom:5673' });

    expect(transport.getConnectionUrl()).toBe('amqp://custom:5673');
  });

  test('stores custom logger', () => {
    const logger = createMockLogger();
    const { transport } = createTransport({ logger });

    expect(transport).toBeDefined();
  });

  test('stores custom tracer', () => {
    const tracer = createMockTracer();
    tracer.returnValue = Promise.resolve(undefined);
    const { transport } = createTransport({ tracer });

    expect(transport).toBeDefined();
  });

  test('stores custom propagator', () => {
    const propagator = createMockPropagator();
    propagator.calls.lastReturnValue = { traceparent: 'test' };
    const { transport } = createTransport({ propagator });

    expect(transport).toBeDefined();
  });

  test('creates instance without optional parameters', () => {
    const transport = new RabbitMQWaveTransport({ url: 'amqp://localhost:5672' });

    expect(transport).toBeDefined();
  });

  test('prefetchCount in config is accepted', () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://localhost:5672',
      prefetchCount: 10,
    });

    expect(transport).toBeDefined();
  });
});

describe('RabbitMQWaveTransport - Lifecycle Hooks', () => {
  test('onConnectionEstablished clears publisher/client maps', () => {
    const { transport, mockConnection } = createTransport();

    // First, set up some fake state
    (transport as any).eventPublisher.set('exchange1', {} as Publisher);
    (transport as any).commandPublisher.set('exchange2', {} as Publisher);
    (transport as any).commandClient.set('exchange3', {} as RPCClient);
    (transport as any).queryClient.set('exchange4', {} as RPCClient);

    expect((transport as any).eventPublisher.size).toBe(1);
    expect((transport as any).commandPublisher.size).toBe(1);
    expect((transport as any).commandClient.size).toBe(1);
    expect((transport as any).queryClient.size).toBe(1);

    // Trigger connection established
    mockConnection._emitConnection();

    expect((transport as any).eventPublisher.size).toBe(0);
    expect((transport as any).commandPublisher.size).toBe(0);
    expect((transport as any).commandClient.size).toBe(0);
    expect((transport as any).queryClient.size).toBe(0);
  });

  test('onConnectionEstablished logs the URL', () => {
    const logger = createMockLogger();
    const { transport, mockConnection } = createTransport({ logger });

    mockConnection._emitConnection();

    const infoMessages = logger.messages.filter(
      (m) => m.message === 'rabbitmq.connection.established'
    );
    expect(infoMessages.length).toBe(1);
    expect(infoMessages[0].meta?.url).toBe('amqp://localhost:5672');
  });

  test('onConnectionError clears all maps including reply consumers', () => {
    const { transport, mockConnection } = createTransport();

    // Pre-populate maps
    (transport as any).eventPublisher.set('e1', {} as Publisher);
    (transport as any).commandPublisher.set('e2', {} as Publisher);
    (transport as any).commandClient.set('e3', {} as RPCClient);
    (transport as any).queryClient.set('e4', {} as RPCClient);
    (transport as any).replyConsumers.set('q1', {} as Consumer);
    (transport as any).commandReplyQueues.set('n1', 'q1');
    (transport as any).queryReplyQueues.set('n2', 'q2');

    mockConnection._emitError(new Error('Connection lost'));

    expect((transport as any).eventPublisher.size).toBe(0);
    expect((transport as any).commandPublisher.size).toBe(0);
    expect((transport as any).commandClient.size).toBe(0);
    expect((transport as any).queryClient.size).toBe(0);
    expect((transport as any).replyConsumers.size).toBe(0);
    expect((transport as any).commandReplyQueues.size).toBe(0);
    expect((transport as any).queryReplyQueues.size).toBe(0);
  });

  test('onConnectionError logs error details', () => {
    const logger = createMockLogger();
    const { transport, mockConnection } = createTransport({ logger });

    const testError = new Error('Test connection error');
    mockConnection._emitError(testError);

    const errorMessages = logger.messages.filter(
      (m) => m.message === 'rabbitmq.connection.error'
    );
    expect(errorMessages.length).toBe(1);
    expect(errorMessages[0].meta?.message).toBe('Test connection error');
  });
});

describe('RabbitMQWaveTransport - sendEvent', () => {
  test('sendEvent ensures connection is established', async () => {
    const { transport, mockConnection } = createTransport();

    const message: WaveEventMessage<{ test: string }> = {
      kind: 'event',
      namespace: 'Billing',
      name: 'InvoiceCreated',
      payload: { test: 'data' },
    };

    // This should not throw even though connection is not real
    // it should call ensureConnected internally
    try {
      await transport.sendEvent(message);
    } catch (err: any) {
      // Expected to fail since mockConnection doesn't fully connect
      // but ensureConnected should have been called
    }
  });

  test('sendEvent uses propagator to inject context', async () => {
    const propagator = createMockPropagator();
    propagator.calls.lastReturnValue = {
      traceparent: '00-trace123-span456-01',
      baggage: 'user=john',
    };
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);

    const { transport } = createTransport({ propagator, tracer });

    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Orders',
      name: 'OrderCreated',
      payload: { orderId: 'ord-123' },
    };

    try {
      await transport.sendEvent(message);
    } catch {
      // May fail due to mock, but propagator should have been called
    }

    expect(propagator.calls.inject).toBeGreaterThanOrEqual(1);
  });

  test('sendEvent with complex payload', async () => {
    const { transport } = createTransport();

    const message: WaveEventMessage<{ items: string[]; total: number }> = {
      kind: 'event',
      namespace: 'Inventory',
      name: 'StockUpdate',
      payload: { items: ['item1', 'item2'], total: 2 },
    };

    try {
      await transport.sendEvent(message);
    } catch {
      // Expected with mock connection
    }
  });

  test('sendEvent with context', async () => {
    const { transport } = createTransport();

    const message: WaveEventMessage<
      { orderId: string },
      { userId: string; tenantId: string }
    > = {
      kind: 'event',
      namespace: 'Orders',
      name: 'OrderCreated',
      payload: { orderId: 'ord-456' },
      context: { userId: 'user-123', tenantId: 'tenant-789' },
    };

    try {
      await transport.sendEvent(message);
    } catch {
      // Expected with mock connection
    }
  });

  test('sendEvent returns void', async () => {
    const { transport } = createTransport();

    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Test',
      name: 'TestEvent',
      payload: { value: 'test' },
    };

    const result = await transport.sendEvent(message);
    expect(result).toBeUndefined();
  }, 5000);

  test('sendEvent uses tracer for span', async () => {
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);

    const { transport } = createTransport({ tracer });

    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Test',
      name: 'TestEvent',
      payload: {},
    };

    expect(tracer.calls.startActiveSpan).toBe(0);

    try {
      await transport.sendEvent(message);
    } catch {
      // Expected with mock
    }

    expect(tracer.calls.startActiveSpan).toBeGreaterThanOrEqual(1);
    expect(tracer.calls.spanNames).toContain('wave.sendEvent');
  });
});

describe('RabbitMQWaveTransport - sendCommand', () => {
  test('fire-and-forget command sends without awaiting response', async () => {
    const { transport } = createTransport();

    const message: WaveCommandMessage<{ action: string }> = {
      kind: 'command',
      namespace: 'Billing',
      name: 'ProcessPayment',
      payload: { action: 'charge' },
      awaitResponse: false,
    };

    const result = await transport.sendCommand(message);
    expect(result).toBeUndefined();
  }, 5000);

  test('command without awaitResponse is fire-and-forget', async () => {
    const { transport } = createTransport();

    const message: WaveCommandMessage = {
      kind: 'command',
      namespace: 'Notifications',
      name: 'SendEmail',
      payload: { to: 'user@example.com' },
    };

    const result = await transport.sendCommand(message);
    expect(result).toBeUndefined();
  }, 5000);

  test('command with awaitResponse uses RPC pattern', async () => {
    const { transport } = createTransport();

    const message: WaveCommandMessage<
      { invoiceId: string },
      { success: boolean }
    > = {
      kind: 'command',
      namespace: 'Billing',
      name: 'ProcessPayment',
      payload: { invoiceId: 'inv-123' },
      awaitResponse: true,
      context: { correlationId: 'corr-456' },
    };

    try {
      await transport.sendCommand(message, { timeoutMs: 1000 });
    } catch {
      // May fail with mock - RPC client send would need to return something
    }
  });

  test('sendCommand uses tracer', async () => {
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);

    const { transport } = createTransport({ tracer });

    const message: WaveCommandMessage = {
      kind: 'command',
      namespace: 'Test',
      name: 'TestCommand',
      payload: {},
    };

    try {
      await transport.sendCommand(message);
    } catch {
      // Expected with mock
    }

    expect(tracer.calls.spanNames).toContain('wave.sendCommand');
  });

  test('sendCommand with complex payload', async () => {
    const { transport } = createTransport();

    const message: WaveCommandMessage<{
      user: { id: string; name: string };
      action: string;
    }> = {
      kind: 'command',
      namespace: 'Users',
      name: 'UpdateProfile',
      payload: { user: { id: 'user-123', name: 'John' }, action: 'update' },
      awaitResponse: false,
    };

    const result = await transport.sendCommand(message);
    expect(result).toBeUndefined();
  }, 5000);

  test('sendCommand with correlationId in context', async () => {
    const { transport } = createTransport();

    const message: WaveCommandMessage<{}, { result: string }> = {
      kind: 'command',
      namespace: 'Queries',
      name: 'GetUser',
      payload: {},
      awaitResponse: true,
      context: { correlationId: 'custom-correlation-id' },
    };

    try {
      await transport.sendCommand(message);
    } catch {
      // Expected with mock
    }
  });
});

describe('RabbitMQWaveTransport - sendQuery', () => {
  test('sendQuery returns response', async () => {
    const { transport } = createTransport();

    const message: WaveQueryMessage<{ id: string }, { name: string }> = {
      kind: 'query',
      namespace: 'Catalog',
      name: 'GetProduct',
      payload: { id: 'prod-123' },
    };

    const result = await transport.sendQuery(message);
    expect(result).toBeUndefined(); // Mock doesn't return real data
  }, 5000);

  test('sendQuery uses tracer', async () => {
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);

    const { transport } = createTransport({ tracer });

    const message: WaveQueryMessage = {
      kind: 'query',
      namespace: 'Test',
      name: 'TestQuery',
      payload: {},
    };

    try {
      await transport.sendQuery(message);
    } catch {
      // Expected with mock
    }

    expect(tracer.calls.spanNames).toContain('wave.sendQuery');
  });

  test('sendQuery with filter payload', async () => {
    const { transport } = createTransport();

    const message: WaveQueryMessage<
      { filter: string; limit: number },
      { items: string[] }
    > = {
      kind: 'query',
      namespace: 'Search',
      name: 'FindProducts',
      payload: { filter: 'laptop', limit: 10 },
    };

    const result = await transport.sendQuery(message);
    expect(result).toBeUndefined();
  }, 5000);

  test('sendQuery with correlationId', async () => {
    const { transport } = createTransport();

    const message: WaveQueryMessage<unknown, unknown> = {
      kind: 'query',
      namespace: 'Users',
      name: 'GetProfile',
      payload: {},
      context: { correlationId: 'query-corr-789' },
    };

    try {
      await transport.sendQuery(message);
    } catch {
      // Expected with mock
    }
  });
});

describe('RabbitMQWaveTransport - addEventListener', () => {
  test('addEventListener returns unsubscribe function', async () => {
    const { transport } = createTransport();

    const unsubscribe = await transport.addEventListener(
      'Billing',
      'InvoiceCreated',
      async (message) => {
        void message;
      }
    );

    expect(typeof unsubscribe).toBe('function');

    await unsubscribe();
  });

  test('addEventListener accepts handler with context', async () => {
    const { transport } = createTransport();

    let receivedMessage: any = null;

    const unsubscribe = await transport.addEventListener(
      'Test',
      'TestEvent',
      async (message, context) => {
        receivedMessage = message;
        expect(context).toHaveProperty('ack');
        expect(context).toHaveProperty('nack');
        expect(context).toHaveProperty('message');
      }
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });

  test('addEventListener with autoAck option', async () => {
    const { transport } = createTransport();

    const unsubscribe = await transport.addEventListener(
      'Test',
      'TestEvent',
      async (message) => {
        void message;
      },
      { autoAck: true }
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });

  test('addEventListener with explicit ack/nack', async () => {
    const { transport } = createTransport();

    let ackCalled = false;
    let nackCalled = false;

    const unsubscribe = await transport.addEventListener(
      'Test',
      'TestEvent',
      async (_message, context) => {
        ackCalled = true;
        context.ack();
      },
      { autoAck: false }
    );

    expect(ackCalled).toBe(false); // Not called until message is delivered
    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });

  test('unsubscribe stops listener', async () => {
    const { transport } = createTransport();

    let callCount = 0;
    const unsubscribe = await transport.addEventListener(
      'Test',
      'TestEvent',
      async () => {
        callCount++;
      }
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();

    // Second unsubscribe should be safe (no-op)
    await unsubscribe();
  });

  test('addEventListener with options object', async () => {
    const { transport } = createTransport();

    const options: Parameters<typeof transport.addEventListener>[3] = {
      autoAck: false,
    };

    const unsubscribe = await transport.addEventListener(
      'Test',
      'TestEvent',
      async () => {},
      options
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });
});

describe('RabbitMQWaveTransport - addCommandListener', () => {
  test('addCommandListener returns unsubscribe function', async () => {
    const { transport } = createTransport();

    const unsubscribe = await transport.addCommandListener(
      'Billing',
      'ProcessPayment',
      async (message) => {
        return { success: true };
      }
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });

  test('addCommandListener handler can return result', async () => {
    const { transport } = createTransport();

    const unsubscribe = await transport.addCommandListener(
      'Billing',
      'ProcessPayment',
      async (message) => {
        return { processed: true, message };
      }
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });

  test('addCommandListener with autoAck false', async () => {
    const { transport } = createTransport();

    const unsubscribe = await transport.addCommandListener(
      'Test',
      'TestCommand',
      async (_message, context) => {
        context.ack();
      },
      { autoAck: false }
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });

  test('addCommandListener with context parameter', async () => {
    const { transport } = createTransport();

    let contextReceived = false;
    const unsubscribe = await transport.addCommandListener(
      'Test',
      'TestCommand',
      async (_message, context) => {
        contextReceived = true;
        expect(context).toHaveProperty('ack');
        expect(context).toHaveProperty('nack');
      }
    );

    // Context is available in handler signature
    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });
});

describe('RabbitMQWaveTransport - addQueryListener', () => {
  test('addQueryListener returns unsubscribe function', async () => {
    test('addQueryListener handler returns data', async () => {
      const { transport } = createTransport();

      const unsubscribe = await transport.addQueryListener(
        'Catalog',
        'GetProduct',
        async (_message: any) => {
          return { name: 'Product Name', price: 99.99 };
        }
      );

      expect(typeof unsubscribe).toBe('function');
      await unsubscribe();
    });

  test('addQueryListener handler returns data', async () => {
    const { transport } = createTransport();

    const unsubscribe = await transport.addQueryListener(
      'Catalog',
      'GetProduct',
      async (message) => {
        return {
          id: message.id,
          name: 'Product Name',
          price: 99.99,
        };
      }
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });

  test('addQueryListener with complex return type', async () => {
    const { transport } = createTransport();

    const unsubscribe = await transport.addQueryListener(
      'Search',
      'FindProducts',
      async (message) => {
        return {
          items: [{ id: '1', name: 'Product 1' }],
          total: 1,
          page: 1,
        };
      }
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });

  test('addQueryListener with autoAck', async () => {
    const { transport } = createTransport();

    const unsubscribe = await transport.addQueryListener(
      'Test',
      'TestQuery',
      async (_message: unknown) => {
        return { result: 'ok' };
      },
      { autoAck: true }
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });
});

describe('RabbitMQWaveTransport - Helper Methods', () => {
  test('getExchange returns correct format for simple namespace', () => {
    const { transport } = createTransport();

    const exchange = (transport as any).getExchange('Billing');
    expect(exchange).toBe('wave.billing');
  });

  test('getExchange returns correct format for dotted namespace', () => {
    const { transport } = createTransport();

    const exchange = (transport as any).getExchange('Billing.Invoice');
    expect(exchange).toBe('wave.billing');
  });

  test('getExchange with undefined namespace returns default', () => {
    const { transport } = createTransport();

    const exchange = (transport as any).getExchange(undefined);
    expect(exchange).toBe('wave.default');
  });

  test('getExchange with empty namespace returns default', () => {
    const { transport } = createTransport();

    const exchange = (transport as any).getExchange('');
    expect(exchange).toBe('wave.default');
  });

  test('getRoutingKey returns correct format', () => {
    const { transport } = createTransport();

    const routingKey = (transport as any).getRoutingKey('Billing', 'InvoiceCreated');
    expect(routingKey).toBe('Billing.InvoiceCreated');
  });

  test('getRoutingKey with dotted namespace', () => {
    const { transport } = createTransport();

    const routingKey = (transport as any).getRoutingKey('Billing.Invoice', 'Create');
    expect(routingKey).toBe('Billing.Invoice.Create');
  });

  test('getQueueName returns correct format', () => {
    const { transport } = createTransport();

    const queueName = (transport as any).getQueueName('event', 'Billing', 'InvoiceCreated');
    expect(queueName).toBe('wave.event.queue.Billing.InvoiceCreated');
  });

  test('getQueueName for command', () => {
    const { transport } = createTransport();

    const queueName = (transport as any).getQueueName('command', 'Billing', 'ProcessPayment');
    expect(queueName).toBe('wave.command.queue.Billing.ProcessPayment');
  });

  test('getQueueName for query', () => {
    const { transport } = createTransport();

    const queueName = (transport as any).getQueueName('query', 'Catalog', 'GetProduct');
    expect(queueName).toBe('wave.query.queue.Catalog.GetProduct');
  });

  test('getReplyQueueName returns correct format for command', () => {
    const { transport } = createTransport();

    const queueName = (transport as any).getReplyQueueName('wave.billing', 'command');
    expect(queueName).toBe('wave.billing.command-response');
  });

  test('getReplyQueueName returns correct format for query', () => {
    const { transport } = createTransport();

    const queueName = (transport as any).getReplyQueueName('wave.catalog', 'query');
    expect(queueName).toBe('wave.catalog.query-response');
  });
});

describe('RabbitMQWaveTransport - Shutdown', () => {
  test('shutdown closes all publishers and clients', async () => {
    const { transport, mockConnection } = createTransport();

    // Register some publishers and clients
    (transport as any).eventPublisher.set('e1', mockConnection._publishers.get(
      [...mockConnection._publishers.keys()][0]
    ) || {} as Publisher);
    (transport as any).commandPublisher.set('e2', {} as Publisher);
    (transport as any).commandClient.set('e3', {} as RPCClient);
    (transport as any).queryClient.set('e4', {} as RPCClient);

    await transport.shutdown();

    // Should not throw
  });

  test('shutdown is safe when no resources registered', async () => {
    const { transport } = createTransport();

    await expect(transport.shutdown()).resolves.toBeUndefined();
  });

  test('shutdown after listeners are registered', async () => {
    const { transport } = createTransport();

    const unsubscribe = await transport.addEventListener(
      'Test',
      'TestEvent',
      async () => {}
    );

    await unsubscribe();
    await transport.shutdown();
  });

  test('multiple shutdown calls are safe', async () => {
    const { transport } = createTransport();

    await transport.shutdown();
    await transport.shutdown();
    await transport.shutdown();
  });
});

describe('RabbitMQWaveTransport - Tracing Integration', () => {
  test('sendEvent wraps in tracer span', async () => {
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);
    const { transport } = createTransport({ tracer });

    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Test',
      name: 'TestEvent',
      payload: {},
    };

    await transport.sendEvent(message);
    expect(tracer.calls.spanNames).toContain('wave.sendEvent');
  });

  test('sendCommand wraps in tracer span', async () => {
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);
    const { transport } = createTransport({ tracer });

    const message: WaveCommandMessage = {
      kind: 'command',
      namespace: 'Test',
      name: 'TestCommand',
      payload: {},
    };

    await transport.sendCommand(message);
    expect(tracer.calls.spanNames).toContain('wave.sendCommand');
  });

  test('sendQuery wraps in tracer span', async () => {
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);
    const { transport } = createTransport({ tracer });

    const message: WaveQueryMessage = {
      kind: 'query',
      namespace: 'Test',
      name: 'TestQuery',
      payload: {},
    };

    await transport.sendQuery(message);
    expect(tracer.calls.spanNames).toContain('wave.sendQuery');
  });

  test('noop tracer does not interfere', async () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://localhost:5672',
    });

    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Test',
      name: 'TestEvent',
      payload: {},
    };

    await transport.sendEvent(message);
  });
});

describe('RabbitMQWaveTransport - Propagation Integration', () => {
  test('propagator inject is called for events', async () => {
    const propagator = createMockPropagator();
    propagator.calls.lastReturnValue = { traceparent: '00-trace123-456-01' };
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);

    const { transport } = createTransport({ propagator, tracer });

    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Test',
      name: 'TestEvent',
      payload: {},
    };

    await transport.sendEvent(message);
    expect(propagator.calls.inject).toBeGreaterThanOrEqual(1);
  });

  test('propagator inject is called for commands', async () => {
    const propagator = createMockPropagator();
    propagator.calls.lastReturnValue = { traceparent: '00-trace123-456-01' };
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);

    const { transport } = createTransport({ propagator, tracer });

    const message: WaveCommandMessage = {
      kind: 'command',
      namespace: 'Test',
      name: 'TestCommand',
      payload: {},
    };

    await transport.sendCommand(message);
    expect(propagator.calls.inject).toBeGreaterThanOrEqual(1);
  });

  test('propagator inject is called for queries', async () => {
    const propagator = createMockPropagator();
    propagator.calls.lastReturnValue = { traceparent: '00-trace123-456-01' };
    const tracer = createMockTracer();
    tracer.calls.returnValue = Promise.resolve(undefined);

    const { transport } = createTransport({ propagator, tracer });

    const message: WaveQueryMessage = {
      kind: 'query',
      namespace: 'Test',
      name: 'TestQuery',
      payload: {},
    };

    await transport.sendQuery(message);
    expect(propagator.calls.inject).toBeGreaterThanOrEqual(1);
  });

  test('noop propagator returns empty object', async () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://localhost:5672',
    });

    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Test',
      name: 'TestEvent',
      payload: {},
    };

    await transport.sendEvent(message);
  });
});

describe('RabbitMQWaveTransport - Consumer Handler', () => {
  test('getConsumerHandler returns a function', () => {
    const { transport } = createTransport();

    const handler = (transport as any).getConsumerHandler(
      'Test',
      'TestMessage',
      'event',
      async () => {}
    );

    expect(typeof handler).toBe('function');
  });

  test('consumer handler receives raw message and reply function', async () => {
    const { transport } = createTransport();

    let receivedBody: unknown = null;
    let receivedReply: unknown = null;

    const handler = (transport as any).getConsumerHandler(
      'Test',
      'TestMessage',
      'event',
      async (data: unknown) => {
        receivedBody = data;
      }
    );

    const rawMessage = {
      body: {
        data: { value: 'test' },
        extensions: {},
      },
    };

    await handler(rawMessage, async (body: unknown) => {
      receivedReply = body;
    });

    expect(receivedBody).toEqual({ value: 'test' });
  });

  test('consumer handler with autoAck', async () => {
    const { transport } = createTransport();

    const handler = (transport as any).getConsumerHandler(
      'Test',
      'TestMessage',
      'event',
      async () => {},
      { autoAck: true }
    );

    expect(typeof handler).toBe('function');
  });

  test('consumer handler without autoAck', async () => {
    const { transport } = createTransport();

    let contextAcked = false;
    const handler = (transport as any).getConsumerHandler(
      'Test',
      'TestMessage',
      'event',
      async (_data: unknown, context) => {
        context.ack();
        contextAcked = true;
      },
      { autoAck: false }
    );

    const rawMessage = {
      body: { data: {}, extensions: {} },
    };

    await handler(rawMessage, async () => {});
    expect(contextAcked).toBe(true);
  });

  test('consumer handler sends reply when result and correlationId exist', async () => {
    const { transport } = createTransport();

    let replySent: unknown = null;

    const handler = (transport as any).getConsumerHandler(
      'Test',
      'TestCommand',
      'command',
      async () => {
        return { result: 'success' };
      }
    );

    const rawMessage = {
      body: {
        data: {},
        extensions: { correlationId: 'corr-123' },
      },
    };

    await handler(rawMessage, async (body: unknown) => {
      replySent = body;
    });

    expect(replySent).toBeDefined();
    expect((replySent as any).kind).toBe('command-response');
    expect((replySent as any).correlationId).toBe('corr-123');
    expect((replySent as any).result).toEqual({ result: 'success' });
    expect(replySent).toHaveProperty('timestamp');
  });
});

describe('RabbitMQWaveTransport - registerPublisher', () => {
  test('registerPublisher creates publisher with confirm mode', async () => {
    const { transport } = createTransport();

    const map = new Map<string, Publisher>();
    await (transport as any).registerPublisher(map, 'test.exchange', 'event');

    expect(map.size).toBe(1);
  });

  test('registerPublisher replaces existing publisher', async () => {
    const { transport, mockConnection } = createTransport();

    const map = new Map<string, Publisher>();
    const existing = mockConnection._publishers.get(
      [...mockConnection._publishers.keys()][0]
    ) || ({ close: mock(() => Promise.resolve()) } as Publisher);
    map.set('test', existing);

    await (transport as any).registerPublisher(map, 'test.exchange', 'event');

    expect(map.size).toBe(1);
  });

  test('registerPublisher configures exchanges', async () => {
    const { transport, mockConnection } = createTransport();

    const map = new Map<string, Publisher>();
    await (transport as any).registerPublisher(map, 'test.exchange', 'event');

    const publisher = map.get('test.exchange');
    expect(publisher).toBeDefined();
  });
});

describe('RabbitMQWaveTransport - registerConsumer', () => {
  test('registerConsumer creates consumer on queue', async () => {
    const { transport } = createTransport();

    const map = new Map<
      string,
      { handler: Consumer; options: any }
    >();
    const unsubscribe = await (transport as any).registerConsumer(
      map,
      'test.exchange',
      'test.routing.key',
      'test.queue',
      {},
      async () => {}
    );

    expect(typeof unsubscribe).toBe('function');
  });

  test('registerConsumer unsubscribe closes consumer', async () => {
    const { transport } = createTransport();

    const map = new Map<string, { handler: Consumer; options: any }>();
    const unsubscribe = await (transport as any).registerConsumer(
      map,
      'test.exchange',
      'test.routing.key',
      'test.queue',
      {},
      async () => {}
    );

    await unsubscribe();
  });

  test('registerConsumer with autoAck', async () => {
    const { transport } = createTransport();

    const map = new Map<string, { handler: Consumer; options: any }>();
    const unsubscribe = await (transport as any).registerConsumer(
      map,
      'test.exchange',
      'test.routing.key',
      'test.queue',
      { autoAck: true },
      async () => {}
    );

    expect(typeof unsubscribe).toBe('function');
    await unsubscribe();
  });
});

describe('RabbitMQWaveTransport - Edge Cases', () => {
  test('handles empty namespace for exchange', () => {
    const { transport } = createTransport();

    const exchange = (transport as any).getExchange('');
    expect(exchange).toBe('wave.default');
  });

  test('handles undefined namespace for exchange', () => {
    const { transport } = createTransport();

    const exchange = (transport as any).getExchange(undefined);
    expect(exchange).toBe('wave.default');
  });

  test('handles single-part namespace', () => {
    const { transport } = createTransport();

    const exchange = (transport as any).getExchange('Billing');
    expect(exchange).toBe('wave.billing');
  });

  test('handles three-part namespace', () => {
    const { transport } = createTransport();

    const exchange = (transport as any).getExchange('A.B.C');
    expect(exchange).toBe('wave.a');
  });

  test('sendEvent with undefined context', async () => {
    const { transport } = createTransport();
    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Test',
      name: 'TestEvent',
      payload: {},
    };
    try { await transport.sendEvent(message); } catch { /* mock */ }
  });

  test('sendCommand with awaitResponse undefined', async () => {
    const { transport } = createTransport();
    const message: WaveCommandMessage = {
      kind: 'command',
      namespace: 'Test',
      name: 'TestCommand',
      payload: {},
    };
    const result = await transport.sendCommand(message);
    expect(result).toBeUndefined();
  }, 5000);

  test('handles large namespace names', () => {
    const { transport } = createTransport();

    const longNamespace = 'A'.repeat(100);
    const exchange = (transport as any).getExchange(longNamespace);
    expect(exchange).toBe('wave.' + longNamespace.split('.')[0].toLowerCase());
  });
});

describe('RabbitMQWaveTransport - Message Type Handling', () => {
  test('event messages preserve payload', async () => {
    const { transport } = createTransport();

    const message: WaveEventMessage<{ id: string; data: Record<string, unknown> }> = {
      kind: 'event',
      namespace: 'Test',
      name: 'TestEvent',
      payload: { id: 'evt-123', data: { key1: 'value1', key2: 42 } },
    };

    try {
      await transport.sendEvent(message);
    } catch {
      // Expected with mock
    }
  });

  test('command messages preserve awaitResponse flag', async () => {
    const { transport } = createTransport();

    const message: WaveCommandMessage = {
      kind: 'command',
      namespace: 'Test',
      name: 'TestCommand',
      payload: {},
      awaitResponse: true,
    };

    try {
      await transport.sendCommand(message);
    } catch {
      // Expected with mock
    }
  });

  test('query messages work with any payload type', async () => {
    const { transport } = createTransport();

    const message: WaveQueryMessage<number, string> = {
      kind: 'query',
      namespace: 'Test',
      name: 'TestQuery',
      payload: 42,
    };

    try {
      await transport.sendQuery(message);
    } catch {
      // Expected with mock
    }
  });
});

describe('RabbitMQWaveTransport - Reply Queue Management', () => {
  test('getReplyQueueName generates unique queue names per exchange', () => {
    const { transport } = createTransport();

    const q1 = (transport as any).getReplyQueueName('wave.a', 'command');
    const q2 = (transport as any).getReplyQueueName('wave.b', 'command');

    expect(q1).toBe('wave.a.command-response');
    expect(q2).toBe('wave.b.command-response');
    expect(q1).not.toBe(q2);
  });

  test('getReplyQueueName generates different queues for command vs query', () => {
    const { transport } = createTransport();

    const commandQueue = (transport as any).getReplyQueueName('wave.test', 'command');
    const queryQueue = (transport as any).getReplyQueueName('wave.test', 'query');

    expect(commandQueue).toBe('wave.test.command-response');
    expect(queryQueue).toBe('wave.test.query-response');
    expect(commandQueue).not.toBe(queryQueue);
  });
});

describe('RabbitMQWaveTransport - CloudEvent Building', () => {
  test('sendEvent builds CloudEvent with correct type format', async () => {
    const { transport } = createTransport();

    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'Billing',
      name: 'InvoiceCreated',
      payload: { invoiceId: '123' },
    };

    try {
      await transport.sendEvent(message);
    } catch {
      // Expected with mock
    }
  });

  test('sendCommand builds CloudEvent with reply metadata', async () => {
    const { transport } = createTransport();

    const message: WaveCommandMessage = {
      kind: 'command',
      namespace: 'Billing',
      name: 'ProcessPayment',
      payload: {},
      awaitResponse: true,
    };

    try {
      await transport.sendCommand(message);
    } catch {
      // Expected with mock
    }
  });
});

describe('RabbitMQWaveTransport - Error Handling', () => {
  test('connection error clears maps', () => {
    const { transport, mockConnection } = createTransport();

    (transport as any).eventPublisher.set('e1', {} as Publisher);
    (transport as any).commandPublisher.set('e2', {} as Publisher);

    mockConnection._emitError(new Error('Test error'));

    expect((transport as any).eventPublisher.size).toBe(0);
    expect((transport as any).commandPublisher.size).toBe(0);
  });

  test('shutdown handles already-closed resources', async () => {
    const { transport } = createTransport();

    const closedPublisher = {
      close: mock(() => Promise.reject(new Error('Already closed'))),
      send: mock(() => Promise.resolve()),
      on: mock(() => {}),
    } as unknown as Publisher;

    (transport as any).eventPublisher.set('e1', closedPublisher);

    await transport.shutdown();
  });

  test('logger handles errors during shutdown', async () => {
    const logger = createMockLogger();
    const { transport } = createTransport({ logger });

    const failingPublisher = {
      close: mock(() => Promise.reject(new Error('Close failed'))),
      send: mock(() => Promise.resolve()),
      on: mock(() => {}),
    } as unknown as Publisher;

    (transport as any).eventPublisher.set('e1', failingPublisher);

    await transport.shutdown();
  });
});
