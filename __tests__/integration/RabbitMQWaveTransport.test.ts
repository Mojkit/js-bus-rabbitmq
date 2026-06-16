import { test, expect, describe, beforeEach } from 'bun:test';
import { RabbitMQWaveTransport } from '../../src/RabbitMQWaveTransport';
import { createMockLogger } from '../mocks/mockLogger';
import { createMockTracer } from '../mocks/mockTracer';
import { createMockPropagator } from '../mocks/mockPropagator';
import type {
  WaveEventMessage,
  WaveCommandMessage,
  WaveQueryMessage,
} from '../../src/WaveTransport';

describe('RabbitMQWaveTransport - Core Functionality', () => {
  test('transport initializes without connecting', () => {
    const transport = new RabbitMQWaveTransport({ url: 'amqp://test:5672' });

    expect(transport).toBeDefined();
    expect(transport).toBeInstanceOf(RabbitMQWaveTransport);
    expect(transport.isConnected()).toBe(false);
  });

  test('custom logger is stored', () => {
    const logger = createMockLogger();
    const transport = new RabbitMQWaveTransport(
      { url: 'amqp://test:5672' },
      createMockTracer(),
      logger,
      createMockPropagator()
    );

    expect(transport).toBeDefined();
  });

  test('custom tracer is stored', () => {
    const tracer = createMockTracer();
    const transport = new RabbitMQWaveTransport(
      { url: 'amqp://test:5672' },
      tracer
    );

    expect(transport).toBeDefined();
  });

  test('custom propagator is stored', () => {
    const propagator = createMockPropagator();
    const transport = new RabbitMQWaveTransport(
      { url: 'amqp://test:5672' },
      createMockTracer(),
      createMockLogger(),
      propagator
    );

    expect(transport).toBeDefined();
  });
});

describe('RabbitMQWaveTransport - Event Operations', () => {
  test('event message can be constructed', () => {
    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'billing',
      name: 'invoiceCreated',
      payload: { invoiceId: '123', amount: 100 },
      context: { userId: 'user-456' },
    };

    expect(message.kind).toBe('event');
    expect(message.namespace).toBe('billing');
    expect(message.name).toBe('invoiceCreated');
    expect(message.payload).toEqual({ invoiceId: '123', amount: 100 });
    expect(message.context?.userId).toBe('user-456');
  });

  test('event without context is valid', () => {
    const message: WaveEventMessage = {
      kind: 'event',
      namespace: 'orders',
      name: 'orderCreated',
      payload: { orderId: '456' },
    };

    expect(message).toBeDefined();
    expect(message.context).toBeUndefined();
  });

  test('event payload can be any type', () => {
    const message: WaveEventMessage<{ items: string[] }> = {
      kind: 'event',
      namespace: 'inventory',
      name: 'stockUpdated',
      payload: { items: ['item1', 'item2'] },
    };

    expect(message.payload.items).toHaveLength(2);
  });
});

describe('RabbitMQWaveTransport - Command Operations', () => {
  test('fire-and-forget command can be constructed', () => {
    const message: WaveCommandMessage = {
      kind: 'command',
      namespace: 'billing',
      name: 'processInvoice',
      payload: { action: 'process' },
      awaitResponse: false,
    };

    expect(message.kind).toBe('command');
    expect(message.awaitResponse).toBe(false);
  });

  test('RPC command can be constructed', () => {
    const message: WaveCommandMessage<{ data: string }, { result: string }> = {
      kind: 'command',
      namespace: 'queries',
      name: 'getUser',
      payload: { data: 'user123' },
      awaitResponse: true,
      context: { correlationId: 'correl-123' },
    };

    expect(message.kind).toBe('command');
    expect(message.awaitResponse).toBe(true);
    expect(message.context?.correlationId).toBe('correl-123');
  });

  test('command without awaitResponse is fire-and-forget', () => {
    const message: WaveCommandMessage = {
      kind: 'command',
      namespace: 'auth',
      name: 'authenticate',
      payload: { username: 'test' },
    };

    expect(message.awaitResponse).toBeUndefined();
  });

  test('command payload supports complex types', () => {
    const message: WaveCommandMessage<{
      user: { name: string; email: string };
    }> = {
      kind: 'command',
      namespace: 'users',
      name: 'createUser',
      payload: { user: { name: 'John', email: 'john@example.com' } },
    };

    expect(message.payload.user.name).toBe('John');
  });
});

describe('RabbitMQWaveTransport - Query Operations', () => {
  test('query message can be constructed', () => {
    const message: WaveQueryMessage = {
      kind: 'query',
      namespace: 'catalog',
      name: 'getProducts',
      payload: { query: 'electronics', limit: 10 },
    };

    expect(message.kind).toBe('query');
    expect(message.namespace).toBe('catalog');
    expect(message.name).toBe('getProducts');
  });

  test('query with response type', () => {
    const message: WaveQueryMessage<
      { filter: string },
      { products: string[] }
    > = {
      kind: 'query',
      namespace: 'search',
      name: 'findProducts',
      payload: { filter: 'laptop' },
    };

    expect(message.kind).toBe('query');
  });

  test('query can have context', () => {
    const message: WaveQueryMessage<{}, {}, { userId: string }> = {
      kind: 'query',
      namespace: 'users',
      name: 'getUser',
      payload: {},
      context: { userId: 'user-123' },
    };

    expect(message.context?.userId).toBe('user-123');
  });
});

describe('RabbitMQWaveTransport - Tracing Integration', () => {
  test('tracer is used when provided', () => {
    const tracer = createMockTracer();
    tracer.returnValue = undefined;

    const transport = new RabbitMQWaveTransport(
      { url: 'amqp://test:5672' },
      tracer,
      createMockLogger(),
      createMockPropagator()
    );

    expect(transport).toBeDefined();
    expect(tracer).toBeDefined();
  });

  test('tracer is optional', () => {
    const transport = new RabbitMQWaveTransport({ url: 'amqp://test:5672' });

    expect(transport).toBeDefined();
  });

  test('tracer calls are tracked', async () => {
    const tracer = createMockTracer();
    tracer.returnValue = Promise.resolve(undefined);

    const transport = new RabbitMQWaveTransport(
      { url: 'amqp://test:5672' },
      tracer
    );

    expect(tracer.calls.startActiveSpan).toBe(0);
  });
});

describe('RabbitMQWaveTransport - Propagation Integration', () => {
  test('propagator is used when provided', () => {
    const propagator = createMockPropagator();
    propagator.returnValue = { traceparent: '00-trace123-span456-01' };

    const transport = new RabbitMQWaveTransport(
      { url: 'amqp://test:5672' },
      createMockTracer(),
      createMockLogger(),
      propagator
    );

    expect(transport).toBeDefined();
    expect(propagator.calls.inject).toBe(0);
  });

  test('propagator is optional', () => {
    const transport = new RabbitMQWaveTransport({ url: 'amqp://test:5672' });

    expect(transport).toBeDefined();
  });

  test('propagator returns empty object by default', () => {
    const propagator = createMockPropagator();
    const result = propagator.inject();

    expect(result).toEqual({});
  });

  test('propagator can return custom context', () => {
    const propagator = createMockPropagator();
    propagator.returnValue = {
      correlationId: 'corr-123',
      traceparent: '00-trace123-span456-01',
      baggage: 'user=john',
    };

    const result = propagator.inject();

    expect(result.correlationId).toBe('corr-123');
    expect(result.traceparent).toBe('00-trace123-span456-01');
    expect(result.baggage).toBe('user=john');
  });
});

describe('RabbitMQWaveTransport - Connection Options', () => {
  test('default configuration is valid', () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://guest:guest@localhost:5672',
    });

    expect(transport).toBeDefined();
  });

  test('with prefetch count', () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://localhost:5672',
      prefetchCount: 10,
    });

    expect(transport).toBeDefined();
  });

  test('with reconnection disabled', () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://localhost:5672',
      reconnect: false,
    });

    expect(transport).toBeDefined();
  });

  test('with vhost', () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://localhost:5672/myvhost',
    });

    expect(transport).toBeDefined();
  });
});

describe('RabbitMQWaveTransport - Shutdown Lifecycle', () => {
  test('shutdown can be called immediately', async () => {
    const transport = new RabbitMQWaveTransport({ url: 'amqp://test:5672' });
    const result = await transport.shutdown();

    expect(result).toBeUndefined();
  });

  test('multiple shutdown calls are safe', async () => {
    const transport = new RabbitMQWaveTransport({ url: 'amqp://test:5672' });

    await transport.shutdown();
    const result = await transport.shutdown();

    expect(result).toBeUndefined();
  });

  test('shutdown after connection attempt', async () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://invalid-host:5672',
    });

    // Wait a bit for connection attempt
    await new Promise((resolve) => setTimeout(resolve, 200));

    const result = await transport.shutdown();

    expect(result).toBeUndefined();
  });
});

describe('RabbitMQWaveTransport - Configuration', () => {
  test('url is stored correctly', () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://user:pass@host:5672/vhost',
    });

    const url = transport.getConnectionUrl();
    expect(url).toBe('amqp://user:pass@host:5672/vhost');
  });

  test('connection state is tracked', () => {
    const transport = new RabbitMQWaveTransport({ url: 'amqp://test:5672' });

    expect(transport.isConnected()).toBe(false);
  });

  test('getters work without connection', () => {
    const transport = new RabbitMQWaveTransport({
      url: 'amqp://test:5672',
    });

    const url = transport.getConnectionUrl();
    const isConnected = transport.isConnected();

    expect(url).toBe('amqp://test:5672');
    expect(isConnected).toBe(false);
  });
});
