/**
 * @file RabbitMQBase.test.ts - Unit tests for RabbitMQBase abstract class.
 *
 * Tests connection management, lifecycle hooks, factory methods, and
 * the ensureConnected/shutdown mechanisms using mocked dependencies.
 */

import { test, expect, describe, beforeEach, spyOn, mock } from 'bun:test';
import { Connection } from 'rabbitmq-client';
import type {
  Consumer,
  ConsumerHandler,
  Publisher,
  RPCClient,
} from 'rabbitmq-client';
import { RabbitMQBase } from '../../src/RabbitMQBase';
import type { RabbitMQConfig } from '../../src/RabbitMQBase';
import type { WaveLogger } from '../../src/Logging';
import { NoopWaveLogger } from '../../src/Logging';
import {
  createMockLogger,
  createMockTracer,
  createMockPropagator,
} from './mocks';

/**
 * Concrete implementation of RabbitMQBase for testing.
 * Exposes protected methods for unit testing.
 */
class TestableRabbitMQBase extends RabbitMQBase {
  public onConnectionEstablishedCalled = 0;
  public onConnectionErrorCalled = 0;
  public beforeShutdownCalled = 0;
  public lastConnectionError: Error | null = null;

  constructor(
    config: RabbitMQConfig,
    logger: WaveLogger = new NoopWaveLogger()
  ) {
    super(config, logger);
  }

  public override onConnectionEstablished(): void {
    super.onConnectionEstablished();
    this.onConnectionEstablishedCalled++;
  }

  public override onConnectionError(error: Error): void {
    super.onConnectionError(error);
    this.onConnectionErrorCalled++;
    this.lastConnectionError = error;
  }

  protected override async beforeShutdown(): Promise<void> {
    this.beforeShutdownCalled++;
  }
}

describe('RabbitMQBase - Constructor', () => {
  test('creates instance with default logger', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://guest:guest@localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    expect(base).toBeDefined();
    expect(base).toBeInstanceOf(RabbitMQBase);
    expect(base.isConnected()).toBe(false);
    expect(base.getConnectionUrl()).toBe('amqp://guest:guest@localhost:5672');
  });

  test('creates instance with custom logger', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);

    expect(base).toBeDefined();
    expect(base.isConnected()).toBe(false);
  });

  test('stores connection URL from config', () => {
    const customUrl = 'amqp://user:pass@custom-host:5673/myvhost';
    const config: RabbitMQConfig = {
      url: customUrl,
    };

    const base = new TestableRabbitMQBase(config);

    expect(base.getConnectionUrl()).toBe(customUrl);
  });

  test('isConnected returns false before ensureConnected', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    expect(base.isConnected()).toBe(false);
  });

  test('handles config with prefetchCount', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
      prefetchCount: 10,
    };

    const base = new TestableRabbitMQBase(config);

    expect(base).toBeDefined();
    expect(base.getConnectionUrl()).toBe('amqp://localhost:5672');
  });

  test('handles config with reconnect option', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
      reconnect: true,
    };

    const base = new TestableRabbitMQBase(config);

    expect(base).toBeDefined();
  });

  test('handles config with all options', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://guest:guest@localhost:5672',
      prefetchCount: 20,
      reconnect: false,
    };

    const base = new TestableRabbitMQBase(config);

    expect(base).toBeDefined();
  });
});

describe('RabbitMQBase - ensureConnected', () => {
  test('ensureConnected returns immediately if already initialized', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);
    base['initialized'] = true;

    const result = await base.ensureConnected();

    expect(result).toBeUndefined();
  });

  test('ensureConnected prevents duplicate connection attempts', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    // Manually set a pending connecting promise
    const pendingPromise = new Promise<void>((resolve) => {
      setTimeout(() => resolve(), 100);
    });
    (base as any).connectingPromise = pendingPromise;

    const start = Date.now();
    const results = await Promise.all([
      base.ensureConnected(),
      base.ensureConnected(),
      base.ensureConnected(),
    ]);
    const elapsed = Date.now() - start;

    // All should return the same promise (no delay from sequential calls)
    expect(results).toHaveLength(3);
    expect(results.every((r) => r === undefined)).toBe(true);
  });

  test('ensureConnected rejects on timeout', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    // Mock Connection to never fire 'connection' event
    const originalConnection = base['connection'];

    // Force a mock connection that never connects
    const mockConn = {
      on: mock(() => {}),
      off: mock(() => {}),
    } as any;

    (base as any).connection = mockConn;

    // Initialize to allow start
    (base as any).initialized = false;

    // Reset the connecting promise to trigger a new one
    (base as any).connectingPromise = null;

    try {
      await base.ensureConnected();
      expect(true).toBe(false); // Should have timed out
    } catch (err: any) {
      expect(err.message).toBe('Connection timeout');
    }
  });

  test('ensureConnected resolves when connection succeeds', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    const mockConn = {
      on: mock(() => {}),
      off: mock(() => {}),
    } as any;

    (base as any).connection = mockConn;

    // Trigger the connection callback directly to simulate successful connection
    const internalOnInit = (base as any).ensureConnected.bind(base);

    // Manually simulate connection success by resolving the internal promise
    // We'll do this by mocking the Connection 'connection' event
    let connectionCallback: (() => void) | undefined;

    const mockConn2 = {
      on: mock((event: string, cb: () => void) => {
        if (event === 'connection') {
          connectionCallback = cb;
        }
      }),
      off: mock(() => {}),
    } as any;

    (base as any).connection = mockConn2;
    (base as any).connectingPromise = null;
    (base as any).initialized = false;

    const promise = base.ensureConnected();

    // Simulate connection event
    setTimeout(() => {
      connectionCallback?.();
    }, 10);

    await expect(promise).resolves.toBeUndefined();
    expect(base.isConnected()).toBe(true);
  });
});

describe('RabbitMQBase - Connection Callbacks', () => {
  test('onConnectionEstablished hook is called on connection event', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);

    // Simulate connection event by accessing internal connection
    const mockConn = {
      on: mock(() => {}),
      off: mock(() => {}),
    } as any;
    (base as any).connection = mockConn;

    // Trigger onConnectionEstablished directly
    (base as any).onConnectionEstablished();

    expect(base.onConnectionEstablishedCalled).toBe(1);
    expect(
      logger.messages.some(
        (m) => m.message === 'rabbitmq.connection.established'
      )
    ).toBe(true);
  });

  test('onConnectionError hook is called on error event', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);

    const error = new Error('Connection failed');

    // Trigger onConnectionError directly
    (base as any).onConnectionError(error);

    expect(base.onConnectionErrorCalled).toBe(1);
    expect(base.lastConnectionError).toBe(error);
    expect(
      logger.messages.some((m) => m.message === 'rabbitmq.connection.error')
    ).toBe(true);
    expect(
      logger.messages.some((m) => m.meta?.message === 'Connection failed')
    ).toBe(true);
  });

  test('connection error callback logs error details', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);

    const error = new Error('Test error with stack');
    (base as any).onConnectionError(error);

    const errorMessages = logger.messages.filter((m) => m.level === 'error');
    expect(errorMessages.length).toBeGreaterThan(0);
    expect(errorMessages[0].message).toBe('rabbitmq.connection.error');
  });

  test('connection established callback logs URL', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://user:pass@host:5672/vhost',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);
    (base as any).onConnectionEstablished();

    const infoMessages = logger.messages.filter((m) => m.level === 'info');
    expect(infoMessages.length).toBeGreaterThan(0);
    expect(infoMessages[0].message).toBe('rabbitmq.connection.established');
  });
});

describe('RabbitMQBase - Factory Methods', () => {
  test('createConsumer is exposed and callable', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const base = new TestableRabbitMQBase(config);

    // spyOn the connection's createConsumer
    const mockConsumer = {
      start: mock(() => {}),
      close: mock(() => Promise.resolve()),
      on: mock(() => {}),
    } as unknown as Consumer;

    const originalCreateConsumer = (base as any).connection.createConsumer;
    (base as any).connection.createConsumer = mock(() => mockConsumer);

    try {
      const handler: ConsumerHandler = async () => {};
      const options = { queue: 'test.queue', exchanges: [] };

      const consumer = (base as any).createConsumer(options, handler);

      expect(consumer).toBe(mockConsumer);
    } finally {
      (base as any).connection.createConsumer = originalCreateConsumer;
    }
  });

  test('createPublisher is exposed and callable', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const base = new TestableRabbitMQBase(config);

    const mockPublisher = {
      send: mock(() => Promise.resolve()),
      close: mock(() => Promise.resolve()),
      on: mock(() => {}),
    } as unknown as Publisher;

    const originalCreatePublisher = (base as any).connection.createPublisher;
    (base as any).connection.createPublisher = mock(() => mockPublisher);

    try {
      const options = { confirm: true };
      const publisher = (base as any).createPublisher(options);

      expect(publisher).toBe(mockPublisher);
    } finally {
      (base as any).connection.createPublisher = originalCreatePublisher;
    }
  });

  test('createRPCClient is exposed and callable', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const base = new TestableRabbitMQBase(config);

    const mockRPCClient = {
      send: mock(() => Promise.resolve({})),
      close: mock(() => Promise.resolve()),
      on: mock(() => {}),
    } as unknown as RPCClient;

    const originalCreateRPCClient = (base as any).connection.createRPCClient;
    (base as any).connection.createRPCClient = mock(() => mockRPCClient);

    try {
      const rpcProps = { confirm: true };
      const client = (base as any).createRPCClient(rpcProps);

      expect(client).toBe(mockRPCClient);
    } finally {
      (base as any).connection.createRPCClient = originalCreateRPCClient;
    }
  });

  test('factory methods forward options to connection', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    let capturedConsumerOptions: any = null;
    let capturedPublisherOptions: any = null;
    let capturedRPCOptions: any = null;

    const mockConnection = {
      on: mock(() => {}),
      off: mock(() => {}),
      createConsumer: mock((options: any, handler: ConsumerHandler) => {
        capturedConsumerOptions = options;
        return {} as Consumer;
      }),
      createPublisher: mock((options: any) => {
        capturedPublisherOptions = options;
        return {} as Publisher;
      }),
      createRPCClient: mock((options: any) => {
        capturedRPCOptions = options;
        return {} as RPCClient;
      }),
    } as any;

    const base = new TestableRabbitMQBase(config);
    (base as any).connection = mockConnection;

    (base as any).createConsumer(
      { queue: 'test', exchanges: [{ exchange: 'test', type: 'topic' }] },
      async () => {}
    );
    (base as any).createPublisher({ confirm: true, maxAttempts: 3 });
    (base as any).createRPCClient({ confirm: true });

    expect(capturedConsumerOptions?.queue).toBe('test');
    expect(capturedPublisherOptions?.confirm).toBe(true);
    expect(capturedRPCOptions?.confirm).toBe(true);
  });
});

describe('RabbitMQBase - Shutdown', () => {
  test('shutdown calls beforeShutdown hook', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    let beforeShutdownCalled = false;
    (base as any).beforeShutdown = async () => {
      beforeShutdownCalled = true;
    };

    // Mock connection close
    const mockConnection = {
      on: mock(() => {}),
      off: mock(() => {}),
      close: mock(() => Promise.resolve()),
    } as any;
    (base as any).connection = mockConnection;

    await base.shutdown();

    expect(beforeShutdownCalled).toBe(true);
    expect(mockConnection.close).toHaveBeenCalled();
  });

  test('shutdown handles connection close errors gracefully', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);

    const mockConnection = {
      on: mock(() => {}),
      off: mock(() => {}),
      close: mock(() => Promise.reject(new Error('Close failed'))),
    } as any;
    (base as any).connection = mockConnection;

    await base.shutdown();

    // Should not throw - errors are caught
    const errorMessages = logger.messages.filter((m) => m.level === 'error');
    expect(errorMessages.length).toBeGreaterThan(0);
  });

  test('shutdown after ensureConnected', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    const mockConnection = {
      on: mock(() => {}),
      off: mock(() => {}),
      close: mock(() => Promise.resolve()),
    } as any;
    (base as any).connection = mockConnection;

    // Initialize as connected
    (base as any).initialized = true;

    await base.shutdown();

    expect(mockConnection.close).toHaveBeenCalled();
  });

  test('multiple shutdown calls are safe', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    const mockConnection = {
      on: mock(() => {}),
      off: mock(() => {}),
      close: mock(() => Promise.resolve()),
    } as any;
    (base as any).connection = mockConnection;

    await base.shutdown();
    await base.shutdown();
    await base.shutdown();

    // Multiple close calls should not throw
    expect(mockConnection.close).toHaveBeenCalledTimes(3);
  });

  test('beforeShutdown can be overridden by subclasses', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    class CustomBase extends RabbitMQBase {
      cleanupCalled = false;
      cleanupResource: string | null = null;

      protected async beforeShutdown(): Promise<void> {
        this.cleanupCalled = true;
        this.cleanupResource = 'test-resource';
      }
    }

    const base = new CustomBase(config);
    const mockConnection = {
      on: mock(() => {}),
      off: mock(() => {}),
      close: mock(() => Promise.resolve()),
    } as any;
    (base as any).connection = mockConnection;

    await base.shutdown();

    expect((base as any).cleanupCalled).toBe(true);
    expect((base as any).cleanupResource).toBe('test-resource');
  });
});

describe('RabbitMQBase - Deprecated init method', () => {
  test('init calls ensureConnected', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    let ensureConnectedCalled = false;
    const originalEnsureConnected = (base as any).ensureConnected.bind(base);
    (base as any).ensureConnected = mock(async () => {
      ensureConnectedCalled = true;
    });

    await (base as any).init();

    expect(ensureConnectedCalled).toBe(true);
  });

  test('init is marked as deprecated but still works', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    // The method exists and is callable (JSDoc @deprecated doesn't affect runtime)
    expect(typeof (base as any).init).toBe('function');
  });
});

describe('RabbitMQBase - isConnected', () => {
  test('returns false before connection', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    expect(base.isConnected()).toBe(false);
  });

  test('returns true after successful connection', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);
    (base as any).initialized = true;

    expect(base.isConnected()).toBe(true);
  });

  test('reflects initialization state not socket state', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);

    // Even if connection is closed, isConnected reflects initialized flag
    (base as any).initialized = true;

    expect(base.isConnected()).toBe(true);
  });
});

describe('RabbitMQBase - getConnectionUrl', () => {
  test('returns the configured URL', () => {
    const url = 'amqp://guest:guest@localhost:5672';
    const config: RabbitMQConfig = { url };

    const base = new TestableRabbitMQBase(config);

    expect(base.getConnectionUrl()).toBe(url);
  });

  test('returns URL with credentials', () => {
    const url = 'amqp://user:password@host:5672';
    const config: RabbitMQConfig = { url };

    const base = new TestableRabbitMQBase(config);

    expect(base.getConnectionUrl()).toBe(url);
  });

  test('returns URL with vhost', () => {
    const url = 'amqp://localhost:5672/myvhost';
    const config: RabbitMQConfig = { url };

    const base = new TestableRabbitMQBase(config);

    expect(base.getConnectionUrl()).toBe(url);
  });

  test('returns URL with all components', () => {
    const url = 'amqp://user:pass@host:5673/vhost?reconnect=true';
    const config: RabbitMQConfig = { url };

    const base = new TestableRabbitMQBase(config);

    expect(base.getConnectionUrl()).toBe(url);
  });
});

describe('RabbitMQBase - Logger Behavior', () => {
  test('noop logger discards all messages', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const logger = new NoopWaveLogger();
    const base = new TestableRabbitMQBase(config, logger);

    // Should not throw with noop logger
    expect(() => {
      (base as any).onConnectionEstablished();
      (base as any).onConnectionError(new Error('test'));
    }).not.toThrow();
  });

  test('custom logger receives all log levels', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);

    (base as any).onConnectionEstablished();
    (base as any).onConnectionError(new Error('test error'));

    const infoMessages = logger.messages.filter((m) => m.level === 'info');
    const errorMessages = logger.messages.filter((m) => m.level === 'error');

    expect(infoMessages.length).toBe(1);
    expect(errorMessages.length).toBe(1);
  });

  test('logger receives metadata in connection established', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);
    (base as any).onConnectionEstablished();

    const infoMessages = logger.messages.filter(
      (m) => m.message === 'rabbitmq.connection.established'
    );
    expect(infoMessages.length).toBe(1);
    expect(infoMessages[0].meta?.url).toBe('amqp://localhost:5672');
  });

  test('logger receives error details in connection error', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);
    const error = new Error('Connection refused');
    (base as any).onConnectionError(error);

    const errorMessages = logger.messages.filter(
      (m) => m.message === 'rabbitmq.connection.error'
    );
    expect(errorMessages.length).toBe(1);
    expect(errorMessages[0].meta?.message).toBe('Connection refused');
    expect(errorMessages[0].meta?.url).toBe('amqp://localhost:5672');
    expect(errorMessages[0].meta?.stack).toBeDefined();
  });
});

describe('RabbitMQBase - Connection Events', () => {
  test('connection error event triggers onConnectionError', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);

    const mockConn = {
      on: mock((event: string, cb: (err: Error) => void) => {
        if (event === 'error') {
          cb(new Error('Socket error'));
        }
      }),
      off: mock(() => {}),
    } as any;
    (base as any).connection = mockConn;

    expect(base.onConnectionErrorCalled).toBe(1);
    expect(
      logger.messages.some((m) => m.message === 'rabbitmq.connection.error')
    ).toBe(true);
  });

  test('connection event triggers onConnectionEstablished', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };
    const logger = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger);

    const mockConn = {
      on: mock((event: string, cb: () => void) => {
        if (event === 'connection') {
          cb();
        }
      }),
      off: mock(() => {}),
    } as any;
    (base as any).connection = mockConn;

    expect(base.onConnectionEstablishedCalled).toBe(1);
    expect(
      logger.messages.some(
        (m) => m.message === 'rabbitmq.connection.established'
      )
    ).toBe(true);
  });
});

describe('RabbitMQBase - Abstract Nature', () => {
  test('RabbitMQBase cannot be instantiated directly', () => {
    expect(() => {
      // @ts-expect-error - RabbitMQBase is abstract
      new RabbitMQBase({ url: 'amqp://localhost:5672' });
    }).toThrow();
  });

  test('subclasses can extend RabbitMQBase', () => {
    class MyTransport extends RabbitMQBase {
      // Minimal implementation
    }

    const transport = new MyTransport({ url: 'amqp://localhost:5672' });
    expect(transport).toBeInstanceOf(RabbitMQBase);
    expect(transport).toBeInstanceOf(MyTransport);
  });

  test('subclass can override all lifecycle hooks', () => {
    let onEstablished = false;
    let onError = false;
    let beforeClose = false;

    class FullOverrideTransport extends RabbitMQBase {
      protected onConnectionEstablished(): void {
        onEstablished = true;
      }

      protected onConnectionError(error: Error): void {
        onError = true;
        super.onConnectionError(error);
      }

      protected async beforeShutdown(): Promise<void> {
        beforeClose = true;
      }
    }

    const transport = new FullOverrideTransport({
      url: 'amqp://localhost:5672',
    });
    const mockConnection = {
      on: mock(() => {}),
      off: mock(() => {}),
      close: mock(() => Promise.resolve()),
    } as any;
    (transport as any).connection = mockConnection;

    (transport as any).onConnectionEstablished();
    (transport as any).onConnectionError(new Error('test'));

    expect(onEstablished).toBe(true);
    expect(onError).toBe(true);

    return transport.shutdown().then(() => {
      expect(beforeClose).toBe(true);
    });
  });
});

describe('RabbitMQBase - Edge Cases', () => {
  test('handles unusual but valid URL formats', () => {
    const urls = [
      'amqp://localhost',
      'amqp://127.0.0.1:5672',
      'amqp://[::1]:5672',
      'amqp://user:@localhost:5672',
      'amqp://:password@localhost:5672',
    ];

    for (const url of urls) {
      const config: RabbitMQConfig = { url };
      const base = new TestableRabbitMQBase(config);
      expect(base.getConnectionUrl()).toBe(url);
    }
  });

  test('prefetchCount of zero is accepted', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
      prefetchCount: 0,
    };

    const base = new TestableRabbitMQBase(config);
    expect(base).toBeDefined();
  });

  test('prefetchCount with large value is accepted', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
      prefetchCount: 10000,
    };

    const base = new TestableRabbitMQBase(config);
    expect(base).toBeDefined();
  });

  test('reconnect option can be explicitly false', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
      reconnect: false,
    };

    const base = new TestableRabbitMQBase(config);
    expect(base).toBeDefined();
  });

  test('ensureConnected can be called many times safely when initialized', async () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const base = new TestableRabbitMQBase(config);
    (base as any).initialized = true;

    for (let i = 0; i < 10; i++) {
      await base.ensureConnected();
    }

    // No errors should occur
  });
});

describe('RabbitMQBase - Integration Patterns', () => {
  test('can be used as a dependency injection target', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    class ServiceThatDependsOnBase {
      constructor(public base: RabbitMQBase) {}
    }

    const base = new TestableRabbitMQBase(config);
    const service = new ServiceThatDependsOnBase(base);

    expect(service.base).toBe(base);
    expect(service.base.getConnectionUrl()).toBe('amqp://localhost:5672');
  });

  test('logger can be swapped at runtime', () => {
    const config: RabbitMQConfig = {
      url: 'amqp://localhost:5672',
    };

    const logger1 = createMockLogger();
    const logger2 = createMockLogger();

    const base = new TestableRabbitMQBase(config, logger1);

    // Replace logger
    (base as any).logger = logger2;

    (base as any).onConnectionEstablished();

    // Should use logger2, not logger1
    expect(logger1.messages.length).toBe(0);
    expect(logger2.messages.length).toBe(1);
  });

  test('protected methods are accessible in subclasses', () => {
    class SubclassWithAccess extends RabbitMQBase {
      testCreateConsumer() {
        return (this as any).createConsumer(
          { queue: 'test', exchanges: [] },
          async () => {}
        );
      }

      testCreatePublisher() {
        return (this as any).createPublisher({ confirm: true });
      }

      testCreateRPCClient() {
        return (this as any).createRPCClient({ confirm: true });
      }
    }

    const base = new SubclassWithAccess({ url: 'amqp://localhost:5672' });

    // These should not throw - protected methods are accessible in subclasses
    expect(base.testCreateConsumer).toBeDefined();
    expect(base.testCreatePublisher).toBeDefined();
    expect(base.testCreateRPCClient).toBeDefined();
  });
});
