/**
 * @file RabbitMQBase.ts - Abstract base class for RabbitMQ-backed Wave transports.
 *
 * Provides connection management, lifecycle hooks, and factory methods for
 * creating consumers, publishers, and RPC clients. Subclasses (e.g.
 * RabbitMQWaveTransport) extend this to add specific message-handling logic.
 */

import { Connection, type RPCProps } from 'rabbitmq-client';
import type {
  Consumer,
  ConsumerHandler,
  Publisher,
  RPCClient,
} from 'rabbitmq-client';
import type { WaveLogger } from './Logging';
import { NoopWaveLogger } from './Logging';

/**
 * Configuration for connecting to a RabbitMQ broker.
 */
export interface RabbitMQConfig {
  /** RabbitMQ connection URL, e.g. `'amqp://guest:guest@localhost:5672'`. */
  url: string;

  /** Number of unacknowledged messages to prefetch per consumer. */
  prefetchCount?: number;

  /** Whether to automatically reconnect on connection loss. */
  reconnect?: boolean;
}

/**
 * Abstract base class providing RabbitMQ connection management and lifecycle hooks.
 *
 * Subclasses should implement message-specific setup (publishers, consumers, RPC clients)
 * in the `beforeShutdown` and `onConnectionEstablished` hooks.
 */
export abstract class RabbitMQBase {
  /** The underlying AMQP connection managed by this base. */
  protected connection: Connection;

  /** The logger used for runtime messages. */
  protected readonly logger: WaveLogger;

  /** Whether the connection has been initialized. */
  private initialized = false;

  /** A pending promise for the in-flight connection attempt, or null. */
  private connectingPromise: Promise<void> | null = null;

  /**
   * Creates a new RabbitMQBase instance.
   *
   * @param config - RabbitMQ connection configuration.
   * @param logger - Logging implementation. Defaults to no-op.
   */
  constructor(
    protected readonly config: RabbitMQConfig,
    logger: WaveLogger = new NoopWaveLogger()
  ) {
    this.logger = logger;
    this.connection = new Connection(config.url);

    this.connection.on('error', (err) => {
      this.onConnectionError(err);
    });

    this.connection.on('connection', () => {
      this.onConnectionEstablished();
    });
  }

  /**
   * Ensures the underlying connection is established.
   *
   * If the connection is already initialized, returns immediately. If a
   * connection attempt is already in progress, returns the same promise
   * to avoid duplicate connection attempts.
   *
   * @returns A promise that resolves when the connection is ready or rejects
   *   with an error if the connection fails or times out after 30 seconds.
   */
  public async ensureConnected(): Promise<void> {
    if (this.initialized) {
      return;
    }

    if (this.connectingPromise) {
      return this.connectingPromise;
    }

    this.connectingPromise = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'));
      }, 30000);

      const onError = (err: Error) => {
        clearTimeout(timeout);
        this.connection.off('connection', onConnected);
        this.connection.off('error', onError);
        reject(err);
      };

      const onConnected = () => {
        clearTimeout(timeout);
        this.connection.off('connection', onConnected);
        this.connection.off('error', onError);
        resolve();
      };

      this.connection.on('error', onError);
      this.connection.on('connection', onConnected);

      // Start connection if not already started
      if (!this.initialized) {
        this.initialized = true;
      }
    });

    try {
      await this.connectingPromise;
    } finally {
      this.connectingPromise = null;
    }
  }

  /**
   * Initializes the connection once (e.g. from your app bootstrap).
   *
   * @deprecated Use {@link ensureConnected} instead.
   * @returns A promise that resolves when the connection is ready.
   */
  public async init(): Promise<void> {
    await this.ensureConnected();
  }

  /**
   * Creates a high-level consumer bound to a queue.
   *
   * @param options - Consumer creation options forwarded to the underlying connection.
   * @param handler - Function called for each delivered message.
   * @returns A new Consumer instance.
   * @protected
   */
  protected createConsumer(
    options: Parameters<Connection['createConsumer']>[0],
    handler: ConsumerHandler
  ): Consumer {
    return this.connection.createConsumer(options, handler);
  }

  /**
   * Creates a high-level publisher for sending messages.
   *
   * @param options - Publisher creation options forwarded to the underlying connection.
   * @returns A new Publisher instance.
   * @protected
   */
  protected createPublisher(
    options: Parameters<Connection['createPublisher']>[0] // TODO: use PublisherProps of rabbitmq-client as type
  ): Publisher {
    return this.connection.createPublisher(options);
  }

  /**
   * Creates an RPC client for request/response communication.
   *
   * @param options - RPC client configuration (e.g. confirm mode, reply queue).
   * @returns A new RPCClient instance.
   * @protected
   */
  protected createRPCClient(options: RPCProps): RPCClient {
    return this.connection.createRPCClient(options);
  }

  /**
   * Performs a graceful shutdown: calls the subclass `beforeShutdown` hook,
   * then closes the underlying connection.
   *
   * Call this from your application's `SIGINT` / `SIGTERM` handlers.
   *
   * @returns A promise that resolves when shutdown is complete.
   */
  public async shutdown(): Promise<void> {
    await this.beforeShutdown();

    try {
      await this.connection.close();
    } catch (err) {
      // last resort, subclasses can override beforeShutdown/onConnectionError for logging
      this.onConnectionError(err as Error);
    }
  }

  /**
   * Hook called after the RabbitMQ connection is (re)established.
   *
   * Subclasses can override this to (re)create publishers, consumers, or RPC clients.
   *
   * @protected
   */
  protected onConnectionEstablished(): void {
    this.logger.info('rabbitmq.connection.established', {
      url: this.config.url,
    });
  }

  /**
   * Hook called when a connection or channel error occurs.
   *
   * Subclasses can override this to handle specific error scenarios.
   *
   * @param error - The error that occurred.
   * @protected
   */
  protected onConnectionError(error: Error): void {
    this.logger.error('rabbitmq.connection.error', {
      url: this.config.url,
      message: error.message,
      stack: error.stack,
    });
  }

  /**
   * Hook called just before the connection is closed during shutdown.
   *
   * Subclasses should override this to close any publishers, consumers, or
   * RPC clients they have created, so they can be properly cleaned up.
   *
   * @protected
   * @returns A promise that resolves when all resources are closed.
   */
  protected async beforeShutdown(): Promise<void> {
    // default: no-op
  }

  /**
   * Checks whether the connection has been initialized.
   *
   * Note: This reflects whether `ensureConnected` was called successfully,
   * not whether the underlying socket is still alive.
   *
   * @returns True if the connection has been initialized.
   */
  public isConnected(): boolean {
    // Connection status is tracked by the initialized flag
    // The connection object itself doesn't expose isClosed
    return this.initialized;
  }

  /**
   * Returns the connection URL, useful for debugging.
   *
   * @returns The RabbitMQ connection URL.
   */
  public getConnectionUrl(): string {
    return this.config.url;
  }
}
