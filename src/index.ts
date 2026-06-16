/**
 * @file index.ts - Main entry point for the RabbitMQ Wave transport module.
 *
 * This module provides a high-level abstraction for communicating with RabbitMQ
 * using the Wave bus pattern. It supports three message types: commands (request/response),
 * events (fire-and-forget), and queries (synchronous data retrieval).
 *
 * @example
 * ```typescript
 * import createBus from '@moj/bus/rabbitmq';
 *
 * const bus = createBus({
 *   prefetchCount: 10
 * });
 *
 * // Send an event
 * await bus.sendEvent({
 *   namespace: 'Billing.Invoice',
 *   name: 'InvoiceCreated',
 *   kind: 'event',
 *   payload: { invoiceId: '123', amount: 100 }
 * });
 *
 * // Register a handler
 * await bus.addEventListener('Billing.Invoice', 'InvoiceCreated', async (message) => {
 *   console.log('Invoice created:', message);
 * });
 * ```
 */

import { RabbitMQWaveTransport } from './RabbitMQWaveTransport';
import type { RabbitMQConfig } from './RabbitMQBase';
import type { WaveLogger } from './Logging';
import { NoopWaveLogger } from './Logging';
import type { WavePropagator } from './Propagation';
import { NoopWavePropagator } from './Propagation';
import type { WaveTracer } from './Tracing';
import { NoopWaveTracer } from './Tracing';

/**
 * Re-exports the WaveTransport interface for external use.
 * @see {@link WaveTransport}
 */
export type { WaveTransport } from './WaveTransport';

/**
 * Re-exports context types for external use.
 */
export type { WaveBaseContext, WaveEventContext, WaveCommandContext, WaveQueryContext } from './WaveTransport';

/**
 * Re-exports the WaveLogger interface for external use.
 * @see {@link WaveLogger}
 */
export type { WaveLogger } from './Logging';

/**
 * Re-exports the WavePropagator interface for external use.
 * @see {@link WavePropagator}
 */
export type { WavePropagator } from './Propagation';

/**
 * Re-exports the WaveTracer interface for external use.
 * @see {@link WaveTracer}
 */
export type { WaveTracer } from './Tracing';

/**
 * Re-exports message handler types, execution context, and listener options.
 * @see {@link WaveHandler}
 * @see {@link ExecutionContext}
 * @see {@link Unsubscribe}
 * @see {@link ListenerOptions}
 */
export type {
  WaveHandler,
  ExecutionContext,
  Unsubscribe,
  ListenerOptions,
} from './WaveTransport';

/**
 * Re-exports message types for external use.
 * @see {@link WaveCommandMessage}
 * @see {@link WaveEventMessage}
 * @see {@link WaveQueryMessage}
 * @see {@link WaveSendOptions}
 */
export type {
  WaveCommandMessage,
  WaveEventMessage,
  WaveQueryMessage,
  WaveSendOptions,
  WaveMessageKind,
  WaveBaseMessage,
  WaveErrorResponse,
} from './WaveTransport';

/**
 * Re-exports RabbitMQ configuration type.
 * @see {@link RabbitMQConfig}
 */
export type { RabbitMQConfig } from './RabbitMQBase';

/**
 * Argument types for RabbitMQWaveTransport public methods.
 * These types allow consumers to import and use the exact parameter types
 * expected by each method without needing to reconstruct them.
 */

/**
 * Arguments for the `sendCommand` method.
 * @see {@link RabbitMQWaveTransport.sendCommand}
 */
export type SendCommandArgs<
  TPayload = unknown,
  TResponse = unknown,
  TContext extends Record<string, any> = Record<string, any>,
> = [
  message: import('./WaveTransport').WaveCommandMessage<TPayload, TResponse, TContext>,
  options?: import('./WaveTransport').WaveSendOptions,
];

/**
 * Arguments for the `sendEvent` method.
 * @see {@link RabbitMQWaveTransport.sendEvent}
 */
export type SendEventArgs<
  TPayload = unknown,
  TContext extends Record<string, any> = Record<string, any>,
> = [
  message: import('./WaveTransport').WaveEventMessage<TPayload, TContext>,
  options?: import('./WaveTransport').WaveSendOptions,
];

/**
 * Arguments for the `sendQuery` method.
 * @see {@link RabbitMQWaveTransport.sendQuery}
 */
export type SendQueryArgs<
  TPayload = unknown,
  TResponse = unknown,
  TContext extends Record<string, any> = Record<string, any>,
> = [
  message: import('./WaveTransport').WaveQueryMessage<TPayload, TResponse, TContext>,
  options?: import('./WaveTransport').WaveSendOptions,
];

/**
 * Arguments for the `addCommandListener` method.
 * @see {@link RabbitMQWaveTransport.addCommandListener}
 */
export type AddCommandListenerArgs = [
  namespace: string,
  name: string,
  handler: import('./WaveTransport').BaseMessageHandler,
  options?: import('./WaveTransport').ListenerOptions,
];

/**
 * Arguments for the `addEventListener` method.
 * @see {@link RabbitMQWaveTransport.addEventListener}
 */
export type AddEventListenerArgs = [
  namespace: string,
  name: string,
  handler: import('./WaveTransport').BaseMessageHandler,
  options?: import('./WaveTransport').ListenerOptions,
];

/**
 * Arguments for the `addQueryListener` method.
 * @see {@link RabbitMQWaveTransport.addQueryListener}
 */
export type AddQueryListenerArgs = [
  namespace: string,
  name: string,
  handler: import('./WaveTransport').BaseMessageHandler,
  options?: import('./WaveTransport').ListenerOptions,
];

/**
 * Arguments for the `shutdown` method.
 * @see {@link RabbitMQWaveTransport.shutdown}
 */
export type ShutdownArgs = [];

/**
 * Configuration options for creating a RabbitMQ Wave bus instance.
 *
 * This function provides a convenient way to create a fully configured
 * RabbitMQWaveTransport instance with sensible defaults. The connection URL
 * defaults to a local RabbitMQ instance if the `RABBITMQ_URL` environment
 * variable is not set.
 *
 * @param config - Partial configuration overriding defaults.
 *   - `url` - RabbitMQ connection URL (defaults to `process.env.RABBITMQ_URL` or `'amqp://guest:guest@localhost:5672'`)
 *   - `prefetchCount` - Number of messages to prefetch per consumer (defaults to `2`)
 *   - `reconnect` - Whether to automatically reconnect on connection loss
 *
 * @param tracer - Distributed tracing implementation for creating spans around operations.
 *   Defaults to a no-op implementation when not provided.
 *
 * @param logger - Logging implementation for runtime messages.
 *   Defaults to a no-op logger when not provided.
 *
 * @param propagator - Trace context propagator for injecting/extracting distributed tracing metadata.
 *   Defaults to a no-op propagator when not provided.
 *
 * @returns A configured {@link RabbitMQWaveTransport} instance ready for use.
 *
 * @example
 * ```typescript
 * // Basic usage with defaults
 * const bus = createBus();
 *
 * // With custom prefetch count
 * const bus = createBus({ prefetchCount: 10 });
 *
 * // With OpenTelemetry integration
 * const bus = createBus(
 *   { prefetchCount: 5 },
 *   myOpenTelemetryTracer,
 *   myLogger,
 *   myW3CPropagator
 * );
 * ```
 */
export default function createBus(
  config: Partial<RabbitMQConfig> = {},
  tracer: WaveTracer = new NoopWaveTracer(),
  logger: WaveLogger = new NoopWaveLogger(),
  propagator: WavePropagator = new NoopWavePropagator()
): RabbitMQWaveTransport {
  const bus = new RabbitMQWaveTransport(
    {
      url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
      prefetchCount: 2,
      ...config,
    },
    tracer,
    logger,
    propagator
  );

  return bus;
}
