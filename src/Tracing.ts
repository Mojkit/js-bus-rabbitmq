/**
 * @file Tracing.ts - Wave tracing abstraction for integration with external telemetry (e.g. OpenTelemetry).
 *
 * This module defines a minimal tracing interface and provides a no-op implementation.
 * A concrete OpenTelemetry-backed implementation should live in the base Wave package
 * and be injected into this transport via dependency injection.
 */

/**
 * Interface for distributed tracing integration.
 *
 * Implementations should create trace spans around async operations to enable
 * end-to-end observability across service boundaries.
 */
export interface WaveTracer {
  /**
   * Run an async operation within a tracing span.
   *
   * The returned span should be automatically activated as the active context
   * for the duration of the provided function. Upon completion, the span
   * should be ended.
   *
   * @param name - A human-readable name for the span operation.
   * @param fn - The async function to execute within the span's context.
   * @returns A promise that resolves with the result of the provided function.
   *
   * @example
   * ```typescript
   * const result = await tracer.startActiveSpan('wave.sendCommand', async () => {
   *   return await bus.sendCommand(myCommand);
   * });
   * ```
   */
  startActiveSpan<T>(name: string, fn: () => Promise<T>): Promise<T>;
}

/**
 * Default no-op implementation of {@link WaveTracer}.
 *
 * This tracer discards all span operations and simply invokes the provided
 * function. It is used by default when no external tracer is injected, keeping
 * the transport independent from any specific telemetry SDK.
 *
 * @example
 * ```typescript
 * // Explicitly using the no-op tracer
 * const bus = createBus({}, new NoopWaveTracer(), myLogger, myPropagator);
 * ```
 */
export class NoopWaveTracer implements WaveTracer {
  async startActiveSpan<T>(name: string, fn: () => Promise<T>): Promise<T> {
    // Intentionally unused - placeholder for future tracing implementation
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _ignored = name;
    void _ignored;
    return fn();
  }
}
