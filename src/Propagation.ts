/**
 * @file Propagation.ts - Trace context propagation abstraction.
 *
 * This module does not depend on OpenTelemetry propagators directly.
 * The base Wave package can provide an implementation that injects W3C
 * trace context (e.g. traceparent/tracestate/baggage) into outgoing messages.
 */

export interface WavePropagator {
  /**
   * Return key/value pairs to be added to outgoing message metadata.
   * For CloudEvents this maps naturally to "extensions".
   */
  inject(): Record<string, string>;
}

export class NoopWavePropagator implements WavePropagator {
  inject(): Record<string, string> {
    return {};
  }
}
