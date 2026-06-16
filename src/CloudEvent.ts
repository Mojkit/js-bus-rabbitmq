// src/CloudEvent.ts
// Minimal CloudEvents v1.0 type and helpers.
// In this step we only define the shape and a helper to build
// CloudEvents from Wave messages.

import { randomUUID } from 'node:crypto';
import type { WaveBaseContext } from './WaveTransport';

export type CloudEventSpecVersion = '1.0';

/**
 * Minimal CloudEvents v1.0 interface (JSON event format).
 * See https://github.com/cloudevents/spec for full details.
 */
export interface CloudEventV1<
  TData = unknown,
  TExtensions extends Record<string, any> = Record<string, any>,
> {
  specversion: CloudEventSpecVersion;
  id: string;
  source: string;
  type: string;
  subject?: string;
  time?: string;
  datacontenttype?: string;
  dataschema?: string;
  data?: TData;
  /**
   * Extension attributes – this is where we will place Wave context
   * (correlation IDs, tenant, user, trace IDs, etc.).
   */
  extensions?: TExtensions;
}

export interface CloudEventBuildOptions {
  /**
   * CloudEvents "source". For Wave this is typically the bounded context
   * or service name, e.g. "wave://billing/Invoice".
   */
  source: string;

  /**
   * Extra extension attributes to merge onto the outgoing CloudEvent.
   * This is where trace propagation fields (e.g. traceparent) should be added.
   */
  extensions?: Record<string, any>;

  /**
   * Optional clock function to generate ISO timestamps, useful for testing.
   */
  now?: () => Date;
}

/**
 * Build a CloudEvent from a Wave message-like object.
 * For now we keep this generic; specific mapping for commands/events/queries
 * will be added in later steps.
 *
 * @example
 * ```typescript
 * ```
 */
export function buildCloudEvent<
  TPayload = unknown,
  TContext extends WaveBaseContext = WaveBaseContext,
>(
  kind: 'command' | 'event' | 'query',
  message: {
    id: string;
    namespace: string;
    name: string;
    payload: TPayload;
    context?: TContext;
  },
  options: CloudEventBuildOptions
): CloudEventV1<TPayload, TContext & Record<string, any>> {
  const now = options.now?.() ?? new Date();

  return {
    specversion: '1.0',
    id: message.id ?? randomUUID(),
    source: options.source,
    type: `wave.${kind}.${message.namespace}.${message.name}`,
    subject: `${message.namespace}.${message.name}`,
    time: now.toISOString(),
    datacontenttype: 'application/json',
    data: message.payload,
    extensions: {
      ...(message.context ?? {}),
      ...(options.extensions ?? {}),
    } as TContext & Record<string, any>,
  };
}
