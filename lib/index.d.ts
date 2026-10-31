/**
 * Types for the `notify` service. Consumers declare `inject: ['notify']` and
 * receive `ctx.notify` typed by the module augmentation below.
 *
 * @module notify-plugin-dsh
 */

/** One published value, frozen before delivery. */
export interface NotifyEnvelope<T = unknown> {
  /** Unique delivery id (UUID). */
  readonly id: string;
  /** Published topic. */
  readonly topic: string;
  /** Payload passed by reference. */
  readonly payload: T;
  /** ISO-8601 publish instant. */
  readonly at: string;
  /** Whether the envelope was retained for replay. */
  readonly retained: boolean;
  /** Optional publisher label. */
  readonly source?: string;
}

/** Counters returned by `publish`. */
export interface PublishResult {
  /** Envelope id. */
  readonly id: string;
  /** Published topic. */
  readonly topic: string;
  /** ISO-8601 publish instant. */
  readonly at: string;
  /** Subscribers that settled successfully. */
  readonly delivered: number;
  /** Subscribers that threw or timed out. */
  readonly failed: number;
  /** Subscribers skipped because the signal was aborted. */
  readonly skipped: number;
  /** Whether the envelope was retained. */
  readonly retained: boolean;
}

/** Options for `publish`. */
export interface PublishOptions {
  /** Retain the envelope for later replay; defaults to the row config. */
  readonly retain?: boolean;
  /** Publisher label copied to the envelope. */
  readonly source?: string;
  /** Stop dispatching to subscribers that have not started yet. */
  readonly signal?: AbortSignal;
}

/** Options for `subscribe`. */
export interface SubscribeOptions {
  /** Deliver matching retained envelopes in a microtask; defaults to the row config. */
  readonly replay?: boolean;
}

/** Service counters. */
export interface NotifyStats {
  readonly patterns: number;
  readonly subscribers: number;
  readonly retained: number;
}

/** The `notify` service. */
export interface NotifyService {
  /**
   * Publish one envelope and await every matching subscriber.
   * Subscriber errors and timeouts are counted and logged, never thrown.
   */
  publish<T>(topic: string, payload?: T, options?: PublishOptions): Promise<PublishResult>;
  /**
   * Subscribe to a literal segment pattern, `*` (one segment), or `**` (zero or more).
   * @returns an idempotent unsubscribe function.
   */
  subscribe<T>(
    pattern: string,
    handler: (envelope: NotifyEnvelope<T>) => void | Promise<void>,
    options?: SubscribeOptions,
  ): () => void;
  /** Subscribe for the first matching delivery only. */
  once<T>(
    pattern: string,
    handler: (envelope: NotifyEnvelope<T>) => void | Promise<void>,
    options?: SubscribeOptions,
  ): () => void;
  /** Subscribed patterns, sorted. */
  patterns(): string[];
  /** Retained topics, sorted. */
  retained(): string[];
  /** Retained envelope of one topic, if any. */
  last<T = unknown>(topic: string): NotifyEnvelope<T> | undefined;
  /** Subscription count, total or for one exact pattern. */
  subscriberCount(pattern?: string): number;
  /** Drop all subscriptions and retained envelopes, or one exact pattern's subscriptions. */
  clear(pattern?: string): void;
  /** Service counters. */
  stats(): NotifyStats;
}

/** Row config. */
export interface NotifyConfig {
  /** Global subscriber cap. */
  maxSubscribers: number;
  /** Retained-topic cap; `0` disables retention. */
  maxRetainedTopics: number;
  /** Retain every publish unless `retain` overrides it. */
  retainByDefault: boolean;
  /** Replay matching retained envelopes to every new subscriber. */
  replayOnSubscribe: boolean;
  /** Per-handler timeout in milliseconds; `0` disables it. */
  handlerTimeoutMs: number;
}

/** Plugin row config, all keys optional. */
export type NotifyConfigInput = Partial<NotifyConfig>;

export declare const name: 'notify-plugin-dsh';
export declare const inject: readonly string[];
export declare function normalizeConfig(raw?: NotifyConfigInput): NotifyConfig;
export declare function apply(ctx: import('@deepseek-ai/cordis').Context, rawConfig?: NotifyConfigInput): void;

declare module '@deepseek-ai/cordis' {
  interface Context {
    notify: NotifyService;
  }
}
