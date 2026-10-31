/**
 * notify-plugin-dsh — topic-based publish/subscribe for other plugins.
 *
 * The plugin only registers a service. Consumers declare `inject: ['notify']`
 * and call `ctx.notify.subscribe(...)` / `ctx.notify.publish(...)`; no plugin
 * ever imports another plugin. Delivery is awaitable and isolated: one broken
 * or slow subscriber is counted and logged, never thrown at the publisher.
 *
 * @module notify-plugin-dsh
 */

import { randomUUID } from 'node:crypto';

/** Topic segment accepted on publish. */
const SEGMENT = /^[a-z0-9][a-z0-9_-]*$/;

/** Subscriber pattern segment: literal, `*` (one segment), or `**` (zero or more). */
const PATTERN_SEGMENT = /^(?:[a-z0-9][a-z0-9_-]*|\*{1,2})$/;

/** Bounds that stop a malformed row config from mounting. */
const LIMITS = {
  maxSubscribers: [1, 100_000],
  maxRetainedTopics: [0, 100_000],
  handlerTimeoutMs: [0, 600_000],
};

const MAX_TOPIC_LENGTH = 128;
const MAX_TOPIC_SEGMENTS = 16;

/**
 * Read one finite integer within an inclusive range.
 * @param value - raw config value.
 * @param fallback - value used when the key is absent.
 * @param bounds - inclusive lower and upper bound.
 * @param field - config field name for the diagnostic.
 * @returns the validated number.
 */
function integer(value, fallback, [min, max], field) {
  if (value === undefined) return fallback;
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new TypeError(`notify: ${field} must be a whole number between ${min} and ${max}`);
  }
  return value;
}

/**
 * Read one boolean.
 * @param value - raw config value.
 * @param fallback - value used when the key is absent.
 * @param field - config field name for the diagnostic.
 * @returns the validated boolean.
 */
function boolean(value, fallback, field) {
  if (value === undefined) return fallback;
  if (typeof value !== 'boolean') throw new TypeError(`notify: ${field} must be a boolean`);
  return value;
}

/**
 * Normalize and validate the row config.
 * @param raw - optional row config.
 * @returns the complete config.
 */
export function normalizeConfig(raw = {}) {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new TypeError('notify: config must be an object');
  }
  const config = {
    maxSubscribers: integer(raw.maxSubscribers, 512, LIMITS.maxSubscribers, 'maxSubscribers'),
    maxRetainedTopics: integer(raw.maxRetainedTopics, 128, LIMITS.maxRetainedTopics, 'maxRetainedTopics'),
    retainByDefault: boolean(raw.retainByDefault, false, 'retainByDefault'),
    replayOnSubscribe: boolean(raw.replayOnSubscribe, false, 'replayOnSubscribe'),
    handlerTimeoutMs: integer(raw.handlerTimeoutMs, 5_000, LIMITS.handlerTimeoutMs, 'handlerTimeoutMs'),
  };
  return config;
}

/**
 * Split and validate a topic.
 * @param topic - published topic.
 * @returns its segments.
 */
function topicSegments(topic) {
  if (typeof topic !== 'string' || topic.length === 0) {
    throw new TypeError('notify: topic must be a non-empty string');
  }
  if (topic.length > MAX_TOPIC_LENGTH) {
    throw new TypeError(`notify: topic must not exceed ${MAX_TOPIC_LENGTH} characters`);
  }
  const segments = topic.split('.');
  if (segments.length > MAX_TOPIC_SEGMENTS) {
    throw new TypeError(`notify: topic must not exceed ${MAX_TOPIC_SEGMENTS} segments`);
  }
  for (const segment of segments) {
    if (!SEGMENT.test(segment)) {
      throw new TypeError(`notify: topic segment ${JSON.stringify(segment)} must match ${SEGMENT}`);
    }
  }
  return segments;
}

/**
 * Split and validate a subscription pattern.
 * @param pattern - subscriber pattern.
 * @returns its segments.
 */
function patternSegments(pattern) {
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new TypeError('notify: pattern must be a non-empty string');
  }
  if (pattern.length > MAX_TOPIC_LENGTH) {
    throw new TypeError(`notify: pattern must not exceed ${MAX_TOPIC_LENGTH} characters`);
  }
  const segments = pattern.split('.');
  if (segments.length > MAX_TOPIC_SEGMENTS) {
    throw new TypeError(`notify: pattern must not exceed ${MAX_TOPIC_SEGMENTS} segments`);
  }
  for (const segment of segments) {
    if (!PATTERN_SEGMENT.test(segment)) {
      throw new TypeError(`notify: pattern segment ${JSON.stringify(segment)} must be literal, "*" or "**"`);
    }
  }
  return segments;
}

/**
 * Match a pattern against topic segments. `**` accepts zero or more segments.
 * @param pattern - pattern segments.
 * @param topic - topic segments.
 * @returns true on a full match.
 */
function matches(pattern, topic) {
  let p = 0;
  let t = 0;
  while (p < pattern.length) {
    if (pattern[p] === '**') {
      if (p === pattern.length - 1) return true;
      for (let skip = t; skip <= topic.length; skip++) {
        if (matches(pattern.slice(p + 1), topic.slice(skip))) return true;
      }
      return false;
    }
    if (t >= topic.length) return false;
    if (pattern[p] !== '*' && pattern[p] !== topic[t]) return false;
    p += 1;
    t += 1;
  }
  return t === topic.length;
}

/**
 * Race a handler against the configured timeout.
 * @param promise - handler result.
 * @param ms - timeout in milliseconds; `0` disables it.
 * @returns the handler result.
 */
function withTimeout(promise, ms) {
  if (ms === 0) return promise;
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`subscriber timed out after ${ms}ms`)), ms);
    timer.unref?.();
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * Build the service value.
 * @param ctx - plugin context.
 * @param config - normalized config.
 * @returns the notify service.
 */
function createNotify(ctx, config) {
  /** @type {Map<string, { segments: string[], handler: Function, once: boolean, active: boolean }[]>} */
  const subscriptions = new Map();
  /** @type {Map<string, object>} */
  const retained = new Map();
  let subscriberTotal = 0;

  /**
   * Log one delivery failure without letting the logger break delivery.
   * @param topic - published topic.
   * @param error - subscriber failure.
   */
  function warn(topic, error) {
    try {
      ctx.logger?.warn?.(`notify: subscriber for ${JSON.stringify(topic)} failed: ${error?.message ?? error}`);
    } catch {
      // A broken logger must not change delivery semantics.
    }
  }

  /**
   * Remove one subscription.
   * @param pattern - owning pattern.
   * @param subscription - subscription record.
   * @returns true when it was active.
   */
  function remove(pattern, subscription) {
    if (!subscription.active) return false;
    subscription.active = false;
    subscriberTotal -= 1;
    const list = subscriptions.get(pattern);
    list?.splice(list.indexOf(subscription), 1);
    if (list?.length === 0) subscriptions.delete(pattern);
    return true;
  }

  /**
   * Deliver one envelope to one subscription, honoring `once`.
   * @param pattern - owning pattern.
   * @param subscription - subscription record.
   * @param envelope - frozen envelope.
   * @returns true when delivered.
   */
  async function deliver(pattern, subscription, envelope) {
    if (subscription.once) remove(pattern, subscription);
    await withTimeout(
      Promise.resolve().then(() => subscription.handler(envelope)),
      config.handlerTimeoutMs,
    );
    return true;
  }

  /**
   * Retain the newest envelope, evicting the oldest topic above the limit.
   * @param topic - published topic.
   * @param envelope - frozen envelope.
   */
  function retain(topic, envelope) {
    retained.delete(topic);
    retained.set(topic, envelope);
    while (retained.size > config.maxRetainedTopics) {
      retained.delete(retained.keys().next().value);
    }
  }

  /**
   * Read and validate the replay flag.
   * @param options - optional subscribe options.
   * @returns the replay decision.
   */
  function replayFlag(options) {
    if (options === undefined) return config.replayOnSubscribe;
    if (options === null || typeof options !== 'object' || Array.isArray(options)) {
      throw new TypeError('notify: subscribe options must be an object');
    }
    if (options.replay === undefined) return config.replayOnSubscribe;
    if (typeof options.replay !== 'boolean') throw new TypeError('notify: option replay must be a boolean');
    return options.replay;
  }

  /**
   * Register one subscription, optionally replaying retained envelopes.
   * @param pattern - literal segments, `*`, or `**`.
   * @param handler - envelope receiver.
   * @param onlyOnce - whether to unsubscribe on first delivery.
   * @param options - optional replay flag.
   * @returns an idempotent unsubscribe function.
   */
  function add(pattern, handler, onlyOnce, options) {
    const segments = patternSegments(pattern);
    if (typeof handler !== 'function') throw new TypeError('notify: handler must be a function');
    const replay = replayFlag(options);
    if (subscriberTotal >= config.maxSubscribers) {
      throw new RangeError(`notify: subscriber limit ${config.maxSubscribers} reached`);
    }
    const subscription = { segments, handler, once: onlyOnce, active: true };
    const list = subscriptions.get(pattern);
    if (list === undefined) subscriptions.set(pattern, [subscription]);
    else list.push(subscription);
    subscriberTotal += 1;

    if (replay && retained.size > 0) {
      const replayable = [...retained.entries()].filter(([topic]) => matches(segments, topic.split('.')));
      if (replayable.length > 0) {
        queueMicrotask(async () => {
          for (const [topic, envelope] of replayable) {
            if (!subscription.active) continue;
            try {
              await deliver(pattern, subscription, envelope);
            } catch (error) {
              warn(topic, error);
            }
          }
        });
      }
    }
    return () => remove(pattern, subscription);
  }

  /**
   * Subscribe to a topic pattern.
   * @param pattern - literal segments, `*`, or `**`.
   * @param handler - envelope receiver.
   * @param options - optional replay flag.
   * @returns an idempotent unsubscribe function.
   */
  function subscribe(pattern, handler, options) {
    return add(pattern, handler, false, options);
  }

  /**
   * Subscribe for exactly one delivery.
   * @param pattern - literal segments, `*`, or `**`.
   * @param handler - envelope receiver.
   * @param options - optional replay flag.
   * @returns an idempotent unsubscribe function.
   */
  function once(pattern, handler, options) {
    return add(pattern, handler, true, options);
  }

  /**
   * Publish one envelope and await every matching subscriber.
   * @param topic - dotted topic.
   * @param payload - any payload, passed by reference.
   * @param options - retention, source, and cancellation.
   * @returns delivery counters.
   */
  async function publish(topic, payload, options = {}) {
    const segments = topicSegments(topic);
    if (options === null || typeof options !== 'object' || Array.isArray(options)) {
      throw new TypeError('notify: publish options must be an object');
    }
    const opts = options;
    const signal = opts.signal;
    const retainValue = opts.retain ?? config.retainByDefault;
    if (typeof retainValue !== 'boolean') throw new TypeError('notify: publish option retain must be a boolean');
    if (opts.source !== undefined && typeof opts.source !== 'string') {
      throw new TypeError('notify: publish option source must be a string');
    }
    if (signal !== undefined && (signal === null || typeof signal.aborted !== 'boolean')) {
      throw new TypeError('notify: publish option signal must be an AbortSignal');
    }

    const id = randomUUID();
    const at = new Date().toISOString();
    const envelope = Object.freeze({
      id,
      topic,
      payload,
      at,
      retained: retainValue,
      ...(opts.source === undefined ? {} : { source: opts.source }),
    });
    if (retainValue) retain(topic, envelope);

    const targets = [];
    for (const [pattern, list] of subscriptions) {
      for (const subscription of list) {
        if (subscription.active && matches(subscription.segments, segments)) {
          targets.push([pattern, subscription]);
        }
      }
    }

    let delivered = 0;
    let failed = 0;
    let skipped = 0;
    await Promise.all(
      targets.map(async ([pattern, subscription]) => {
        if (signal?.aborted) {
          skipped += 1;
          return;
        }
        try {
          await deliver(pattern, subscription, envelope);
          delivered += 1;
        } catch (error) {
          failed += 1;
          warn(topic, error);
        }
      }),
    );
    return { id, topic, at, delivered, failed, skipped, retained: retainValue };
  }

  /**
   * List subscribed patterns.
   * @returns sorted patterns.
   */
  function patterns() {
    return [...subscriptions.keys()].sort();
  }

  /**
   * List retained topics.
   * @returns sorted topics.
   */
  function retainedTopics() {
    return [...retained.keys()].sort();
  }

  /**
   * Read the retained envelope of one topic.
   * @param topic - dotted topic.
   * @returns the envelope, or undefined.
   */
  function last(topic) {
    topicSegments(topic);
    return retained.get(topic);
  }

  /**
   * Count subscriptions.
   * @param pattern - optional exact pattern.
   * @returns the count.
   */
  function subscriberCount(pattern) {
    if (pattern === undefined) return subscriberTotal;
    patternSegments(pattern);
    return subscriptions.get(pattern)?.length ?? 0;
  }

  /**
   * Drop subscriptions and/or retained envelopes.
   * @param pattern - optional exact pattern; absent clears everything.
   */
  function clear(pattern) {
    if (pattern === undefined) {
      for (const list of subscriptions.values()) {
        for (const subscription of list) subscription.active = false;
      }
      subscriptions.clear();
      retained.clear();
      subscriberTotal = 0;
      return;
    }
    patternSegments(pattern);
    for (const subscription of subscriptions.get(pattern) ?? []) subscription.active = false;
    subscriptions.delete(pattern);
    subscriberTotal = [...subscriptions.values()].reduce((total, list) => total + list.length, 0);
  }

  /**
   * Read service counters.
   * @returns pattern, subscriber, and retained counts.
   */
  function stats() {
    return { patterns: subscriptions.size, subscribers: subscriberTotal, retained: retained.size };
  }

  return {
    publish,
    subscribe,
    once,
    patterns,
    retained: retainedTopics,
    last,
    subscriberCount,
    clear,
    stats,
  };
}

export const name = 'notify-plugin-dsh';

/** The service needs nothing else to mount. */
export const inject = [];

/**
 * Register the service for the lifetime of the row.
 * @param ctx - Cordis context of the profile row.
 * @param rawConfig - optional row config.
 */
export function apply(ctx, rawConfig = {}) {
  const config = normalizeConfig(rawConfig);
  const notify = createNotify(ctx, config);
  ctx.provide('notify', notify);
  ctx.effect(() => () => notify.clear());
}
