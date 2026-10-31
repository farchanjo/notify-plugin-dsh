import assert from 'node:assert/strict';
import { test } from 'node:test';

import { apply, name, normalizeConfig } from '../lib/index.js';

/**
 * Mount the plugin on a minimal context.
 * @param config - optional row config.
 * @returns service, warnings, and teardown.
 */
function harness(config = {}) {
  const services = new Map();
  const warnings = [];
  const disposers = [];
  const ctx = {
    logger: { warn: (message) => warnings.push(String(message)) },
    provide(serviceName, value) {
      services.set(serviceName, value);
      const off = () => services.delete(serviceName);
      disposers.push(off);
      return off;
    },
    effect(fn) {
      const off = fn();
      disposers.push(off);
      return off;
    },
    get(serviceName) {
      return services.get(serviceName);
    },
  };
  apply(ctx, config);
  return {
    notify: services.get('notify'),
    warnings,
    dispose() {
      while (disposers.length > 0) disposers.pop()?.();
    },
  };
}

/**
 * Let queued microtasks run.
 * @returns a promise settled on the next macrotask.
 */
function tick() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

test('registers the notify service and removes it on dispose', () => {
  const h = harness();
  assert.equal(name, 'notify-plugin-dsh');
  assert.equal(typeof h.notify.publish, 'function');
  h.dispose();
  assert.equal(h.notify.stats().subscribers, 0);
});

test('delivers an exact topic and reports counters', async () => {
  const h = harness();
  const seen = [];
  h.notify.subscribe('build.done', (envelope) => seen.push(envelope));
  const result = await h.notify.publish('build.done', { ok: true }, { source: 'unit' });
  assert.equal(result.delivered, 1);
  assert.equal(result.failed, 0);
  assert.equal(result.skipped, 0);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, 'build.done');
  assert.deepEqual(seen[0].payload, { ok: true });
  assert.equal(seen[0].source, 'unit');
  assert.equal(seen[0].retained, false);
  assert.match(seen[0].id, /^[0-9a-f-]{36}$/);
  assert.ok(Object.isFrozen(seen[0]));
});

test('passes the payload by reference', async () => {
  const h = harness();
  const payload = { nested: new Map([['a', 1]]) };
  let received;
  h.notify.subscribe('ref', (envelope) => {
    received = envelope.payload;
  });
  await h.notify.publish('ref', payload);
  assert.equal(received, payload);
});

test('* matches one segment and ** matches the rest', async () => {
  const h = harness();
  const one = [];
  const tail = [];
  const middle = [];
  h.notify.subscribe('build.*', (envelope) => one.push(envelope.topic));
  h.notify.subscribe('build.**', (envelope) => tail.push(envelope.topic));
  h.notify.subscribe('a.**.z', (envelope) => middle.push(envelope.topic));

  await h.notify.publish('build.done');
  await h.notify.publish('build.done.fast');
  await h.notify.publish('a.z');
  await h.notify.publish('a.b.c.z');
  await h.notify.publish('build');

  assert.deepEqual(one, ['build.done']);
  assert.deepEqual(tail, ['build.done', 'build.done.fast', 'build']);
  assert.deepEqual(middle, ['a.z', 'a.b.c.z']);
});

test('does not deliver non-matching topics', async () => {
  const h = harness();
  const seen = [];
  h.notify.subscribe('build.done', (envelope) => seen.push(envelope.topic));
  const result = await h.notify.publish('build.failed');
  assert.equal(result.delivered, 0);
  assert.deepEqual(seen, []);
});

test('isolates a throwing subscriber', async () => {
  const h = harness();
  const seen = [];
  h.notify.subscribe('isolate', () => {
    throw new Error('boom');
  });
  h.notify.subscribe('isolate', (envelope) => seen.push(envelope.topic));
  const result = await h.notify.publish('isolate');
  assert.equal(result.delivered, 1);
  assert.equal(result.failed, 1);
  assert.equal(seen.length, 1);
  assert.equal(h.warnings.length, 1);
  assert.match(h.warnings[0], /boom/);
});

test('times a hanging subscriber out', async () => {
  const h = harness({ handlerTimeoutMs: 10 });
  h.notify.subscribe('slow', () => new Promise(() => {}));
  const result = await h.notify.publish('slow');
  assert.equal(result.failed, 1);
  assert.equal(result.delivered, 0);
  assert.match(h.warnings[0], /timed out/);
});

test('retains and replays the last value on subscribe', async () => {
  const h = harness();
  await h.notify.publish('status', 'ready', { retain: true });
  const seen = [];
  h.notify.subscribe('status', (envelope) => seen.push(envelope), { replay: true });
  await tick();
  assert.equal(seen.length, 1);
  assert.equal(seen[0].payload, 'ready');
  assert.equal(seen[0].retained, true);
  assert.equal(h.notify.last('status').payload, 'ready');
});

test('replays matching retained topics for a wildcard pattern', async () => {
  const h = harness();
  await h.notify.publish('build.one', 1, { retain: true });
  await h.notify.publish('build.two', 2, { retain: true });
  await h.notify.publish('other', 3, { retain: true });
  const seen = [];
  h.notify.subscribe('build.*', (envelope) => seen.push(envelope.payload), { replay: true });
  await tick();
  assert.deepEqual(
    seen.sort((a, b) => a - b),
    [1, 2],
  );
});

test('does not replay by default', async () => {
  const h = harness();
  await h.notify.publish('quiet', 1, { retain: true });
  const seen = [];
  h.notify.subscribe('quiet', (envelope) => seen.push(envelope));
  await tick();
  assert.deepEqual(seen, []);
});

test('replayOnSubscribe makes replay the default', async () => {
  const h = harness({ replayOnSubscribe: true });
  await h.notify.publish('auto', 1, { retain: true });
  const seen = [];
  h.notify.subscribe('auto', (envelope) => seen.push(envelope));
  await tick();
  assert.equal(seen.length, 1);
});

test('retainByDefault retains every publish', async () => {
  const h = harness({ retainByDefault: true });
  const result = await h.notify.publish('default', 1);
  assert.equal(result.retained, true);
  assert.equal(h.notify.last('default').payload, 1);
});

test('explicit retain false overrides the default', async () => {
  const h = harness({ retainByDefault: true });
  const result = await h.notify.publish('override', 1, { retain: false });
  assert.equal(result.retained, false);
  assert.equal(h.notify.last('override'), undefined);
});

test('evicts the oldest retained topic above the cap', async () => {
  const h = harness({ maxRetainedTopics: 1 });
  await h.notify.publish('one', 1, { retain: true });
  await h.notify.publish('two', 2, { retain: true });
  assert.deepEqual(h.notify.retained(), ['two']);
});

test('once delivers a single time', async () => {
  const h = harness();
  const seen = [];
  h.notify.once('once', (envelope) => seen.push(envelope.payload));
  await h.notify.publish('once', 1);
  await h.notify.publish('once', 2);
  assert.deepEqual(seen, [1]);
  assert.equal(h.notify.stats().subscribers, 0);
});

test('an unsubscribe function is idempotent', async () => {
  const h = harness();
  let count = 0;
  const off = h.notify.subscribe('idem', () => {
    count += 1;
  });
  off();
  off();
  await h.notify.publish('idem');
  assert.equal(count, 0);
  assert.equal(h.notify.stats().subscribers, 0);
});

test('skips subscribers when the signal is already aborted', async () => {
  const h = harness();
  const controller = new AbortController();
  controller.abort();
  let count = 0;
  h.notify.subscribe('abort', () => {
    count += 1;
  });
  const result = await h.notify.publish('abort', undefined, { signal: controller.signal });
  assert.equal(result.skipped, 1);
  assert.equal(result.delivered, 0);
  assert.equal(count, 0);
});

test('enforces the subscriber cap', () => {
  const h = harness({ maxSubscribers: 1 });
  h.notify.subscribe('cap', () => {});
  assert.throws(() => h.notify.subscribe('cap', () => {}), /subscriber limit 1/);
  assert.throws(() => h.notify.once('cap', () => {}), /subscriber limit 1/);
});

test('validates topics, patterns, handlers, and options', async () => {
  const h = harness();
  assert.throws(() => h.notify.subscribe('Bad.Topic', () => {}), /must be literal/);
  assert.throws(() => h.notify.subscribe('', () => {}), /non-empty/);
  assert.throws(() => h.notify.subscribe('a..b', () => {}), /must be literal/);
  assert.throws(() => h.notify.subscribe('a.b', undefined), /handler must be a function/);
  assert.throws(() => h.notify.subscribe('a.b', () => {}, { replay: 'yes' }), /must be a boolean/);
  assert.throws(() => h.notify.subscribe('a.b', () => {}, []), /options must be an object/);
  await assert.rejects(h.notify.publish('Bad'), /must match/);
  await assert.rejects(h.notify.publish('ok', 1, null), /options must be an object/);
  await assert.rejects(h.notify.publish('ok', 1, { retain: 'yes' }), /must be a boolean/);
  await assert.rejects(h.notify.publish('ok', 1, { source: 1 }), /must be a string/);
  await assert.rejects(h.notify.publish('ok', 1, { signal: {} }), /AbortSignal/);
  assert.throws(() => h.notify.last('Bad'), /must match/);
  assert.throws(() => normalizeConfig({ maxSubscribers: 0 }), /maxSubscribers/);
  assert.throws(() => normalizeConfig({ maxRetainedTopics: -1 }), /maxRetainedTopics/);
  assert.throws(() => normalizeConfig({ handlerTimeoutMs: 1.5 }), /handlerTimeoutMs/);
  assert.throws(() => normalizeConfig({ retainByDefault: 'yes' }), /retainByDefault/);
  assert.throws(() => normalizeConfig([]), /config must be an object/);
});

test('clear drops one pattern or everything', async () => {
  const h = harness();
  await h.notify.publish('keep', 1, { retain: true });
  h.notify.subscribe('a', () => {});
  h.notify.subscribe('b', () => {});
  h.notify.clear('a');
  assert.deepEqual(h.notify.patterns(), ['b']);
  assert.equal(h.notify.subscriberCount(), 1);
  assert.deepEqual(h.notify.retained(), ['keep']);
  h.notify.clear();
  assert.deepEqual(h.notify.patterns(), []);
  assert.deepEqual(h.notify.retained(), []);
  assert.deepEqual(h.notify.stats(), { patterns: 0, subscribers: 0, retained: 0 });
});

test('reports patterns, counts, and stats', () => {
  const h = harness();
  h.notify.subscribe('z.last', () => {});
  h.notify.subscribe('a.first', () => {});
  h.notify.subscribe('a.first', () => {});
  assert.deepEqual(h.notify.patterns(), ['a.first', 'z.last']);
  assert.equal(h.notify.subscriberCount('a.first'), 2);
  assert.equal(h.notify.subscriberCount(), 3);
  assert.deepEqual(h.notify.stats(), { patterns: 2, subscribers: 3, retained: 0 });
});

test('does not let a broken logger break delivery', async () => {
  const services = new Map();
  const ctx = {
    logger: {
      warn() {
        throw new Error('logger down');
      },
    },
    provide(serviceName, value) {
      services.set(serviceName, value);
      return () => services.delete(serviceName);
    },
    effect(fn) {
      return fn();
    },
  };
  apply(ctx);
  const notify = services.get('notify');
  notify.subscribe('log', () => {
    throw new Error('boom');
  });
  const result = await notify.publish('log');
  assert.equal(result.failed, 1);
});

test('once can replay a retained value', async () => {
  const h = harness();
  await h.notify.publish('replay-once', 1, { retain: true });
  await h.notify.publish('replay-once', 2, { retain: true });
  const seen = [];
  h.notify.once('replay-once', (envelope) => seen.push(envelope.payload), { replay: true });
  await tick();
  assert.deepEqual(seen, [2]);
  assert.equal(h.notify.stats().subscribers, 0);
});

test('snapshots targets at publish time', async () => {
  const h = harness();
  let second = 0;
  h.notify.subscribe('snapshot', () => offSecond());
  const offSecond = h.notify.subscribe('snapshot', () => {
    second += 1;
  });
  const result = await h.notify.publish('snapshot');
  assert.equal(result.delivered, 2);
  assert.equal(second, 1);
  await h.notify.publish('snapshot');
  assert.equal(second, 1);
});

test('rejects a topic above the segment cap', async () => {
  const h = harness();
  const topic = Array.from({ length: 17 }, () => 'a').join('.');
  await assert.rejects(h.notify.publish(topic), /segments/);
  assert.throws(() => h.notify.subscribe(topic, () => {}), /segments/);
});
