import assert from 'node:assert/strict';
import { test } from 'node:test';

import { Context } from '@deepseek-ai/cordis';

import * as plugin from '../lib/index.js';

test('mounts and unloads on a real Cordis root', async () => {
  const root = new Context();
  const fiber = root.plugin(plugin, {});
  await fiber.await();
  assert.equal(typeof root.get('notify').publish, 'function');

  const seen = [];
  const consumer = root.plugin({
    inject: ['notify'],
    apply(ctx) {
      ctx.effect(() => ctx.notify.subscribe('real.**', (envelope) => seen.push(envelope.payload)));
    },
  });
  await consumer.await();

  const result = await root.get('notify').publish('real.check', 1);
  assert.equal(result.delivered, 1);
  assert.deepEqual(seen, [1]);

  await consumer.dispose();
  await fiber.dispose();
  assert.equal(root.get('notify'), undefined);
});

test('fails the mount on invalid config', async () => {
  const root = new Context();
  const fiber = root.plugin(plugin, { maxSubscribers: 0 });
  await assert.rejects(fiber.await(), /maxSubscribers/);
  assert.equal(root.get('notify'), undefined);
});
