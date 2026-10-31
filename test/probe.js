/**
 * Throwaway smoke probe: injects the notify service, subscribes, publishes,
 * and prints the delivery result. Loaded through a `--patch` overlay only.
 */

export const name = 'notify-probe';

export const inject = ['notify'];

export async function apply(ctx) {
  const off = ctx.notify.subscribe('probe.**', (envelope) => {
    console.log('PROBE_GOT', envelope.topic, JSON.stringify(envelope.payload));
  });
  const result = await ctx.notify.publish('probe.smoke', { ok: true }, { retain: true });
  console.log('PROBE_RESULT', JSON.stringify(result));
  off();
}
