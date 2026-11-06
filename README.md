# notify-plugin-dsh

Topic-based publish/subscribe for DSH plugins. One plugin publishes, any number of plugins subscribe, and no plugin imports another. The service is awaitable and isolated: a broken or slow subscriber is counted and logged, never thrown at the publisher.

Plugin-only. It does not change dsh, the TUI profile, or another plugin. The package inserts one profile row and registers the `notify` service on `ctx`.

## Service

```js
export const inject = ['notify'];

export function apply(ctx) {
  const off = ctx.notify.subscribe('build.done', async (envelope) => {
    ctx.logger.info(`build ${envelope.payload.id} finished`);
  });
  ctx.effect(() => off);
}
```

```js
export const inject = ['notify'];

export async function apply(ctx) {
  await ctx.notify.publish('build.done', { id: 'b-42' }, { source: 'build-runner' });
}
```

`publish` resolves after every matching handler settles. The result carries `delivered`, `failed`, and `skipped` counters. Subscriber errors and timeouts land in `ctx.logger.warn`.

## API

### `publish(topic, payload?, options?)`

Publishes one frozen envelope and awaits every matching subscriber.

| Option | Default | Meaning |
| --- | --- | --- |
| `retain` | `retainByDefault` | Keep the newest envelope for that topic. |
| `source` | absent | Publisher label copied to the envelope. |
| `signal` | absent | `AbortSignal`; subscribers that have not started are skipped. |

Result: `{ id, topic, at, delivered, failed, skipped, retained }`.

### `subscribe(pattern, handler, options?)`

Returns an idempotent unsubscribe function. The handler receives a frozen envelope `{ id, topic, payload, at, retained, source? }`.

`options.replay` (default `replayOnSubscribe`) delivers matching retained envelopes in a microtask.

### `once(pattern, handler, options?)`

Same as `subscribe`, but the subscription is removed before the first delivery.

### Diagnostics and cleanup

| Method | Result |
| --- | --- |
| `patterns()` | Subscribed patterns, sorted. |
| `retained()` | Retained topics, sorted. |
| `last(topic)` | Retained envelope of one topic, or `undefined`. |
| `subscriberCount(pattern?)` | Subscriber total, or the count for one exact pattern. |
| `stats()` | `{ patterns, subscribers, retained }`. |
| `clear(pattern?)` | Drop one exact pattern's subscribers, or everything plus retention. |

Unloading the row clears every subscription and retained envelope.

## Topics and patterns

Topics are lower-case dotted segments: `build.done`, `session.idle`, `mcp.ssh.connected`. Segments use `a-z`, `0-9`, `_`, and `-`; a topic is at most 128 characters and 16 segments.

Patterns add two wildcards, each occupying a whole segment:

| Pattern | Matches |
| --- | --- |
| `build.done` | Exactly that topic. |
| `build.*` | `build.done`, but not `build.done.fast`. |
| `build.**` | `build`, `build.done`, and any deeper topic. |
| `a.**.z` | `a.z`, `a.b.z`, `a.b.c.z`. |

`**` accepts zero or more segments.

## Config

| Key | Default | Notes |
| --- | --- | --- |
| `maxSubscribers` | `512` | Global cap. Subscribe throws above it. |
| `maxRetainedTopics` | `128` | Oldest topic is evicted first; `0` disables retention. |
| `retainByDefault` | `false` | Retain every publish unless `retain` overrides it. |
| `replayOnSubscribe` | `false` | Replay matching retained envelopes by default. |
| `handlerTimeoutMs` | `5000` | Per-handler timeout; `0` disables it. |

Invalid config fails the mount instead of degrading silently.

## Delivery semantics

- Handlers run concurrently; `publish` settles when all of them do. Subscriber order is not guaranteed.
- The target list is snapshotted at publish time. A subscriber removed during a publish can still receive that publish.
- A handler that throws or times out is counted in `failed` and logged. The other handlers still run.
- Payloads pass by reference. Nothing is serialized, copied, or persisted.
- Retention lives in memory and dies with the row.
- `publish` before any subscriber is valid and retains the value when asked.
- A subscriber added with `inject: ['notify']` waits for the service; no load-order coordination is needed.

## Install

Every tagged version ships a packed tarball on the
[Releases](https://github.com/farchanjo/notify-plugin-dsh/releases) page:

```sh
dsh plugin --profile <profile> add \
  https://github.com/farchanjo/notify-plugin-dsh/releases/download/v<version>/notify-plugin-dsh-<version>.tgz
```

Verify the asset against the `.sha256` published beside it before installing.
Remove with `dsh plugin --profile <profile> remove notify-plugin-dsh`.

A git clone works too, for development. The release contract — bump, tag,
assets, checks — lives in [AGENTS.md](AGENTS.md).

## Plugin-only contract

| Point | Use |
| --- | --- |
| Bundle patch | Inserts the `notify-plugin-dsh` row. |
| `ctx.provide('notify', service)` | Registers the service for the row lifetime. |
| `ctx.effect` | Clears subscribers and retention on unload. |
| `ctx.logger.warn` | Reports subscriber failures. |

No dsh package is imported at runtime and no other plugin is read or changed.

## Types

`lib/index.d.ts` exports `NotifyService`, `NotifyEnvelope`, `PublishOptions`, `SubscribeOptions`, `NotifyConfig`, and augments the Cordis `Context` with `notify`. The package declares `@deepseek-ai/cordis` as a peer for those types only.

## Test

```bash
node --test "test/*.test.js"
```
