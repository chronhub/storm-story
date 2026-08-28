# Storm Story

Command / query / event buses on Symfony Messenger: context stamps, consumer-side idempotency (inbox),
batched consume, an outbox publisher seam and a neutral wire serializer.

## When you reach for it

- **dispatching and handling**: your handlers declare `#[AsMessageHandler(bus: 'storm.command.bus')]`
  (or query/event); the middleware below does the rest — you mostly never import this package;
- **asking the read side**: `QueryBus` — `ask()` one query, `askAllSettled()` a fan-out where a
  failed panel degrades instead of failing the page;
- **draining a hot transport**: `storm:bus:consume-batched` — N messages per inbox transaction;
- **crossing a runtime edge**: declare a neutral transport (`storm.neutral_transports`) instead of
  putting PHP-serialized objects on the wire. A TRUST edge takes two more things the transport does
  not do for you: declare `allowed_types` (the same-trust default accepts every resolvable type) and
  validate the payload CONTENT at your reception — the serializer validates the wire form, never
  what a foreign producer put inside it;
- **sending inside an inbox transaction**: a handler that owns the at-least-once consequence signs
  with `#[DispatchesUnderInboxTransaction]` — undeclared dual-writes are refused.

## The three buses

The bundle wires `storm.command.bus`, `storm.query.bus`, `storm.event.bus` with this middleware
order:

| Order | Middleware | Buses | Job |
|---|---|---|---|
| 1 | `AssignMessageMetadata` | all | stable `MessageIdStamp` + `CorrelationStamp` before anything can fail |
| 2 | `ValidateUnlessReceived` / `validation` | command / query | fail-fast validation of external input |
| 3 | `BindMessageContext` | all | ambient context for the handlers |
| 4 | `BindStoredHeader` | event | a republished event's stored header, for handlers |
| 5 | `RecoverConcurrencyConflict` | command, event | OCC split → Messenger retry markers |
| 6 | `DeduplicateConsumer` | command, event | at-most-once handling per `(transport, message-id)` |

## Invariants the package holds (and expects)

- **Identity is mandatory and non-blank.** `MessageIdStamp` is the consumer-side dedup key; the stamp
  refuses a blank id at construction and the neutral wire decoder refuses an absent or blank
  `__message_id`. A blank id would collide every such message into ONE inbox entry — the first handled,
  every later different message silently skipped-acked.
- **Validation runs before the queue, not after.** A command is validated on dispatch; a `ReceivedStamp`
  skips re-validation on the consume path (a profiled ~7.5 % CPU of drain). The trust boundary is broker /
  DB write access: every producer is expected to dispatch through the local command bus. A future
  untrusted edge must validate at ITS reception, per transport, not re-tax every local message.
- **No broker send inside an inbox transaction — unless signed.** The inbox transaction cannot roll a
  broker send back (a dual-write): the decorated senders locator REFUSES an undeclared send while one is
  open. A handler that owns the at-least-once consequence declares
  `#[DispatchesUnderInboxTransaction]` and provides one of the two defenses: a deterministic
  `MessageIdStamp` on the nested dispatch, or a domain-idempotent target. Everything else routes nested
  async through an outbox.
- **OCC is retried forward, deliberately unbounded — on BOTH consume paths.** `StaleVersion` means a
  competitor committed, so the redelivered command re-decides against the advanced stream;
  `DuplicateVersion` dead-letters at once and is the poison backstop. The retry delay is jittered
  exponential (capped) so hot-stream losers spread out. The Worker path rides `ReceivedStamp`, the
  batched path rides `BatchModeStamp` into the same translation (its redelivery is
  `RedeliveryStamp`-counted and capped by the batch consumer).

## Two consume paths, one contract

`messenger:consume` (the Worker) and `storm:bus:consume-batched` (N messages per inbox transaction and
COMMIT, poison isolated per message with one bounded second attempt) share: the inbox dedup, the failure
transport with the `messenger:failed:*` stamps, and the **terminal failure contract** — both emit a
never-retried `WorkerMessageFailedEvent`, so terminal listeners (the saga settle, telemetry) behave the
same on either path. A batch-machinery failure (transaction, inbox INSERT, connection) is systemic, never
a poison: it propagates and the deliveries stay un-acked.

Deliberate batched-path divergences: no `ReceivedStamp` (it would re-arm the consume-path validation
skip), therefore no `from_transport` handler filtering, and no received/handled lifecycle events. Do not
batch-consume a transport whose handlers rely on either; the shipped topology batches event transports.
The missing stamp has a COST as well as a mechanism: every batched message runs the full validation
middleware again, giving back on this path the ~7.5 % validation CPU the Worker path saves — free today
because the event bus carries no validation middleware, paid silently the day a COMMAND transport is
batch-consumed.

## Neutral wire transport

`NeutralMessageSerializer` puts `{type, version, header, content}` JSON on the wire — no PHP
serialization, so any runtime can produce or consume the channel. Declared per transport under
`storm.neutral_transports`, and the entry IS the wiring: each one registers a
`storm.neutral_transport.<name>` service you reference from that transport's `serializer` key.

- **The bus is transport configuration, never producer input** — the wire's bus header is
  advisory, ignored on decode.
- **Trust is declared per channel**: a transport fed by a foreign producer must list
  `allowed_types` — decode rejects anything else before resolving a class or touching
  `fromPayload()`. Empty trusts every resolvable type: in-process / same-trust transports only.
- **`version` is required at decode** and the `UpcasterChain` migrates the payload right there,
  with the same fail-fast as the store — an absent version would be "presumed current", the silent
  lie the moment a schema moves, so it is refused instead.
- **Retry state survives the wire**: the redelivery count travels as the `X-Redelivery-Count`
  transport header, so a retry cap holds across broker requeues and consumer restarts.
- **Never a failure transport**: the failure-capture stamps are PHP-internal ops data and are
  deliberately not carried; a failure transport stays on the default PhpSerializer so
  `messenger:failed:*` keeps its full history.

## The outbox publisher seam

`MessengerOutboxPublisher` is the bridge Chronicler's relay publishes through: outbox rows become
Messenger envelopes on the configured event transports. `ShardedSender` composes behind any
single-transport seam to split one lane into shards, keyed by correlation id (then message id) so
one workflow's messages always land in the same shard — parallelism without reordering a
conversation.

## Tests

```bash
vendor/bin/phpunit src/Story/Tests   # from the storm root (the monorepo autoloader)
```

`src/Story/phpunit.dist.xml` is the SPLIT package's configuration: its `bootstrap` resolves
`vendor/autoload.php` relative to the package, which only exists on a standalone install — from the
monorepo root that command fails before running a single test.

## Resources

This package is developed in the `chronhub/storm` monorepo; a standalone repository for it is a
READ-ONLY subtree split. Report issues and open pull requests on the monorepo, where the tests,
the architecture gates and the full internal documentation live.

---

*Pre-version: this package changes without deprecation cycles — pin a commit if you need
stability, expect resets rather than migrations until the first tagged version.*
