<?php

declare(strict_types=1);

namespace Storm\Story\Consume;

use LogicException;
use Storm\Chronicler\Inbox\BatchInboxStore;
use Storm\Chronicler\Inbox\InboxItem;
use Storm\Chronicler\Inbox\InboxStore;
use Storm\Story\Console\ConsumeBatchedCommand;
use Storm\Story\Stamp\BatchModeStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\RecoverableExceptionInterface;
use Symfony\Component\Messenger\Exception\RecoverableMessageHandlingException;
use Symfony\Component\Messenger\Exception\UnrecoverableExceptionInterface;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Throwable;

/**
 * The testable core of the batched consumer: runs a batch of received envelopes through the inbox in one
 * transaction, and on failure isolates the poison by replaying the batch one message at a time.
 *
 * Decoupled from the transport receive loop `ConsumeBatchedCommand`, so the batch-versus-isolation behavior
 * is unit-pinned without a broker. Each envelope is dispatched on the bus carrying a `BatchModeStamp` so
 * `DeduplicateConsumer` skips its per-message transaction, leaving the dedup INSERT and the atomic boundary
 * to the shared batch transaction, and an empty `TransportNamesStamp`, which pins the senders to none: this
 * loop IS the consumer, so the dispatch must be terminal. Without that pin, a message whose class routes to
 * a transport such as a command lane would be RE-SENT to the broker by `SendMessageMiddleware` instead of
 * handled, the received copy re-enqueued forever and multiplying on every delivery. The empty stamp forces
 * local handling for every batched message; nested dispatches from handlers are fresh envelopes and route
 * normally, under the dual-write guard: a nested BROKER send inside the batch transaction requires the
 * handler's signed `#[DispatchesUnderInboxTransaction]`, since the rollback below cannot unsend it.
 *
 * A handler failure is NOT an infrastructure failure, and the two part ways here:
 *
 * - A HANDLER failure, every bus-dispatch throw wrapped in the `BatchHandlerFailed` marker at the
 *   dispatch seam so it is recognizable by construction, rolls the batch back and isolates the poison:
 *   each message is replayed through the per-message `InboxStore::once()` DIRECTLY, in its own transaction
 *   and dedup INSERT, the dispatch inside still batch-stamped so `DeduplicateConsumer` does not open a
 *   second transaction;
 *
 * - Anything else out of `onceBatch()`/`once()` is the batch MACHINERY failing, a transaction begin or
 *   commit, the inbox INSERT, the store's connection, and propagates as-is: the deliveries stay un-acked
 *   and come back, at-least-once preserved. Condemning per-message here would migrate a healthy queue to
 *   the failure transport for an outage that is nobody's poison.
 *
 * Each isolation replay gets one bounded second attempt after a short pause: the Worker's retry policy this
 * loop replaced would have absorbed a designed-transient such as a saga fence held by a concurrent step,
 * and capturing on the first replay turned it terminal. A failure that declares itself final, Messenger's
 * `UnrecoverableExceptionInterface`, directly or as every wrapped handler failure, is condemned on the
 * FIRST attempt: re-running what says "never retry me" only risks repeating a non-transactional side
 * effect. Only a message failing its allotted attempts is rejected, the decision carrying the UNWRAPPED
 * handler failure for the loop's failure transport, while the innocents commit WITH their dedup rows, so a
 * later redelivery of the same ids stays a no-op because the relay is at-least-once. A message with no id
 * at all cannot enter any inbox: it is rejected-with-cause on its own, without wedging the batch; a crash
 * here would block the whole transport on one malformed message, redelivered forever.
 *
 * A failure that declares itself TRANSIENT, Messenger's `RecoverableExceptionInterface` directly or on any
 * wrapped handler failure, is not condemned when it outlives the second chance. This mirrors Symfony's own
 * asymmetry: ALL unrecoverable condemns, ANY recoverable retries. The decision is a REDELIVER, back to the
 * origin transport with the exception's own `retryDelay` or this consumer's default, the broker holding
 * the wait. The retry state rides ON the message, a `RedeliveryStamp` count the loop increments, so this
 * loop still owns no durable retry state; rebuilding the Worker's full strategy here stays declined. The
 * in-process pause-and-second-chance remains the budget for the millisecond transients, a fence or a row
 * lock, and the broker redelivery is the escape for the SECONDS-scale ones a process cannot sit on: the
 * canonical case is a saga child's birth race, a consumer-safe delivery reaction throwing recoverable
 * until the spawn command lands. The count is capped: a recoverable that reaches the cap is condemned like
 * any poison, so a wait whose completion never comes still surfaces on the failure transport.
 *
 * @see BatchProcessor the seam this implements
 * @see ConsumeBatchedCommand the receive loop that drives this
 * @see BatchModeStamp the stamp set on each dispatched envelope
 * @see BatchHandlerFailed the handler-versus-machinery classification marker
 * @see \Storm\Chronicler\Inbox\InboxStore the per-message path the isolation replays through
 * @see InboxTransactionContext the ambient entry each batched dispatch opens for the dual-write guard
 */
final readonly class BatchConsumer implements BatchProcessor
{
    public function __construct(
        private BatchInboxStore $inbox,
        private MessageBusInterface $bus,
        private InboxStore $perMessage,
        /**
         * The pause before the isolation replay's SECOND and last attempt. The batch rolled back
         * moments before the replay, so a transient contention such as a saga fence held by a
         * concurrent step or a row lock is very often still in flight on the first attempt; one
         * bounded pause absorbs a contention that resolves itself moments later.
         */
        private int $replayRetryDelayMs = 100,
        /** Null runs unguarded for standalone use; the bundle wires the shared ambient context. */
        private ?InboxTransactionContext $context = null,
        /**
         * How many broker redeliveries a declared-recoverable failure may spend before it is condemned
         * like any poison. Bounds the seconds-scale escape: at the default delay this is minutes of
         * patience for a wait whose completion is in flight, then the failure transport for one that
         * never comes.
         */
        private int $recoverableRedeliveryCap = 300,
        /** The broker hold between redeliveries when the recoverable exception carries no retryDelay of its own. */
        private int $recoverableRedeliveryDelayMs = 1000,
    ) {}

    /**
     * {@inheritDoc}
     *
     * @throws Throwable a batch-machinery failure, a transaction begin/commit, inbox INSERT, or connection,
     *                   propagated as-is so the deliveries stay un-acked; never a handler failure, which
     *                   is isolated into a per-message decision instead
     */
    public function process(string $consumer, array $envelopes): array
    {
        if ($envelopes === []) {
            return [];
        }

        $decisions = [];
        $identified = [];

        foreach ($envelopes as $i => $envelope) {
            $id = $envelope->last(MessageIdStamp::class)?->id;

            if ($id === null) {
                // no id = no inbox key on ANY path; captured alone, reject-with-cause to the loop's
                // failure transport, never a crash that would wedge the whole transport on one message
                $decisions[$i] = BatchDecision::reject(new LogicException(
                    'Batched consume requires a MessageIdStamp on every message; this one carries none, so it cannot enter the inbox.',
                ));

                continue;
            }

            $identified[$i] = ['id' => $id, 'envelope' => $envelope];
        }

        if ($identified !== []) {
            try {
                array_map(
                    fn (array $one): InboxItem => new InboxItem($one['id'], fn (): Envelope => $this->dispatch($one['envelope'])),
                    $identified,
                )
                    |> array_values(...)
                    |> (fn ($x) => $this->inbox->onceBatch($consumer, $x));

                foreach (array_keys($identified) as $i) {
                    $decisions[$i] = BatchDecision::ack();
                }
            } catch (BatchHandlerFailed) {
                // a handler poisoned the batch; the transaction rolled back, isolate per message
                foreach ($this->isolate($consumer, $identified) as $i => $decision) {
                    $decisions[$i] = $decision;
                }
            }
        }

        ksort($decisions);

        return array_values($decisions);
    }

    /**
     * The batch rolled back on a handler failure: replays each message through the per-message inbox, where
     * `once()` restores its own transaction and its dedup row so an already-processed duplicate is a skip
     * that is acked. The poison is rejected alone with its failure captured, while the innocents commit
     * deduplicated.
     *
     * @param  array<int, array{id: string, envelope: Envelope}>  $identified  keyed by original position
     * @return array<int, BatchDecision> keyed by the same positions
     */
    private function isolate(string $consumer, array $identified): array
    {
        return array_map(fn ($one) => $this->replay($consumer, $one['id'], $one['envelope']), $identified);
    }

    /**
     * One message's isolation replay, given a SECOND and last chance after a bounded pause before
     * being condemned. The Worker path absorbs a designed-transient: `SagaFenceBusy` means "retry the
     * delivery" and rides Messenger's retry. This loop replaced the Worker, so a capture on the FIRST
     * replay turns a milliseconds-transient into a terminal failure row; the concurrent step that
     * rolled the batch back is usually still in flight.
     *
     * One pause, one re-attempt:
     * - A transient is absorbed;
     *
     * - A deterministic poison fails twice, pays the pause once, and its capture stays the terminal
     *   outcome, never a retry loop;
     *
     * - An UNRECOVERABLE-declared failure is condemned at once, no second run of what says "never retry";
     *
     * - A RECOVERABLE-declared failure that fails both attempts is redelivered, not condemned: the
     *   declaration means the message is sound and the world is not ready, a wait a process cannot sit
     *   on, so the broker holds it for the exception's own delay, until the redelivery cap, where it
     *   is condemned like any poison.
     *
     * A rolled-back first attempt leaves no dedup row, so the second `once()` is clean. Only a handler
     * failure is caught: the store failing `once()` itself is systemic and propagates, un-acked.
     */
    private function replay(string $consumer, string $id, Envelope $envelope): BatchDecision
    {
        try {
            $this->perMessage->once($consumer, $id, fn (): Envelope => $this->dispatch($envelope));

            return BatchDecision::ack(); // ran, or an already-processed duplicate; both are done
        } catch (BatchHandlerFailed $e) {
            if ($this->declaredUnrecoverable($e->handlerFailure())) {
                return BatchDecision::reject($e->handlerFailure());
            }

            $this->pauseBeforeRetry();
        }

        try {
            $this->perMessage->once($consumer, $id, fn (): Envelope => $this->dispatch($envelope));

            return BatchDecision::ack(); // the transient released; absorbed, not captured
        } catch (BatchHandlerFailed $e) {
            $failure = $e->handlerFailure();

            if ($this->declaredRecoverable($failure) && $this->redeliveries($envelope) < $this->recoverableRedeliveryCap) {
                return BatchDecision::redeliver($failure, $this->retryDelay($failure));
            }

            return BatchDecision::reject($failure); // failed twice; condemned, captured with its cause
        }
    }

    /**
     * Messenger's own nested-exception rule, mirrored: an exception is final when it declares
     * `UnrecoverableExceptionInterface` itself, or when EVERY handler failure wrapped in a
     * `HandlerFailedException` does.
     */
    private function declaredUnrecoverable(Throwable $e): bool
    {
        if ($e instanceof HandlerFailedException) {
            // recursive: a nested sync dispatch wraps the leaf in a second HandlerFailedException,
            // and the leaves are what declare; a wrapper is neither recoverable nor unrecoverable
            $wrapped = $e->getWrappedExceptions(recursive: true);

            return $wrapped !== []
                && array_all($wrapped, static fn (Throwable $nested): bool => $nested instanceof UnrecoverableExceptionInterface);
        }

        return $e instanceof UnrecoverableExceptionInterface;
    }

    /**
     * The retry side of the same rule, and Symfony's own asymmetry with it: where condemning requires
     * EVERY wrapped failure to declare final, retrying takes ANY wrapped failure declaring transient;
     * one handler asking for another chance outweighs its silent siblings, exactly as the Worker's
     * retry listener decides.
     */
    private function declaredRecoverable(Throwable $e): bool
    {
        if ($e instanceof HandlerFailedException) {
            return array_any(
                $e->getWrappedExceptions(recursive: true),
                static fn (Throwable $nested): bool => $nested instanceof RecoverableExceptionInterface,
            );
        }

        return $e instanceof RecoverableExceptionInterface;
    }

    /**
     * The broker hold the failure asked for: the first wrapped recoverable carrying its own `retryDelay`
     * wins, since the exception knows how long its wait usually lasts; none declared falls back to this
     * consumer's default.
     */
    private function retryDelay(Throwable $e): int
    {
        $failures = $e instanceof HandlerFailedException ? $e->getWrappedExceptions(recursive: true) : [$e];

        foreach ($failures as $failure) {
            if ($failure instanceof RecoverableMessageHandlingException && $failure->getRetryDelay() !== null) {
                return $failure->getRetryDelay();
            }
        }

        return $this->recoverableRedeliveryDelayMs;
    }

    /**
     * How many times the broker has already redelivered this envelope on the recoverable path.
     */
    private function redeliveries(Envelope $envelope): int
    {
        return $envelope->last(RedeliveryStamp::class)?->getRetryCount() ?? 0;
    }

    /**
     * One terminal, batch-stamped dispatch: `BatchModeStamp` keeps `DeduplicateConsumer` out of the way,
     * since the inbox transaction is owned here, and the empty `TransportNamesStamp` pins the senders to
     * none so the message is HANDLED, never re-sent to a transport it happens to route to.
     *
     * The single classification seam: any throw out of the bus IS a handler failure and leaves wrapped in
     * the `BatchHandlerFailed` marker, so the callers can tell it from the store's own machinery failing
     * around it. The ambient inbox-transaction entry brackets the dispatch for the dual-write guard.
     */
    private function dispatch(Envelope $envelope): Envelope
    {
        $this->context?->enter($envelope->getMessage()::class);

        try {
            return $this->bus->dispatch($envelope->with(new BatchModeStamp, new TransportNamesStamp([])));
        } catch (Throwable $e) {
            throw BatchHandlerFailed::wrap($e);
        } finally {
            $this->context?->leave();
        }
    }

    /**
     * The one pause of the isolation replay, between its two attempts.
     *
     * @infection-ignore-all the sleep IS the whole effect: its arithmetic is observable only by
     *                       measuring wall-clock pauses, which a unit test cannot do without flaking.
     *                       The unit suite pins that the pause HAPPENS, with a one-sided bound, and
     *                       accepts this method as the sleep sink; same pin shape as
     *                       `\Storm\Chronicler\Store\RetryEventStore::pause`, whose reasoning also sits at
     *                       the site, never in `source.excludes`.
     */
    private function pauseBeforeRetry(): void
    {
        usleep($this->replayRetryDelayMs * 1000);
    }
}
