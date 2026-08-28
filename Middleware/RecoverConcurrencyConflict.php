<?php

declare(strict_types=1);

namespace Storm\Story\Middleware;

use Override;
use Storm\Chronicler\Exception\DuplicateVersion;
use Storm\Chronicler\Exception\StaleVersion;
use Storm\Contracts\Chronicler\RetryableConcurrencyConflict;
use Storm\Contracts\Serializer\SubjectForgotten;
use Storm\Story\Stamp\BatchModeStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\RecoverableMessageHandlingException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Throwable;

/**
 * Maps the Chronicler's optimistic-concurrency split onto Messenger's retry markers, so a version
 * race off transport is retried-forward instead of dead-lettered by the infra-calibrated
 * `max_retries`.
 *
 * The event store guards append with a stream-head CAS. Under contention two consumers load the
 * same version and one loses the race, surfacing as `StaleVersion`, where the lost writer is merely
 * behind, or, past the pre-check, `DuplicateVersion`, where the `(stream, version)` unique rejected a
 * redelivery or a genuine clash. Neither carries a Messenger marker, so by default both inherit the
 * transport's `max_retries`, tuned for infra blips such as a broker or network outage, and a busy
 * aggregate's loser dead-letters after a handful of tries even though it would have won on the next
 * reload. This middleware translates the split:
 *
 *  - `StaleVersion`, matched via its contracted `RetryableConcurrencyConflict` marker so any retryable
 *    conflict a store surfaces rides the same lane, becomes `RecoverableMessageHandlingException`:
 *    transient, retry-forward with `max_retries` ignored. The redelivery re-runs the handler, which
 *    reloads the now-current aggregate and re-decides against it; the command is replayed, not the stale
 *    append retried, since the pending events carry a fixed expected-version and re-appending them would
 *    only re-conflict. Progress is guaranteed GLOBALLY, as a competitor committed a higher version and
 *    the stream advanced; an individual command can still lose repeatedly on one hot stream, which is why
 *    the delay is jittered and grows. With `retryMaxDelayMs` set, each redelivery waits a random slice of
 *    an exponentially-widening window, the base doubled per attempt and capped, so simultaneous losers
 *    spread out instead of re-colliding on the same tick, the thundering-herd shape; at zero the delay
 *    stays the fixed base, the pre-jitter behavior.
 *
 *  - `DuplicateVersion` becomes `UnrecoverableMessageHandlingException`: already applied, or a true
 *    clash a reload cannot fix, so it dead-letters at once rather than burn the retry budget.
 *
 *  - `SubjectForgotten` rides the same terminal lane: a command writing personal data about a
 *    tombstoned subject cannot succeed on ANY retry, since the tombstone is durable by design, so
 *    retrying it only delays the dead-letter an operator must see.
 *
 * The poison backstop is therefore the split itself, since a duplicate is terminal, not a retry count.
 * The handler raises the conflict, so it arrives wrapped in `HandlerFailedException`; the wrapped
 * exceptions are scanned, a recoverable one winning over an unrecoverable, mirroring Messenger's own
 * nested-exception rule. Anything else propagates untouched.
 *
 * Placed before `DeduplicateConsumer`, outside the inbox transaction: the failed attempt's inbox
 * row has already rolled back with the append, so the redelivery the marker triggers re-runs the
 * handler cleanly and the winning attempt is still recorded exactly once.
 *
 * The translation acts on BOTH consume proofs, a Worker delivery's `ReceivedStamp` and a batched
 * dispatch's `BatchModeStamp`. The batch consumer honors the same markers, redelivering a
 * recoverable to its own transport with this middleware's delay and capturing an unrecoverable at
 * once, so a version race loses none of its progress semantics for riding a batch; without this arm
 * the batch's isolation replay would give a hot-stream loser two 100 ms-spaced attempts and then
 * condemn it as poison, where the Worker path redelivers until the stream advances. In-process
 * dispatch, with neither stamp, has no retry machinery to drive, so it passes straight through and
 * a sync conflict keeps its original type for the caller.
 *
 * @see \Storm\Chronicler\Exception\StaleVersion
 */
final readonly class RecoverConcurrencyConflict implements MiddlewareInterface
{
    /** The exponential window stops widening past this many doublings; the cap rules beyond it anyway. */
    private const int MAX_BACKOFF_SHIFTS = 6;

    public function __construct(
        private int $retryDelayMs = 0,
        /** Caps the jittered exponential window; 0 = no backoff, the fixed `retryDelayMs` alone. */
        private int $retryMaxDelayMs = 0,
    ) {}

    /**
     * {@inheritDoc}
     *
     * @throws RecoverableMessageHandlingException a handler lost a version race via `StaleVersion`; retry-forward
     * @throws UnrecoverableMessageHandlingException a handler hit a duplicate version; dead-letter at once
     * @throws Throwable any other failure from the rest of the stack, propagated untouched
     */
    #[Override]
    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        // either consume proof earns the translation: the Worker's ReceivedStamp, or the batched
        // dispatch's non-sendable BatchModeStamp; never added artificially, it IS the batch's
        // internal evidence, so the other Worker semantics the batch deliberately sheds stay shed
        if ($envelope->last(ReceivedStamp::class) === null && $envelope->last(BatchModeStamp::class) === null) {
            return $stack->next()->handle($envelope, $stack);
        }

        try {
            return $stack->next()->handle($envelope, $stack);
        } catch (HandlerFailedException $e) {
            $stale = null;
            $duplicate = null;

            // recursive: a handler dispatching SYNCHRONOUSLY wraps the inner failure in a second
            // HandlerFailedException, and a one-level read would miss the leaf, dead-lettering a
            // command that would win on the next reload
            foreach ($e->getWrappedExceptions(recursive: true) as $wrapped) {
                // the contracted marker, not the concrete: any retryable conflict a third-party
                // store surfaces rides the same retry-forward lane as StaleVersion
                if ($wrapped instanceof RetryableConcurrencyConflict) {
                    $stale = $wrapped;
                } elseif ($wrapped instanceof DuplicateVersion || $wrapped instanceof SubjectForgotten) {
                    // both terminal: no retry can un-append a duplicate, and no retry can un-forget
                    // a tombstoned subject
                    $duplicate = $wrapped;
                }
            }

            if ($stale !== null) {
                throw new RecoverableMessageHandlingException($stale->getMessage(), previous: $e, retryDelay: $this->retryDelay($envelope));
            }

            if ($duplicate !== null) {
                throw new UnrecoverableMessageHandlingException($duplicate->getMessage(), previous: $e);
            }

            throw $e;
        }
    }

    /**
     * Equal-jitter exponential backoff, per redelivery: the window is the base doubled once per prior
     * attempt, the `RedeliveryStamp` count, capped at `retryMaxDelayMs`; the delay lands randomly in the
     * window's upper half, so concurrent losers of one stream spread out while still waiting at least
     * half the window. `retryMaxDelayMs` 0 keeps the fixed base, no growth, no jitter.
     */
    private function retryDelay(Envelope $envelope): int
    {
        if ($this->retryMaxDelayMs <= 0 || $this->retryDelayMs <= 0) {
            return $this->retryDelayMs;
        }

        $attempt = min(RedeliveryStamp::getRetryCountFromEnvelope($envelope), self::MAX_BACKOFF_SHIFTS);
        $window = min($this->retryMaxDelayMs, $this->retryDelayMs << $attempt);

        return random_int(intdiv($window, 2), $window);
    }
}
