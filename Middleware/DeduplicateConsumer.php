<?php

declare(strict_types=1);

namespace Storm\Story\Middleware;

use Override;
use Storm\Chronicler\Inbox\InboxStore;
use Storm\Story\Attribute\DispatchesUnderInboxTransaction;
use Storm\Story\Consume\InboxTransactionContext;
use Storm\Story\Stamp\BatchModeStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Throwable;

/**
 * Consumer-side idempotency for at-least-once delivery: runs the rest of the stack, the handlers, at most
 * once per `(transport, message-id)` via the `InboxStore`.
 *
 * Only acts on a message received from a transport, where a `ReceivedStamp` is present: in-process dispatch
 * has no broker and no retries, so it passes straight through. The dedup id is the `MessageIdStamp`, the
 * message's own id, present on both transport serializers: kept by `PhpSerializer`, rebuilt from the header
 * by `NeutralMessageSerializer`. The consumer key is the receiving transport, so the same event fanned out
 * to several transports is deduplicated per consumer. A skipped duplicate runs no handler and is
 * acknowledged.
 *
 * Placed last in a bus's middleware stack so its `next()`, the handlers, runs inside the inbox transaction:
 * the handler's DB effects and the inbox row commit or roll back together. A `BatchModeStamp` inverts this:
 * the batch consumer owns the dedup and the shared transaction, so this middleware passes straight through
 * with no per-message transaction.
 *
 * Constraint this places on handlers, ENFORCED at the sender seam: a handler consumed via the inbox must
 * not dispatch an async-routed message directly; that broker send would happen inside the still-open inbox
 * transaction and a rollback could not unsend it, a dual-write. The handlers run inside an ambient
 * `InboxTransactionContext` entry, and the guarded senders locator refuses an undeclared broker send while
 * one is open. Route nested async through an outbox, a same-connection table write that commits atomically
 * with the row, or sign the at-least-once contract per handler with `#[DispatchesUnderInboxTransaction]`.
 *
 * @see BatchModeStamp the batched-consume pass-through marker
 * @see InboxTransactionContext the ambient entry the handlers run inside
 * @see DispatchesUnderInboxTransaction the signed escape hatch
 */
final readonly class DeduplicateConsumer implements MiddlewareInterface
{
    public function __construct(
        private InboxStore $inbox,
        /** Null runs unguarded for standalone use; the bundle wires the shared ambient context. */
        private ?InboxTransactionContext $context = null,
    ) {}

    /**
     * {@inheritDoc}
     *
     * @throws Throwable from the handlers run inside the inbox transaction and rolled back with it, or from
     *                   the `InboxStore` recording the row, whose store contract owns the concrete DBAL type
     */
    #[Override]
    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        // Inside a batched consume the batch consumer already holds the dedup INSERT and the shared
        // transaction for the whole batch; a per-message once() here would nest a second transaction
        // inside it. Pass straight through; the batch transaction is the atomic boundary.
        if ($envelope->last(BatchModeStamp::class) !== null) {
            return $stack->next()->handle($envelope, $stack);
        }

        $received = $envelope->last(ReceivedStamp::class);
        $idStamp = $envelope->last(MessageIdStamp::class);

        if ($received === null || $idStamp === null) {
            return $stack->next()->handle($envelope, $stack);
        }

        // On a duplicate, inbox->once() skips the closure and $handled stays the original envelope;
        // no HandledStamp, but the worker acks a returned envelope regardless, so a dropped duplicate
        // runs no side effect and raises no error.
        $handled = $envelope;

        $this->inbox->once(
            $received->getTransportName(),
            $idStamp->id,
            function () use (&$handled, $envelope, $stack): void {
                // the handlers run inside the inbox transaction: mark it ambient, finally-paired, so
                // the guarded senders locator can refuse an undeclared broker send, a dual-write
                $this->context?->enter($envelope->getMessage()::class);

                try {
                    $handled = $stack->next()->handle($envelope, $stack);
                } finally {
                    $this->context?->leave();
                }
            },
        );

        return $handled;
    }
}
