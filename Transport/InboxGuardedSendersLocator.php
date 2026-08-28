<?php

declare(strict_types=1);

namespace Storm\Story\Transport;

use LogicException;
use Override;
use Storm\Contracts\Message\SendableUnderInboxTransaction;
use Storm\Story\Attribute\DispatchesUnderInboxTransaction;
use Storm\Story\Consume\InboxTransactionContext;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SendersLocatorInterface;
use Symfony\Component\Messenger\Transport\Sync\SyncTransport;

/**
 * The dual-write guard: decorates the senders locator so a broker send attempted while an inbox
 * transaction is open is refused, loud, at the send site, unless the handled message class carries the
 * signed `#[DispatchesUnderInboxTransaction]` grant.
 *
 * The transaction cannot roll a broker send back, so an undeclared send from a handler running inside
 * the inbox is a latent dual-write: on rollback the message stands, and the replay re-sends a fresh copy
 * the downstream inbox cannot dedup. Refusing at the SENDER seam, not in a bus middleware, keeps the
 * routing decision where it already lives: only a message that actually routes to a transport reaches a
 * sender, so a sync or handled-in-process dispatch is never touched.
 *
 * Two declared ways through, and a structural one:
 *
 * - A `SyncTransport` sender passes: its "send" IS in-process handling, inside the same transaction,
 *   no dual-write;
 *
 * - A message implementing `SendableUnderInboxTransaction` passes: the MESSAGE declares that loss and
 *   duplication are harmless by design, fire-and-forget observability such as a saga history entry;
 *
 * - A handled message whose class matches the compiled grant set passes: its HANDLER signed the
 *   at-least-once contract with `#[DispatchesUnderInboxTransaction]`.
 *
 * The checks are instanceof and an `is_a` walk over a handful of grants on the hot path, and
 * nothing at all when no inbox transaction is open.
 *
 * @see DispatchesUnderInboxTransaction the handler-side grant and its contract
 * @see \Storm\Contracts\Message\SendableUnderInboxTransaction the message-side declaration
 * @see InboxTransactionContext written by both consume paths, `finally`-paired
 * @see \Storm\Symfony\Compiler\GrantInboxDispatchPass compiles the grant set
 */
final readonly class InboxGuardedSendersLocator implements SendersLocatorInterface
{
    /**
     * @param  list<class-string>  $granted  handled message types whose handlers signed the
     *                                       `#[DispatchesUnderInboxTransaction]` contract; a
     *                                       concrete class, or the interface or parent class an
     *                                       `#[AsMessageHandler]` declares through `handles:` or
     *                                       its parameter type
     */
    public function __construct(
        private SendersLocatorInterface $senders,
        private InboxTransactionContext $context,
        private array $granted = [],
    ) {}

    /**
     * {@inheritDoc}
     *
     * @throws LogicException when a broker send is attempted inside an open inbox transaction and the
     *                        handled message class holds no `#[DispatchesUnderInboxTransaction]` grant
     */
    #[Override]
    public function getSenders(Envelope $envelope): iterable
    {
        foreach ($this->senders->getSenders($envelope) as $alias => $sender) {
            if (! $sender instanceof SyncTransport
                && $this->context->active()
                && ! $envelope->getMessage() instanceof SendableUnderInboxTransaction
                && ! $this->grantedNow()) {
                throw new LogicException(sprintf(
                    'Refusing to send [%s] to transport "%s": the inbox transaction of [%s] is still open, and a broker'
                    .' send cannot be rolled back with it — a dual-write. Route the nested dispatch through an outbox,'
                    .' or declare #[DispatchesUnderInboxTransaction] on the handler to sign the at-least-once contract'
                    .' (deterministic MessageIdStamp on the dispatch, or a domain-idempotent target).',
                    $envelope->getMessage()::class,
                    $alias,
                    $this->context->current() ?? 'unknown',
                ));
            }

            yield $alias => $sender;
        }
    }

    /**
     * Matches the handled class against the grant set by HIERARCHY, not string equality: the
     * compiled grant carries whatever type the signed `#[AsMessageHandler]` declared, and Messenger
     * resolves interface-typed and parent-typed handlers onto concrete messages, so the context
     * always holds the concrete class while the grant may hold its interface or parent. `is_a`
     * makes the two halves agree: the grant fires exactly when the signed handler runs.
     */
    private function grantedNow(): bool
    {
        $current = $this->context->current();

        return $current !== null
            && array_any($this->granted, static fn (string $granted): bool => is_a($current, $granted, true));
    }
}
