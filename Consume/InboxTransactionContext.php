<?php

declare(strict_types=1);

namespace Storm\Story\Consume;

use LogicException;
use Storm\Story\Attribute\DispatchesUnderInboxTransaction;
use Storm\Story\Middleware\DeduplicateConsumer;
use Storm\Story\Transport\InboxGuardedSendersLocator;

/**
 * Ambient marker of the open inbox transaction: which message's handlers are currently running INSIDE
 * an inbox-owned transaction, on either consume path.
 *
 * Two writers, both `finally`-paired so an exception never leaks an entry:
 *
 * - `DeduplicateConsumer` enters around the handlers it runs inside the per-message `once()` transaction,
 *   the Worker path;
 *
 * - `BatchConsumer` enters around each batched dispatch, inside the shared batch transaction and the
 *   isolation replay alike.
 *
 * One reader: `InboxGuardedSendersLocator`, which refuses a broker send while an entry is open unless the
 * handled message class was granted via `#[DispatchesUnderInboxTransaction]`. A broker send inside the
 * transaction is a dual-write, since a rollback cannot unsend it; the guard makes the accidental one
 * impossible to write and the deliberate one a signed, greppable declaration.
 *
 * A stack, not a flag: a nested SYNC dispatch, with no senders and handled in place, re-enters through
 * `DeduplicateConsumer` only on the received path, but a same-process re-entry must restore the parent's
 * entry on leave, not clear it.
 *
 * @see DispatchesUnderInboxTransaction the per-handler grant
 * @see InboxGuardedSendersLocator the enforcement point
 */
final class InboxTransactionContext
{
    /** @var list<class-string> the handled message class per open inbox transaction, innermost last */
    private array $stack = [];

    /**
     * @param  class-string  $messageClass  the message whose handlers are about to run inside the
     *                                      transaction
     */
    public function enter(string $messageClass): void
    {
        $this->stack[] = $messageClass;
    }

    /**
     * @throws LogicException when no enter() frame is open: the consume machinery is unbalanced,
     *                        and a silent no-op would let a LATER leave() pop a parent frame,
     *                        disarming the dual-write guard on the outer transaction
     */
    public function leave(): void
    {
        if ($this->stack === []) {
            throw new LogicException('InboxTransactionContext::leave() without a matching enter(): unbalanced consume machinery; a silent pop here would later disarm the dual-write guard on a parent frame.');
        }

        array_pop($this->stack);
    }

    public function active(): bool
    {
        return $this->stack !== [];
    }

    /**
     * The innermost handled message class, null when no inbox transaction is open.
     */
    public function current(): ?string
    {
        return $this->stack === [] ? null : array_last($this->stack);
    }
}
