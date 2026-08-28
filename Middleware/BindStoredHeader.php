<?php

declare(strict_types=1);

namespace Storm\Story\Middleware;

use Override;
use Storm\Message\CurrentStoredHeader;
use Storm\Message\Exception\InvalidMessageException;
use Storm\Message\Exception\UnbalancedContextFrame;
use Storm\Message\Message;
use Storm\Story\Stamp\StoredHeaderStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;

/**
 * Exposes a republished event's stored header to its handlers via `CurrentStoredHeader`.
 *
 * A bus handler only gets the message object; the stored header, meaning `occurred_at` plus aggregate
 * type, id-type and version, rides on the `StoredHeaderStamp` the outbox publisher attaches. This
 * middleware materializes it into the ambient `CurrentStoredHeader` for the duration of handling, then
 * clears it. With no stamp, for instance a plain in-process dispatch, an EMPTY frame is bound so the holder
 * reads null; the same holds when the dispatch is nested under a stamped, republished event, where the
 * empty frame shadows the parent's stored header instead of leaking it into the inner handler.
 *
 * Sits on the event bus only, since the stamp exists only on outbox-republished events.
 */
final readonly class BindStoredHeader implements MiddlewareInterface
{
    public function __construct(
        private CurrentStoredHeader $current,
    ) {}

    /**
     * {@inheritDoc}
     *
     * @throws InvalidMessageException never in practice, since the bus message is a domain event, not a
     *                                 Message; declared because `new Message()` may throw it
     * @throws UnbalancedContextFrame never in practice, since bind() always precedes the `finally` clear() here
     */
    #[Override]
    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $stamp = $envelope->last(StoredHeaderStamp::class);

        // bind unconditionally, a null empty frame when there is no stamp. A nested in-process
        // dispatch must SHADOW its parent's stored header, not inherit it: without the empty frame,
        // a handler run under a republished stamped event would read the parent's values.
        // fromStored: the stamp carries a DURABLE header. Tolerant hydration, never the write gate.
        $this->current->bind($stamp === null ? null : Message::fromStored($envelope->getMessage(), $stamp->header));

        try {
            return $stack->next()->handle($envelope, $stack);
        } finally {
            $this->current->clear();
        }
    }
}
