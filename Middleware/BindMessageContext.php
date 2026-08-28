<?php

declare(strict_types=1);

namespace Storm\Story\Middleware;

use Override;
use Storm\Message\ContextValues;
use Storm\Message\CurrentMessageContext;
use Storm\Message\Exception\UnbalancedContextFrame;
use Storm\Story\Stamp\ActorStamp;
use Storm\Story\Stamp\ContextBagStamp;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Storm\Story\Stamp\TenantStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;

/**
 * The bridge: turns the envelope's stamps into the ambient CurrentMessageContext for the
 * duration of the handler, then clears it.
 *
 * This is what lets the writing path, the aggregate repository's enricher chain, stamp the headers
 * of recorded events without any handler threading identifiers. The handled message's id becomes
 * the causation of those events; its correlation, actor, tenant and declared bag are inherited
 * as-is. Bound before the handler, cleared in `finally` so the next message starts from an empty
 * context.
 *
 * Must run after AssignMessageMetadata, which guarantees the id/correlation stamps exist.
 *
 * @see AssignMessageMetadata
 */
final readonly class BindMessageContext implements MiddlewareInterface
{
    public function __construct(
        private CurrentMessageContext $context,
    ) {}

    /**
     * {@inheritDoc}
     *
     * @throws UnbalancedContextFrame never in practice, since bind() always precedes the `finally` clear() here
     */
    #[Override]
    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $actor = $envelope->last(ActorStamp::class)?->actor;

        $this->context->bind(new ContextValues(
            correlationId: $envelope->last(CorrelationStamp::class)?->id,
            causationId: $envelope->last(MessageIdStamp::class)?->id,
            actorId: $actor?->id,
            actorType: $actor?->type,
            tenantId: $envelope->last(TenantStamp::class)?->id,
            bag: $envelope->last(ContextBagStamp::class)->values ?? [],
        ));

        try {
            return $stack->next()->handle($envelope, $stack);
        } finally {
            $this->context->clear();
        }
    }
}
