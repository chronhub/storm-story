<?php

declare(strict_types=1);

namespace Storm\Story\Middleware;

use Override;
use Storm\Contracts\Message\MetaIdentityGenerator;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;

/**
 * Boundary middleware: gives every dispatched message a stable identity and trace.
 *
 * Messenger assigns no message id, so this stamps one when missing; that id is the causation of whatever
 * the handler records. The `CorrelationStamp` is set to that id when absent, so a root message starts its
 * own trace, while descendants carry an inherited correlation and are left untouched. Already-stamped
 * envelopes pass through unchanged and idempotent, so an inbound message off a transport keeps the identity
 * it traveled with.
 */
final readonly class AssignMessageMetadata implements MiddlewareInterface
{
    public function __construct(
        private MetaIdentityGenerator $identity,
    ) {}

    #[Override]
    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $idStamp = $envelope->last(MessageIdStamp::class);

        if ($idStamp === null) {
            $idStamp = new MessageIdStamp($this->identity->generate());
            $envelope = $envelope->with($idStamp);
        }

        if ($envelope->last(CorrelationStamp::class) === null) {
            $envelope = $envelope->with(new CorrelationStamp($idStamp->id));
        }

        return $stack->next()->handle($envelope, $stack);
    }
}
