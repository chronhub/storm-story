<?php

declare(strict_types=1);

namespace Storm\Story\Middleware;

use Override;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\ExceptionInterface;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Throwable;

/**
 * Runs Symfony's validation middleware only on a FRESH dispatch; a message received from a transport is
 * passed straight through, unvalidated.
 *
 * On the command bus, `validation` sits before the send middleware, so a command dispatched to an async
 * transport is validated BEFORE it is queued. When the worker consumes it, with a `ReceivedStamp` present,
 * re-validating the same, unchanged, already-validated payload is redundant work on the hot drain path,
 * a measurable share of the consume CPU. Skipping it there loses no guarantee: nothing reaches the
 * queue without having passed validation on the way in. A fresh in-process or boundary dispatch, carrying no
 * `ReceivedStamp`, still validates, since that is where untrusted input enters.
 *
 * Mirrors {@see DeduplicateConsumer}, the other received-only consume-side middleware: same `ReceivedStamp`
 * gate, opposite intent, since dedup acts only when received while validation acts only when NOT received.
 */
final readonly class ValidateUnlessReceived implements MiddlewareInterface
{
    public function __construct(
        #[Autowire(service: 'messenger.middleware.validation')]
        private MiddlewareInterface $validation,
    ) {}

    /**
     * {@inheritDoc}
     *
     * @throws ExceptionInterface a ValidationFailedException from the wrapped validation on a fresh, invalid
     *                            payload, or an error propagated from the rest of the stack
     * @throws Throwable from the handlers deeper in the stack, business failures on the consume path
     */
    #[Override]
    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if ($envelope->last(ReceivedStamp::class) !== null) {
            return $stack->next()->handle($envelope, $stack); // received from a transport = already validated pre-queue, so skip
        }

        return $this->validation->handle($envelope, $stack);
    }
}
