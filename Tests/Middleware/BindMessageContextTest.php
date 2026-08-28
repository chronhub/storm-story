<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Middleware;

use Closure;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Storm\Bureau\Actor;
use Storm\Message\CurrentMessageContext;
use Storm\Story\Middleware\BindMessageContext;
use Storm\Story\Stamp\ActorStamp;
use Storm\Story\Stamp\ContextBagStamp;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Storm\Story\Stamp\TenantStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;

final class BindMessageContextTest extends TestCase
{
    #[Test]
    public function binds_the_stamps_as_context_during_handling(): void
    {
        $context = new CurrentMessageContext;
        $middleware = new BindMessageContext($context);

        /** @var array<string, ?string> $seen */
        $seen = [];

        $middleware->handle(
            new Envelope(new stdClass, [
                new CorrelationStamp('trace-1'),
                new MessageIdStamp('cmd-1'),
                new ActorStamp(new Actor('user-1', 'App\\User')),
                new TenantStamp('tenant-1'),
            ]),
            $this->terminal(function () use ($context, &$seen): void {
                $seen = [
                    'correlation' => $context->correlationId(),
                    'causation' => $context->causationId(),
                    'actorId' => $context->actorId(),
                    'actorType' => $context->actorType(),
                    'tenant' => $context->tenantId(),
                ];
            }),
        );

        $this->assertSame([
            'correlation' => 'trace-1',
            // the handled message's id is the causation of the events it records
            'causation' => 'cmd-1',
            'actorId' => 'user-1',
            'actorType' => 'App\\User',
            'tenant' => 'tenant-1',
        ], $seen);
    }

    #[Test]
    public function binds_the_bag_stamp_into_the_ambient_context(): void
    {
        $context = new CurrentMessageContext;
        $middleware = new BindMessageContext($context);

        /** @var array<string, bool|float|int|string> $seenBag */
        $seenBag = [];

        $middleware->handle(
            new Envelope(new stdClass, [
                new MessageIdStamp('cmd-1'),
                new ContextBagStamp(['origin' => 'http']),
            ]),
            $this->terminal(function () use ($context, &$seenBag): void {
                $seenBag = $context->bag();
            }),
        );

        $this->assertSame(['origin' => 'http'], $seenBag);
        $this->assertSame([], $context->bag());
    }

    #[Test]
    public function clears_the_context_after_handling(): void
    {
        $context = new CurrentMessageContext;
        $middleware = new BindMessageContext($context);

        $middleware->handle(
            new Envelope(new stdClass, [new CorrelationStamp('trace-1'), new MessageIdStamp('cmd-1')]),
            $this->terminal(),
        );

        $this->assertNull($context->correlationId());
        $this->assertNull($context->causationId());
    }

    #[Test]
    public function an_envelope_with_no_message_id_binds_a_null_causation(): void
    {
        // The causation IS the handled message's own id, so a message that carries none causes
        // nothing nameable, and the frame must say so. Reading the id off an absent stamp would kill
        // the middleware on the first unstamped dispatch, which is exactly what a first-hop command
        // entering from a controller looks like before AssignMessageMetadata has run.
        $context = new CurrentMessageContext;
        $seen = 'unset';

        new BindMessageContext($context)->handle(
            new Envelope(new stdClass, [new CorrelationStamp('trace-1')]),
            $this->terminal(function () use ($context, &$seen): void {
                $seen = $context->causationId();
            }),
        );

        $this->assertNull($seen);
    }

    #[Test]
    public function clears_the_context_even_when_the_handler_throws(): void
    {
        $context = new CurrentMessageContext;
        $middleware = new BindMessageContext($context);

        try {
            $middleware->handle(
                new Envelope(new stdClass, [new MessageIdStamp('cmd-1')]),
                $this->terminal(fn () => throw new RuntimeException('boom')),
            );
        } catch (RuntimeException) {
        }

        $this->assertNull($context->causationId());
    }

    private function terminal(?Closure $onHandle = null): StackInterface
    {
        return new readonly class($onHandle) implements StackInterface
        {
            public function __construct(private ?Closure $onHandle) {}

            public function next(): MiddlewareInterface
            {
                return new readonly class($this->onHandle) implements MiddlewareInterface
                {
                    public function __construct(private ?Closure $onHandle) {}

                    public function handle(Envelope $envelope, StackInterface $stack): Envelope
                    {
                        if ($this->onHandle !== null) {
                            ($this->onHandle)();
                        }

                        return $envelope;
                    }
                };
            }
        };
    }
}
