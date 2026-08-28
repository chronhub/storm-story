<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Middleware;

use Closure;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use stdClass;
use Storm\Contracts\Message\MetaIdentityGenerator;
use Storm\Story\Middleware\AssignMessageMetadata;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;

final class AssignMessageMetadataTest extends TestCase
{
    #[Test]
    public function assigns_id_and_correlation_when_absent(): void
    {
        $middleware = new AssignMessageMetadata($this->identityReturning('msg-1'));

        $envelope = $middleware->handle(new Envelope(new stdClass), $this->terminal());

        $this->assertSame('msg-1', $envelope->last(MessageIdStamp::class)?->id);
        // A root message starts its own trace: correlation = its id.
        $this->assertSame('msg-1', $envelope->last(CorrelationStamp::class)?->id);
    }

    #[Test]
    public function correlation_falls_back_to_the_existing_id_when_only_the_id_is_present(): void
    {
        $middleware = new AssignMessageMetadata($this->identityReturning('generated'));

        $envelope = $middleware->handle(
            new Envelope(new stdClass, [new MessageIdStamp('existing-id')]),
            $this->terminal(),
        );

        $this->assertSame('existing-id', $envelope->last(MessageIdStamp::class)?->id);
        $this->assertSame('existing-id', $envelope->last(CorrelationStamp::class)?->id);
    }

    #[Test]
    public function keeps_existing_id_and_correlation(): void
    {
        $middleware = new AssignMessageMetadata($this->identityReturning('generated'));

        $envelope = $middleware->handle(
            new Envelope(new stdClass, [new MessageIdStamp('existing-id'), new CorrelationStamp('trace-1')]),
            $this->terminal(),
        );

        $this->assertSame('existing-id', $envelope->last(MessageIdStamp::class)?->id);
        $this->assertSame('trace-1', $envelope->last(CorrelationStamp::class)?->id);
    }

    private function identityReturning(string $id): MetaIdentityGenerator
    {
        return new readonly class($id) implements MetaIdentityGenerator
        {
            public function __construct(private string $id) {}

            public function generate(): string
            {
                return $this->id;
            }
        };
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
