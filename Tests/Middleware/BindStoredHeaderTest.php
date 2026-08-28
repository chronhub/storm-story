<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Middleware;

use Closure;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Storm\Message\CurrentStoredHeader;
use Storm\Message\Header;
use Storm\Story\Middleware\BindStoredHeader;
use Storm\Story\Stamp\StoredHeaderStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;

final class BindStoredHeaderTest extends TestCase
{
    #[Test]
    public function binds_the_stored_header_during_handling(): void
    {
        $current = new CurrentStoredHeader;

        /** @var array<string, mixed> $seen */
        $seen = [];

        new BindStoredHeader($current)->handle(
            $this->envelopeWithStoredHeader(),
            $this->terminal(function () use ($current, &$seen): void {
                $seen = [
                    'type' => $current->aggregateType(),
                    'idType' => $current->aggregateIdType(),
                    'version' => $current->aggregateVersion(),
                    'hasOccurredAt' => $current->occurredAt() !== null,
                ];
            }),
        );

        $this->assertSame([
            'type' => 'App\\Account',
            'idType' => 'App\\AccountId',
            'version' => 3,
            'hasOccurredAt' => true,
        ], $seen);
    }

    #[Test]
    public function clears_the_stored_header_after_handling(): void
    {
        $current = new CurrentStoredHeader;

        new BindStoredHeader($current)->handle($this->envelopeWithStoredHeader(), $this->terminal());

        $this->assertNull($current->aggregateType());
        $this->assertNull($current->occurredAt());
    }

    #[Test]
    public function clears_even_when_the_handler_throws(): void
    {
        $current = new CurrentStoredHeader;

        try {
            new BindStoredHeader($current)->handle(
                $this->envelopeWithStoredHeader(),
                $this->terminal(fn () => throw new RuntimeException('boom')),
            );
        } catch (RuntimeException) {
        }

        $this->assertNull($current->aggregateType());
    }

    #[Test]
    public function passes_through_and_stays_empty_without_a_stored_header_stamp(): void
    {
        $current = new CurrentStoredHeader;
        $ran = 0;

        new BindStoredHeader($current)->handle(
            new Envelope(new stdClass),
            $this->terminal(function () use ($current, &$ran): void {
                $ran++;
                $this->assertNull($current->aggregateType());
            }),
        );

        $this->assertSame(1, $ran);
    }

    #[Test]
    public function a_nested_unstamped_dispatch_shadows_the_parent_stored_header(): void
    {
        $current = new CurrentStoredHeader;
        $middleware = new BindStoredHeader($current);

        /** @var array<string, mixed> $seen */
        $seen = [];

        $middleware->handle(
            $this->envelopeWithStoredHeader(),
            $this->terminal(function () use ($middleware, $current, &$seen): void {
                // a reaction dispatching another event in-process: no StoredHeaderStamp on the inner
                // envelope; the inner handler must see an EMPTY holder, never the parent's values
                $middleware->handle(
                    new Envelope(new stdClass),
                    $this->terminal(function () use ($current, &$seen): void {
                        $seen['inner'] = $current->aggregateType();
                    }),
                );

                $seen['outerAfterInner'] = $current->aggregateType();
            }),
        );

        $this->assertNull($seen['inner']);
        $this->assertSame('App\\Account', $seen['outerAfterInner']);
    }

    private function envelopeWithStoredHeader(): Envelope
    {
        return new Envelope(new stdClass, [new StoredHeaderStamp([
            Header::OccurredAt->value => '2026-05-21T10:00:00.000000+00:00',
            Header::AggregateId->value => 'account-7',
            Header::AggregateType->value => 'App\\Account',
            Header::AggregateIdType->value => 'App\\AccountId',
            Header::AggregateVersion->value => 3,
        ])]);
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
