<?php

declare(strict_types=1);

namespace Storm\Story\Tests;

use Closure;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Storm\Story\QueryBus;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\StampInterface;
use Throwable;

final class QueryBusTest extends TestCase
{
    #[Test]
    public function returns_the_single_handler_result(): void
    {
        $queryBus = new QueryBus(
            $this->busReturning(new Envelope(new stdClass, [new HandledStamp('result-42', 'handler')])),
        );

        $this->assertSame('result-42', $queryBus->ask(new stdClass));
    }

    #[Test]
    public function throws_when_no_handler_produced_a_result(): void
    {
        $queryBus = new QueryBus($this->busReturning(new Envelope(new stdClass)));

        $this->expectException(LogicException::class);

        $queryBus->ask(new stdClass);
    }

    #[Test]
    public function throws_when_several_handlers_produced_a_result(): void
    {
        $queryBus = new QueryBus($this->busReturning(
            new Envelope(new stdClass, [new HandledStamp('a', 'h1'), new HandledStamp('b', 'h2')]),
        ));

        $this->expectException(LogicException::class);

        $queryBus->ask(new stdClass);
    }

    #[Test]
    public function ask_all_returns_the_results_keyed(): void
    {
        $queryBus = new QueryBus($this->busHandling(static fn (stdClass $q): string => $q->tag.'-ok'));

        $results = $queryBus->askAll([
            'a' => (object) ['tag' => 'a'],
            'b' => (object) ['tag' => 'b'],
        ]);

        $this->assertSame(['a' => 'a-ok', 'b' => 'b-ok'], $results);
    }

    #[Test]
    public function ask_all_short_circuits_on_the_first_failure(): void
    {
        $seen = [];
        $queryBus = new QueryBus($this->busHandling(static function (stdClass $q) use (&$seen): string {
            $seen[] = $q->tag;

            if ($q->tag === 'b') {
                throw new RuntimeException('boom');
            }

            return $q->tag.'-ok';
        }));

        try {
            $queryBus->askAll([
                'a' => (object) ['tag' => 'a'],
                'b' => (object) ['tag' => 'b'],
                'c' => (object) ['tag' => 'c'],
            ]);
            $this->fail('askAll should raise on the first failure');
        } catch (HandlerFailedException) {
            // expected; the run stops at 'b'
        }

        $this->assertSame(['a', 'b'], $seen, "'c' must not run after 'b' failed");
    }

    #[Test]
    public function ask_all_settled_captures_each_outcome(): void
    {
        $queryBus = new QueryBus($this->busHandling(static function (stdClass $q): string {
            if ($q->tag === 'b') {
                throw new RuntimeException('boom');
            }

            return $q->tag.'-ok';
        }));

        $results = $queryBus->askAllSettled([
            'a' => (object) ['tag' => 'a'],
            'b' => (object) ['tag' => 'b'],
            'c' => (object) ['tag' => 'c'],
        ]);

        $this->assertSame(['a' => 'a-ok', 'c' => 'c-ok'], $results->okValues());
        $this->assertSame(['a' => 'a-ok', 'b' => null, 'c' => 'c-ok'], $results->valuesOrNull());
        $this->assertTrue($results->hasErrors());
    }

    #[Test]
    #[Group('adversarial')]
    public function ask_all_settled_captures_the_leaf_of_a_nested_sync_dispatch_never_a_wrapper(): void
    {
        // a query handler asking another query synchronously nests a second HandlerFailedException;
        // the captured error must be the type a consumer branches on, not an envelope of envelopes
        $leaf = new RuntimeException('inner boom');

        $queryBus = new QueryBus($this->busHandling(static function (stdClass $q) use ($leaf): string {
            throw new HandlerFailedException(new Envelope(new stdClass), ['inner' => $leaf]);
        }));

        $results = $queryBus->askAllSettled(['a' => (object) ['tag' => 'a']]);

        $this->assertSame(['a' => $leaf], $results->errors());
    }

    #[Test]
    public function ask_all_settled_captures_plain_throwable_as_error(): void
    {
        $boom = new RuntimeException('plain boom');

        $queryBus = new QueryBus(new readonly class($boom) implements MessageBusInterface
        {
            public function __construct(private Throwable $throwable) {}

            /**
             * @param  array<int, StampInterface>  $stamps
             */
            public function dispatch(object $message, array $stamps = []): Envelope
            {
                throw $this->throwable;
            }
        });

        $results = $queryBus->askAllSettled([
            'a' => new stdClass,
        ]);

        $this->assertTrue($results->hasErrors());
        $this->assertSame(['a' => null], $results->valuesOrNull());
        $this->assertSame($boom, $results->errors()['a']);
    }

    #[Test]
    public function ask_all_settled_unwraps_the_handler_exception(): void
    {
        $boom = new RuntimeException('boom');

        $queryBus = new QueryBus($this->busHandling(static function (stdClass $q) use ($boom): string {
            if ($q->tag === 'b') {
                throw $boom;
            }

            return $q->tag.'-ok';
        }));

        $results = $queryBus->askAllSettled([
            'a' => (object) ['tag' => 'a'],
            'b' => (object) ['tag' => 'b'],
        ]);

        // The captured error is the handler's own exception, not Messenger's HandlerFailedException wrapper.
        $this->assertSame($boom, $results->errors()['b']);
    }

    private function busReturning(Envelope $envelope): MessageBusInterface
    {
        return new readonly class($envelope) implements MessageBusInterface
        {
            public function __construct(private Envelope $envelope) {}

            /**
             * @param  array<int, StampInterface>  $stamps
             */
            public function dispatch(object $message, array $stamps = []): Envelope
            {
                return $this->envelope;
            }
        };
    }

    /**
     * A fake bus mapping each dispatched query to a result, or throwing as Messenger would, wrapped in
     * HandlerFailedException, the wrapper askAllSettled must unwrap.
     *
     * @param  callable(stdClass): mixed  $handle  returns the result, or throws to simulate a handler failure
     */
    private function busHandling(callable $handle): MessageBusInterface
    {
        return new readonly class($handle) implements MessageBusInterface
        {
            private Closure $handle;

            public function __construct(callable $handle)
            {
                $this->handle = $handle(...);
            }

            /**
             * @param  array<int, StampInterface>  $stamps
             */
            public function dispatch(object $message, array $stamps = []): Envelope
            {
                try {
                    $result = ($this->handle)($message);
                } catch (Throwable $e) {
                    throw new HandlerFailedException(new Envelope($message), [$e]);
                }

                return new Envelope($message, [new HandledStamp($result, 'handler')]);
            }
        };
    }
}
