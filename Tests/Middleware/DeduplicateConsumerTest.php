<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Middleware;

use Closure;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Storm\Chronicler\Inbox\InboxStore;
use Storm\Story\Consume\InboxTransactionContext;
use Storm\Story\Middleware\DeduplicateConsumer;
use Storm\Story\Stamp\BatchModeStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

final class DeduplicateConsumerTest extends TestCase
{
    #[Test]
    public function passes_through_without_consulting_the_inbox_when_not_received_from_a_transport(): void
    {
        $inbox = $this->inbox();
        $ran = 0;

        new DeduplicateConsumer($inbox)->handle(
            new Envelope(new stdClass, [new MessageIdStamp('evt-1')]),
            $this->terminal(function () use (&$ran): void {
                $ran++;
            }),
        );

        $this->assertSame(1, $ran);
        $this->assertSame([], $inbox->calls);
    }

    #[Test]
    public function passes_through_when_there_is_no_message_id_to_dedup_on(): void
    {
        $inbox = $this->inbox();
        $ran = 0;

        new DeduplicateConsumer($inbox)->handle(
            new Envelope(new stdClass, [new ReceivedStamp('events')]),
            $this->terminal(function () use (&$ran): void {
                $ran++;
            }),
        );

        $this->assertSame(1, $ran);
        $this->assertSame([], $inbox->calls);
    }

    #[Test]
    public function dedups_on_transport_plus_message_id_and_runs_the_handler_on_first_delivery(): void
    {
        $inbox = $this->inbox();
        $ran = 0;

        new DeduplicateConsumer($inbox)->handle(
            new Envelope(new stdClass, [new ReceivedStamp('events'), new MessageIdStamp('evt-1')]),
            $this->terminal(function () use (&$ran): void {
                $ran++;
            }),
        );

        $this->assertSame([['events', 'evt-1']], $inbox->calls);
        $this->assertSame(1, $ran);
    }

    #[Test]
    public function skips_the_handler_when_the_inbox_reports_a_duplicate(): void
    {
        $inbox = $this->inbox(runHandler: false);
        $ran = 0;

        new DeduplicateConsumer($inbox)->handle(
            new Envelope(new stdClass, [new ReceivedStamp('events'), new MessageIdStamp('evt-1')]),
            $this->terminal(function () use (&$ran): void {
                $ran++;
            }),
        );

        $this->assertSame([['events', 'evt-1']], $inbox->calls);
        $this->assertSame(0, $ran);
    }

    #[Test]
    public function passes_through_without_consulting_the_inbox_in_batch_mode(): void
    {
        $inbox = $this->inbox();
        $ran = 0;

        new DeduplicateConsumer($inbox)->handle(
            new Envelope(new stdClass, [new ReceivedStamp('events'), new MessageIdStamp('evt-1'), new BatchModeStamp]),
            $this->terminal(function () use (&$ran): void {
                $ran++;
            }),
        );

        $this->assertSame(1, $ran);           // the handler ran
        $this->assertSame([], $inbox->calls); // the inbox was NOT consulted; the batch owns dedup + tx
    }

    #[Test]
    public function the_handlers_run_inside_an_inbox_transaction_context_entry(): void
    {
        // the dual-write guard reads this ambient entry: open exactly while the handlers run inside
        // once(), naming the handled message class, and left afterwards
        $context = new InboxTransactionContext;
        $observed = null;

        new DeduplicateConsumer($this->inbox(), $context)->handle(
            new Envelope(new stdClass, [new ReceivedStamp('events'), new MessageIdStamp('evt-1')]),
            $this->terminal(function () use ($context, &$observed): void {
                $observed = [$context->active(), $context->current()];
            }),
        );

        $this->assertSame([true, stdClass::class], $observed);
        $this->assertFalse($context->active(), 'the entry is left after the transaction');
    }

    #[Test]
    public function the_context_entry_is_left_when_the_handler_throws(): void
    {
        // finally-paired: a poisoned handler must not leak an open entry into the next delivery,
        // which would refuse innocent sends
        $context = new InboxTransactionContext;

        try {
            new DeduplicateConsumer($this->inbox(), $context)->handle(
                new Envelope(new stdClass, [new ReceivedStamp('events'), new MessageIdStamp('evt-1')]),
                $this->terminal(static function (): void {
                    throw new RuntimeException('poison');
                }),
            );
            $this->fail('expected the handler failure to propagate');
        } catch (RuntimeException) {
        }

        $this->assertFalse($context->active());
    }

    /**
     * @return InboxStore&object{calls: list<array{string, string}>}
     */
    private function inbox(bool $runHandler = true): InboxStore
    {
        return new class($runHandler) implements InboxStore
        {
            /** @var list<array{string, string}> */
            public array $calls = [];

            public function __construct(private readonly bool $runHandler) {}

            public function once(string $consumer, string $messageId, Closure $handler): bool
            {
                $this->calls[] = [$consumer, $messageId];

                if ($this->runHandler) {
                    $handler();

                    return true;
                }

                return false;
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
