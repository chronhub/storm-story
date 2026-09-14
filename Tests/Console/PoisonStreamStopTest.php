<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Console;

use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use stdClass;
use Storm\Story\Console\ConsumeBatchedCommand;
use Storm\Story\Consume\BatchDecision;
use Storm\Story\Consume\BatchProcessor;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;

final class PoisonStreamStopTest extends TestCase
{
    #[TestWith([false])]
    #[TestWith([true])]
    public function test_stop_request_is_observed_between_undecodable_deliveries(bool $partial): void
    {
        $message = new Envelope(new stdClass);
        $receiver = new class($partial, $message) implements ReceiverInterface
        {
            public ?Envelope $acked = null;

            public function __construct(private bool $partial, private Envelope $message) {}

            public int $polls = 0;

            public ConsumeBatchedCommand $command;

            public function get(): iterable
            {
                $this->polls++;
                if ($this->polls === 1) {
                    if ($this->partial) {
                        yield $this->message;
                    }
                    $this->command->handleSignal(15);
                }
                // Finite safety bound, not a timeout or a simulated collector result.
                if ($this->polls <= 5) {
                    throw new MessageDecodingFailedException('raw receiver rejected junk');
                }

                return [];
            }

            public function ack(Envelope $envelope): void
            {
                $this->acked = $envelope;
            }

            public function reject(Envelope $envelope): void {}
        };
        $processor = new class() implements BatchProcessor
        {
            public ?Envelope $processed = null;

            public function process(string $consumer, array $envelopes): array
            {
                $this->processed = $envelopes[0] ?? null;

                return array_map(static fn (): BatchDecision => BatchDecision::ack(), $envelopes);
            }
        };
        $receiver->command = new ConsumeBatchedCommand(
            new ServiceLocator(['events' => static fn () => $receiver]), $processor, 16, 1,
        );
        new CommandTester($receiver->command)->execute(['transport' => 'events', '--run-once' => true]);
        self::assertSame(1, $receiver->polls, 'After SIGTERM, a raw poison stream must not keep the collector polling.');
        self::assertSame($partial ? $message : null, $processor->processed);
        self::assertSame($partial ? $message : null, $receiver->acked);
    }
}
