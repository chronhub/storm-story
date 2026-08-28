<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Console;

use Closure;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;
use Psr\EventDispatcher\EventDispatcherInterface;
use RuntimeException;
use stdClass;
use Storm\Story\Console\ConsumeBatchedCommand;
use Storm\Story\Consume\BatchDecision;
use Storm\Story\Consume\BatchProcessor;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Event\WorkerMessageRetriedEvent;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\ErrorDetailsStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\SentToFailureTransportStamp;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;

/**
 * The receiver loop, the command itself; the transactional batch and poison isolation is unit-pinned
 * separately in `BatchConsumerTest`. Here the processor is a stub, the `BatchProcessor` seam, so the test
 * exercises only what the loop owns: collecting into batches, idle-flushing a partial, mapping decisions to
 * ack/reject, the `--run-once`/`--limit` exits, the unknown-transport guard, and the graceful stop on signal,
 * where the in-flight batch finishes before the loop breaks.
 */
final class ConsumeBatchedCommandTest extends TestCase
{
    #[Test]
    public function collects_a_partial_batch_acks_it_and_reports_the_count(): void
    {
        [$e1, $e2, $e3] = [$this->envelope(), $this->envelope(), $this->envelope()];
        $receiver = $this->receiver($e1, $e2, $e3);

        $tester = new CommandTester($this->command($receiver, $this->processor($this->ackAll())));
        $exit = $tester->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame([$e1, $e2, $e3], $receiver->acked);
        self::assertSame([], $receiver->rejected);
        self::assertStringContainsString('Consumed 3 message(s)', $this->display($tester));
    }

    #[Test]
    public function splits_the_queue_into_batches_of_the_size(): void
    {
        $receiver = $this->receiver($this->envelope(), $this->envelope(), $this->envelope());
        $processor = $this->processor($this->ackAll());

        new CommandTester($this->command($receiver, $processor, batch: 2))
            ->execute(['transport' => 'events', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame([2, 1], array_map(count(...), $processor->batches)); // full batch of 2, then the idle-flushed tail
    }

    #[Test]
    public function without_a_failure_transport_a_reject_falls_back_to_the_broker_with_a_warning(): void
    {
        [$e1, $e2, $e3] = [$this->envelope(), $this->envelope(), $this->envelope()];
        $receiver = $this->receiver($e1, $e2, $e3);
        $decide = static fn (array $envelopes): array => [BatchDecision::ack(), BatchDecision::reject(new RuntimeException('boom')), BatchDecision::ack()];

        $tester = new CommandTester($this->command($receiver, $this->processor($decide)));
        $tester->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame([$e1, $e3], $receiver->acked);
        self::assertSame([$e2], $receiver->rejected);
        // the fallback DESTROYS the message on a DLX-less queue; the loop must say so, not stay silent
        self::assertStringContainsString('No failure transport configured', $this->display($tester));
    }

    #[Test]
    public function a_reject_is_captured_to_the_failure_transport_and_the_delivery_acked(): void
    {
        [$e1, $e2] = [$this->envelope(), $this->envelope()];
        $receiver = $this->receiver($e1, $e2);
        $failure = $this->failureSender();
        $decide = static fn (array $envelopes): array => [BatchDecision::ack(), BatchDecision::reject(new RuntimeException('poison'))];

        new CommandTester($this->command($receiver, $this->processor($decide), failureSender: $failure))
            ->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame([$e1, $e2], $receiver->acked); // BOTH acked; the failure row replaces the poison's delivery
        self::assertSame([], $receiver->rejected);

        self::assertCount(1, $failure->sent);
        $captured = $failure->sent[0];
        self::assertSame('events', $captured->last(SentToFailureTransportStamp::class)?->getOriginalReceiverName());
        self::assertSame('poison', $captured->last(ErrorDetailsStamp::class)?->getExceptionMessage());
        // The retry count is RESET on the way in: messenger:failed:retry starts its own budget from
        // this row, and a count carried over from the origin transport would spend someone else's.
        self::assertSame(0, $captured->last(RedeliveryStamp::class)?->getRetryCount());
    }

    #[Test]
    #[Group('adversarial')]
    public function the_missing_failure_transport_is_warned_once_per_run_not_once_per_message(): void
    {
        // Without a failure transport a reject falls back to the broker, which DESTROYS the message
        // unless the queue declares a dead-letter exchange; the operator must see that. Once, though:
        // a warning repeated per message drowns the run it is meant to make readable, and a flag that
        // never latches is exactly what a per-message repeat looks like.
        [$e1, $e2] = [$this->envelope(), $this->envelope()];
        $receiver = $this->receiver($e1, $e2);
        $decide = static fn (array $envelopes): array => array_values(array_map(
            static fn (): BatchDecision => BatchDecision::reject(new RuntimeException('poison')),
            $envelopes,
        ));

        $tester = new CommandTester($this->command($receiver, $this->processor($decide), batch: 16));
        $tester->execute(['transport' => 'events', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame([$e1, $e2], $receiver->rejected); // both went to the broker
        self::assertSame(1, substr_count($this->display($tester), 'No failure transport configured'));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_generous_time_budget_does_not_stop_the_drain(): void
    {
        // The deadline is computed once, ahead of the loop, and compared on every turn. Both halves
        // are silent when wrong: a deadline built by SUBTRACTING the budget is already past, so the
        // worker exits having consumed nothing and a supervisor restarts it forever; a comparison
        // inverted the same way stops on the first turn. Neither shows up as an error anywhere.
        [$e1, $e2] = [$this->envelope(), $this->envelope()];
        $receiver = $this->receiver($e1, $e2);

        new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 1))
            ->execute(['transport' => 'events', '--idle-ms' => '1', '--run-once' => true, '--time-limit' => '60']);

        self::assertSame([$e1, $e2], $receiver->acked);
    }

    #[Test]
    public function an_omitted_limit_means_no_limit(): void
    {
        // The fallback of a budget is not the same kind of number as its floor: omitted, --limit must
        // mean unbounded, and a fallback of one would silently turn every supervisor worker into a
        // one-message-per-start worker.
        [$e1, $e2, $e3] = [$this->envelope(), $this->envelope(), $this->envelope()];
        $receiver = $this->receiver($e1, $e2, $e3);

        new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 1))
            ->execute(['transport' => 'events', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame([$e1, $e2, $e3], $receiver->acked);
    }

    #[Test]
    public function a_redeliver_is_sent_back_to_its_own_transport_delayed_and_the_original_acked(): void
    {
        // the seconds-scale wait: the broker holds the pause, the retry state rides ON the message:
        // DelayStamp for the hold, RedeliveryStamp incremented for the consumer's cap
        $sound = $this->envelope();
        $original = new Envelope(new stdClass, [new RedeliveryStamp(2)]);
        $second = $this->envelope();
        $receiver = $this->sendingReceiver($sound, $original, $second);
        $dispatcher = $this->dispatcherSpy();
        $decide = static fn (array $envelopes): array => [
            BatchDecision::ack(),
            BatchDecision::redeliver(new RuntimeException('not born yet'), 1500),
            BatchDecision::redeliver(new RuntimeException('not born yet'), 1500),
        ];

        $tester = new CommandTester($this->command($receiver, $this->processor($decide), dispatcher: $dispatcher));
        $tester->execute(
            ['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true],
            ['verbosity' => OutputInterface::VERBOSITY_VERBOSE],
        );

        self::assertCount(2, $receiver->sent);
        $delayed = $receiver->sent[0];
        self::assertSame(1500, $delayed->last(DelayStamp::class)?->getDelay());
        self::assertSame(3, $delayed->last(RedeliveryStamp::class)?->getRetryCount());

        self::assertSame([$sound, $original, $second], $receiver->acked); // the send precedes the ack; nothing rejected
        self::assertSame([], $receiver->rejected);

        // the Worker's retry contract, not its terminal one: a waiting leg must not settle as failed
        self::assertCount(2, $dispatcher->events);
        self::assertInstanceOf(WorkerMessageRetriedEvent::class, $dispatcher->events[0]);

        // the operator sees the wait, its cause, and the batch counters telling redelivered from rejected
        self::assertStringContainsString('redelivered stdClass in 1500ms — not born yet', $this->display($tester));
        self::assertStringContainsString('1 acked, 2 redelivered, 0 rejected', $this->display($tester));
    }

    #[Test]
    public function a_first_redelivery_without_a_prior_stamp_counts_from_one(): void
    {
        // the first round: no RedeliveryStamp on the envelope yet, the increment starts the count
        $receiver = $this->sendingReceiver($this->envelope());
        $decide = static fn (array $envelopes): array => [BatchDecision::redeliver(new RuntimeException('busy'), 1000)];

        new CommandTester($this->command($receiver, $this->processor($decide)))
            ->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame(1, $receiver->sent[0]->last(RedeliveryStamp::class)?->getRetryCount());
    }

    #[Test]
    #[Group('adversarial')]
    public function a_redeliver_on_a_sendless_transport_falls_back_to_the_capture(): void
    {
        // a receiver that cannot re-send cannot honor the delay; the failure must stay visible on the
        // failure transport, never a silent wedge nor a lost message
        $receiver = $this->receiver($this->envelope());
        $failure = $this->failureSender();
        $decide = static fn (array $envelopes): array => [BatchDecision::redeliver(new RuntimeException('busy'), 1000)];

        new CommandTester($this->command($receiver, $this->processor($decide), failureSender: $failure))
            ->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);

        self::assertCount(1, $failure->sent);
        self::assertSame('busy', $failure->sent[0]->last(ErrorDetailsStamp::class)?->getExceptionMessage());
    }

    #[Test]
    public function a_decision_count_mismatch_is_a_loud_bug_never_a_silent_default(): void
    {
        $receiver = $this->receiver($this->envelope(), $this->envelope());
        $short = static fn (array $envelopes): array => [BatchDecision::ack()]; // one decision for two messages

        $this->expectException(LogicException::class);
        $this->expectExceptionMessageIsOrContains('1 decision(s) for 2 message(s)');

        new CommandTester($this->command($receiver, $this->processor($short)))
            ->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);
    }

    #[Test]
    public function run_once_exits_when_the_queue_is_empty(): void
    {
        $receiver = $this->receiver();

        $tester = new CommandTester($this->command($receiver, $this->processor($this->ackAll())));
        $exit = $tester->execute(['transport' => 'events', '--run-once' => true]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame([], $receiver->acked);
        self::assertStringContainsString('Consumed 0 message(s)', $this->display($tester));
    }

    #[Test]
    public function limit_caps_the_message_count(): void
    {
        [$e1, $e2, $e3] = [$this->envelope(), $this->envelope(), $this->envelope()];
        $receiver = $this->receiver($e1, $e2, $e3);

        // one message per batch, size 1, so the limit bites at exactly 2; the third is never received
        new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 1))
            ->execute(['transport' => 'events', '--limit' => '2']);

        self::assertSame([$e1, $e2], $receiver->acked);
    }

    #[Test]
    #[Group('adversarial')]
    #[DataProvider('options_below_their_bound')]
    public function a_numeric_option_under_its_bound_refuses_and_names_the_bound(string $option, string $value, string $bound): void
    {
        // The refusal above proves a mistyped option is caught; it says nothing about WHERE each
        // option's floor sits, and the floors differ on purpose: a batch of zero and an idle window of
        // zero are both degenerate, one collecting nothing forever and one turning the poll into a
        // core-pinning busy loop, while a limit of zero is the legitimate word for "no limit".
        $receiver = $this->receiver($this->envelope());
        $tester = new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 1));

        $exit = $tester->execute(['transport' => 'events', $option => $value]);

        self::assertSame(Command::INVALID, $exit);
        self::assertStringContainsString(
            sprintf('Option %s expects an integer >= %s', $option, $bound),
            // the error block wraps at the terminal width, so the bound can land on the next line
            (string) preg_replace('/\s+/', ' ', $tester->getDisplay()),
        );
        self::assertSame([], $receiver->acked); // it refused before touching the transport
    }

    /**
     * @return iterable<string, array{string, string, string}>
     */
    public static function options_below_their_bound(): iterable
    {
        yield 'a batch of nothing' => ['--batch-size', '0', '1'];
        yield 'an idle window of nothing' => ['--idle-ms', '0', '1'];
        yield 'a negative limit' => ['--limit', '-1', '0'];
        yield 'a negative time limit' => ['--time-limit', '-1', '0'];
    }

    #[Test]
    public function zero_is_the_word_for_no_limit_on_both_budgets(): void
    {
        // The other side of those two floors: zero is IN range for the budgets, and it means unbounded.
        // Refusing it would break the supervisor worker, whose whole posture is to run without one.
        $receiver = $this->receiver($this->envelope(), $this->envelope());

        new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 1))
            ->execute(['transport' => 'events', '--limit' => '0', '--time-limit' => '0', '--idle-ms' => '1', '--run-once' => true]);

        self::assertCount(2, $receiver->acked); // neither budget stopped the drain
    }

    #[Test]
    public function the_clock_cap_is_finite_by_default_so_an_unbounded_consumer_has_to_be_asked_for(): void
    {
        // the longest-lived process of the surface, held to the same promise as its sisters: a bare
        // invocation recycles, and unlimited is a word an operator types. `--limit` keeps its own 0,
        // a message budget being a different question from a clock
        $definition = $this->command($this->receiver(), $this->processor($this->ackAll()))->getDefinition();

        self::assertSame('3600', $definition->getOption('time-limit')->getDefault());
        self::assertSame('0', $definition->getOption('limit')->getDefault());
    }

    #[Test]
    #[Group('adversarial')]
    public function a_mistyped_numeric_option_refuses_instead_of_widening_the_bound(): void
    {
        // a bare (int) cast turned --limit=abc into 0, which means UNLIMITED: the typo opened the
        // worker wider than asked; every numeric option now parses strictly and refuses loud
        $receiver = $this->receiver($this->envelope());
        $tester = new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 1));

        $exit = $tester->execute(['transport' => 'events', '--limit' => 'abc']);

        self::assertSame(Command::INVALID, $exit);
        // the offending value belongs in the message: an operator reading "expects an integer" alone
        // still has to guess WHICH of their options they mistyped, and what they typed into it
        self::assertMatchesRegularExpression(
            '/--limit expects an integer.*"abc"/',
            (string) preg_replace('/\s+/', ' ', $tester->getDisplay()),
        );
        self::assertSame([], $receiver->acked);
    }

    #[Test]
    #[Group('adversarial')]
    public function an_eager_receiver_yielding_past_the_batch_size_loses_nothing(): void
    {
        // the fetch-size is a best-effort hint by the Receiver contract: a receiver may
        // over-deliver, and an envelope it yielded is OWNED from that moment; breaking out
        // mid-iteration would leave it unhandled and unacked, invisible on an explicit-ack
        // broker until the connection dies; the batch overshoots its size instead
        [$e1, $e2, $e3] = [$this->envelope(), $this->envelope(), $this->envelope()];

        $receiver = new class($e1, $e2, $e3) implements ReceiverInterface
        {
            /** @var list<Envelope> */
            public array $acked = [];

            /** @var list<Envelope> */
            public array $rejected = [];

            /** @var list<Envelope> */
            private array $queue;

            public function __construct(Envelope ...$envelopes)
            {
                $this->queue = array_values($envelopes);
            }

            public function get(): iterable
            {
                $all = $this->queue; // the whole backlog in ONE poll, whatever the hint asked
                $this->queue = [];

                return $all;
            }

            public function ack(Envelope $envelope): void
            {
                $this->acked[] = $envelope;
            }

            public function reject(Envelope $envelope): void
            {
                $this->rejected[] = $envelope;
            }
        };

        new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 1))
            ->execute(['transport' => 'events', '--idle-ms' => '1', '--run-once' => true]);

        self::assertCount(3, $receiver->acked, 'every yielded envelope is handled and acked, none abandoned');
    }

    #[Test]
    #[Group('adversarial')]
    public function a_limit_smaller_than_the_batch_size_is_exact(): void
    {
        // the finding this pins: the loop collected a FULL batch then counted it after;
        // --limit=2 --batch-size=16 received and acked 16, an overshoot of batch-size − 1
        $envelopes = array_map(fn (int $i): Envelope => $this->envelope(), range(1, 16));
        $receiver = $this->receiver(...$envelopes);

        new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 16))
            ->execute(['transport' => 'events', '--limit' => '2', '--idle-ms' => '1']);

        self::assertCount(2, $receiver->acked); // exactly the limit, never the batch size
    }

    #[Test]
    public function a_limit_that_is_not_a_multiple_of_the_batch_size_is_exact(): void
    {
        $receiver = $this->receiver($this->envelope(), $this->envelope(), $this->envelope(), $this->envelope());
        $processor = $this->processor($this->ackAll());

        new CommandTester($this->command($receiver, $processor, batch: 2))
            ->execute(['transport' => 'events', '--limit' => '3', '--idle-ms' => '1']);

        self::assertSame([2, 1], array_map(count(...), $processor->batches)); // the tail collect asks for the remainder
    }

    #[Test]
    public function a_capture_emits_the_terminal_worker_failed_event(): void
    {
        // the Worker contract, reimbursed: terminal-failure listeners, the saga settle and telemetry, must
        // see the same event on this loop as on messenger:consume
        $receiver = $this->receiver($this->envelope());
        $failure = $this->failureSender();
        $dispatcher = $this->dispatcherSpy();
        $decide = static fn (array $envelopes): array => [BatchDecision::reject(new RuntimeException('poison'))];

        new CommandTester($this->command($receiver, $this->processor($decide), failureSender: $failure, dispatcher: $dispatcher))
            ->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);

        self::assertCount(1, $dispatcher->events);
        $event = $dispatcher->events[0];
        self::assertInstanceOf(WorkerMessageFailedEvent::class, $event);
        self::assertSame('events', $event->getReceiverName());
        self::assertFalse($event->willRetry(), 'terminal: SagaCommandFailureListener acts only on a never-retried failure');
        // wrapped unrecoverable so Symfony's retry listener stands down; a re-send here would duplicate
        self::assertInstanceOf(UnrecoverableMessageHandlingException::class, $event->getThrowable());
        self::assertSame('poison', $event->getThrowable()->getPrevious()?->getMessage());
        // the CAPTURED envelope rides the event: its failure stamp names this receiver, so Symfony's
        // failure-transport listener skips its own re-capture
        self::assertSame('events', $event->getEnvelope()->last(SentToFailureTransportStamp::class)?->getOriginalReceiverName());
    }

    #[Test]
    public function the_broker_reject_fallback_still_emits_the_terminal_event(): void
    {
        // no failure transport: the reject falls back to the broker, but the terminal contract still
        // fires; where a framework-level failure transport exists, Symfony's own listener captures there
        $envelope = $this->envelope();
        $receiver = $this->receiver($envelope);
        $dispatcher = $this->dispatcherSpy();
        $decide = static fn (array $envelopes): array => [BatchDecision::reject(new RuntimeException('poison'))];

        new CommandTester($this->command($receiver, $this->processor($decide), dispatcher: $dispatcher))
            ->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame([$envelope], $receiver->rejected);
        self::assertCount(1, $dispatcher->events);
        self::assertInstanceOf(WorkerMessageFailedEvent::class, $dispatcher->events[0]);
    }

    #[Test]
    public function without_run_once_it_stays_up_and_polls_through_idle_cycles_until_a_message_arrives(): void
    {
        // the default mode, no --run-once, is the actual supervisor-worker shape; every other test in
        // this file pins --run-once, so this loop's idle usleep+continue, taken when a poll finds nothing,
        // has never run for real. Simulate a queue that is empty for two polls before the message lands,
        // proving the loop truly resumes instead of exiting or stalling after the first empty batch.
        $envelope = $this->envelope();
        $receiver = new class($envelope) implements ReceiverInterface
        {
            /** @var list<Envelope> */
            public array $acked = [];

            /** @var list<Envelope> */
            public array $rejected = [];

            private int $calls = 0;

            public function __construct(private readonly Envelope $envelope) {}

            public function get(): iterable
            {
                $this->calls++;

                return $this->calls >= 3 ? [$this->envelope] : []; // idle on the first 2 polls, then arrives
            }

            public function ack(Envelope $envelope): void
            {
                $this->acked[] = $envelope;
            }

            public function reject(Envelope $envelope): void
            {
                $this->rejected[] = $envelope;
            }
        };

        // no --run-once: --limit=1 bounds the otherwise-infinite poll once the delayed message is consumed
        $tester = new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 1, idle: 1));
        $exit = $tester->execute(['transport' => 'events', '--limit' => '1']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame([$envelope], $receiver->acked);
        self::assertStringContainsString('Consumed 1 message(s)', $this->display($tester));
    }

    #[Test]
    #[Group('slow')]
    public function an_idle_supervisor_paces_its_polls_instead_of_spinning_on_the_transport(): void
    {
        // both pauses are wall-clock effects, and elapsed time cannot hold either: a run ends when the
        // transport speaks, which bounds the paced run and the spinning one alike. Their ABSENCE is
        // countable though, and hugely so, since a spin re-polls as fast as the CPU allows. A receiver
        // silent for a fixed stretch turns each pause into a poll count, and the bound is one-sided the
        // safe way: a slower machine polls LESS, never more.
        //
        // An empty batch returns from collect at once, so the OUTER pause is the only thing pacing an
        // idle supervisor: without it this loop polls a quiet queue flat out.
        $envelope = $this->envelope();
        $receiver = $this->silentFor(0.03, $envelope);

        $tester = new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 1, idle: 10));
        $exit = $tester->execute(['transport' => 'events', '--limit' => '1']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame([$envelope], $receiver->acked);
        self::assertLessThan(500, $receiver->polls, 'an idle poll must pace itself, never spin on the transport');
    }

    #[Test]
    public function a_partial_batch_paces_its_polls_while_it_waits_for_the_rest(): void
    {
        // the tight pause is the other half: a batch with room left re-polls until the idle window
        // closes, and only this pause keeps that wait from being a spin. Same countable instrument,
        // with a first envelope in hand so the loop is holding a PARTIAL batch, which is the only
        // state that reaches it.
        [$first, $second] = [$this->envelope(), $this->envelope()];
        $receiver = $this->silentFor(0.03, $second, $first);

        // idle 200 keeps the window open across the silence, so the partial batch waits rather than
        // flushing early, and the tight pause alone is under test
        $tester = new CommandTester($this->command($receiver, $this->processor($this->ackAll()), batch: 2, idle: 200));
        $exit = $tester->execute(['transport' => 'events', '--limit' => '2']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame([$first, $second], $receiver->acked);
        self::assertLessThan(500, $receiver->polls, 'a waiting batch must pace its polls, never spin on the transport');
    }

    #[Test]
    public function stops_gracefully_on_signal_after_finishing_the_in_flight_batch(): void
    {
        [$e1, $e2, $e3] = [$this->envelope(), $this->envelope(), $this->envelope()];
        $receiver = $this->receiver($e1, $e2, $e3);

        $holder = new class()
        {
            public ?ConsumeBatchedCommand $command = null;
        };
        $processor = $this->processor(function (array $envelopes) use ($holder): array {
            $holder->command?->handleSignal(defined('SIGTERM') ? SIGTERM : 15); // a signal lands while the first batch runs

            return array_fill(0, count($envelopes), BatchDecision::ack());
        });
        $holder->command = $this->command($receiver, $processor, batch: 1);

        $tester = new CommandTester($holder->command);
        $exit = $tester->execute(['transport' => 'events']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame([$e1], $receiver->acked); // first batch committed + acked; e2/e3 never received
        self::assertStringContainsString('stopped gracefully on signal', $this->display($tester));
    }

    #[Test]
    public function handle_signal_defers_the_exit_and_term_int_are_subscribed(): void
    {
        $command = $this->command($this->receiver(), $this->processor($this->ackAll()));

        // returning false, not an exit code, is the contract: don't exit now, let the batch finish first
        self::assertFalse($command->handleSignal(defined('SIGTERM') ? SIGTERM : 15));

        // both, and the name says both: a supervisor sends TERM, an operator at a terminal sends INT,
        // and a worker that subscribes to one of the two dies mid-batch under the other
        if (defined('SIGTERM')) {
            self::assertContains(SIGTERM, $command->getSubscribedSignals());
        }

        if (defined('SIGINT')) {
            self::assertContains(SIGINT, $command->getSubscribedSignals());
        }
    }

    #[Test]
    public function an_unknown_transport_is_invalid(): void
    {
        $tester = new CommandTester($this->command(null, $this->processor($this->ackAll())));
        $exit = $tester->execute(['transport' => 'nope', '--run-once' => true]);

        self::assertSame(Command::INVALID, $exit);
        self::assertStringContainsString('Unknown transport', $this->display($tester));
    }

    #[Test]
    public function a_service_present_under_the_name_but_not_a_receiver_is_invalid(): void
    {
        // the realistic ops mistake: the id resolves, unlike an_unknown_transport_is_invalid where
        // it does not resolve at all, but to a mis-wired service of the wrong kind, e.g. a sender
        // registered under the receiver's name. ContainerInterface::get() is untyped, returning mixed, so
        // only the instanceof guard in receiver() catches this before the loop ever polls it.
        $locator = new readonly class() implements ContainerInterface
        {
            public function get(string $id): stdClass
            {
                return new stdClass; // present, but not a ReceiverInterface
            }

            public function has(string $id): bool
            {
                return true;
            }
        };

        $tester = new CommandTester(new ConsumeBatchedCommand($locator, $this->processor($this->ackAll()), 10, 1));
        $exit = $tester->execute(['transport' => 'events', '--run-once' => true]);

        self::assertSame(Command::INVALID, $exit);
        self::assertStringContainsString('the transport is not a receiver', $this->display($tester));
    }

    #[Test]
    #[Group('adversarial')]
    public function survives_an_undecodable_message_and_keeps_consuming(): void
    {
        // Worker parity: the Worker catches a decode failure at get() and keeps going; one foreign
        // junk message must not kill the loop. The receiver has already rejected the delivery; the
        // sound messages around it are consumed normally.
        [$e1, $e2] = [$this->envelope(), $this->envelope()];
        $receiver = $this->decodeFailingReceiver($e1, 'THROW', $e2);

        $tester = new CommandTester($this->command($receiver, $this->processor($this->ackAll())));
        $exit = $tester->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame([$e1, $e2], $receiver->acked);
        self::assertStringContainsString('Undecodable message dropped', $this->display($tester));
        self::assertStringContainsString('junk body', $this->display($tester));
        self::assertStringContainsString('Consumed 2 message(s)', $this->display($tester));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_junk_only_queue_drains_to_empty_under_run_once(): void
    {
        $receiver = $this->decodeFailingReceiver('THROW', 'THROW');

        $tester = new CommandTester($this->command($receiver, $this->processor($this->ackAll())));
        $exit = $tester->execute(['transport' => 'events', '--batch-size' => '16', '--idle-ms' => '1', '--run-once' => true]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame([], $receiver->acked);
        self::assertStringContainsString('Consumed 0 message(s)', $this->display($tester));
    }

    /**
     * A queue mixing sound envelopes with 'THROW' markers: the marker polls raise the receiver's own
     * decode-failure, after its transport-side reject, exactly like a real receiver on foreign junk.
     *
     * @return ReceiverInterface&object{acked: list<Envelope>, rejected: list<Envelope>}
     */
    private function decodeFailingReceiver(Envelope|string ...$items): ReceiverInterface
    {
        return new class(...$items) implements ReceiverInterface
        {
            /** @var list<Envelope> */
            public array $acked = [];

            /** @var list<Envelope> */
            public array $rejected = [];

            /** @var list<Envelope|string> */
            private array $queue;

            public function __construct(Envelope|string ...$items)
            {
                $this->queue = array_values($items);
            }

            public function get(): iterable
            {
                $next = array_shift($this->queue);

                if ($next === null) {
                    return [];
                }

                if (is_string($next)) {
                    throw new MessageDecodingFailedException('junk body');
                }

                return [$next];
            }

            public function ack(Envelope $envelope): void
            {
                $this->acked[] = $envelope;
            }

            public function reject(Envelope $envelope): void
            {
                $this->rejected[] = $envelope;
            }
        };
    }

    private function envelope(): Envelope
    {
        return new Envelope(new stdClass);
    }

    /**
     * @return Closure(list<Envelope>): list<BatchDecision>
     */
    private function ackAll(): Closure
    {
        return static fn (array $envelopes): array => array_fill(0, count($envelopes), BatchDecision::ack());
    }

    /**
     * @return ReceiverInterface&object{acked: list<Envelope>, rejected: list<Envelope>}
     */
    private function receiver(Envelope ...$envelopes): ReceiverInterface
    {
        return new class(...$envelopes) implements ReceiverInterface
        {
            /** @var list<Envelope> */
            public array $acked = [];

            /** @var list<Envelope> */
            public array $rejected = [];

            /** @var list<Envelope> */
            private array $queue;

            public function __construct(Envelope ...$envelopes)
            {
                $this->queue = array_values($envelopes);
            }

            public function get(): iterable
            {
                $next = array_shift($this->queue); // at most one per poll, like AMQP basic_get

                return $next === null ? [] : [$next];
            }

            public function ack(Envelope $envelope): void
            {
                $this->acked[] = $envelope;
            }

            public function reject(Envelope $envelope): void
            {
                $this->rejected[] = $envelope;
            }
        };
    }

    /**
     * A transport that counts its polls and goes quiet for a stretch, the shape a pacing bound needs:
     * the optional envelope lands on the first poll, then nothing until $silence seconds have passed,
     * then the delayed one.
     *
     * @return ReceiverInterface&object{acked: list<Envelope>, rejected: list<Envelope>, polls: int}
     */
    private function silentFor(float $silence, Envelope $delayed, ?Envelope $immediate = null): ReceiverInterface
    {
        return new class($silence, $delayed, $immediate) implements ReceiverInterface
        {
            /** @var list<Envelope> */
            public array $acked = [];

            /** @var list<Envelope> */
            public array $rejected = [];

            public int $polls = 0;

            private ?float $firstPollAt = null;

            public function __construct(
                private readonly float $silence,
                private readonly Envelope $delayed,
                private ?Envelope $immediate,
            ) {}

            public function get(): iterable
            {
                $this->polls++;
                $this->firstPollAt ??= microtime(true);

                if ($this->immediate !== null) {
                    $handed = $this->immediate;
                    $this->immediate = null;

                    return [$handed];
                }

                return microtime(true) - $this->firstPollAt >= $this->silence ? [$this->delayed] : [];
            }

            public function ack(Envelope $envelope): void
            {
                $this->acked[] = $envelope;
            }

            public function reject(Envelope $envelope): void
            {
                $this->rejected[] = $envelope;
            }
        };
    }

    /**
     * A transport that is both ends, like every AMQP transport: the redeliver path re-sends on the same
     * service it received from.
     *
     * @return ReceiverInterface&SenderInterface&object{acked: list<Envelope>, rejected: list<Envelope>, sent: list<Envelope>}
     */
    private function sendingReceiver(Envelope ...$envelopes): ReceiverInterface&SenderInterface
    {
        return new class(...$envelopes) implements ReceiverInterface, SenderInterface
        {
            /** @var list<Envelope> */
            public array $acked = [];

            /** @var list<Envelope> */
            public array $rejected = [];

            /** @var list<Envelope> */
            public array $sent = [];

            /** @var list<Envelope> */
            private array $queue;

            public function __construct(Envelope ...$envelopes)
            {
                $this->queue = array_values($envelopes);
            }

            public function get(): iterable
            {
                $next = array_shift($this->queue);

                return $next === null ? [] : [$next];
            }

            public function ack(Envelope $envelope): void
            {
                $this->acked[] = $envelope;
            }

            public function reject(Envelope $envelope): void
            {
                $this->rejected[] = $envelope;
            }

            public function send(Envelope $envelope): Envelope
            {
                $this->sent[] = $envelope;

                return $envelope;
            }
        };
    }

    /**
     * @param  Closure(list<Envelope>): list<BatchDecision>  $decide
     * @return BatchProcessor&object{batches: list<list<Envelope>>}
     */
    private function processor(Closure $decide): BatchProcessor
    {
        return new class($decide) implements BatchProcessor
        {
            /** @var list<list<Envelope>> */
            public array $batches = [];

            /** @param  Closure(list<Envelope>): list<BatchDecision>  $decide */
            public function __construct(private readonly Closure $decide) {}

            public function process(string $consumer, array $envelopes): array
            {
                $this->batches[] = $envelopes;

                return ($this->decide)($envelopes);
            }
        };
    }

    /**
     * @return SenderInterface&object{sent: list<Envelope>}
     */
    private function failureSender(): SenderInterface
    {
        return new class() implements SenderInterface
        {
            /** @var list<Envelope> */
            public array $sent = [];

            public function send(Envelope $envelope): Envelope
            {
                $this->sent[] = $envelope;

                return $envelope;
            }
        };
    }

    /**
     * @return EventDispatcherInterface&object{events: list<object>}
     */
    private function dispatcherSpy(): EventDispatcherInterface
    {
        return new class() implements EventDispatcherInterface
        {
            /** @var list<object> */
            public array $events = [];

            public function dispatch(object $event): object
            {
                $this->events[] = $event;

                return $event;
            }
        };
    }

    private function command(?ReceiverInterface $receiver, BatchProcessor $processor, int $batch = 10, int $idle = 1, ?SenderInterface $failureSender = null, ?EventDispatcherInterface $dispatcher = null): ConsumeBatchedCommand
    {
        $services = $receiver === null ? [] : ['events' => $receiver];

        $locator = new readonly class($services) implements ContainerInterface
        {
            /** @param  array<string, ReceiverInterface>  $services */
            public function __construct(private array $services) {}

            public function get(string $id): ReceiverInterface
            {
                if (! array_key_exists($id, $this->services)) {
                    throw new RuntimeException(sprintf('No receiver "%s".', $id));
                }

                return $this->services[$id];
            }

            public function has(string $id): bool
            {
                return array_key_exists($id, $this->services);
            }
        };

        return new ConsumeBatchedCommand($locator, $processor, $batch, $idle, $failureSender, $dispatcher);
    }

    private function display(CommandTester $tester): string
    {
        return preg_replace('/\s+/', ' ', $tester->getDisplay()) ?? ''; // collapse box padding/wrapping
    }
}
