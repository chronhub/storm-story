<?php

declare(strict_types=1);

namespace Storm\Story\Console;

use InvalidArgumentException;
use LogicException;
use Override;
use Psr\Container\ContainerInterface;
use Psr\EventDispatcher\EventDispatcherInterface;
use RuntimeException;
use Storm\Story\Consume\BatchDecision;
use Storm\Story\Consume\BatchProcessor;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Event\WorkerMessageRetriedEvent;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\UnrecoverableExceptionInterface;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\ErrorDetailsStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\SentToFailureTransportStamp;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Throwable;

/**
 * Batched consume of one transport, amortizing the per-event commit: pulls up to `--batch-size` messages,
 * runs them through the inbox in one transaction and one COMMIT, then acks the batch.
 *
 * A partial batch flushes after `--idle-ms` so a quiet queue does not stall. This command is the receiver
 * loop; the processing and poison isolation live in `BatchConsumer`, which is unit-pinned. By default it
 * stays up and polls as a supervisor worker, recycling on `--time-limit`; `--run-once` drains the queue to
 * empty then exits for smoke or bounded runs, and `--limit` caps the message count. It stops gracefully on
 * SIGTERM/SIGINT: the in-flight batch finishes its single COMMIT and acks before the loop exits, so nothing
 * is lost or half-acked.
 *
 * Where it deliberately diverges from Symfony's `Worker`, these are choices, not gaps:
 *
 * - Memory hygiene is time-based, not memory-based: `--time-limit` plus a supervisor relaunch recycles the
 *   process, the same idiom as the projection daemon `storm:projection:run`, so there is no `--memory-limit`.
 *
 * - No retry-with-backoff loop: transient errors are already retried by Storm's internal DBAL retry layer
 *   via `RetryableException`, and a deterministic poison is isolated per-message by `BatchConsumer`. A
 *   Symfony retry strategy would only re-run an event that fails the same way, or double-retry a transient
 *   already handled one layer down. The one exception is a failure DECLARED transient, Messenger's
 *   `RecoverableExceptionInterface`: outliving the consumer's in-process second chance, it comes back as a
 *   redeliver decision and this loop sends it back to its own transport delayed, `RedeliveryStamp`-counted
 *   and capped, the seconds-scale wait a consumer-safe handler throws, a saga child's birth race, that no
 *   in-process pause can absorb.
 *
 * A rejected message is captured, not destroyed. The Worker's failure-transport and terminal-failure
 * machinery does not run in this loop that replaces it, so the loop reimburses both: a reject decision is
 * sent to the configured failure transport `storm.inbox.failure_transport`, and the capture emits the same
 * never-retried `WorkerMessageFailedEvent` the Worker path emits, so the listeners that settle on a
 * terminal failure, `SagaCommandFailureListener` and telemetry, see it identically. With NO failure
 * transport configured, the fallback is the broker `reject`; that is a dead-letter exchange hand-off only
 * if the queue declares one via `x-dead-letter-exchange`, and on a bare queue it destroys the message,
 * warned once per run. The stamps, the event ordering and the wrapping are detailed on `capture()` and
 * `emitTerminalFailure()`. The Worker's tolerance of an UNDECODABLE delivery is reimbursed too, on
 * two layers. Under the framework's signing wrapper, a decode failure never throws: the wrapper
 * returns an envelope WRAPPING the `MessageDecodingFailedException`, raw body inside, which carries
 * no message id, so the id-less guard captures it to the failure transport, a de facto quarantine,
 * nothing destroyed, inspectable via `messenger:failed:show`. On a RAW receiver whose serializer
 * throws, the loop catches the `MessageDecodingFailedException` and keeps consuming, warned per
 * message; that transport has already rejected the delivery, which on a bare queue is the same
 * destruction caveat as the fallback above.
 *
 * What this loop still does NOT reproduce of the Worker: `from_transport` handler filtering, with no
 * `ReceivedStamp` by design since it would re-arm the consume-path validation skip, and the
 * received/handled lifecycle events; do not batch-consume a transport whose handlers rely on either.
 *
 * Examples:
 *
 * ```bash
 * bin/console storm:bus:consume-batched storm_events --batch-size=16 --time-limit=3600
 * bin/console storm:bus:consume-batched storm_events --batch-size=16 --run-once --limit=1000
 * ```
 *
 * @see BatchProcessor the batch-to-decisions seam this loop drives
 * @see \Symfony\Component\Console\Command\SignalableCommandInterface the graceful-stop contract
 */
#[AsCommand(
    name: 'storm:bus:consume-batched',
    description: 'Consume one transport in batches: N messages per inbox transaction / COMMIT.',
)]
final class ConsumeBatchedCommand extends Command
{
    private bool $stopRequested = false;

    /** One warning per run when a reject falls back to the broker because no failure transport is configured. */
    private bool $warnedBrokerReject = false;

    public function __construct(
        private readonly ContainerInterface $receivers,
        private readonly BatchProcessor $consumer,
        private readonly int $defaultBatchSize,
        private readonly int $defaultIdleMs,
        /**
         * The capture destination for a reject decision, the `storm.inbox.failure_transport` sender.
         * Null = no capture: the fallback broker reject DESTROYS the message unless the queue declares
         * a dead-letter exchange.
         */
        private readonly ?SenderInterface $failureSender = null,
        /**
         * Emits the terminal `WorkerMessageFailedEvent` on each capture, so terminal-failure listeners,
         * the saga settle and telemetry, see the Worker contract on this loop too. Null = no emission.
         */
        private readonly ?EventDispatcherInterface $dispatcher = null,
    ) {
        parent::__construct();
    }

    #[Override]
    protected function configure(): void
    {
        $this->addArgument('transport', InputArgument::REQUIRED, 'The transport (receiver) name to consume')
            ->addOption('batch-size', null, InputOption::VALUE_REQUIRED, 'Messages per batch transaction')
            ->addOption('idle-ms', null, InputOption::VALUE_REQUIRED, 'Flush a partial batch after this idle window (ms)')
            ->addOption('limit', null, InputOption::VALUE_REQUIRED, 'Stop after this many messages (0 = unlimited)', '0')
            ->addOption('time-limit', null, InputOption::VALUE_REQUIRED, 'Stop after this many seconds (default 3600 — a supervisor relaunches for memory hygiene); 0 = unlimited, explicit', '3600')
            ->addOption('run-once', null, InputOption::VALUE_NONE, 'Drain the queue to empty then exit (smoke/bounded), instead of staying up and polling');
    }

    /**
     * {@inheritDoc}
     *
     * @return list<int>
     */
    #[Override]
    public function getSubscribedSignals(): array
    {
        return array_values(array_filter(
            [defined('SIGTERM') ? SIGTERM : null, defined('SIGINT') ? SIGINT : null],
            static fn (?int $signal): bool => $signal !== null,
        ));
    }

    #[Override]
    public function handleSignal(int $signal, int|false $previousExitCode = false): int|false
    {
        $this->stopRequested = true;

        return false; // don't exit now; let the in-flight batch finish, one COMMIT + acks, then stop cleanly
    }

    #[Override]
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $transport = (string) $input->getArgument('transport');

        try {
            $batchSize = $this->boundedOption($input, 'batch-size', min: 1, fallback: $this->defaultBatchSize);
            $idleMs = $this->boundedOption($input, 'idle-ms', min: 1, fallback: $this->defaultIdleMs);
            // These two declare their own default on the option, '0' for limit and '3600' for the
            // clock cap, so the fallback below is the unreachable half of the pair; batch-size and
            // idle-ms declare none, and theirs is the live one, carrying the values the bundle
            // injected. Kept symmetrical rather than split into two call shapes, the argument being
            // required either way.
            $limit = $this->boundedOption($input, 'limit', min: 0, fallback: 0);
            $timeLimit = $this->boundedOption($input, 'time-limit', min: 0, fallback: 0);
        } catch (InvalidArgumentException $e) {
            // a mistyped bound is a malformed invocation: the operator gets the house error line and
            // the exit its eleven sisters give, never an uncaught frame that exits 1 like a runtime fault
            $io->error($e->getMessage());

            return Command::INVALID;
        }

        $runOnce = (bool) $input->getOption('run-once');

        try {
            $receiver = $this->receiver($transport);
        } catch (Throwable $e) {
            $io->error(sprintf('Unknown transport "%s": %s', $transport, $e->getMessage()));

            return Command::INVALID;
        }

        $processed = 0;
        $deadline = $timeLimit > 0 ? microtime(true) + $timeLimit : null;

        while (true) {
            if ($this->stopRequested) {
                break; // SIGTERM/SIGINT: the previous batch already committed + acked, stop between batches
            }

            if ($limit > 0 && $processed >= $limit) {
                break;
            }

            if ($deadline !== null && microtime(true) >= $deadline) {
                break;
            }

            // an active --limit caps the collect size, so the loop never receives, nor acks, past it:
            // with limit 2 and batch-size 16, the last collect asks for 2, not 16
            $size = $limit > 0 ? min($batchSize, $limit - $processed) : $batchSize;

            $batch = $this->collect($io, $receiver, $size, $idleMs);

            if ($batch === []) {
                if ($runOnce) {
                    break; // drained to empty
                }

                $this->pauseIdlePoll($idleMs); // idle: stay up and poll, as a supervisor worker

                continue;
            }

            $decisions = $this->consumer->process($transport, $batch);

            if (count($decisions) !== count($batch)) {
                // a structural bug in the processor. Loud, never a silent default in EITHER direction:
                // a default-ack re-runs side effects, a default-reject destroys/captures sound messages
                throw new LogicException(sprintf(
                    'BatchProcessor returned %d decision(s) for %d message(s).',
                    count($decisions),
                    count($batch),
                ));
            }

            foreach ($batch as $i => $envelope) {
                match (true) {
                    $decisions[$i]->ack => $receiver->ack($envelope),
                    $decisions[$i]->redeliverDelayMs !== null => $this->redeliver($io, $receiver, $transport, $envelope, $decisions[$i]),
                    default => $this->capture($io, $receiver, $transport, $envelope, $decisions[$i]),
                };
            }

            $processed += count($batch);

            if ($io->isVerbose()) {
                $acked = count(array_filter($decisions, static fn (BatchDecision $d): bool => $d->ack));
                $redelivered = count(array_filter($decisions, static fn (BatchDecision $d): bool => $d->redeliverDelayMs !== null));
                $batch
                    |> count(...)
                    |> (static fn ($x) => sprintf('  batch of %d — %d acked, %d redelivered, %d rejected (%d total)', $x, $acked, $redelivered, $x - $acked - $redelivered, $processed))
                    |> $io->writeln(...);
            }
        }

        $io->success(sprintf(
            'Consumed %d message(s) from "%s" in batches of up to %d%s.',
            $processed,
            $transport,
            $batchSize,
            $this->stopRequested ? ' (stopped gracefully on signal)' : '',
        ));

        return Command::SUCCESS;
    }

    /**
     * A redeliver decision sends the message back to its OWN transport, delayed: a declared-recoverable
     * failure means the message is sound and its wait is in flight, so the broker holds the pause a
     * process cannot sit on. The retry state rides on the message, a `RedeliveryStamp` the consumer reads
     * against its cap, so the loop stays stateless; the send precedes the ack, at-least-once preserved,
     * and a crash between the two leaves a duplicate the inbox dedup absorbs by construction. The Worker's
     * retry contract is reimbursed with a `WorkerMessageRetriedEvent`, never the terminal failed event, so
     * `SagaCommandFailureListener` does not settle a leg that is merely waiting. A transport that cannot
     * send, none of ours, falls back to the capture, keeping the failure visible over silently wedging.
     */
    private function redeliver(SymfonyStyle $io, ReceiverInterface $receiver, string $transport, Envelope $envelope, BatchDecision $decision): void
    {
        if (! $receiver instanceof SenderInterface) {
            $this->capture($io, $receiver, $transport, $envelope, $decision);

            return;
        }

        // the `??` fallbacks here and in the verbose line below narrow nullables for the type system
        // only: the decision loop routes here exactly when redeliverDelayMs is non-null, and both
        // decision factories require the cause, so the fallbacks are unreachable by construction
        $delayed = $envelope->with(
            new DelayStamp($decision->redeliverDelayMs ?? 0),
            new RedeliveryStamp(($envelope->last(RedeliveryStamp::class)?->getRetryCount() ?? 0) + 1),
        );

        $receiver->send($delayed);
        $this->dispatcher?->dispatch(new WorkerMessageRetriedEvent($delayed, $transport));
        $receiver->ack($envelope);

        if ($io->isVerbose()) {
            $io->writeln(sprintf(
                '  redelivered %s in %dms — %s',
                $envelope->getMessage()::class,
                $decision->redeliverDelayMs ?? 0,
                $decision->error?->getMessage() ?? 'recoverable',
            ));
        }
    }

    /**
     * A reject decision leaves the loop holding the failure, not the broker a corpse: sends the envelope to
     * the failure transport with the stamps the `messenger:failed:*` tooling expects, the origin transport
     * via `SentToFailureTransportStamp`, a redelivery marker, and the captured cause via `ErrorDetailsStamp`,
     * emits the terminal `WorkerMessageFailedEvent`, then ACKs so the failure row replaces the delivery. The
     * Worker order holds: the event fires before the delivery leaves the broker. Transport-internal stamps
     * are non-sendable and stripped by the sender. A failure-transport outage propagates: the delivery stays
     * un-acked and comes back, at-least-once preserved; the same holds for a terminal listener crashing
     * before the ack, an at-least-once double capture that is the Worker's own semantics. Without a
     * configured failure transport, falls back to the broker `reject`, still preceded by the terminal event;
     * this is a DLX hand-off if the queue declares one and a destruction otherwise, warned once per run.
     * Where a framework-level failure transport exists for this receiver, Symfony's own listener captures
     * there.
     *
     * @throws Throwable when the failure transport itself rejects the capture, for instance a broker or DB
     *                   outage, or a terminal-failure listener fails before the ack
     */
    private function capture(SymfonyStyle $io, ReceiverInterface $receiver, string $transport, Envelope $envelope, BatchDecision $decision): void
    {
        if ($this->failureSender === null) {
            if (! $this->warnedBrokerReject) {
                $this->warnedBrokerReject = true;
                $io->warning('No failure transport configured (storm.inbox.failure_transport): a rejected message falls back to the broker reject — DESTROYED unless the queue declares a dead-letter exchange.');
            }

            $this->emitTerminalFailure($transport, $envelope, $decision);
            $receiver->reject($envelope);

            return;
        }

        $stamps = [new SentToFailureTransportStamp($transport), new RedeliveryStamp(0)];

        if ($decision->error !== null) {
            $stamps[] = ErrorDetailsStamp::create($decision->error);
        }

        $captured = $envelope->with(...$stamps);

        $this->failureSender->send($captured);
        $this->emitTerminalFailure($transport, $captured, $decision);
        $receiver->ack($envelope);
    }

    /**
     * The Worker's terminal contract, reimbursed like the failure transport is: a `WorkerMessageFailedEvent`
     * that will never retry, carrying the captured envelope. The error is wrapped unrecoverable so Symfony's
     * retry listener stands down, since the batch spent its attempts and a re-send here would duplicate the
     * message, and the captured `SentToFailureTransportStamp` naming this receiver makes Symfony's
     * failure-transport listener skip its own re-capture. What remains listening is exactly what must: the
     * terminal-failure listeners, `SagaCommandFailureListener` first, seeing the same event as on the Worker
     * path.
     */
    private function emitTerminalFailure(string $transport, Envelope $envelope, BatchDecision $decision): void
    {
        if ($this->dispatcher === null) {
            return;
        }

        $error = $decision->error ?? new RuntimeException('Rejected by the batch processor without a cause.');

        if (! $error instanceof UnrecoverableExceptionInterface) {
            $error = new UnrecoverableMessageHandlingException($error->getMessage(), previous: $error);
        }

        $this->dispatcher->dispatch(new WorkerMessageFailedEvent($envelope, $transport, $error));
    }

    /**
     * Pull up to `$size` envelopes; if the queue runs dry mid-batch, wait up to `$idleMs` for more, then flush
     * what we have. Returns `[]` only when nothing is available at all.
     *
     * The size is a TUNING bound, not a correctness bound: `get()`'s fetch-size argument is a
     * best-effort hint by the Receiver contract, and an envelope the receiver actually yielded is
     * OWNED from that moment; breaking out mid-iteration would abandon it, neither handled nor
     * acked, invisible on an explicit-ack broker until the connection dies. So the loop drains
     * every yielded envelope and lets a batch overshoot its size on an over-delivering receiver,
     * rather than ever dropping one.
     *
     * @return list<Envelope>
     */
    private function collect(SymfonyStyle $io, ReceiverInterface $receiver, int $size, int $idleMs): array
    {
        $batch = [];
        $idleDeadline = null;

        while (count($batch) < $size) {
            $got = false;

            try {
                // @phpstan-ignore arguments.count (the fetch-size hint is the interface's own commented-future param; implementations that predate it ignore extra args)
                foreach ($receiver->get($size - count($batch)) as $envelope) {
                    $batch[] = $envelope;
                    $got = true;
                }
            } catch (MessageDecodingFailedException $e) {
                // Worker parity, reimbursed: the Worker catches a decode failure at get() and keeps
                // consuming; this loop must not die on a producer's bug. The receiver has already
                // rejected the undecodable delivery; on a queue with no dead-letter exchange that is
                // a destruction, the same caveat as the no-failure-transport fallback. Envelopes
                // yielded before the throw are already owned by the batch and stay.
                $io->warning(sprintf('Undecodable message dropped by the transport: %s', $e->getMessage()));

                continue;
            }

            if ($got) {
                $idleDeadline = null; // fresh messages; keep filling

                continue;
            }

            if ($batch === []) {
                return [];
            }

            $idleDeadline ??= microtime(true) + $idleMs / 1000;

            if (microtime(true) >= $idleDeadline) {
                break; // idle flush of the partial batch
            }

            $this->pauseTightPoll();
        }

        return $batch;
    }

    /**
     * The idle pause of the outer loop, between empty polls of a quiet queue.
     *
     * The milliseconds-to-microseconds arithmetic lives HERE rather than at the call site: a sink that
     * takes microseconds leaves the conversion outside its own pin, mutable and unobservable alike.
     *
     * @infection-ignore-all the sleep IS the whole effect: its arithmetic is observable only by
     *                       measuring wall-clock pauses, which a unit test cannot do without flaking.
     *                       The pin covers what sits inside this sink and stops there: the CALL is a
     *                       mutant of its own, held by a bound on how often a silent transport is
     *                       polled, since elapsed time cannot separate a paced run from a spinning
     *                       one, both ending when the transport speaks. Same pin shape as
     *                       `\Storm\Chronicler\Store\RetryEventStore::pause`, whose reasoning also sits at
     *                       the site, never in `source.excludes`.
     */
    private function pauseIdlePoll(int $idleMs): void
    {
        usleep($idleMs * 1000);
    }

    /**
     * The tight re-read of a partial batch, between the poll that found nothing and its idle flush.
     *
     * @infection-ignore-all same reasoning as `pauseIdlePoll()`: the value is a wall-clock effect,
     *                       and the pin stops at this body; the call is held by the poll count of a
     *                       partial batch waiting for its rest, the only state that reaches it.
     */
    private function pauseTightPoll(): void
    {
        usleep(1000);
    }

    private function receiver(string $transport): ReceiverInterface
    {
        $receiver = $this->receivers->get($transport);

        if (! $receiver instanceof ReceiverInterface) {
            throw new RuntimeException('the transport is not a receiver');
        }

        return $receiver;
    }

    /**
     * The one strict parser for every numeric option: a typo on an operator BOUND must refuse,
     * never widen. A bare `(int)` cast turns `--limit=abc` into `0`, which here means UNLIMITED,
     * and silently swaps a mistyped batch or idle value for its default. Digits only, refused on
     * overflow since `FILTER_VALIDATE_INT` saturates nothing, floored per option; `0` stays
     * available where it is a deliberate value, such as no-limit, by asking `min: 0`.
     *
     * @throws InvalidArgumentException when the value is not a plain integer or sits below the floor
     */
    private function boundedOption(InputInterface $input, string $name, int $min, int $fallback): int
    {
        $value = $input->getOption($name);

        if ($value === null) {
            return $fallback;
        }

        $parsed = filter_var($value, FILTER_VALIDATE_INT);

        if ($parsed === false || $parsed < $min) {
            throw new InvalidArgumentException(sprintf(
                'Option --%s expects an integer >= %d, got "%s" — a mistyped bound must refuse, never fall back to a wider one.',
                $name,
                $min,
                is_scalar($value) ? (string) $value : get_debug_type($value),
            ));
        }

        return $parsed;
    }
}
