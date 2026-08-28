<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Consume;

use Closure;
use LogicException;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Storm\Chronicler\Inbox\BatchInboxStore;
use Storm\Chronicler\Inbox\InboxStore;
use Storm\Story\Consume\BatchConsumer;
use Storm\Story\Consume\BatchDecision;
use Storm\Story\Consume\InboxTransactionContext;
use Storm\Story\Stamp\BatchModeStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\RecoverableMessageHandlingException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Throwable;

final class BatchConsumerTest extends TestCase
{
    #[Test]
    public function commits_the_whole_batch_and_acks_every_message(): void
    {
        $bus = $this->bus();
        $consumer = new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox(), replayRetryDelayMs: 0);

        $decisions = $consumer->process('events', [$this->envelope('a'), $this->envelope('b'), $this->envelope('c')]);

        $this->assertSame([true, true, true], array_map(static fn (BatchDecision $d): bool => $d->ack, $decisions));
        // the batch ran every handler ONCE, each dispatched in batch mode, so dedup skips its own tx
        $this->assertSame([['a', true], ['b', true], ['c', true]], $bus->dispatched);
    }

    #[Test]
    public function every_batched_dispatch_is_pinned_terminal(): void
    {
        // the empty TransportNamesStamp forces "no senders": a message whose class routes to a transport
        // such as a command lane must be HANDLED here, never re-sent to the broker; the loop IS the consumer
        $bus = $this->bus();
        new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox(), replayRetryDelayMs: 0)
            ->process('events', [$this->envelope('a')]);

        $stamp = $bus->envelopes[0]->last(TransportNamesStamp::class);
        $this->assertNotNull($stamp);
        $this->assertSame([], $stamp->getTransportNames());
    }

    #[Test]
    public function isolates_the_poison_through_the_per_message_inbox(): void
    {
        // a HANDLER poison propagates through onceBatch, the transaction rolled back, so each message
        // replays through InboxStore::once(), its own transaction AND its dedup row. The regression this
        // pins: the replay dispatched on the bare bus, and DeduplicateConsumer, gated on a
        // ReceivedStamp the loop never sets, silently skipped the inbox
        $bus = $this->bus(throwFor: 'b');
        $perMessage = $this->perMessageInbox();
        $consumer = new BatchConsumer($this->batchInbox(), $bus, $perMessage, replayRetryDelayMs: 0);

        $decisions = $consumer->process('events', [$this->envelope('a'), $this->envelope('b'), $this->envelope('c')]);

        $this->assertSame([true, false, true], array_map(static fn (BatchDecision $d): bool => $d->ack, $decisions));
        // the decision carries the handler's OWN failure, unwrapped from the classification marker;
        // a PLAIN failure is condemned, never redelivered; only a DECLARED transient earns that
        $this->assertNull($decisions[1]->redeliverDelayMs);
        $this->assertInstanceOf(RuntimeException::class, $decisions[1]->error);
        $this->assertSame('poison', $decisions[1]->error->getMessage());
        // the poison is attempted TWICE in the replay, the bounded second chance, before being condemned;
        // c never ran in the batch attempt, b poisoned it first, so the replay is its first run
        $this->assertSame([['events', 'a'], ['events', 'b'], ['events', 'b'], ['events', 'c']], $perMessage->onced);
        // batch attempt with a then b poisoning, then the replays; every dispatch STAYS batch-stamped: once() owns
        // the transaction, dedup must not nest another
        $this->assertSame([['a', true], ['b', true], ['a', true], ['b', true], ['b', true], ['c', true]], $bus->dispatched);
    }

    #[Test]
    public function a_transient_failure_is_absorbed_by_the_second_replay_attempt(): void
    {
        // the live case, two SagaFenceBusy captures whose sagas were fine seconds later: the concurrent
        // step that rolled the batch back is usually still in flight on the FIRST replay; the bounded
        // second attempt absorbs the transient instead of condemning a designed-retryable delivery
        $bus = $this->busThrowingTimes(throwFor: 'b', times: 2); // the batch attempt, then the first replay
        $perMessage = $this->perMessageInbox();
        $consumer = new BatchConsumer($this->batchInbox(), $bus, $perMessage, replayRetryDelayMs: 0);

        $decisions = $consumer->process('events', [$this->envelope('a'), $this->envelope('b')]);

        $this->assertSame([true, true], array_map(static fn (BatchDecision $d): bool => $d->ack, $decisions));
        // replay: first attempt threw, second ran clean; attempted twice, captured zero times
        $this->assertSame([['events', 'a'], ['events', 'b'], ['events', 'b']], $perMessage->onced);
    }

    #[Test]
    #[Group('adversarial')]
    public function an_unrecoverable_failure_is_condemned_on_the_first_replay_attempt(): void
    {
        // UnrecoverableExceptionInterface says "never retry me": re-running it only risks repeating a
        // non-transactional side effect; no second chance, condemned at once
        $poison = new UnrecoverableMessageHandlingException('never retry');
        $bus = $this->busThrowing('b', static fn (): Throwable => $poison);
        $perMessage = $this->perMessageInbox();
        $consumer = new BatchConsumer($this->batchInbox(), $bus, $perMessage, replayRetryDelayMs: 0);

        $decisions = $consumer->process('events', [$this->envelope('a'), $this->envelope('b')]);

        $this->assertSame([true, false], array_map(static fn (BatchDecision $d): bool => $d->ack, $decisions));
        $this->assertSame($poison, $decisions[1]->error);
        // ONE replay attempt for b, not two
        $this->assertSame([['events', 'a'], ['events', 'b']], $perMessage->onced);
    }

    #[Test]
    #[Group('adversarial')]
    public function a_handler_failed_wrapper_of_only_unrecoverables_is_condemned_at_once(): void
    {
        // Messenger's own nested rule, mirrored: the bus wraps handler failures in HandlerFailedException;
        // when EVERY wrapped failure is unrecoverable, the whole delivery is
        $bus = $this->busThrowing('a', static fn (): Throwable => new HandlerFailedException(
            new Envelope(new stdClass),
            ['handler' => new UnrecoverableMessageHandlingException('final')],
        ));
        $perMessage = $this->perMessageInbox();

        $decisions = new BatchConsumer($this->batchInbox(), $bus, $perMessage, replayRetryDelayMs: 0)
            ->process('events', [$this->envelope('a')]);

        $this->assertFalse($decisions[0]->ack);
        $this->assertSame([['events', 'a']], $perMessage->onced); // one attempt only
    }

    #[Test]
    #[Group('adversarial')]
    public function an_unrecoverable_from_a_nested_sync_dispatch_is_still_condemned_at_once(): void
    {
        // a nested sync dispatch wraps the leaf in a SECOND HandlerFailedException; a one-level
        // read would see a wrapper that declares nothing and run a second attempt of something
        // that said "never retry me"
        $bus = $this->busThrowing('a', static fn (): Throwable => new HandlerFailedException(
            new Envelope(new stdClass),
            ['outer' => new HandlerFailedException(
                new Envelope(new stdClass),
                ['inner' => new UnrecoverableMessageHandlingException('final, nested')],
            )],
        ));
        $perMessage = $this->perMessageInbox();

        $decisions = new BatchConsumer($this->batchInbox(), $bus, $perMessage, replayRetryDelayMs: 0)
            ->process('events', [$this->envelope('a')]);

        $this->assertFalse($decisions[0]->ack);
        $this->assertSame([['events', 'a']], $perMessage->onced); // one attempt only, the leaf declared final
    }

    #[Test]
    #[Group('adversarial')]
    public function a_recoverable_from_a_nested_sync_dispatch_is_redelivered_with_the_leaf_delay(): void
    {
        // the recoverable twin of the nested-unrecoverable case: the leaf's declaration AND its own
        // retryDelay both live below a second wrapper, and a one-level read would condemn the
        // delivery to the failure transport instead of the broker hold the leaf asked for
        $busy = new RecoverableMessageHandlingException('not born yet, nested', retryDelay: 1800);
        $bus = $this->busThrowing('a', static fn (): Throwable => new HandlerFailedException(
            new Envelope(new stdClass),
            ['outer' => new HandlerFailedException(new Envelope(new stdClass), ['inner' => $busy])],
        ));
        $perMessage = $this->perMessageInbox();

        $decisions = new BatchConsumer($this->batchInbox(), $bus, $perMessage, replayRetryDelayMs: 0)
            ->process('events', [$this->envelope('a')]);

        $this->assertFalse($decisions[0]->ack);
        $this->assertSame(1800, $decisions[0]->redeliverDelayMs, "the LEAF's own delay rides out, not the default");
    }

    #[Test]
    public function a_recoverable_failure_outliving_both_attempts_is_redelivered_with_its_own_delay(): void
    {
        // the seconds-scale wait, a saga child's birth race: the message is sound and the world is not
        // ready, so the condemnation becomes a redeliver, back to the origin transport, the broker
        // holding the exception's own retryDelay; the in-process second chance is still spent first
        $busy = new RecoverableMessageHandlingException('not born yet', retryDelay: 2500);
        $bus = $this->busThrowing('a', static fn (): Throwable => $busy);
        $perMessage = $this->perMessageInbox();

        $decisions = new BatchConsumer($this->batchInbox(), $bus, $perMessage, replayRetryDelayMs: 0)
            ->process('events', [$this->envelope('a')]);

        $this->assertFalse($decisions[0]->ack);
        $this->assertSame(2500, $decisions[0]->redeliverDelayMs);
        $this->assertSame($busy, $decisions[0]->error); // the cause travels for the loop's logging
        $this->assertSame([['events', 'a'], ['events', 'a']], $perMessage->onced); // both attempts spent first
    }

    #[Test]
    public function a_recoverable_without_a_declared_delay_uses_the_consumer_default(): void
    {
        $bus = $this->busThrowing('a', static fn (): Throwable => new RecoverableMessageHandlingException('busy'));

        $decisions = new BatchConsumer(
            $this->batchInbox(), $bus, $this->perMessageInbox(),
            replayRetryDelayMs: 0, recoverableRedeliveryDelayMs: 750,
        )->process('events', [$this->envelope('a')]);

        $this->assertSame(750, $decisions[0]->redeliverDelayMs);
    }

    #[Test]
    #[Group('adversarial')]
    public function a_recoverable_at_the_redelivery_cap_is_condemned_like_any_poison(): void
    {
        // the escape is bounded: a wait whose completion never comes, a spawn command dead-lettered
        // whose child is never born, must surface on the failure transport, not redeliver forever
        $bus = $this->busThrowing('a', static fn (): Throwable => new RecoverableMessageHandlingException('never comes'));
        $atCap = new Envelope(new stdClass, [new MessageIdStamp('a'), new RedeliveryStamp(3)]);

        $decisions = new BatchConsumer(
            $this->batchInbox(), $bus, $this->perMessageInbox(),
            replayRetryDelayMs: 0, recoverableRedeliveryCap: 3,
        )->process('events', [$atCap]);

        $this->assertFalse($decisions[0]->ack);
        $this->assertNull($decisions[0]->redeliverDelayMs); // a reject, not one more round
        $this->assertSame('never comes', $decisions[0]->error?->getMessage());
    }

    #[Test]
    #[Group('adversarial')]
    public function the_declared_defaults_are_the_ones_a_bundle_less_caller_gets(): void
    {
        // Every test above names its knobs, which is what a test should do and also what leaves the
        // DEFAULTS unproven: they are only ever reached by a caller that omits the argument. These are
        // the numbers a standalone consumer runs on, and the two that decide an operator's patience.
        $bus = $this->busThrowing('a', static fn (): Throwable => new RecoverableMessageHandlingException('busy'));

        $justUnderTheCap = new Envelope(new stdClass, [new MessageIdStamp('a'), new RedeliveryStamp(299)]);
        $decisions = new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox())
            ->process('events', [$justUnderTheCap]);

        $this->assertSame(1000, $decisions[0]->redeliverDelayMs); // the default hold between redeliveries

        $atTheCap = new Envelope(new stdClass, [new MessageIdStamp('a'), new RedeliveryStamp(300)]);
        $decisions = new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox())
            ->process('events', [$atTheCap]);

        $this->assertFalse($decisions[0]->ack); // the default cap, condemned one redelivery later
        $this->assertNull($decisions[0]->redeliverDelayMs);
    }

    #[Test]
    #[Group('adversarial')]
    #[Group('slow')]
    public function the_isolation_replay_pauses_before_its_second_attempt(): void
    {
        // The pause is the whole point of the two-attempt replay: the batch rolled back moments ago,
        // so a contention still in flight resolves during it. Bound on ONE side only, never an upper
        // one: a slow machine may take longer, and a test that forbids that is a flake by design.
        $bus = $this->busThrowing('a', static fn (): Throwable => new RuntimeException('contended'));

        $startedAt = microtime(true);
        new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox())->process('events', [$this->envelope('a')]);
        $elapsed = microtime(true) - $startedAt;

        $this->assertGreaterThan(0.05, $elapsed, 'the replay must pause between its two attempts');
    }

    #[Test]
    public function an_envelope_under_the_cap_is_still_redelivered(): void
    {
        $bus = $this->busThrowing('a', static fn (): Throwable => new RecoverableMessageHandlingException('busy'));
        $underCap = new Envelope(new stdClass, [new MessageIdStamp('a'), new RedeliveryStamp(2)]);

        $decisions = new BatchConsumer(
            $this->batchInbox(), $bus, $this->perMessageInbox(),
            replayRetryDelayMs: 0, recoverableRedeliveryCap: 3,
        )->process('events', [$underCap]);

        $this->assertNotNull($decisions[0]->redeliverDelayMs);
    }

    #[Test]
    #[Group('adversarial')]
    public function a_handler_failed_wrapper_with_no_recoverable_nested_is_condemned_not_redelivered(): void
    {
        // the declaration rule is strict: a wrapper of plain failures asks for nothing; treating it
        // recoverable would spin an ordinary poison through the broker instead of capturing it
        $bus = $this->busThrowing('a', static fn (): Throwable => new HandlerFailedException(
            new Envelope(new stdClass),
            ['plain' => new RuntimeException('ordinary')],
        ));

        $decisions = new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox(), replayRetryDelayMs: 0)
            ->process('events', [$this->envelope('a')]);

        $this->assertFalse($decisions[0]->ack);
        $this->assertNull($decisions[0]->redeliverDelayMs);
    }

    #[Test]
    public function with_no_prior_stamp_the_redelivery_count_starts_at_zero(): void
    {
        // first round against a cap of one: 0 < 1, the redelivery is granted; the count must start
        // at zero for an envelope the broker never redelivered
        $bus = $this->busThrowing('a', static fn (): Throwable => new RecoverableMessageHandlingException('busy'));

        $decisions = new BatchConsumer(
            $this->batchInbox(), $bus, $this->perMessageInbox(),
            replayRetryDelayMs: 0, recoverableRedeliveryCap: 1,
        )->process('events', [$this->envelope('a')]);

        $this->assertNotNull($decisions[0]->redeliverDelayMs);
    }

    #[Test]
    #[Group('adversarial')]
    public function a_cap_of_zero_disables_the_redelivery_path_entirely(): void
    {
        // the knob's floor is meaningful: cap 0 restores the pre-redelivery doctrine, recoverable or
        // not, everything that outlives the second chance is captured
        $bus = $this->busThrowing('a', static fn (): Throwable => new RecoverableMessageHandlingException('busy'));

        $decisions = new BatchConsumer(
            $this->batchInbox(), $bus, $this->perMessageInbox(),
            replayRetryDelayMs: 0, recoverableRedeliveryCap: 0,
        )->process('events', [$this->envelope('a')]);

        $this->assertFalse($decisions[0]->ack);
        $this->assertNull($decisions[0]->redeliverDelayMs);
    }

    #[Test]
    #[Group('adversarial')]
    public function any_nested_recoverable_in_a_handler_failed_wrapper_wins_the_redelivery(): void
    {
        // Symfony's own asymmetry, mirrored: condemning requires EVERY wrapped failure final, retrying
        // takes ANY wrapped failure transient; one handler asking for another chance outweighs its
        // silent siblings
        $bus = $this->busThrowing('a', static fn (): Throwable => new HandlerFailedException(
            new Envelope(new stdClass),
            ['silent' => new RuntimeException('plain'), 'asking' => new RecoverableMessageHandlingException('wait', retryDelay: 400)],
        ));

        $decisions = new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox(), replayRetryDelayMs: 0)
            ->process('events', [$this->envelope('a')]);

        $this->assertSame(400, $decisions[0]->redeliverDelayMs); // and the recoverable's own delay is found nested
    }

    #[Test]
    #[Group('adversarial')]
    public function a_batch_machinery_failure_is_systemic_and_propagates_unacked(): void
    {
        // the store failing AROUND the handlers, such as a transaction begin/commit, the inbox INSERT, or a dead
        // connection, is nobody's poison: condemning per message would migrate a healthy queue to the
        // failure transport. It propagates; the deliveries stay un-acked and come back.
        $inbox = new readonly class() implements BatchInboxStore
        {
            public function onceBatch(string $consumer, array $items): array
            {
                throw new RuntimeException('connection lost'); // the machinery, not a handler
            }
        };
        $perMessage = $this->perMessageInbox();
        $consumer = new BatchConsumer($inbox, $this->bus(), $perMessage, replayRetryDelayMs: 0);

        try {
            $consumer->process('events', [$this->envelope('a'), $this->envelope('b')]);
            $this->fail('expected the systemic failure to propagate');
        } catch (RuntimeException $e) {
            $this->assertSame('connection lost', $e->getMessage());
        }

        $this->assertSame([], $perMessage->onced); // nothing was isolated, nothing condemned
    }

    #[Test]
    #[Group('adversarial')]
    public function a_store_failure_during_the_replay_propagates_unacked(): void
    {
        // same rule inside the isolation: once() itself failing is systemic; the replay must not turn
        // an inbox outage into a poison capture
        $bus = $this->bus(throwFor: 'b'); // a real poison rolls the batch back first
        $perMessage = new class() implements InboxStore
        {
            public function once(string $consumer, string $messageId, Closure $handler): bool
            {
                throw new RuntimeException('inbox down');
            }
        };

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageIsOrContains('inbox down');

        new BatchConsumer($this->batchInbox(), $bus, $perMessage, replayRetryDelayMs: 0)
            ->process('events', [$this->envelope('a'), $this->envelope('b')]);
    }

    #[Test]
    public function each_batched_dispatch_brackets_the_inbox_transaction_context(): void
    {
        // the dual-write guard reads this ambient entry: it must be open exactly while a handler runs,
        // carry the handled message class, and be left even when the handler throws
        $context = new InboxTransactionContext;
        $bus = new class($context) implements MessageBusInterface
        {
            /** @var list<array{bool, ?string}> */
            public array $observed = [];

            public function __construct(private readonly InboxTransactionContext $context) {}

            public function dispatch(object $message, array $stamps = []): Envelope
            {
                $this->observed[] = [$this->context->active(), $this->context->current()];

                $envelope = $message instanceof Envelope ? $message : new Envelope($message, $stamps);
                if ($envelope->last(MessageIdStamp::class)?->id === 'b') {
                    throw new RuntimeException('poison');
                }

                return $envelope;
            }
        };

        new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox(), replayRetryDelayMs: 0, context: $context)
            ->process('events', [$this->envelope('a'), $this->envelope('b')]);

        $this->assertNotSame([], $bus->observed);
        foreach ($bus->observed as [$active, $current]) {
            $this->assertTrue($active, 'the context is open during every dispatch');
            $this->assertSame(stdClass::class, $current);
        }
        $this->assertFalse($context->active(), 'the context is left after the batch — even on throws');
    }

    #[Test]
    public function an_already_processed_duplicate_in_the_isolation_replay_is_acked(): void
    {
        // once() skips a recorded (consumer, id) without running the handler; done is done, ack it
        $bus = $this->busThrowingTimes(throwFor: 'a', times: 1); // poisons the batch attempt only
        $perMessage = $this->perMessageInbox(duplicates: ['a']);
        $consumer = new BatchConsumer($this->batchInbox(), $bus, $perMessage, replayRetryDelayMs: 0);

        $decisions = $consumer->process('events', [$this->envelope('a')]);

        $this->assertTrue($decisions[0]->ack);
        $this->assertSame([['a', true]], $bus->dispatched); // the batch attempt only; the replay's dedup row skipped the handler
    }

    #[Test]
    #[Group('adversarial')]
    public function a_message_without_an_id_is_rejected_alone_and_the_batch_survives(): void
    {
        // an id-less message cannot enter ANY inbox; it must be captured on its own, reject-with-cause,
        // never crash the loop: a throw here would wedge the whole transport on one malformed message,
        // redelivered forever
        $bus = $this->bus();
        $consumer = new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox(), replayRetryDelayMs: 0);

        $decisions = $consumer->process('events', [$this->envelope('a'), new Envelope(new stdClass), $this->envelope('c')]);

        $this->assertSame([true, false, true], array_map(static fn (BatchDecision $d): bool => $d->ack, $decisions));
        $this->assertInstanceOf(LogicException::class, $decisions[1]->error);
        $this->assertSame([['a', true], ['c', true]], $bus->dispatched); // the identified ones still batched
    }

    #[Test]
    public function an_empty_batch_is_a_no_op(): void
    {
        $bus = $this->bus();

        $this->assertSame([], new BatchConsumer($this->batchInbox(), $bus, $this->perMessageInbox(), replayRetryDelayMs: 0)->process('events', []));
        $this->assertSame([], $bus->dispatched);
    }

    private function envelope(string $id): Envelope
    {
        return new Envelope(new stdClass, [new MessageIdStamp($id)]);
    }

    /**
     * A faithful BatchInboxStore shape: runs every handler in order, and a handler's throw propagates
     * as the rolled-back transaction would, which is exactly how a poison surfaces from the real store.
     */
    #[Test]
    public function an_empty_batch_opens_nothing_at_all(): void
    {
        // A poll on a quiet queue is the common case, and it must cost no transaction. The property
        // holds twice over: the early return short-circuits, and the loop below it would find nothing
        // to identify anyway, so removing the guard changes nothing observable. What is pinned here is
        // the property at the boundary, not the guard; keep both and the refactor that drops one stays
        // honest.
        $bus = $this->busThrowing('never', static fn (): Throwable => new RuntimeException('the bus must not be reached'));
        $inbox = new readonly class() implements BatchInboxStore
        {
            public function onceBatch(string $consumer, array $items): array
            {
                throw new RuntimeException('an empty batch must not open a transaction');
            }
        };

        $this->assertSame([], new BatchConsumer($inbox, $bus, $this->perMessageInbox())->process('events', []));
    }

    private function batchInbox(): BatchInboxStore
    {
        return new readonly class() implements BatchInboxStore
        {
            public function onceBatch(string $consumer, array $items): array
            {
                $ran = [];
                foreach ($items as $item) {
                    ($item->handler)();
                    $ran[] = true;
                }

                return $ran;
            }
        };
    }

    /**
     * A per-message InboxStore recording every once() call; ids in $duplicates are skipped as already recorded.
     *
     * @param  list<string>  $duplicates
     * @return InboxStore&object{onced: list<array{string, string}>}
     */
    private function perMessageInbox(array $duplicates = []): InboxStore
    {
        return new class($duplicates) implements InboxStore
        {
            /** @var list<array{string, string}> */
            public array $onced = [];

            /** @param  list<string>  $duplicates */
            public function __construct(private readonly array $duplicates) {}

            public function once(string $consumer, string $messageId, Closure $handler): bool
            {
                $this->onced[] = [$consumer, $messageId];

                if (in_array($messageId, $this->duplicates, true)) {
                    return false;
                }

                $handler();

                return true;
            }
        };
    }

    /**
     * A bus whose named message id throws on its first `$times` dispatches then succeeds, the transient
     * shape where the contention released between attempts.
     *
     * @return MessageBusInterface&object{dispatched: list<array{?string, bool}>}
     */
    private function busThrowingTimes(string $throwFor, int $times): MessageBusInterface
    {
        return new class($throwFor, $times) implements MessageBusInterface
        {
            /** @var list<array{?string, bool}> */
            public array $dispatched = [];

            private int $thrown = 0;

            public function __construct(private readonly string $throwFor, private readonly int $times) {}

            public function dispatch(object $message, array $stamps = []): Envelope
            {
                $envelope = $message instanceof Envelope ? $message : new Envelope($message, $stamps);
                $id = $envelope->last(MessageIdStamp::class)?->id;

                $this->dispatched[] = [$id, $envelope->last(BatchModeStamp::class) !== null];

                if ($id === $this->throwFor && $this->thrown < $this->times) {
                    $this->thrown++;

                    throw new RuntimeException('transient — released after the pause');
                }

                return $envelope;
            }
        };
    }

    /**
     * A bus whose named message id always throws what the factory builds, for typed poisons.
     *
     * @param  Closure(): Throwable  $poison
     * @return MessageBusInterface&object{dispatched: list<array{?string, bool}>}
     */
    private function busThrowing(string $throwFor, Closure $poison): MessageBusInterface
    {
        return new class($throwFor, $poison) implements MessageBusInterface
        {
            /** @var list<array{?string, bool}> */
            public array $dispatched = [];

            /** @param  Closure(): Throwable  $poison */
            public function __construct(private readonly string $throwFor, private readonly Closure $poison) {}

            public function dispatch(object $message, array $stamps = []): Envelope
            {
                $envelope = $message instanceof Envelope ? $message : new Envelope($message, $stamps);
                $id = $envelope->last(MessageIdStamp::class)?->id;

                $this->dispatched[] = [$id, $envelope->last(BatchModeStamp::class) !== null];

                if ($id === $this->throwFor) {
                    throw ($this->poison)();
                }

                return $envelope;
            }
        };
    }

    /**
     * A bus recording each dispatch as `[messageId, inBatchMode]` and the raw envelopes, throwing for the
     * named message id.
     *
     * @return MessageBusInterface&object{dispatched: list<array{?string, bool}>, envelopes: list<Envelope>}
     */
    private function bus(?string $throwFor = null): MessageBusInterface
    {
        return new class($throwFor) implements MessageBusInterface
        {
            /** @var list<array{?string, bool}> */
            public array $dispatched = [];

            /** @var list<Envelope> */
            public array $envelopes = [];

            public function __construct(private readonly ?string $throwFor) {}

            public function dispatch(object $message, array $stamps = []): Envelope
            {
                $envelope = $message instanceof Envelope ? $message : new Envelope($message, $stamps);
                $id = $envelope->last(MessageIdStamp::class)?->id;

                $this->dispatched[] = [$id, $envelope->last(BatchModeStamp::class) !== null];
                $this->envelopes[] = $envelope;

                if ($id === $this->throwFor) {
                    throw new RuntimeException('poison');
                }

                return $envelope;
            }
        };
    }
}
