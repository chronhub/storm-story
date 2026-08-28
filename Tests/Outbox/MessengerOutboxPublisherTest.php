<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Outbox;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use stdClass;
use Storm\Message\Exception\InvalidMessageException;
use Storm\Message\Header;
use Storm\Message\Message;
use Storm\Story\Outbox\MessengerOutboxPublisher;
use Storm\Story\Stamp\ActorStamp;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Storm\Story\Stamp\StoredHeaderStamp;
use Storm\Story\Stamp\TenantStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;

final class MessengerOutboxPublisherTest extends TestCase
{
    #[Test]
    public function sends_the_bare_event_to_the_transport_with_stamps_rebuilt_from_headers(): void
    {
        $event = new stdClass;
        $message = new Message($event)
            ->withHeader(Header::MessageId, 'evt-1')
            ->withHeader(Header::CorrelationId, 'trace-1')
            ->withHeader(Header::ActorId, 'user-1')
            ->withHeader(Header::ActorType, 'App\\User')
            ->withHeader(Header::TenantId, 'tenant-1');

        $transport = $this->capturingSender();
        new MessengerOutboxPublisher($transport)->publish($message);

        // the bare domain event rides the wire, not the Message envelope
        $envelope = $transport->envelope;
        $this->assertInstanceOf(Envelope::class, $envelope);
        $this->assertSame($event, $envelope->getMessage());

        $this->assertSame('evt-1', $envelope->last(MessageIdStamp::class)?->id);
        $this->assertSame('trace-1', $envelope->last(CorrelationStamp::class)?->id);
        $this->assertSame('tenant-1', $envelope->last(TenantStamp::class)?->id);

        $actor = $envelope->last(ActorStamp::class);
        $this->assertInstanceOf(ActorStamp::class, $actor);
        $this->assertSame('user-1', $actor->actor->id);
        $this->assertSame('App\\User', $actor->actor->type);
    }

    #[Test]
    public function stamps_the_envelope_to_be_consumed_on_the_event_bus(): void
    {
        // the publisher sends to the transport DIRECTLY, no bus, no routing; the handoff is the
        // transport. The BusNameStamp is what routes the consumption back onto storm.event.bus
        $message = new Message(new stdClass)->withHeader(Header::MessageId, 'evt-1');

        $transport = $this->capturingSender();
        new MessengerOutboxPublisher($transport)->publish($message);

        $this->assertSame('storm.event.bus', $transport->envelope?->last(BusNameStamp::class)?->getBusName());
    }

    #[Test]
    public function attaches_the_full_stored_header_as_a_stamp(): void
    {
        $event = new stdClass;
        $message = new Message($event)
            ->withHeader(Header::MessageId, 'evt-1')
            ->withHeader(Header::OccurredAt, '2026-05-21T10:00:00.000000')
            ->withHeader(Header::CausationId, 'cmd-1')
            ->withHeader(Header::AggregateId, 'order-7')
            ->withHeader(Header::AggregateVersion, 3);

        $transport = $this->capturingSender();
        new MessengerOutboxPublisher($transport)->publish($message);

        $stored = $transport->envelope?->last(StoredHeaderStamp::class);

        // the wire-fidelity fields the context stamps drop ride along in the full header
        $this->assertInstanceOf(StoredHeaderStamp::class, $stored);
        $this->assertSame('cmd-1', $stored->header[Header::CausationId->value]);
        $this->assertSame('order-7', $stored->header[Header::AggregateId->value]);
        $this->assertSame(3, $stored->header[Header::AggregateVersion->value]);
    }

    #[Test]
    public function omits_stamps_for_absent_headers(): void
    {
        $event = new stdClass;
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $transport = $this->capturingSender();
        new MessengerOutboxPublisher($transport)->publish($message);

        $envelope = $transport->envelope;
        $this->assertInstanceOf(Envelope::class, $envelope);
        $this->assertNotNull($envelope->last(MessageIdStamp::class));
        $this->assertNull($envelope->last(CorrelationStamp::class));
        $this->assertNull($envelope->last(ActorStamp::class));
        $this->assertNull($envelope->last(TenantStamp::class));
    }

    #[Test]
    public function refuses_a_message_carrying_half_an_actor_identity(): void
    {
        // silently dropping the half erased provenance on the next hop with zero signal; a lone half
        // is corruption, refused loud by the central normalizer. The outbox rows hydrate tolerantly,
        // so a corrupt row reaches the publisher constructible.
        $event = new stdClass;
        $message = new Message($event)
            ->withHeader(Header::MessageId, 'evt-1')
            ->withHeader(Header::ActorId, 'user-1'); // no ActorType means corruption, not "no actor"

        $transport = $this->capturingSender();

        $this->expectException(InvalidMessageException::class);

        new MessengerOutboxPublisher($transport)->publish($message);
    }

    #[Test]
    public function routes_an_awaited_event_type_to_the_priority_transport(): void
    {
        $event = new AwaitedEvent;
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        new MessengerOutboxPublisher($normal, $priority, [AwaitedEvent::class])->publish($message);

        $this->assertSame($event, $priority->envelope?->getMessage(), 'an awaited type rides the priority lane');
        $this->assertNull($normal->envelope, 'the normal transport is untouched');
    }

    #[Test]
    public function routes_a_non_awaited_event_to_the_normal_transport(): void
    {
        $event = new stdClass; // not in the awaited set
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        new MessengerOutboxPublisher($normal, $priority, [AwaitedEvent::class])->publish($message);

        $this->assertSame($event, $normal->envelope?->getMessage(), 'a non-awaited event stays on the bulk transport');
        $this->assertNull($priority->envelope, 'the priority lane is untouched');
    }

    #[Test]
    public function matches_an_awaited_type_by_instanceof_so_an_interface_in_the_set_catches_its_implementors(): void
    {
        $event = new ConcreteAwaited; // implements AwaitedContract
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        new MessengerOutboxPublisher($normal, $priority, [AwaitedContract::class])->publish($message);

        $this->assertSame($event, $priority->envelope?->getMessage(), 'an interface in the set matches via instanceof');
        $this->assertNull($normal->envelope);
    }

    #[Test]
    public function routes_everything_to_the_normal_transport_when_no_priority_lane_is_configured(): void
    {
        $event = new AwaitedEvent; // even an awaited type
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        // a type set is present, but no priority transport, so still no lane, the off-by-default guard
        new MessengerOutboxPublisher($normal, null, [AwaitedEvent::class])->publish($message);

        $this->assertSame($event, $normal->envelope?->getMessage());
    }

    #[Test]
    public function routes_a_laned_awaited_type_to_its_level_transport(): void
    {
        $event = new AwaitedEvent;
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        $critical = $this->capturingSender();
        new MessengerOutboxPublisher(
            $normal,
            $priority,
            [AwaitedEvent::class],
            eventLanes: [AwaitedEvent::class => 40],
            laneTransports: [40 => $critical],
        )->publish($message);

        $this->assertSame($event, $critical->envelope?->getMessage(), 'a laned type rides its level transport');
        $this->assertNull($priority->envelope, 'the default priority lane is untouched');
        $this->assertNull($normal->envelope, 'the bulk transport is untouched');
    }

    /**
     * @param  list<class-string>  $awaited
     * @param  array<class-string, int>  $lanes
     */
    #[Test]
    #[Group('adversarial')]
    #[DataProvider('lane_declaration_orders')]
    public function the_highest_level_wins_when_an_event_matches_two_laned_types(array $awaited, array $lanes): void
    {
        // One event can match two declared awaited types at once, its class and an interface it
        // implements, and each may carry its own level. The scan takes the HIGHEST, so criticality is
        // a property of the event rather than of the order the compiler happened to emit the types
        // in. Both orders are run, since taking the first match would be right half the time.
        $event = new ConcreteAwaited;
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        $urgent = $this->capturingSender();
        $routine = $this->capturingSender();

        new MessengerOutboxPublisher(
            $normal,
            $priority,
            $awaited,
            eventLanes: $lanes,
            laneTransports: [10 => $routine, 40 => $urgent],
        )->publish($message);

        $this->assertSame($event, $urgent->envelope?->getMessage(), 'the higher level takes the event');
        $this->assertNull($routine->envelope);
        $this->assertNull($priority->envelope);
        $this->assertNull($normal->envelope);
    }

    /**
     * @return iterable<string, array{list<string>, array<string, int>}>
     */
    public static function lane_declaration_orders(): iterable
    {
        yield 'the lower level declared first' => [
            [AwaitedContract::class, ConcreteAwaited::class],
            [AwaitedContract::class => 10, ConcreteAwaited::class => 40],
        ];

        yield 'the higher level declared first' => [
            [ConcreteAwaited::class, AwaitedContract::class],
            [ConcreteAwaited::class => 40, AwaitedContract::class => 10],
        ];
    }

    #[Test]
    public function an_unlaned_awaited_type_stays_on_the_priority_floor_when_lanes_exist(): void
    {
        $event = new AwaitedEvent; // awaited, but carries no level
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        $critical = $this->capturingSender();
        new MessengerOutboxPublisher(
            $normal,
            $priority,
            [AwaitedEvent::class, ConcreteAwaited::class],
            eventLanes: [ConcreteAwaited::class => 40], // a DIFFERENT type is laned
            laneTransports: [40 => $critical],
        )->publish($message);

        $this->assertSame($event, $priority->envelope?->getMessage(), 'no level = the default priority lane');
        $this->assertNull($critical->envelope);
    }

    #[Test]
    public function an_unmatched_awaited_type_declared_first_does_not_hide_the_laned_one_behind_it(): void
    {
        // the scan must SKIP a type the event does not match and keep reading: ending the scan there
        // would leave the event unmatched, and an unmatched event rides the bulk transport, the one
        // place the lane contract forbids a priority event to land
        $event = new AwaitedEvent;
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        $critical = $this->capturingSender();
        new MessengerOutboxPublisher(
            $normal,
            $priority,
            [ConcreteAwaited::class, AwaitedEvent::class], // the event matches the SECOND entry alone
            eventLanes: [AwaitedEvent::class => 40],
            laneTransports: [40 => $critical],
        )->publish($message);

        $this->assertSame($event, $critical->envelope?->getMessage(), 'the laned level takes the event');
        $this->assertNull($priority->envelope);
        $this->assertNull($normal->envelope, 'a priority event never falls back to the bulk flow');
    }

    #[Test]
    public function a_level_with_no_mapped_transport_falls_back_to_the_priority_floor_never_to_bulk(): void
    {
        $event = new AwaitedEvent;
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        new MessengerOutboxPublisher(
            $normal,
            $priority,
            [AwaitedEvent::class],
            eventLanes: [AwaitedEvent::class => 40],
            laneTransports: [], // the level is declared, its realization is not
        )->publish($message);

        $this->assertSame($event, $priority->envelope?->getMessage(), 'the floor: an awaited event never falls back to bulk');
        $this->assertNull($normal->envelope);
    }

    #[Test]
    public function a_non_awaited_event_still_stays_on_bulk_once_lanes_are_configured(): void
    {
        // the majority of real traffic: most events are awaited by nobody. The unsplit path proves this
        // fallback in routes_a_non_awaited_event_to_the_normal_transport, the SPLIT path never did, and it
        // is the one prod runs once lanes exist. A break here would silently misroute the bulk of the feed
        // onto the priority lanes, drowning exactly the signals the lanes were carved out to protect.
        $event = new stdClass; // awaited by nobody
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        $critical = $this->capturingSender();
        new MessengerOutboxPublisher(
            $normal,
            $priority,
            [AwaitedEvent::class],
            eventLanes: [AwaitedEvent::class => 40],
            laneTransports: [40 => $critical],
        )->publish($message);

        $this->assertSame($event, $normal->envelope?->getMessage(), 'unawaited traffic keeps riding bulk');
        $this->assertNull($priority->envelope);
        $this->assertNull($critical->envelope);
    }

    #[Test]
    public function the_highest_level_among_matched_types_wins(): void
    {
        // ConcreteAwaited matches BOTH its own entry at 50 and its interface's at 30: finish-over-start.
        $event = new ConcreteAwaited;
        $message = new Message($event)->withHeader(Header::MessageId, 'evt-1');

        $normal = $this->capturingSender();
        $priority = $this->capturingSender();
        $low = $this->capturingSender();
        $high = $this->capturingSender();
        new MessengerOutboxPublisher(
            $normal,
            $priority,
            [AwaitedContract::class, ConcreteAwaited::class],
            eventLanes: [AwaitedContract::class => 30, ConcreteAwaited::class => 50],
            laneTransports: [30 => $low, 50 => $high],
        )->publish($message);

        $this->assertSame($event, $high->envelope?->getMessage(), 'the highest matched level wins');
        $this->assertNull($low->envelope);
        $this->assertNull($priority->envelope);
    }

    /**
     * @return SenderInterface&object{envelope: ?Envelope}
     */
    private function capturingSender(): SenderInterface
    {
        return new class() implements SenderInterface
        {
            public ?Envelope $envelope = null;

            public function send(Envelope $envelope): Envelope
            {
                $this->envelope = $envelope;

                return $envelope;
            }
        };
    }
}

/** Fixtures for the priority-lane routing tests. */
interface AwaitedContract {}

final class AwaitedEvent {}

final class ConcreteAwaited implements AwaitedContract {}
