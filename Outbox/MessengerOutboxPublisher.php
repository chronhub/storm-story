<?php

declare(strict_types=1);

namespace Storm\Story\Outbox;

use Storm\Chronicler\Outbox\OutboxPublisher;
use Storm\Message\Message;
use Storm\Story\Middleware\AssignMessageMetadata;
use Storm\Story\Stamp\ContextStamps;
use Storm\Story\Stamp\StoredHeaderStamp;
use Symfony\Component\DependencyInjection\Attribute\AsAlias;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\ExceptionInterface;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;

/**
 * The default `OutboxPublisher`: hands a persisted `Message` straight to the events transport named by the
 * bundle config `storm.outbox.events_transport`, default `storm_events`. The app owns the transport's DSN as
 * a deployment choice, the framework owns the guarantee; a missing transport fails this service's wiring at
 * boot, loud by construction.
 *
 * The envelope carries a `BusNameStamp('storm.event.bus')`: when a worker consumes the transport, Messenger
 * redispatches on the regular event bus, a `ReceivedStamp` prevents a re-send, so handlers, `BindStoredHeader`
 * and the inbox dedup run exactly as they always did. It dispatches the bare domain event rather than the
 * `Message` so handlers type-hint the concrete event class, and rebuilds the cross-cutting context as stamps
 * from the message headers via `ContextStamps`, so a downstream reaction inherits the same correlation,
 * causation and actor. `AssignMessageMetadata` sees these stamps already present and leaves them untouched,
 * so a republished event keeps the id and trace it was stored with.
 *
 * It also attaches a `StoredHeaderStamp` carrying the full stored header: `occurred_at`, the event's own
 * causation, `aggregate{id,type,version}` and the rest. On the wire the transport serializer reads it, so a
 * wire-bound integration event is a faithful event-store row rather than context-only, and at consumption it
 * feeds `CurrentStoredHeader`.
 *
 * @see AssignMessageMetadata
 */
#[AsAlias(OutboxPublisher::class)]
final readonly class MessengerOutboxPublisher implements OutboxPublisher
{
    /**
     * @param  list<class-string>  $priorityEventTypes  event types routed to $priorityTransport, the priority lane
     * @param  array<class-string, int>  $eventLanes  per-type opaque level splitting the priority lane, higher meaning more urgent; a type absent here rides $priorityTransport
     * @param  array<int, SenderInterface>  $laneTransports  level => sender; a level absent here falls back to $priorityTransport, the floor: a priority event never falls back to the bulk flow
     */
    public function __construct(
        /** The default reference; the StormBundle re-points it from `storm.outbox.events_transport`. */
        #[Autowire(service: 'messenger.transport.storm_events')]
        private SenderInterface $transport,
        /**
         * Optional priority lane: an event whose type is in $priorityEventTypes is sent HERE instead,
         * drained ahead of the bulk event flow; null = no lane, every event to $transport. The bundle
         * owns the type set; this class only routes by type membership, a generic notion.
         */
        private ?SenderInterface $priorityTransport = null,
        private array $priorityEventTypes = [],
        /**
         * Optional split of the priority lane by criticality: a per-type opaque LEVEL plus a
         * level-to-sender table. Both empty = one undivided priority lane.
         */
        private array $eventLanes = [],
        private array $laneTransports = [],
        /**
         * The bag keys re-stamped across the hop, declared under `storm.context.propagated_keys`; the bundle owns the list.
         *
         * @var list<string>
         */
        private array $propagatedKeys = [],
    ) {}

    /**
     * {@inheritDoc}
     *
     * Narrows the contract's open-ended publish failure to the transport handoff this implementation performs.
     *
     * @throws ExceptionInterface when the transport rejects the handoff
     */
    public function publish(Message $message): void
    {
        $event = $message->message();

        $stamps = ContextStamps::fromMessage($message, $this->propagatedKeys);
        $stamps[] = new StoredHeaderStamp($message->headers());
        $stamps[] = new BusNameStamp('storm.event.bus');

        $this->senderFor($event)->send(new Envelope($event, $stamps));
    }

    /**
     * The priority-lane sender when the event matches a priority type, else the normal transport.
     * `instanceof` covers an exact class, a subclass, or an interface present in the set, which may hold
     * either; the bundle validates entries with both `class_exists` and `interface_exists`.
     *
     * When the priority lane is split by level, the HIGHEST level among the matched types wins,
     * finish-over-start; a matched event whose level maps to no sender, or carries no level, rides
     * $priorityTransport, the floor. A priority event may ride ABOVE the default lane, never below
     * it, and never back on the bulk flow.
     *
     * @see \Storm\Symfony\Compiler\ExtractSagaRoutingEventsPass
     */
    private function senderFor(object $event): SenderInterface
    {
        if ($this->priorityTransport === null) {
            return $this->transport;
        }

        // The unsplit lane keeps the short-circuit: this runs for EVERY published event, the relay's
        // hot path, and only a configured split needs the full scan for the highest matched level.
        if ($this->eventLanes === []) {
            return array_any($this->priorityEventTypes, fn ($type) => $event instanceof $type)
                ? $this->priorityTransport
                : $this->transport;
        }

        $matched = false;
        $level = null;

        foreach ($this->priorityEventTypes as $type) {
            if (! $event instanceof $type) {
                continue;
            }

            $matched = true;
            $laneLevel = $this->eventLanes[$type] ?? null;
            if ($laneLevel !== null && ($level === null || $laneLevel > $level)) {
                $level = $laneLevel;
            }
        }

        if (! $matched) {
            return $this->transport;
        }

        return $level === null
            ? $this->priorityTransport
            : ($this->laneTransports[$level] ?? $this->priorityTransport);
    }
}
