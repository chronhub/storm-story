<?php

declare(strict_types=1);

namespace Storm\Story\Stamp;

use Storm\Message\Header;
use Storm\Story\Outbox\MessengerOutboxPublisher;
use Storm\Story\Transport\NeutralMessageSerializer;
use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Carries the full stored header of an event-store `Message` alongside the event on the bus, so a
 * transport can put a faithful event-store row on the wire, not just the cross-cutting context.
 *
 * Attached by `MessengerOutboxPublisher` on every republished event. The in-process saga path never reads
 * it, since it binds context from the individual context stamps, so it is free for sync handlers; only
 * `NeutralMessageSerializer` materializes it, emitting the whole header on the wire and rebuilding it on
 * decoding. The header holds `occurred_at`, the event's own causation, `aggregate{id,type,version}` and the
 * rest.
 *
 * The header includes `Header::MessageType`, the FQCN; the wire externalizes that as the portable `type`
 * alias instead, so the serializer strips it on encoding and re-injects it on decoding.
 *
 * @see MessengerOutboxPublisher
 * @see NeutralMessageSerializer
 * @see \Storm\Message\Header::MessageType
 */
final readonly class StoredHeaderStamp implements StampInterface
{
    /**
     * @param  array<string, scalar|array<mixed>|null>  $header
     */
    public function __construct(
        public array $header,
    ) {}
}
