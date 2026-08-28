<?php

declare(strict_types=1);

namespace Storm\Story\Stamp;

use InvalidArgumentException;
use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * The id of the message currently on the bus.
 *
 * Messenger does not assign message ids, so the boundary middleware stamps one. It is the
 * causation of whatever the handler produces: events recorded while handling this message
 * inherit `Header::CausationId = this id`. Distinct from an event's own `Header::MessageId`,
 * assigned per event at persistence time by the enricher chain.
 *
 * A blank id is refused at construction: this id is the consumer-side dedup key, and every
 * blank-id message would share ONE inbox entry per transport; the first is handled, every later
 * DIFFERENT message silently skipped-acked. Refusing here turns that silent loss into a loud
 * producer bug. A PHP-serialized envelope bypasses constructors, so the wire decoder enforces the
 * same rule at its edge.
 */
final readonly class MessageIdStamp implements StampInterface
{
    /**
     * @throws InvalidArgumentException when the id is blank
     */
    public function __construct(
        public string $id,
    ) {
        if (trim($id) === '') {
            throw new InvalidArgumentException('A message id cannot be blank: it is the consumer-side dedup key, and blank ids would all collide into one inbox entry, silently losing every message after the first.');
        }
    }
}
