<?php

declare(strict_types=1);

namespace Storm\Story\Stamp;

use Storm\Bureau\Actor;
use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Who initiated the work: wraps a Bureau `Actor`, its id and type. Absent for system-originated
 * messages, a missing stamp rather than a null actor. Maps to `Header::ActorId` and `Header::ActorType`
 * on recorded events.
 */
final readonly class ActorStamp implements StampInterface
{
    public function __construct(
        public Actor $actor,
    ) {}
}
