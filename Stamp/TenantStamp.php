<?php

declare(strict_types=1);

namespace Storm\Story\Stamp;

use InvalidArgumentException;
use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * The tenant the work belongs to, in a multi-tenant deployment. Absent in single-tenant setups.
 * Maps to `Header::TenantId` on recorded events.
 *
 * A blank id is refused at construction, the same rule as the other context stamps: a tenant header that reads back as an empty string is worse
 * than an absent one, since every tenant-scoped read would silently match the "" tenant.
 */
final readonly class TenantStamp implements StampInterface
{
    /**
     * @throws InvalidArgumentException when the id is blank
     */
    public function __construct(
        public string $id,
    ) {
        if (trim($id) === '') {
            throw new InvalidArgumentException('A tenant id cannot be blank: single-tenant work carries NO TenantStamp at all, and a "" tenant would silently pool every blank-stamped message into one phantom tenant.');
        }
    }
}
