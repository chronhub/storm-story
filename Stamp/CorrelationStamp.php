<?php

declare(strict_types=1);

namespace Storm\Story\Stamp;

use InvalidArgumentException;
use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * The trace id of the whole flow, stable across the chain of messages that descend from one
 * originating action. A new root message gets its correlation set to its own id; descendants
 * copy it, so every event and command in the saga shares the same correlation.
 *
 * A blank id is refused at construction, the same rule as `MessageIdStamp`: the correlation is
 * the trace and saga routing identity, and a blank one would silently cross-route every
 * blank-correlated flow into the same saga instance.
 */
final readonly class CorrelationStamp implements StampInterface
{
    /**
     * @throws InvalidArgumentException when the id is blank
     */
    public function __construct(
        public string $id,
    ) {
        if (trim($id) === '') {
            throw new InvalidArgumentException('A correlation id cannot be blank: it is the trace and saga routing identity, and blank correlations would cross-route unrelated flows into one saga.');
        }
    }
}
