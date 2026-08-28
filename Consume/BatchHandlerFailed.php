<?php

declare(strict_types=1);

namespace Storm\Story\Consume;

use RuntimeException;
use Throwable;

/**
 * Classification marker for the batched consume: separates a HANDLER failure, a poison candidate the
 * isolation replay owns, from an infrastructure failure of the batch machinery itself, a transaction
 * begin/commit or an inbox INSERT, which must propagate and leave the deliveries un-acked.
 *
 * `BatchConsumer` cannot tell the two apart from outside `onceBatch()`/`once()`: both surface as "the
 * store threw". So the consumer wraps every bus-dispatch failure in this marker at the only place a
 * handler can fail, its own dispatch seam, and classifies by construction: a marker means "the handler
 * threw, replay/condemn this message", anything else means "the batch machinery failed, this is
 * systemic, not a poison; do not condemn a healthy queue for it".
 *
 * @internal to the batch consume path; never leaves `BatchConsumer`, decisions carry the unwrapped
 *           handler failure, and a systemic failure propagates as its original type
 *
 * @see BatchConsumer::dispatch() the single wrap site
 */
final class BatchHandlerFailed extends RuntimeException
{
    public static function wrap(Throwable $handlerFailure): self
    {
        return new self('A batched handler failed: '.$handlerFailure->getMessage(), previous: $handlerFailure);
    }

    /**
     * The handler's own failure, what a reject decision and the failure transport must carry.
     */
    public function handlerFailure(): Throwable
    {
        return $this->getPrevious() ?? $this;
    }
}
