<?php

declare(strict_types=1);

namespace Storm\Story\Consume;

use Storm\Story\Console\ConsumeBatchedCommand;
use Throwable;

/**
 * What the consumer tells the transport for one envelope after a batch round: ack, reject carrying the
 * failure that condemned it, or redeliver carrying the delay a still-recoverable failure asked for. The
 * loop routes a reject to the failure transport, and the captured row needs its cause via
 * `ErrorDetailsStamp`, so the decision transports it; a redeliver goes back to the origin transport
 * delayed, so the decision transports the delay.
 *
 * @see BatchConsumer
 * @see ConsumeBatchedCommand the loop that materializes these
 */
final readonly class BatchDecision
{
    private function __construct(
        public bool $ack,
        public ?Throwable $error,
        /** Non-null = redeliver: re-send to the origin transport delayed by this many milliseconds, then ack. */
        public ?int $redeliverDelayMs = null,
    ) {}

    public static function ack(): self
    {
        return new self(true, null);
    }

    /**
     * @param  Throwable  $error  the failure that condemned the message; never null, since a reject without
     *                            a cause would capture an uninspectable failure row
     */
    public static function reject(Throwable $error): self
    {
        return new self(false, $error);
    }

    /**
     * A declared-recoverable failure that outlived the in-process second chance: not condemned, sent back
     * to its own transport to try again after the delay, the broker holding the wait.
     *
     * @param  Throwable  $error  the recoverable failure, kept for the loop's logging; the message is sound
     * @param  int  $delayMs  how long the broker holds the redelivery, from the exception's own retryDelay
     *                        or the consumer's default
     */
    public static function redeliver(Throwable $error, int $delayMs): self
    {
        return new self(false, $error, $delayMs);
    }
}
