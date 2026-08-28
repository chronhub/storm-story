<?php

declare(strict_types=1);

namespace Storm\Story\Outbox;

use InvalidArgumentException;
use Override;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;

/**
 * A sender split into N shards, the partitioned-consumer shape: every envelope routes to the shard
 * its correlation id hashes to, so all the events of ONE correlation ride ONE queue and keep their
 * order by construction, while distinct correlations spread across the shards for scale-out. The
 * hash is `crc32`, stable across processes and deployments, deliberately not cryptographic; the
 * shard count is fixed by the wiring, and re-sharding is a renumbering, safe on poll queues drained
 * to empty.
 *
 * The key precedence: the `CorrelationStamp`, the identity the saga routing keys on; then the
 * `MessageIdStamp`, so a correlation-less envelope still routes deterministically; then the empty
 * string. The composite implements `SenderInterface`, so any single-transport seam, the signal lane
 * or one of its criticality splits, shards by receiving this in place of the bare transport.
 *
 * @see MessengerOutboxPublisher the publisher whose lane senders this composes behind
 */
final readonly class ShardedSender implements SenderInterface
{
    /**
     * @param  list<SenderInterface>  $shards
     *
     * @throws InvalidArgumentException when no shard is given
     */
    public function __construct(
        private array $shards,
    ) {
        if ($this->shards === []) {
            throw new InvalidArgumentException('A sharded sender needs at least one shard.');
        }
    }

    #[Override]
    public function send(Envelope $envelope): Envelope
    {
        return $this->shards[$this->shardFor($envelope)]->send($envelope);
    }

    /**
     * The stable shard index of this envelope: same key, same shard, whatever the process.
     *
     * @throws InvalidArgumentException when the envelope carries neither a correlation nor a
     *                                  message id; ordering for a key that does not exist is
     *                                  meaningless, and a silent default would drain every such
     *                                  envelope onto shard 0, unbalancing the fan-out with no
     *                                  signal
     */
    private function shardFor(Envelope $envelope): int
    {
        $key = $envelope->last(CorrelationStamp::class)->id
            ?? $envelope->last(MessageIdStamp::class)->id
            ?? throw new InvalidArgumentException(sprintf(
                'A sharded sender needs a CorrelationStamp or MessageIdStamp to key on; the %s envelope carries neither.',
                $envelope->getMessage()::class,
            ));

        return (crc32($key) & 0x7FFFFFFF) % count($this->shards);
    }
}
