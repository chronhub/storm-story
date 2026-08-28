<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Outbox;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use stdClass;
use Storm\Story\Outbox\ShardedSender;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;

final class ShardedSenderTest extends TestCase
{
    #[Test]
    public function the_same_correlation_always_rides_the_same_shard(): void
    {
        $shards = [$this->capturingSender(), $this->capturingSender()];
        $sender = new ShardedSender($shards);

        $sender->send($this->envelope(correlation: 'saga-1'));
        $sender->send($this->envelope(correlation: 'saga-1'));
        $sender->send($this->envelope(correlation: 'saga-1'));

        $counts = [count($shards[0]->envelopes), count($shards[1]->envelopes)];
        sort($counts);
        $this->assertSame([0, 3], $counts, 'one correlation must land on exactly one shard, every time');
    }

    #[Test]
    public function distinct_correlations_spread_across_the_shards(): void
    {
        $shards = [$this->capturingSender(), $this->capturingSender()];
        $sender = new ShardedSender($shards);

        for ($i = 0; $i < 16; $i++) {
            $sender->send($this->envelope(correlation: 'saga-'.$i));
        }

        $this->assertNotEmpty($shards[0]->envelopes, 'crc32 over 16 distinct keys must hit shard 0');
        $this->assertNotEmpty($shards[1]->envelopes, 'crc32 over 16 distinct keys must hit shard 1');
        $this->assertSame(16, count($shards[0]->envelopes) + count($shards[1]->envelopes));
    }

    #[Test]
    public function a_correlationless_envelope_routes_deterministically_by_message_id(): void
    {
        // The key must be the id, not the empty string the chain ends on. `evt-9` is chosen because
        // it lands on the OTHER shard than the empty key does; `evt-7` shares shard 0 with it, and a
        // fixture that collides like that cannot tell the fallback from its own floor.
        $shards = [$this->capturingSender(), $this->capturingSender()];
        $sender = new ShardedSender($shards);

        $sender->send($this->envelope(messageId: 'evt-9'));
        $sender->send($this->envelope(messageId: 'evt-9'));

        $this->assertCount(0, $shards[0]->envelopes);
        $this->assertCount(2, $shards[1]->envelopes); // shard 1, where the id hashes, not shard 0
    }

    #[Test]
    public function a_keyless_envelope_is_refused_never_silently_drained_onto_shard_zero(): void
    {
        // ordering for a key that does not exist is meaningless: the old empty-key fallback hashed
        // every such envelope onto shard 0, unbalancing the fan-out with no signal; a generic
        // sender seam must refuse instead
        $sender = new ShardedSender([$this->capturingSender(), $this->capturingSender()]);

        $this->expectException(InvalidArgumentException::class);

        $sender->send(new Envelope(new stdClass));
    }

    #[Test]
    #[Group('adversarial')]
    public function the_correlation_outranks_the_message_id_when_both_are_carried(): void
    {
        // THE rule the lane exists for: one saga rides one shard, so its per-saga order is a property
        // of the routing rather than of luck. Every message of a saga carries a distinct id, so an id
        // consulted first would scatter one saga across every shard and lose that order. The two keys
        // here hash to different shards on purpose, which is the only way the priority is visible.
        $shards = [$this->capturingSender(), $this->capturingSender()];
        $sender = new ShardedSender($shards);

        $sender->send($this->envelope(correlation: 'saga-1', messageId: 'evt-7'));

        $this->assertCount(0, $shards[0]->envelopes); // where evt-7 alone would have gone
        $this->assertCount(1, $shards[1]->envelopes); // where saga-1 goes
    }

    #[Test]
    public function a_single_shard_takes_everything(): void
    {
        $only = $this->capturingSender();
        $sender = new ShardedSender([$only]);

        $sender->send($this->envelope(correlation: 'saga-1'));
        $sender->send($this->envelope(messageId: 'evt-1'));

        $this->assertCount(2, $only->envelopes);
    }

    #[Test]
    public function refuses_an_empty_shard_list(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new ShardedSender([]);
    }

    private function envelope(?string $correlation = null, ?string $messageId = null): Envelope
    {
        $stamps = [];
        if ($correlation !== null) {
            $stamps[] = new CorrelationStamp($correlation);
        }
        if ($messageId !== null) {
            $stamps[] = new MessageIdStamp($messageId);
        }

        return new Envelope(new stdClass, $stamps);
    }

    /**
     * @return SenderInterface&object{envelopes: list<Envelope>}
     */
    private function capturingSender(): SenderInterface
    {
        return new class() implements SenderInterface
        {
            /** @var list<Envelope> */
            public array $envelopes = [];

            public function send(Envelope $envelope): Envelope
            {
                $this->envelopes[] = $envelope;

                return $envelope;
            }
        };
    }
}
