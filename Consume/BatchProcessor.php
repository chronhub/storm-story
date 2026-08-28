<?php

declare(strict_types=1);

namespace Storm\Story\Consume;

use Symfony\Component\Messenger\Envelope;

/**
 * Turns a received batch into a per-envelope ack, reject or redeliver decision: the seam between the
 * transport receive loop `ConsumeBatchedCommand` and the transactional batch plus poison isolation of
 * `BatchConsumer`. The loop owns receiving and acking; the processor owns the one-transaction batch.
 */
interface BatchProcessor
{
    /**
     * Decides, for each received envelope, whether the transport should ack, reject or redeliver it.
     *
     * @param  list<Envelope>  $envelopes  all received from the one transport named `$consumer`
     * @return list<BatchDecision> one decision per envelope, in input order
     */
    public function process(string $consumer, array $envelopes): array;
}
