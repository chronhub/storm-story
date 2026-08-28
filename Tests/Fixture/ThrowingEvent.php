<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Fixture;

use InvalidArgumentException;
use Storm\Contracts\Message\DomainEvent;

/**
 * A domain event whose fromPayload rejects bad content with a plain exception, so a transport test
 * can prove the neutral serializer turns a poison payload into a MessageDecodingFailedException
 * rather than letting a raw exception crash the worker.
 */
final class ThrowingEvent implements DomainEvent
{
    public function __construct(public int $amount = 0) {}

    public function aggregateId(): string
    {
        return 'sample';
    }

    public function toPayload(): array
    {
        return ['amount' => $this->amount];
    }

    public static function fromPayload(array $payload): static
    {
        if (! isset($payload['amount']) || ! is_int($payload['amount'])) {
            throw new InvalidArgumentException('amount must be a present int');
        }

        return new self($payload['amount']);
    }
}
