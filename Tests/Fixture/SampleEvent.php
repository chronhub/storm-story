<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Fixture;

use Storm\Contracts\Message\DomainEvent;

final class SampleEvent implements DomainEvent
{
    public function __construct(
        public string $what,
    ) {}

    public function aggregateId(): string
    {
        return 'sample';
    }

    public function toPayload(): array
    {
        return ['what' => $this->what];
    }

    public static function fromPayload(array $payload): static
    {
        return new self((string) $payload['what']);
    }
}
