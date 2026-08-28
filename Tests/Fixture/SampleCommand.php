<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Fixture;

use Symfony\Component\Validator\Constraints as Assert;

/**
 * A command fixture carrying a structural constraint, to exercise the validation middleware.
 */
final readonly class SampleCommand
{
    public function __construct(
        #[Assert\NotBlank]
        public string $reference = '',
    ) {}
}
