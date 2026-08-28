<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Stamp;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Storm\Story\Stamp\ContextBagStamp;

final class ContextBagStampTest extends TestCase
{
    #[Test]
    public function carries_scalar_values_under_application_keys(): void
    {
        $stamp = new ContextBagStamp(['origin' => 'http', 'attempt' => 2]);

        $this->assertSame(['origin' => 'http', 'attempt' => 2], $stamp->values);
    }

    #[Test]
    #[Group('adversarial')]
    public function an_empty_bag_is_refused_absence_is_no_stamp_at_all(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageIsOrContains('context bag stamp cannot be empty');

        new ContextBagStamp([]);
    }

    #[Test]
    #[Group('adversarial')]
    #[DataProvider('blank_keys')]
    public function a_blank_or_padded_bag_key_is_refused(string $key): void
    {
        // The bag's keys become header names, so a blank one names a header nobody can read back and
        // nobody can declare as propagated. A padded one is worse: accepted here, "x " and "x" would
        // ride as two silently distinct headers, and Message's own door refuses it mid-enrichment,
        // far from this producer; the boundary refuses first.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageIsOrContains('non-blank and unpadded');

        new ContextBagStamp([$key => 'value']);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function blank_keys(): iterable
    {
        yield 'the empty string' => [''];
        yield 'a single space' => [' '];
        yield 'a tab' => ["\t"];
        yield 'a leading pad' => [' x'];
        yield 'a trailing pad' => ['x '];
    }

    #[Test]
    public function an_integer_bag_key_is_validated_not_died_on(): void
    {
        // PHP normalizes '7' to int 7 at array write; under strict types an uncast key would die
        // on trim() with a TypeError the contract never declared
        $stamp = new ContextBagStamp(['7' => 'value']); // @phpstan-ignore argument.type (hostile on purpose: the guard under test)

        $this->assertSame(['7' => 'value'], $stamp->values);
    }

    #[Test]
    #[Group('adversarial')]
    public function the_reserved_framework_prefix_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageIsOrContains('__');

        new ContextBagStamp(['__correlation_id' => 'smuggled']);
    }
}
