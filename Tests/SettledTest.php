<?php

declare(strict_types=1);

namespace Storm\Story\Tests;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Storm\Story\Query\Settled;

final class SettledTest extends TestCase
{
    #[Test]
    public function ok_unwraps_to_the_value(): void
    {
        $settled = Settled::ok('result-42');

        $this->assertTrue($settled->ok);
        $this->assertSame('result-42', $settled->unwrap());
        $this->assertNull($settled->error());
    }

    #[Test]
    public function err_rethrows_the_captured_error_on_unwrap(): void
    {
        $boom = new RuntimeException('boom');
        $settled = Settled::err($boom);

        $this->assertFalse($settled->ok);
        $this->assertSame($boom, $settled->error());

        $this->expectExceptionObject($boom);
        (void) $settled->unwrap(); // called purely for its throw; discard explicitly per #[\NoDiscard]
    }

    #[Test]
    public function value_or_returns_the_value_when_ok(): void
    {
        $this->assertSame('v', Settled::ok('v')->valueOr('default'));
    }

    #[Test]
    public function value_or_returns_the_default_when_err(): void
    {
        $this->assertSame('default', Settled::err(new RuntimeException)->valueOr('default'));
    }
}
