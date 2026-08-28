<?php

declare(strict_types=1);

namespace Storm\Story\Tests;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Storm\Story\Query\QueryResults;
use Storm\Story\Query\Settled;

final class QueryResultsTest extends TestCase
{
    #[Test]
    public function unwrap_all_returns_the_values_keyed(): void
    {
        $settled = [
            'a' => Settled::ok(1),
            'b' => Settled::ok(2),
        ];

        $results = QueryResults::from($settled);

        $this->assertSame($settled, $results->settled());
        $this->assertSame(['a' => 1, 'b' => 2], $results->unwrapAll());
        $this->assertFalse($results->hasErrors());
    }

    #[Test]
    public function unwrap_all_raises_the_first_error(): void
    {
        $boom = new RuntimeException('boom');
        $results = QueryResults::from([
            'a' => Settled::ok(1),
            'b' => Settled::err($boom),
        ]);

        $this->expectExceptionObject($boom);
        (void) $results->unwrapAll(); // called purely for its throw; discard explicitly per #[\NoDiscard]
    }

    #[Test]
    public function assert_all_ok_returns_nothing_when_every_query_succeeded(): void
    {
        $results = QueryResults::from([
            'a' => Settled::ok(1),
            'b' => Settled::ok(2),
        ]);

        // the behavior is the absence of a throw; assertAllOk neither returns nor consumes the values
        $this->expectNotToPerformAssertions();
        $results->assertAllOk();
    }

    #[Test]
    public function assert_all_ok_raises_the_first_error(): void
    {
        $boom = new RuntimeException('boom');
        $results = QueryResults::from([
            'a' => Settled::ok(1),
            'b' => Settled::err($boom),
        ]);

        $this->expectExceptionObject($boom);
        $results->assertAllOk();
    }

    #[Test]
    public function values_or_null_maps_a_failure_to_null(): void
    {
        $results = QueryResults::from([
            'a' => Settled::ok(1),
            'b' => Settled::err(new RuntimeException),
        ]);

        $this->assertSame(['a' => 1, 'b' => null], $results->valuesOrNull());
    }

    #[Test]
    public function ok_values_keeps_only_the_successes(): void
    {
        $results = QueryResults::from([
            'a' => Settled::ok(1),
            'b' => Settled::err(new RuntimeException),
            'c' => Settled::ok(3),
        ]);

        $this->assertSame(['a' => 1, 'c' => 3], $results->okValues());
    }

    #[Test]
    public function errors_keeps_only_the_failures(): void
    {
        // Two failures, not one: a map built from a single-entry fixture proves nothing about
        // completeness, and the caller logs what this returns. One error swallowed is one incident
        // an amber panel never shows.
        $boom = new RuntimeException('boom');
        $bang = new RuntimeException('bang');
        $results = QueryResults::from([
            'a' => Settled::ok(1),
            'b' => Settled::err($boom),
            'c' => Settled::ok(2),
            'd' => Settled::err($bang),
        ]);

        $this->assertSame(['b' => $boom, 'd' => $bang], $results->errors()); // keyed, and all of them
        $this->assertTrue($results->hasErrors());
    }

    #[Test]
    public function iterates_over_the_settled_outcomes_and_counts_them(): void
    {
        $results = QueryResults::from([
            'a' => Settled::ok(1),
            'b' => Settled::err(new RuntimeException),
        ]);

        $this->assertCount(2, $results);

        $keys = [];
        foreach ($results as $key => $settled) {
            $this->assertInstanceOf(Settled::class, $settled);
            $keys[] = $key;
        }

        $this->assertSame(['a', 'b'], $keys);
    }
}
