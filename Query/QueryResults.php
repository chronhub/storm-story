<?php

declare(strict_types=1);

namespace Storm\Story\Query;

use Countable;
use IteratorAggregate;
use NoDiscard;
use Override;
use Throwable;
use Traversable;

/**
 * The aggregate of a settled multi-query run via `QueryBus::askAllSettled`: one `Settled` per input key,
 * in input order. Already executed, so the caller picks the read mode:
 *
 * - `unwrapAll()` raises the first error;
 * - `assertAllOk()` raises but returns nothing;
 * - `valuesOrNull()`, `okValues()` and `errors()` degrade gracefully;
 * - Or iterate it directly, as an `IteratorAggregate` over `Settled` that is also `Countable`.
 *
 * The run is sequential, not concurrent; it borrows the `Promise.allSettled` vocabulary of partial-failure
 * aggregation, not its parallelism. A value object, never a service.
 *
 * Every value-returning read carries #[\NoDiscard]: the run already happened, so dropping the result means
 * the call computed nothing, a bug. Three members are deliberately unmarked:
 *
 * - `from()`, the factory: the dominant return-or-assign position already consumes it, so the attribute
 *   would only ever catch the pathological bare call, not worth the noise;
 *
 * - `assertAllOk()`: its `void` return is the intent;
 *
 * - `getIterator()`: consumed implicitly by `foreach`, never assigned.
 *
 * @implements IteratorAggregate<array-key, Settled>
 */
final readonly class QueryResults implements Countable, IteratorAggregate
{
    /**
     * @param  array<array-key, Settled>  $settled
     */
    private function __construct(private array $settled) {}

    /**
     * @param  array<array-key, Settled>  $settled
     */
    public static function from(array $settled): self
    {
        return new self($settled);
    }

    /**
     * "all" view: every value, keyed; raises the first error.
     *
     * @return array<array-key, mixed>
     *
     * @throws Throwable the first captured handler exception
     */
    #[NoDiscard('the keyed values are the whole output; to only assert success use assertAllOk()')]
    public function unwrapAll(): array
    {
        return array_map(static fn (Settled $s) => $s->unwrap(), $this->settled);
    }

    /**
     * Guard view: raises the first captured error, returns nothing on success. The `void` return is
     * deliberate: this is the "all ok or throw" intent that does not consume the values, which is also why
     * it carries no #[\NoDiscard], since calling it for its potential throw is the entire point.
     *
     * @throws Throwable the first captured handler exception
     */
    public function assertAllOk(): void
    {
        foreach ($this->settled as $settled) {
            // consumed purely for the throw-or-pass guard; the value is intentionally discarded
            (void) $settled->unwrap();
        }
    }

    /**
     * Graceful view: every key mapped to its value, or null when that query failed. Never raises.
     *
     * @return array<array-key, mixed>
     */
    #[NoDiscard('graceful read; discarding it computed nothing')]
    public function valuesOrNull(): array
    {
        return array_map(static fn (Settled $s) => $s->valueOr(null), $this->settled);
    }

    /**
     * Only the successful results, keyed; the failed keys are dropped.
     *
     * @return array<array-key, mixed>
     */
    #[NoDiscard('the successful subset is the output')]
    public function okValues(): array
    {
        $out = [];

        foreach ($this->settled as $key => $settled) {
            if ($settled->ok) {
                // valueOr, which has no @throws, over unwrap, which does: under the ok guard it can never raise,
                // so reading the non-throwing path keeps this method honestly throws-free.
                $out[$key] = $settled->valueOr(null);
            }
        }

        return $out;
    }

    /**
     * Only the failures, keyed, for logging, telemetry or amber panels.
     *
     * @return array<array-key, Throwable>
     */
    #[NoDiscard('errors are for logging/telemetry; dropping them defeats the call')]
    public function errors(): array
    {
        $out = [];

        foreach ($this->settled as $key => $settled) {
            $error = $settled->error();

            if ($error !== null) {
                $out[$key] = $error;
            }
        }

        return $out;
    }

    /**
     * The raw outcomes, keyed: the "allSettled" view for a custom per-entry read.
     *
     * @return array<array-key, Settled>
     */
    #[NoDiscard('raw outcomes for a custom per-entry read')]
    public function settled(): array
    {
        return $this->settled;
    }

    #[NoDiscard('a boolean check is meaningless if its answer is ignored')]
    public function hasErrors(): bool
    {
        return array_any($this->settled, fn ($settled) => ! $settled->ok);
    }

    /**
     * A fresh, key-preserving iteration per `foreach`; a generator rather than an `ArrayIterator`
     * so no object-backing mode is even reachable. Not rewindable; that is irrelevant to the
     * aggregate contract, which mints a new iterator per loop.
     *
     * @return Traversable<array-key, Settled>
     */
    #[Override]
    public function getIterator(): Traversable
    {
        yield from $this->settled;
    }

    #[NoDiscard('the count is the whole result')]
    #[Override]
    public function count(): int
    {
        return count($this->settled);
    }
}
