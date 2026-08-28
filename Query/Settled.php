<?php

declare(strict_types=1);

namespace Storm\Story\Query;

use NoDiscard;
use Throwable;

/**
 * One settled query outcome from a combinator run: either ok with the handler's value, or err with the
 * already-unwrapped handler exception. The caller picks how to read it: `unwrap()` raises, `valueOr()`
 * degrades.
 *
 * A value object with a private constructor and named factories, never a service. The value is `mixed`:
 * precise per-result typing waits on the deferred `Query<T>` contract; until queries carry a result type the
 * bus hands back `mixed`, so generics here would buy nothing.
 *
 * @see QueryResults the aggregate it composes into
 */
final readonly class Settled
{
    private function __construct(
        public bool $ok,
        private mixed $value,
        private ?Throwable $error,
    ) {}

    public static function ok(mixed $value): self
    {
        return new self(true, $value, null);
    }

    public static function err(Throwable $error): self
    {
        return new self(false, null, $error);
    }

    /**
     * "all" view: the value, or raise the captured error.
     *
     * Ignoring the result is a bug, since you neither read the value nor asserted success, hence #[\NoDiscard].
     * To call it purely for its throw-or-pass guard, discard explicitly with a `(void)` cast.
     *
     * @throws Throwable the captured handler exception when not ok
     */
    #[NoDiscard('either consume the value or (void)-cast to assert success')]
    public function unwrap(): mixed
    {
        if ($this->error !== null) {
            throw $this->error;
        }

        return $this->value;
    }

    /**
     * Graceful view: the value, or `$default` when this query failed. Never raises.
     */
    #[NoDiscard('the value/default is the whole result of reading a Settled')]
    public function valueOr(mixed $default): mixed
    {
        return $this->ok ? $this->value : $default;
    }

    /**
     * The captured exception, or null when ok.
     */
    #[NoDiscard('the captured exception is the point; drop it and the failure goes silent')]
    public function error(): ?Throwable
    {
        return $this->error;
    }
}
