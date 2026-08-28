<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Middleware;

use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use RuntimeException;
use stdClass;
use Storm\Chronicler\Exception\DuplicateVersion;
use Storm\Chronicler\Exception\StaleVersion;
use Storm\Contracts\Serializer\SubjectForgotten;
use Storm\Story\Middleware\RecoverConcurrencyConflict;
use Storm\Story\Stamp\BatchModeStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\RecoverableMessageHandlingException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Throwable;

final class RecoverConcurrencyConflictTest extends TestCase
{
    #[Test]
    public function a_stale_version_off_a_transport_becomes_a_recoverable_retry(): void
    {
        $envelope = $this->received();
        $wrapped = new HandlerFailedException($envelope, ['handler' => StaleVersion::forStream('account-1', 9, 10)]);

        try {
            new RecoverConcurrencyConflict(retryDelayMs: 25)->handle($envelope, $this->throwing($wrapped));
            $this->fail('expected a RecoverableMessageHandlingException');
        } catch (RecoverableMessageHandlingException $e) {
            $this->assertSame(25, $e->getRetryDelay(), 'the configured short delay rides on the marker');
            $this->assertSame($wrapped, $e->getPrevious(), 'the original handler failure is preserved as the cause');
        }
    }

    #[Test]
    public function the_retry_delay_backs_off_with_jitter_inside_the_capped_window(): void
    {
        // equal-jitter exponential: the window doubles per prior attempt, the RedeliveryStamp count, up to
        // the cap, and the delay lands in the window's upper half; hot-stream losers spread out
        // instead of re-colliding on the same tick
        $middleware = new RecoverConcurrencyConflict(retryDelayMs: 50, retryMaxDelayMs: 1000);

        foreach ([0 => 50, 1 => 100, 2 => 200, 5 => 1000, 9 => 1000] as $attempts => $window) {
            $envelope = new Envelope(new stdClass, [new ReceivedStamp('async'), new RedeliveryStamp($attempts)]);
            $wrapped = new HandlerFailedException($envelope, ['handler' => StaleVersion::forStream('account-1', 9, 10)]);

            try {
                $middleware->handle($envelope, $this->throwing($wrapped));
                $this->fail('expected a RecoverableMessageHandlingException');
            } catch (RecoverableMessageHandlingException $e) {
                $delay = $e->getRetryDelay();
                $this->assertNotNull($delay);
                $this->assertGreaterThanOrEqual(intdiv($window, 2), $delay, "attempt {$attempts}: at least half the window");
                $this->assertLessThanOrEqual($window, $delay, "attempt {$attempts}: capped by the window");
            }
        }
    }

    #[Test]
    public function the_default_delay_is_zero_an_immediate_redelivery(): void
    {
        // defaults on purpose: unconfigured, the marker carries NO delay, so redeliver at once
        $envelope = new Envelope(new stdClass, [new ReceivedStamp('async'), new RedeliveryStamp(2)]);
        $wrapped = new HandlerFailedException($envelope, ['handler' => StaleVersion::forStream('account-1', 9, 10)]);

        try {
            new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($wrapped));
            $this->fail('expected a RecoverableMessageHandlingException');
        } catch (RecoverableMessageHandlingException $e) {
            $this->assertSame(0, $e->getRetryDelay());
        }
    }

    #[Test]
    public function the_jittered_delay_actually_jitters_below_the_window_top(): void
    {
        // the in-window bounds above cannot see a dead jitter (a constant full-window delay sits
        // inside them): sample the delay and require a draw BELOW the top. P(30 draws all landing
        // on the single top value of a 501-wide half-window) ≈ (1/501)^30, not a flake source.
        $middleware = new RecoverConcurrencyConflict(retryDelayMs: 1000, retryMaxDelayMs: 1000);
        $delays = [];

        for ($i = 0; $i < 30; $i++) {
            $envelope = new Envelope(new stdClass, [new ReceivedStamp('async'), new RedeliveryStamp(0)]);
            $wrapped = new HandlerFailedException($envelope, ['handler' => StaleVersion::forStream('account-1', 9, 10)]);

            try {
                $middleware->handle($envelope, $this->throwing($wrapped));
                $this->fail('expected a RecoverableMessageHandlingException');
            } catch (RecoverableMessageHandlingException $e) {
                $delays[] = $e->getRetryDelay();
            }
        }

        $this->assertLessThan(1000, min($delays), 'a dead jitter would always return the window top');
    }

    #[Test]
    public function the_jittered_delay_never_falls_below_the_window_half(): void
    {
        // the floor needs sampling too, for the mirror reason: a widened lower bound still lands
        // inside the window most of the time, so a single draw sees it only when the draw happens
        // to fall in the widened part. P(60 draws all missing a quarter-sized gap) = 0.75^60,
        // about one in thirty million; the true floor holds on EVERY draw, so this cannot flake.
        $middleware = new RecoverConcurrencyConflict(retryDelayMs: 1000, retryMaxDelayMs: 1000);
        $delays = [];

        for ($i = 0; $i < 60; $i++) {
            $envelope = new Envelope(new stdClass, [new ReceivedStamp('async'), new RedeliveryStamp(0)]);
            $wrapped = new HandlerFailedException($envelope, ['handler' => StaleVersion::forStream('account-1', 9, 10)]);

            try {
                $middleware->handle($envelope, $this->throwing($wrapped));
                $this->fail('expected a RecoverableMessageHandlingException');
            } catch (RecoverableMessageHandlingException $e) {
                $delays[] = $e->getRetryDelay();
            }
        }

        $this->assertGreaterThanOrEqual(500, min($delays), 'the delay must sit in the window UPPER half');
    }

    #[Test]
    public function without_a_max_delay_the_fixed_base_is_kept(): void
    {
        // retryMaxDelayMs 0 = the pre-jitter behavior: no growth, no randomness, whatever the attempt count
        $envelope = new Envelope(new stdClass, [new ReceivedStamp('async'), new RedeliveryStamp(4)]);
        $wrapped = new HandlerFailedException($envelope, ['handler' => StaleVersion::forStream('account-1', 9, 10)]);

        try {
            new RecoverConcurrencyConflict(retryDelayMs: 25)->handle($envelope, $this->throwing($wrapped));
            $this->fail('expected a RecoverableMessageHandlingException');
        } catch (RecoverableMessageHandlingException $e) {
            $this->assertSame(25, $e->getRetryDelay());
        }
    }

    #[Test]
    #[Group('adversarial')]
    public function a_stale_version_from_a_nested_sync_dispatch_still_retries_forward(): void
    {
        // a handler dispatching SYNCHRONOUSLY wraps the leaf in a second HandlerFailedException; a
        // one-level read would miss it and the command would dead-letter on infra-tuned retries
        $envelope = $this->received();
        $inner = new HandlerFailedException($envelope, ['inner' => StaleVersion::forStream('account-1', 9, 10)]);
        $wrapped = new HandlerFailedException($envelope, ['outer' => $inner]);

        $this->expectException(RecoverableMessageHandlingException::class);

        new RecoverConcurrencyConflict(retryDelayMs: 25)->handle($envelope, $this->throwing($wrapped));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_duplicate_version_from_a_nested_sync_dispatch_is_still_condemned(): void
    {
        // the terminal twin of the nested case: an unseen leaf would burn the whole retry budget
        // before reaching the failure transport, the exact thing this middleware exists to prevent
        $envelope = $this->received();
        $inner = new HandlerFailedException($envelope, ['inner' => DuplicateVersion::forStream('account-1')]);
        $wrapped = new HandlerFailedException($envelope, ['outer' => $inner]);

        $this->expectException(UnrecoverableMessageHandlingException::class);

        new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($wrapped));
    }

    #[Test]
    public function a_duplicate_version_off_a_transport_becomes_unrecoverable(): void
    {
        $envelope = $this->received();
        $wrapped = new HandlerFailedException($envelope, ['handler' => DuplicateVersion::forStream('account-1')]);

        $this->expectException(UnrecoverableMessageHandlingException::class);

        new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($wrapped));
    }

    #[Test]
    public function a_forgotten_subject_off_a_transport_becomes_unrecoverable(): void
    {
        // the tombstone is durable by design: no retry can un-forget the subject, so retrying only
        // delays the dead-letter an operator must see
        $envelope = $this->received();
        $forgotten = new class('Subject [cus-1] is forgotten.') extends RuntimeException implements SubjectForgotten {};
        $wrapped = new HandlerFailedException($envelope, ['handler' => $forgotten]);

        $this->expectException(UnrecoverableMessageHandlingException::class);

        new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($wrapped));
    }

    #[Test]
    public function a_stale_version_wins_over_a_duplicate_when_both_are_wrapped(): void
    {
        $envelope = $this->received();
        $wrapped = new HandlerFailedException($envelope, [
            'a' => DuplicateVersion::forStream('account-1'),
            'b' => StaleVersion::forStream('account-1', 9, 10),
        ]);

        // a recoverable conflict anywhere means progress is still possible, so prefer retry-forward
        $this->expectException(RecoverableMessageHandlingException::class);

        new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($wrapped));
    }

    #[Test]
    public function a_non_concurrency_failure_propagates_untouched(): void
    {
        $envelope = $this->received();
        $original = new HandlerFailedException($envelope, ['handler' => new RuntimeException('boom')]);

        try {
            new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($original));
            $this->fail('expected the original failure to propagate');
        } catch (Throwable $e) {
            $this->assertSame($original, $e, 'a non-OCC failure keeps its infra retry budget — not translated');
        }
    }

    #[Test]
    public function an_in_process_dispatch_is_not_translated(): void
    {
        $envelope = new Envelope(new stdClass); // no ReceivedStamp: a sync, worker-less dispatch
        $original = new HandlerFailedException($envelope, ['handler' => StaleVersion::forStream('account-1', 9, 10)]);

        try {
            new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($original));
            $this->fail('expected the original StaleVersion failure to propagate on the sync path');
        } catch (Throwable $e) {
            $this->assertSame($original, $e, 'no transport behind a sync dispatch → nothing to retry, so do not translate');
        }
    }

    #[Test]
    public function a_clean_handle_passes_through(): void
    {
        $envelope = $this->received();

        $this->assertSame($envelope, new RecoverConcurrencyConflict()->handle($envelope, $this->passing($envelope)));
    }

    #[Test]
    public function a_stale_version_in_a_batch_becomes_a_recoverable_retry(): void
    {
        // the batch carries no ReceivedStamp by design; BatchModeStamp is its consume proof, and
        // it earns the SAME translation: without it a hot-stream loser got two 100ms-spaced
        // isolation attempts then a poison capture, where the Worker path redelivers to progress
        $envelope = $this->batched();
        $wrapped = new HandlerFailedException($envelope, ['handler' => StaleVersion::forStream('account-1', 9, 10)]);

        try {
            new RecoverConcurrencyConflict(retryDelayMs: 25)->handle($envelope, $this->throwing($wrapped));
            $this->fail('expected a RecoverableMessageHandlingException');
        } catch (RecoverableMessageHandlingException $e) {
            $this->assertSame(25, $e->getRetryDelay(), 'the batch redelivery rides the same configured delay');
        }
    }

    #[Test]
    public function a_duplicate_version_in_a_batch_becomes_unrecoverable_at_once(): void
    {
        // terminal must stay terminal under batch: the isolation replay must never re-run a
        // handler whose duplicate already proves the append landed
        $envelope = $this->batched();
        $wrapped = new HandlerFailedException($envelope, ['handler' => DuplicateVersion::forStream('account-1')]);

        $this->expectException(UnrecoverableMessageHandlingException::class);

        new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($wrapped));
    }

    #[Test]
    public function a_forgotten_subject_in_a_batch_becomes_unrecoverable_at_once(): void
    {
        $envelope = $this->batched();
        $forgotten = new class('Subject [cus-1] is forgotten.') extends RuntimeException implements SubjectForgotten {};
        $wrapped = new HandlerFailedException($envelope, ['handler' => $forgotten]);

        $this->expectException(UnrecoverableMessageHandlingException::class);

        new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($wrapped));
    }

    #[Test]
    public function zero_delay_returns_zero_even_with_max_delay(): void
    {
        $middleware = new RecoverConcurrencyConflict(retryDelayMs: 0, retryMaxDelayMs: 1000);
        $envelope = new Envelope(new stdClass, [new ReceivedStamp('async'), new RedeliveryStamp(2)]);
        $wrapped = new HandlerFailedException($envelope, ['handler' => StaleVersion::forStream('account-1', 9, 10)]);

        try {
            $middleware->handle($envelope, $this->throwing($wrapped));
            $this->fail('expected a RecoverableMessageHandlingException');
        } catch (RecoverableMessageHandlingException $e) {
            $this->assertSame(0, $e->getRetryDelay());
        }
    }

    #[Test]
    public function defaults_to_zero_delay_and_zero_max_delay(): void
    {
        $middleware = new RecoverConcurrencyConflict;
        $refClass = new ReflectionClass($middleware);

        $this->assertSame(0, $refClass->getProperty('retryDelayMs')->getValue($middleware));
        $this->assertSame(0, $refClass->getProperty('retryMaxDelayMs')->getValue($middleware));
    }

    private function received(): Envelope
    {
        return new Envelope(new stdClass, [new ReceivedStamp('async')]);
    }

    private function batched(): Envelope
    {
        return new Envelope(new stdClass, [new BatchModeStamp]);
    }

    private function throwing(Throwable $e): StackInterface
    {
        return new readonly class($e) implements StackInterface
        {
            public function __construct(private Throwable $e) {}

            public function next(): MiddlewareInterface
            {
                return new readonly class($this->e) implements MiddlewareInterface
                {
                    public function __construct(private Throwable $e) {}

                    public function handle(Envelope $envelope, StackInterface $stack): Envelope
                    {
                        throw $this->e;
                    }
                };
            }
        };
    }

    private function passing(Envelope $return): StackInterface
    {
        return new readonly class($return) implements StackInterface
        {
            public function __construct(private Envelope $return) {}

            public function next(): MiddlewareInterface
            {
                return new readonly class($this->return) implements MiddlewareInterface
                {
                    public function __construct(private Envelope $return) {}

                    public function handle(Envelope $envelope, StackInterface $stack): Envelope
                    {
                        return $this->return;
                    }
                };
            }
        };
    }
}
