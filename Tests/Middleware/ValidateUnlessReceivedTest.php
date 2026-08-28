<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Middleware;

use Closure;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use stdClass;
use Storm\Story\Middleware\ValidateUnlessReceived;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

final class ValidateUnlessReceivedTest extends TestCase
{
    #[Test]
    public function validates_a_fresh_dispatch(): void
    {
        $validation = $this->spy();
        $ran = 0;

        new ValidateUnlessReceived($validation)->handle(
            new Envelope(new stdClass), // no ReceivedStamp, a fresh boundary/in-process dispatch
            $this->terminal(function () use (&$ran): void {
                $ran++;
            }),
        );

        $this->assertSame(1, $validation->calls); // validation ran
        $this->assertSame(1, $ran);               // and the rest of the stack after it
    }

    #[Test]
    public function skips_validation_for_a_message_received_from_a_transport(): void
    {
        $validation = $this->spy();
        $ran = 0;

        new ValidateUnlessReceived($validation)->handle(
            new Envelope(new stdClass, [new ReceivedStamp('async')]), // came off a transport = already validated pre-queue
            $this->terminal(function () use (&$ran): void {
                $ran++;
            }),
        );

        $this->assertSame(0, $validation->calls); // validation SKIPPED
        $this->assertSame(1, $ran);               // the rest of the stack still ran
    }

    /**
     * @return MiddlewareInterface&object{calls: int}
     */
    private function spy(): MiddlewareInterface
    {
        return new class() implements MiddlewareInterface
        {
            public int $calls = 0;

            public function handle(Envelope $envelope, StackInterface $stack): Envelope
            {
                $this->calls++;

                return $stack->next()->handle($envelope, $stack); // well-behaved: continue the stack
            }
        };
    }

    private function terminal(Closure $onHandle): StackInterface
    {
        return new readonly class($onHandle) implements StackInterface
        {
            public function __construct(private Closure $onHandle) {}

            public function next(): MiddlewareInterface
            {
                return new readonly class($this->onHandle) implements MiddlewareInterface
                {
                    public function __construct(private Closure $onHandle) {}

                    public function handle(Envelope $envelope, StackInterface $stack): Envelope
                    {
                        ($this->onHandle)();

                        return $envelope;
                    }
                };
            }
        };
    }
}
