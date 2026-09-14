<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Middleware;

use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Storm\Chronicler\Exception\StaleVersion;
use Storm\Contracts\Chronicler\ConcurrencyException;
use Storm\Story\Middleware\RecoverConcurrencyConflict;
use Storm\Story\Stamp\BatchModeStamp;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\EventListener\SendFailedMessageForRetryListener;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\RecoverableMessageHandlingException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Retry\MultiplierRetryStrategy;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Transport\InMemory\InMemoryTransport;
use Throwable;

final class ContractsTerminalConflictProofTest extends TestCase
{
    public function test_an_unmarked_contracted_concurrency_conflict_is_terminal_on_delivery(): void
    {
        $envelope = new Envelope(new stdClass, [new ReceivedStamp('async')]);
        $terminal = new class('The decision was already applied') extends RuntimeException implements ConcurrencyException {};
        $handlerFailure = new HandlerFailedException($envelope, ['handler' => $terminal]);
        $handler = $this->createStub(MiddlewareInterface::class);
        $handler->method('handle')->willThrowException($handlerFailure);
        $stack = $this->createStub(StackInterface::class);
        $stack->method('next')->willReturn($handler);
        try {
            new RecoverConcurrencyConflict()->handle($envelope, $stack);
            self::fail('A terminal contract must become unrecoverable.');
        } catch (UnrecoverableMessageHandlingException $e) {
            self::assertSame('The decision was already applied', $e->getMessage());
            self::assertSame($handlerFailure, $e->getPrevious());
        }
    }

    public function test_a_terminal_contract_does_not_schedule_a_transport_retry(): void
    {
        $envelope = new Envelope(new stdClass, [new ReceivedStamp('async')]);
        $terminal = new class('The decision was already applied') extends RuntimeException implements ConcurrencyException {};
        $handlerFailure = new HandlerFailedException($envelope, ['handler' => $terminal]);
        $handler = $this->createStub(MiddlewareInterface::class);
        $handler->method('handle')->willThrowException($handlerFailure);
        $stack = $this->createStub(StackInterface::class);
        $stack->method('next')->willReturn($handler);
        $escaped = null;
        try {
            new RecoverConcurrencyConflict()->handle($envelope, $stack);
        } catch (Throwable $e) {
            $escaped = $e;
        }
        self::assertNotNull($escaped);
        $transport = new InMemoryTransport;
        $listener = new SendFailedMessageForRetryListener(
            new ServiceLocator(['async' => static fn () => $transport]),
            new ServiceLocator(['async' => static fn () => new MultiplierRetryStrategy(maxRetries: 3, delayMilliseconds: 0)]),
        );
        $event = new WorkerMessageFailedEvent($envelope, 'async', $escaped);
        $listener->onMessageFailed($event);
        self::assertCount(0, $transport->getSent(), 'A contracted terminal conflict must not be re-sent by the Messenger retry listener.');
    }

    public function test_a_terminal_contract_is_preserved_during_synchronous_dispatch(): void
    {
        $envelope = new Envelope(new stdClass);
        $terminal = new class() extends RuntimeException implements ConcurrencyException {};
        $failure = new HandlerFailedException($envelope, [$terminal]);
        $this->expectExceptionObject($failure);
        new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($failure));
    }

    public function test_a_terminal_contract_is_terminal_during_batch_dispatch(): void
    {
        $envelope = new Envelope(new stdClass, [new BatchModeStamp]);
        $terminal = new class() extends RuntimeException implements ConcurrencyException {};
        $failure = new HandlerFailedException($envelope, [$terminal]);
        $this->expectException(UnrecoverableMessageHandlingException::class);
        new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($failure));
    }

    public function test_a_retryable_conflict_wins_over_a_nested_terminal_contract(): void
    {
        $envelope = new Envelope(new stdClass, [new ReceivedStamp('async')]);
        $terminal = new class() extends RuntimeException implements ConcurrencyException {};
        $nested = new HandlerFailedException($envelope, [$terminal]);
        $failure = new HandlerFailedException($envelope, [$nested, StaleVersion::forStream('account-1', 9, 10)]);
        $this->expectException(RecoverableMessageHandlingException::class);
        new RecoverConcurrencyConflict()->handle($envelope, $this->throwing($failure));
    }

    private function throwing(HandlerFailedException $failure): StackInterface
    {
        $handler = $this->createStub(MiddlewareInterface::class);
        $handler->method('handle')->willThrowException($failure);
        $stack = $this->createStub(StackInterface::class);
        $stack->method('next')->willReturn($handler);

        return $stack;
    }
}
