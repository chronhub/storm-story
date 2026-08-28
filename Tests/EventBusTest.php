<?php

declare(strict_types=1);

namespace Storm\Story\Tests;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Storm\Story\Tests\Fixture\SampleEvent;
use Symfony\Component\Messenger\Handler\HandlerDescriptor;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Stamp\HandledStamp;

/**
 * The event bus's fan-out contract. Storm's event bus is a plain Symfony Messenger bus configured
 * `allow_no_handlers` via StormBundle::prependExtension; it owns no dispatch logic of its own, so this
 * pins the Messenger guarantee it leans on: a dispatched event reaches EVERY registered handler, once
 * each, in the order the handler locator yields them, the order where a handler's priority materializes
 * after the compile-time MessengerPass sort. The command and query buses require exactly one handler;
 * this multi-handler fan-out is the event bus's alone.
 */
final class EventBusTest extends TestCase
{
    #[Test]
    public function an_event_reaches_every_registered_handler(): void
    {
        $ran = [];
        $bus = $this->busFor(SampleEvent::class, [
            $this->recorder('first', $ran),
            $this->recorder('second', $ran),
        ]);

        $envelope = $bus->dispatch(new SampleEvent('shipped'));

        $this->assertCount(2, $ran);                                // both handlers ran, once each
        $this->assertContains('first', $ran);
        $this->assertContains('second', $ran);
        $this->assertCount(2, $envelope->all(HandledStamp::class)); // the bus recorded one handled result per handler
    }

    #[Test]
    public function the_handlers_run_in_the_order_the_locator_yields_them(): void
    {
        $ran = [];
        // the locator order is where a handler's priority lands after the compile-time sort; the bus keeps it
        $bus = $this->busFor(SampleEvent::class, [
            $this->recorder('high', $ran),
            $this->recorder('low', $ran),
        ]);

        $bus->dispatch(new SampleEvent('shipped'));

        $this->assertSame(['high', 'low'], $ran);
    }

    /**
     * The event bus stack reduced to its dispatch core: HandleMessageMiddleware over a HandlersLocator.
     * `allowNoHandlers: true` mirrors the framework's event-bus setting, inert here since handlers are present.
     *
     * @param  list<HandlerDescriptor>  $handlers
     */
    private function busFor(string $messageClass, array $handlers): MessageBus
    {
        return new MessageBus([
            new HandleMessageMiddleware(new HandlersLocator([$messageClass => $handlers]), allowNoHandlers: true),
        ]);
    }

    /**
     * A recording handler under an explicit alias; the alias makes each handler's name distinct, so the
     * bus's "already handled by this handler" guard does not collapse two bare closures into one.
     *
     * @param  list<string>  $log  captured by reference; the handler appends its name when it runs
     */
    private function recorder(string $name, array &$log): HandlerDescriptor
    {
        $handler = static function (SampleEvent $event) use ($name, &$log): void {
            $log[] = $name;
        };

        return new HandlerDescriptor($handler, ['alias' => $name]);
    }
}
