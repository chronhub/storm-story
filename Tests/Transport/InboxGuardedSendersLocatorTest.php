<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Transport;

use LogicException;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use stdClass;
use Storm\Contracts\Message\SendableUnderInboxTransaction;
use Storm\Story\Consume\InboxTransactionContext;
use Storm\Story\Tests\Fixture\ContractedCommand;
use Storm\Story\Tests\Fixture\SampleCommand;
use Storm\Story\Tests\Fixture\SampleContract;
use Storm\Story\Transport\InboxGuardedSendersLocator;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Sender\SendersLocatorInterface;
use Symfony\Component\Messenger\Transport\Sync\SyncTransport;

final class InboxGuardedSendersLocatorTest extends TestCase
{
    #[Test]
    #[Group('adversarial')]
    public function an_undeclared_broker_send_inside_an_inbox_transaction_is_refused_loud(): void
    {
        // the dual-write: the transaction cannot roll a broker send back; without a signed grant the
        // send must fail AT THE SEND SITE, naming both messages and the way out
        $context = new InboxTransactionContext;
        $context->enter(stdClass::class); // a handler of stdClass is running inside the inbox tx

        $locator = new InboxGuardedSendersLocator($this->inner('async', $this->sender()), $context, granted: []);

        try {
            iterator_to_array($locator->getSenders(new Envelope(new SampleCommand('nested'))));
            $this->fail('expected the undeclared broker send to be refused');
        } catch (LogicException $e) {
            $this->assertStringContainsString(SampleCommand::class, $e->getMessage());
            $this->assertStringContainsString(stdClass::class, $e->getMessage());
            $this->assertStringContainsString('DispatchesUnderInboxTransaction', $e->getMessage());
        }
    }

    #[Test]
    #[Group('adversarial')]
    public function a_nested_non_granted_frame_refuses_even_under_a_granted_parent(): void
    {
        // the guard reads the INNERMOST frame: a grant covers the handler that signed it, never
        // whatever runs nested beneath it, or the signed attribute would be an ambient blanket
        $context = new InboxTransactionContext;
        $context->enter(stdClass::class);      // the granted parent
        $context->enter(SampleCommand::class); // the nested frame, NOT granted

        $locator = new InboxGuardedSendersLocator($this->inner('async', $this->sender()), $context, granted: [stdClass::class]);

        $this->expectException(LogicException::class);

        iterator_to_array($locator->getSenders(new Envelope(new SampleCommand('nested'))));
    }

    #[Test]
    public function a_granted_message_class_passes_its_nested_sends(): void
    {
        // the signed contract: while a granted class is being handled, its broker sends pass
        $context = new InboxTransactionContext;
        $context->enter(stdClass::class);

        $sender = $this->sender();
        $locator = new InboxGuardedSendersLocator($this->inner('async', $sender), $context, granted: [stdClass::class]);

        $senders = iterator_to_array($locator->getSenders(new Envelope(new SampleCommand('nested'))));

        $this->assertSame(['async' => $sender], $senders);
    }

    #[Test]
    #[Group('adversarial')]
    public function an_interface_grant_passes_the_concrete_message_being_handled(): void
    {
        // the seam defect this pins: an interface-typed #[AsMessageHandler] compiles the INTERFACE
        // into the grant set while the context holds the CONCRETE handled class; string equality
        // would refuse the very send the signed handler's shape licenses, so the match is is_a
        $context = new InboxTransactionContext;
        $context->enter(ContractedCommand::class);

        $sender = $this->sender();
        $locator = new InboxGuardedSendersLocator($this->inner('async', $sender), $context, granted: [SampleContract::class]);

        $senders = iterator_to_array($locator->getSenders(new Envelope(new SampleCommand('nested'))));

        $this->assertSame(['async' => $sender], $senders);
    }

    #[Test]
    public function a_non_implementing_frame_stays_refused_under_an_interface_grant(): void
    {
        // hierarchy matching must not widen the gate: a handled class that does NOT implement the
        // granted interface holds no grant, and its broker send is still refused
        $context = new InboxTransactionContext;
        $context->enter(SampleCommand::class);

        $locator = new InboxGuardedSendersLocator($this->inner('async', $this->sender()), $context, granted: [SampleContract::class]);

        $this->expectException(LogicException::class);

        iterator_to_array($locator->getSenders(new Envelope(new SampleCommand('nested'))));
    }

    #[Test]
    public function outside_an_inbox_transaction_every_send_passes(): void
    {
        $sender = $this->sender();
        $locator = new InboxGuardedSendersLocator($this->inner('async', $sender), new InboxTransactionContext, granted: []);

        $senders = iterator_to_array($locator->getSenders(new Envelope(new SampleCommand('fresh'))));

        $this->assertSame(['async' => $sender], $senders);
    }

    #[Test]
    public function a_message_declared_sendable_under_the_transaction_passes(): void
    {
        // the message-side declaration: fire-and-forget observability, a saga history entry, may leave
        // from under ANY handler; its loss and duplication are harmless by design
        $context = new InboxTransactionContext;
        $context->enter(stdClass::class);

        $sendable = new class() implements SendableUnderInboxTransaction {};
        $sender = $this->sender();
        $locator = new InboxGuardedSendersLocator($this->inner('telemetry', $sender), $context, granted: []);

        $senders = iterator_to_array($locator->getSenders(new Envelope($sendable)));

        $this->assertSame(['telemetry' => $sender], $senders);
    }

    #[Test]
    public function a_sync_transport_passes_through_even_inside_the_transaction(): void
    {
        // a SyncTransport "send" IS in-process handling inside the same transaction, no dual-write
        $context = new InboxTransactionContext;
        $context->enter(stdClass::class);

        $sync = new SyncTransport(new MessageBus);
        $locator = new InboxGuardedSendersLocator($this->inner('sync', $sync), $context, granted: []);

        $senders = iterator_to_array($locator->getSenders(new Envelope(new SampleCommand('inline'))));

        $this->assertSame(['sync' => $sync], $senders);
    }

    private function inner(string $alias, SenderInterface $sender): SendersLocatorInterface
    {
        return new readonly class($alias, $sender) implements SendersLocatorInterface
        {
            public function __construct(private string $alias, private SenderInterface $sender) {}

            public function getSenders(Envelope $envelope): iterable
            {
                yield $this->alias => $this->sender;
            }
        };
    }

    private function sender(): SenderInterface
    {
        return new class() implements SenderInterface
        {
            public function send(Envelope $envelope): Envelope
            {
                return $envelope;
            }
        };
    }
}
