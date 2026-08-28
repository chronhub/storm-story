<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Stamp;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Storm\Story\Stamp\BatchModeStamp;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Storm\Story\Stamp\TenantStamp;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final class IdentityStampsTest extends TestCase
{
    #[Test]
    public function a_message_id_carries_its_string(): void
    {
        $this->assertSame('cmd-1', new MessageIdStamp('cmd-1')->id);
    }

    #[Test]
    #[Group('adversarial')]
    #[DataProvider('blank_identities')]
    public function a_blank_identity_is_refused_at_construction(callable $build, string $reason): void
    {
        // Each of the three carries a different disaster, and each guards with the same trim, so each
        // is run against the same shapes. The empty string alone leaves the whitespace half of every
        // guard unproven, and whitespace is the one a hand-edited config or a trimmed-nowhere producer
        // actually produces.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageIsOrContains($reason);

        $build();
    }

    /**
     * @return iterable<string, array{callable(): object, string}>
     */
    public static function blank_identities(): iterable
    {
        // the dedup-key collision: every blank-id message shares ONE inbox entry per transport; the
        // first handled, every later DIFFERENT message silently skipped-acked
        $messageId = 'A message id cannot be blank';
        // the correlation is the saga routing identity: blank ones would cross-route unrelated flows
        $correlation = 'A correlation id cannot be blank';
        // single-tenant work carries NO stamp at all, so a blank one pools everything into a phantom
        $tenant = 'A tenant id cannot be blank';

        foreach (['the empty string' => '', 'a single space' => ' ', 'a tab' => "\t"] as $shape => $value) {
            yield "a message id that is $shape" => [static fn (): object => new MessageIdStamp($value), $messageId];
            yield "a correlation that is $shape" => [static fn (): object => new CorrelationStamp($value), $correlation];
            yield "a tenant that is $shape" => [static fn (): object => new TenantStamp($value), $tenant];
        }
    }

    #[Test]
    #[Group('adversarial')]
    public function the_batch_mode_stamp_is_not_sendable(): void
    {
        // sendable, a forged or accidentally-carried copy on a PhpSerializer transport would switch the
        // inbox OFF for that delivery; the senders must strip it, so only the batch consumer can set it
        $this->assertInstanceOf(NonSendableStampInterface::class, new BatchModeStamp);
    }
}
