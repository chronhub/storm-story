<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Stamp;

use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use stdClass;
use Storm\Message\Exception\InvalidMessageException;
use Storm\Message\Header;
use Storm\Message\Message;
use Storm\Story\Stamp\ActorStamp;
use Storm\Story\Stamp\ContextBagStamp;
use Storm\Story\Stamp\ContextStamps;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Storm\Story\Stamp\TenantStamp;
use Symfony\Component\Messenger\Envelope;

/**
 * The CENTRAL header-and-stamp normalizer's actor-pair invariant: both halves or neither, a lone
 * half is corruption, refused loud. The enricher and the neutral wire edge already refuse it at
 * their own frontiers; this is the last one, and the only one the outbox publishers cross. They
 * hydrate stored rows TOLERANTLY, so a corrupt row reaches here constructible; silently dropping
 * the half would erase provenance on the next hop with zero signal.
 *
 * `$trustAmbientIdentity: false`, the untrusted-wire mode, skips correlation, actor and tenant
 * reading entirely: nothing propagates, so the pair invariant above never fires there either, and
 * a producer's claim to any of the three is inert data rather than a refusal.
 */
final class ContextStampsTest extends TestCase
{
    #[Test]
    public function a_full_actor_pair_becomes_one_actor_stamp(): void
    {
        $stamps = ContextStamps::fromMessage($this->message([
            Header::ActorId->value => 'user-1',
            Header::ActorType->value => 'user',
        ]));

        $actors = array_values(array_filter($stamps, static fn (object $s): bool => $s instanceof ActorStamp));

        $this->assertCount(1, $actors);
        $this->assertSame('user-1', $actors[0]->actor->id);
    }

    #[Test]
    public function no_actor_headers_means_no_actor_stamp(): void
    {
        $stamps = ContextStamps::fromMessage($this->message([]));

        $this->assertSame([], array_filter($stamps, static fn (object $s): bool => $s instanceof ActorStamp));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_lone_actor_half_is_refused_never_silently_dropped(): void
    {
        // The refusal names which half it HAS and which it wants, in that order: the operator reading
        // it has one header to go add, and a message with the two swapped sends them to the wrong one.
        // Mentioning the missing key alone passes whichever way round the pair is printed.
        $this->expectException(InvalidMessageException::class);
        $this->expectExceptionMessageIsOrContains(sprintf('"%s" is present but "%s" is missing', Header::ActorId->value, Header::ActorType->value));

        ContextStamps::fromMessage($this->message([Header::ActorId->value => 'user-1']));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_lone_actor_type_is_refused_too(): void
    {
        $this->expectException(InvalidMessageException::class);
        $this->expectExceptionMessageIsOrContains(sprintf('"%s" is present but "%s" is missing', Header::ActorType->value, Header::ActorId->value));

        ContextStamps::fromMessage($this->message([Header::ActorType->value => 'user']));
    }

    #[Test]
    #[Group('adversarial')]
    public function untrusted_ambient_identity_yields_no_correlation_actor_or_tenant_stamp(): void
    {
        // the untrusted wire calls with $trustAmbientIdentity: false; a fully well-formed pair,
        // correlation and tenant must still surface as nothing, since honoring a foreign producer's
        // own claim is exactly the capability this mode refuses
        $stamps = ContextStamps::fromMessage($this->message([
            Header::CorrelationId->value => 'trace-1',
            Header::ActorId->value => 'user-1',
            Header::ActorType->value => 'user',
            Header::TenantId->value => 'tenant-1',
        ]), trustAmbientIdentity: false);

        $this->assertSame([], array_filter($stamps, static fn (object $s): bool => $s instanceof CorrelationStamp));
        $this->assertSame([], array_filter($stamps, static fn (object $s): bool => $s instanceof ActorStamp));
        $this->assertSame([], array_filter($stamps, static fn (object $s): bool => $s instanceof TenantStamp));
    }

    #[Test]
    public function untrusted_ambient_identity_still_keeps_the_message_id_and_declared_bag(): void
    {
        // the message id is the dedup key, not a routing claim, and the declared bag is a separate,
        // legitimate channel; neither is what this mode refuses
        $stamps = ContextStamps::fromMessage(
            $this->message([Header::CorrelationId->value => 'trace-1', 'origin' => 'http']),
            ['origin'],
            trustAmbientIdentity: false,
        );

        $this->assertSame('m-1', array_values(array_filter($stamps, static fn (object $s): bool => $s instanceof MessageIdStamp))[0]->id);
        $this->assertSame(['origin' => 'http'], array_values(array_filter($stamps, static fn (object $s): bool => $s instanceof ContextBagStamp))[0]->values);
    }

    #[Test]
    #[Group('adversarial')]
    public function untrusted_ambient_identity_never_throws_on_a_lone_actor_half(): void
    {
        // the pair invariant guards PROPAGATION of a corrupt actor; untrusted, nothing propagates it
        // at all, so a lone half here is inert data, not a refusal
        $stamps = ContextStamps::fromMessage($this->message([Header::ActorId->value => 'user-1']), trustAmbientIdentity: false);

        $this->assertSame([], array_filter($stamps, static fn (object $s): bool => $s instanceof ActorStamp));
    }

    #[Test]
    public function only_declared_keys_lift_into_one_bag_stamp(): void
    {
        $stamps = ContextStamps::fromMessage(
            $this->message(['origin' => 'http', 'undeclared' => 'stays-behind']),
            ['origin', 'absent-key'],
        );

        $bags = array_values(array_filter($stamps, static fn (object $s): bool => $s instanceof ContextBagStamp));

        $this->assertCount(1, $bags);
        $this->assertSame(['origin' => 'http'], $bags[0]->values);
    }

    #[Test]
    public function no_declared_keys_means_no_bag_stamp(): void
    {
        $stamps = ContextStamps::fromMessage($this->message(['origin' => 'http']));

        $this->assertSame([], array_filter($stamps, static fn (object $s): bool => $s instanceof ContextBagStamp));
    }

    #[Test]
    public function to_message_lands_the_bag_as_plain_headers(): void
    {
        $message = ContextStamps::toMessage(new Envelope(new stdClass, [
            new MessageIdStamp('m-1'),
            new ContextBagStamp(['origin' => 'http']),
        ]));

        $this->assertSame('http', $message->headers()['origin']);
    }

    #[Test]
    #[Group('adversarial')]
    public function to_message_refuses_an_unjsonable_bag_value_at_the_constructor_door(): void
    {
        // the door this path takes is the CONSTRUCTOR, and it must hold the same JSON-tree net as
        // withHeader(): a nested object silently flattened on the wire would come back a different
        // value, far from its writer
        $this->expectException(InvalidMessageException::class);

        ContextStamps::toMessage(new Envelope(new stdClass, [
            new MessageIdStamp('m-1'),
            // @phpstan-ignore argument.type (the value shape is phpdoc-only by design; the runtime net under test is what catches the analyzer-invisible caller)
            new ContextBagStamp(['origin' => new stdClass]),
        ]));
    }

    /**
     * @param  array<string, string>  $extra
     */
    private function message(array $extra): Message
    {
        return new Message(new stdClass, [
            Header::MessageId->value => 'm-1',
            ...$extra,
        ]);
    }
}
