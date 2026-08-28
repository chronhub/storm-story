<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Transport;

use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Storm\Bureau\Actor;
use Storm\Chronicler\Evolution\IdentityEventTypeMapper;
use Storm\Chronicler\Evolution\Upcaster;
use Storm\Chronicler\Evolution\UpcasterChain;
use Storm\Chronicler\Exception\UnknownEventType;
use Storm\Contracts\Chronicler\EventTypeMapper;
use Storm\Message\Header;
use Storm\Serializer\DefaultMessageSerializer;
use Storm\Serializer\Exception\SerializationException;
use Storm\Story\Stamp\ActorStamp;
use Storm\Story\Stamp\ContextBagStamp;
use Storm\Story\Stamp\CorrelationStamp;
use Storm\Story\Stamp\MessageIdStamp;
use Storm\Story\Stamp\StoredHeaderStamp;
use Storm\Story\Stamp\TenantStamp;
use Storm\Story\Tests\Fixture\SampleEvent;
use Storm\Story\Tests\Fixture\ThrowingEvent;
use Storm\Story\Transport\NeutralMessageSerializer;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;

final class NeutralMessageSerializerTest extends TestCase
{
    #[Test]
    public function a_missing_body_lands_on_the_decoding_failure_contract(): void
    {
        // a bodyless envelope is malformed input like any other at the untrusted edge: it must land
        // on the contract Messenger's receivers catch, never on a warning plus an empty-string parse
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $this->expectException(MessageDecodingFailedException::class);

        // @phpstan-ignore argument.type (the @param shape requires body; the runtime belt under test is for the shape-violating transport this simulates)
        $serializer->decode(['headers' => []]);
    }

    #[Test]
    public function encode_carries_the_message_id_and_context_headers_in_the_wire_body(): void
    {
        // encode() is the trusted, outgoing direction: OUR OWN context still rides the wire body,
        // observability metadata for whatever consumer reads this neutral shape
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hello'), [
            new MessageIdStamp('evt-1'),
            new CorrelationStamp('trace-1'),
            new ActorStamp(new Actor('user-1', 'App\\User')),
            new TenantStamp('tenant-1'),
        ]));

        /** @var array{header: array<string, mixed>} $body */
        $body = json_decode($encoded['body'], true, flags: JSON_THROW_ON_ERROR);

        $this->assertSame('evt-1', $body['header'][Header::MessageId->value]);
        $this->assertSame('trace-1', $body['header'][Header::CorrelationId->value]);
        $this->assertSame('user-1', $body['header'][Header::ActorId->value]);
        $this->assertSame('App\\User', $body['header'][Header::ActorType->value]);
        $this->assertSame('tenant-1', $body['header'][Header::TenantId->value]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_never_hydrates_ambient_identity_from_the_wire(): void
    {
        // decode() is the untrusted, incoming direction: a foreign producer's own claim to a
        // correlation, actor or tenant must not become ambient identity, since that identity is what
        // routes a live saga and what a handler trusts as "who did this", capabilities this edge must
        // never grant. The message id is unaffected: it is the dedup key, not a routing claim.
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [
                Header::MessageId->value => 'evt-1',
                Header::CorrelationId->value => 'forged-trace',
                Header::ActorId->value => 'forged-actor',
                Header::ActorType->value => 'App\\User',
                Header::TenantId->value => 'forged-tenant',
            ],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $decoded = $serializer->decode(['body' => $body, 'headers' => []]);

        $event = $decoded->getMessage();
        $this->assertInstanceOf(SampleEvent::class, $event);
        $this->assertSame('hi', $event->what);

        $this->assertSame('evt-1', $decoded->last(MessageIdStamp::class)?->id);
        $this->assertNull($decoded->last(CorrelationStamp::class));
        $this->assertNull($decoded->last(ActorStamp::class));
        $this->assertNull($decoded->last(TenantStamp::class));
    }

    #[Test]
    public function decode_still_lifts_a_declared_bag_key_from_an_untrusted_wire(): void
    {
        // the declared bag is a separate, legitimate channel, an app-level opt-in never treated as
        // framework identity, and it is unaffected by the untrusted producer's ambient-identity claims
        $serializer = new NeutralMessageSerializer(
            new DefaultMessageSerializer,
            new IdentityEventTypeMapper,
            propagatedKeys: ['trace_id'],
        );

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [
                Header::MessageId->value => 'evt-1',
                Header::CorrelationId->value => 'forged-trace',
                'trace_id' => 'app-trace-1',
            ],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $decoded = $serializer->decode(['body' => $body, 'headers' => []]);

        $this->assertNull($decoded->last(CorrelationStamp::class));
        $this->assertSame(['trace_id' => 'app-trace-1'], $decoded->last(ContextBagStamp::class)?->values);
    }

    #[Test]
    public function encodes_the_neutral_wire_shape(): void
    {
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [new MessageIdStamp('evt-1')]));

        $this->assertSame(SampleEvent::class, $encoded['headers']['type']);

        /** @var array{type: string, version: int, header: array<string, mixed>, content: array<string, mixed>} $body */
        $body = json_decode($encoded['body'], true, flags: JSON_THROW_ON_ERROR);

        $this->assertSame(SampleEvent::class, $body['type']); // identity mapper maps to alias = FQCN
        $this->assertSame(1, $body['version']); // the type's current schema version rides inside the body
        $this->assertSame(['what' => 'hi'], $body['content']);
        $this->assertSame('evt-1', $body['header'][Header::MessageId->value]);
    }

    #[Test]
    public function carries_the_full_stored_header_on_the_wire_when_a_stored_header_stamp_is_present(): void
    {
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $storedHeader = [
            Header::MessageId->value => 'evt-1',
            Header::MessageType->value => SampleEvent::class, // FQCN, externalized as `type`, not on the wire
            Header::OccurredAt->value => '2026-05-21T10:00:00.000000',
            Header::CorrelationId->value => 'trace-1',
            Header::CausationId->value => 'cmd-1',
            Header::AggregateId->value => 'order-7',
            Header::AggregateType->value => 'App\\Order',
            Header::AggregateVersion->value => 3,
        ];

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [new StoredHeaderStamp($storedHeader)]));

        /** @var array{type: string, header: array<string, mixed>, content: array<string, mixed>} $body */
        $body = json_decode($encoded['body'], true, flags: JSON_THROW_ON_ERROR);

        // the faithful row: occurred_at, causation and aggregate coordinates make it onto the wire
        $this->assertSame('2026-05-21T10:00:00.000000', $body['header'][Header::OccurredAt->value]);
        $this->assertSame('cmd-1', $body['header'][Header::CausationId->value]);
        $this->assertSame('order-7', $body['header'][Header::AggregateId->value]);
        $this->assertSame(3, $body['header'][Header::AggregateVersion->value]);

        // the class travels as the portable `type` alias, never as the FQCN header
        $this->assertArrayNotHasKey(Header::MessageType->value, $body['header']);
        $this->assertSame(SampleEvent::class, $body['type']);
    }

    #[Test]
    public function decode_rebuilds_the_full_header_as_a_stored_header_stamp(): void
    {
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [new StoredHeaderStamp([
            Header::MessageId->value => 'evt-1',
            Header::MessageType->value => SampleEvent::class,
            Header::OccurredAt->value => '2026-05-21T10:00:00.000000',
            Header::AggregateId->value => 'order-7',
            Header::AggregateVersion->value => 3,
        ])]));

        $stored = $serializer->decode($encoded)->last(StoredHeaderStamp::class);

        $this->assertInstanceOf(StoredHeaderStamp::class, $stored);
        $this->assertSame('order-7', $stored->header[Header::AggregateId->value]);
        $this->assertSame(3, $stored->header[Header::AggregateVersion->value]);
        $this->assertSame('2026-05-21T10:00:00.000000', $stored->header[Header::OccurredAt->value]);
        // MessageType is re-injected from the `type` alias on decode
        $this->assertSame(SampleEvent::class, $stored->header[Header::MessageType->value]);
    }

    #[Test]
    public function decode_stamps_the_transport_configured_bus(): void
    {
        // the bus a decoded message routes to is transport config, not producer input
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper, bus: 'storm.event.bus');

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [new MessageIdStamp('evt-1')]));

        $this->assertSame('storm.event.bus', $serializer->decode($encoded)->last(BusNameStamp::class)?->getBusName());
    }

    #[Test]
    public function decode_stamps_no_bus_when_the_transport_configures_none(): void
    {
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [new MessageIdStamp('evt-1')]));

        $this->assertNull($serializer->decode($encoded)->last(BusNameStamp::class));
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_ignores_a_producer_supplied_bus_header(): void
    {
        // a foreign producer must not choose which internal bus its message is dispatched to: the wire
        // X-Message-Bus is never read back, only the transport's own configured bus wins.
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper, bus: 'storm.event.bus');

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $decoded = $serializer->decode(['body' => $body, 'headers' => ['X-Message-Bus' => 'storm.command.bus']]);

        $this->assertSame('storm.event.bus', $decoded->last(BusNameStamp::class)?->getBusName());
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_turns_a_propagated_key_the_bag_refuses_into_a_decoding_failure(): void
    {
        // the OTHER arm of the same catch, and it needs its own delivery: this one is thrown by the
        // context bag, not by the actor, so a catch that kept only the actor type would let it climb
        // out of the decoder as itself.
        $serializer = new NeutralMessageSerializer(
            new DefaultMessageSerializer,
            new IdentityEventTypeMapper,
            propagatedKeys: [' padded'],
        );

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1', ' padded' => 'value'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageMatches('/Cannot decode the neutral wire message/');
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_wire_type_outside_the_channel_allowlist(): void
    {
        // a loadable-but-unregistered payload FQCN must be refused BEFORE it is instantiated or dispatched
        $serializer = new NeutralMessageSerializer(
            new DefaultMessageSerializer,
            new IdentityEventTypeMapper,
            allowedTypes: ['bank.account_opened'], // this channel accepts one alias, not SampleEvent's FQCN
        );

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageMatches('/not allowed/');
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    public function decode_accepts_a_wire_type_inside_the_channel_allowlist(): void
    {
        $serializer = new NeutralMessageSerializer(
            new DefaultMessageSerializer,
            new IdentityEventTypeMapper,
            allowedTypes: [SampleEvent::class],
        );

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [new MessageIdStamp('evt-1')]));

        $this->assertInstanceOf(SampleEvent::class, $serializer->decode($encoded)->getMessage());
    }

    #[Test]
    public function carries_the_bus_name_in_a_transport_header_not_the_neutral_body(): void
    {
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [
            new MessageIdStamp('evt-1'),
            new BusNameStamp('storm.event.bus'),
        ]));

        // routing metadata lives in the transport header...
        $this->assertSame('storm.event.bus', $encoded['headers']['X-Message-Bus']);

        // ...never in the cross-language body
        /** @var array{type: string, header: array<string, mixed>, content: array<string, mixed>} $body */
        $body = json_decode($encoded['body'], true, flags: JSON_THROW_ON_ERROR);
        $this->assertArrayNotHasKey('X-Message-Bus', $body);
        $this->assertArrayNotHasKey('X-Message-Bus', $body['header']);
    }

    #[Test]
    public function omits_the_bus_header_when_no_bus_name_stamp_is_present(): void
    {
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [new MessageIdStamp('evt-1')]));

        $this->assertArrayNotHasKey('X-Message-Bus', $encoded['headers']);
        $this->assertNull($serializer->decode($encoded)->last(BusNameStamp::class));
    }

    #[Test]
    public function carries_the_redelivery_count_in_a_transport_header_not_the_neutral_body(): void
    {
        // every retry re-send passes through encode(): a wire that dropped the RedeliveryStamp would reset
        // the counter each attempt and max_retries would never be reached, a poison in unbounded retry
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [
            new MessageIdStamp('evt-1'),
            new RedeliveryStamp(3),
        ]));

        $this->assertSame('3', $encoded['headers']['X-Redelivery-Count']);

        // operational retry state is transport metadata, like the bus name: never in the cross-language body
        /** @var array{type: string, header: array<string, mixed>, content: array<string, mixed>} $body */
        $body = json_decode($encoded['body'], true, flags: JSON_THROW_ON_ERROR);
        $this->assertArrayNotHasKey('X-Redelivery-Count', $body);
        $this->assertArrayNotHasKey('X-Redelivery-Count', $body['header']);
    }

    #[Test]
    public function decode_reattaches_the_carried_redelivery_count_as_a_stamp(): void
    {
        // the Worker retry strategy and the batch consumer's redeliver cap both read the stamp off the
        // received envelope: without the re-attach the count would restart at zero on every delivery
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [
            new MessageIdStamp('evt-1'),
            new RedeliveryStamp(7),
        ]));

        $this->assertSame(7, $serializer->decode($encoded)->last(RedeliveryStamp::class)?->getRetryCount());
    }

    #[Test]
    public function omits_the_redelivery_header_on_a_first_delivery(): void
    {
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $encoded = $serializer->encode(new Envelope(new SampleEvent('hi'), [new MessageIdStamp('evt-1')]));

        $this->assertArrayNotHasKey('X-Redelivery-Count', $encoded['headers']);
        $this->assertNull($serializer->decode($encoded)->last(RedeliveryStamp::class));
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_non_integer_redelivery_count_as_a_decoding_failure(): void
    {
        // dropping a mistyped count silently would reset the retry cap and re-arm the very
        // unbounded-retry poison the header exists to close: rejected at the untrusted edge
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageMatches('/X-Redelivery-Count/');
        $serializer->decode(['body' => $body, 'headers' => ['X-Redelivery-Count' => 'many']]);
    }

    #[Test]
    public function decode_upcasts_an_older_wire_version_to_the_current_shape(): void
    {
        // the same chain as a store read: a payload written by an older producer is migrated to the
        // current shape before fromPayload sees it, never trusted as current
        $serializer = new NeutralMessageSerializer(
            new DefaultMessageSerializer,
            self::mapperAtVersion(2),
            new UpcasterChain([new class() implements Upcaster
            {
                public function supports(string $alias, int $fromVersion): bool
                {
                    return $alias === SampleEvent::class && $fromVersion === 1;
                }

                public function upcast(array $payload): array
                {
                    return ['what' => $payload['msg']]; // v1 named the field `msg`
                }
            }]),
        );

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['msg' => 'written at v1'],
        ], JSON_THROW_ON_ERROR);

        $event = $serializer->decode(['body' => $body, 'headers' => []])->getMessage();

        $this->assertInstanceOf(SampleEvent::class, $event);
        $this->assertSame('written at v1', $event->what);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_wire_version_ahead_of_the_runtime_as_a_decoding_failure(): void
    {
        // a newer producer's payload must never be hydrated partially by an older consumer: the
        // future version is refused loud at the edge, same posture as the store read
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 2, // the identity mapper's current version is 1
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_version_gap_with_no_upcaster_as_a_decoding_failure(): void
    {
        // a missing migration step is a deployment bug, surfaced as a rejection to the failure
        // transport, never a stale payload silently handed to fromPayload
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, self::mapperAtVersion(2));

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1, // current is 2 and the chain is empty: the 1->2 step has no owner
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['msg' => 'written at v1'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_contested_migration_step_as_a_decoding_failure(): void
    {
        // Two upcasters claiming one step makes the migrated payload depend on discovery order, so
        // the chain refuses rather than rank them. On this edge the refusal must arrive as a DECODING
        // failure: raw, the storm exception is one Messenger's receiver does not recognize, and the
        // worker dies on a message the failure transport was there to capture.
        $serializer = new NeutralMessageSerializer(
            new DefaultMessageSerializer,
            self::mapperAtVersion(2),
            upcasters: new UpcasterChain([self::claimingStep(1), self::claimingStep(1)]),
        );

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['msg' => 'written at v1'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageIsOrContains('claimed by 2 upcasters');
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_an_upcaster_that_throws_as_a_decoding_failure(): void
    {
        // The other half: the step has exactly one owner and that owner blows up on a payload shaped
        // otherwise than it assumed. Same disposition, and for the same reason: a user upcaster's bug
        // must not be indistinguishable from an infrastructure crash on this edge.
        $serializer = new NeutralMessageSerializer(
            new DefaultMessageSerializer,
            self::mapperAtVersion(2),
            upcasters: new UpcasterChain([new class() implements Upcaster
            {
                public function supports(string $alias, int $fromVersion): bool
                {
                    return true;
                }

                public function upcast(array $payload): array
                {
                    throw new RuntimeException('the shape I assumed is not the shape I got');
                }
            }]),
        );

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['msg' => 'written at v1'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageIsOrContains('the shape I assumed is not the shape I got');
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    /** An upcaster claiming exactly one version step, payload untouched. */
    private static function claimingStep(int $fromVersion): Upcaster
    {
        return new readonly class($fromVersion) implements Upcaster
        {
            public function __construct(private int $from) {}

            public function supports(string $alias, int $fromVersion): bool
            {
                return $fromVersion === $this->from;
            }

            public function upcast(array $payload): array
            {
                return $payload;
            }
        };
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_missing_wire_version_as_a_decoding_failure(): void
    {
        // an absent version would have to be presumed current, a silent lie the moment the type's
        // schema moves past the producer's: required on the wire, like the message id
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageMatches('/version/');
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_non_integer_wire_version_as_a_decoding_failure(): void
    {
        // validated, never coerced: a "1" or a zero must fail with an accurate diagnostic, not
        // become a version the upcast then trusts
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => '1',
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageMatches('/version/');
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    /**
     * An identity-shaped mapper whose types are at `$version`, for wires older than the runtime.
     *
     * @param  positive-int  $version
     */
    private static function mapperAtVersion(int $version): EventTypeMapper
    {
        return new readonly class($version) implements EventTypeMapper
        {
            /** @param positive-int $version */
            public function __construct(private int $version) {}

            public function toType(string $class): string
            {
                return $class;
            }

            public function toClass(string $type): string
            {
                if (! class_exists($type)) {
                    throw UnknownEventType::cannotResolve($type);
                }

                return $type;
            }

            public function versionOf(string $class): int
            {
                return $this->version;
            }

            public function storedTypesOf(string $class): array
            {
                return [$class];
            }
        };
    }

    #[Test]
    public function decode_rejects_a_malformed_body_as_a_decoding_failure(): void
    {
        // Messenger's receivers catch MessageDecodingFailedException to reject a message, sending it to the failure transport;
        // a storm exception they don't recognize would instead bubble up and crash the worker.
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $this->expectException(MessageDecodingFailedException::class);
        $serializer->decode(['body' => 'not json at all', 'headers' => []]);
    }

    #[Test]
    public function decode_rejects_an_unknown_type_as_a_decoding_failure(): void
    {
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode(['type' => 'Not\A\Real\Class', 'version' => 1, 'header' => [], 'content' => []], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    public function decode_rejects_a_mistyped_reserved_header_as_a_decoding_failure(): void
    {
        // a foreign/buggy producer puts __aggregate_version on the wire as a string when it must be an int: rejected at
        // the boundary, not passed through to fail deep in a handler.
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::AggregateVersion->value => '3'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    public function decode_rejects_a_message_without_an_id_as_a_decoding_failure(): void
    {
        // no __message_id = no consumer-side dedup key: the boundary middleware would mint a FRESH id per
        // delivery and every broker redelivery would re-run the handlers, the inbox silently void. The
        // producer's bug is rejected where it is visible: at the decoding boundary.
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::CorrelationId->value => 'trace-1'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageMatches('/__message_id/');
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_blank_message_id_as_a_decoding_failure(): void
    {
        // WORSE than absent: a blank id passes the presence check but becomes the SHARED dedup key;
        // every blank-id message collides into one inbox entry, the first handled, every later DIFFERENT
        // message silently skipped-acked. Same edge, same rejection.
        //
        // Which rule rejects it is worth naming, because it is NOT this class's own id check. The
        // header vocabulary runs first, inside deserialize, and refuses a blank framework header
        // wholesale; the id check below it only ever meets an ABSENT id. Asserting the mention of
        // `__message_id` alone would pass for either, and did: with the blank half of the id check
        // removed, this test stayed green.
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => '   '],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageIsOrContains('Header "__message_id" is present but blank');
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_blank_correlation_as_a_decoding_failure_not_a_worker_crash(): void
    {
        // a malformed context header is refused by the stamp constructors; still the untrusted edge,
        // so it must reject-to-failure-transport, never crash the worker with an unrecognized type
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1', Header::CorrelationId->value => ''],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_half_an_actor_identity_as_a_decoding_failure(): void
    {
        // the actor is an atomic pair: a lone wire half would silently drop from ambient propagation,
        // no ActorStamp, and completing it downstream would forge a two-provenance identity
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1', Header::ActorId->value => 'wire-actor'],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessageMatches('/half an actor identity/');
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_ignores_an_actor_over_the_bureaus_ceiling_rather_than_crashing(): void
    {
        // with ambient identity untrusted, an actor id over Bureau's MAX_BYTES ceiling never reaches
        // the Actor constructor at all here: it is simply never surfaced as a stamp, the same as any
        // other well-formed-but-unhonored wire claim. Never a worker crash, never a decoding failure
        // for a value this edge does not act on either way.
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        // both halves present, so the wire-shape pair check passes; the id is 257 bytes, one over the
        // actor's documented ceiling, which is the shape a hostile or buggy producer actually sends
        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [
                Header::MessageId->value => 'evt-1',
                Header::ActorId->value => str_repeat('a', Actor::MAX_BYTES + 1),
                Header::ActorType->value => 'App\\User',
            ],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $decoded = $serializer->decode(['body' => $body, 'headers' => []]);

        $this->assertNull($decoded->last(ActorStamp::class));
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_rejects_a_blank_actor_pair_as_a_decoding_failure(): void
    {
        // both actor halves present but blank, a distinct shape from the half-actor case: the untrusted
        // edge must reject it to the failure transport, never dispatch a blank actor identity nor crash
        // the worker with a raw exception. Refused at the header boundary, before the stamp seam.
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => SampleEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1', Header::ActorId->value => '', Header::ActorType->value => ''],
            'content' => ['what' => 'hi'],
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    #[Group('adversarial')]
    public function decode_turns_a_poison_payload_into_a_decoding_failure_not_a_worker_crash(): void
    {
        // a fromPayload that throws must surface as MessageDecodingFailedException,
        // sent to the failure transport, never a raw exception that bubbles up and kills the worker
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $body = json_encode([
            'type' => ThrowingEvent::class,
            'version' => 1,
            'header' => [Header::MessageId->value => 'evt-1'],
            'content' => ['amount' => 'not-an-int'], // fromPayload throws InvalidArgumentException
        ], JSON_THROW_ON_ERROR);

        $this->expectException(MessageDecodingFailedException::class);
        $serializer->decode(['body' => $body, 'headers' => []]);
    }

    #[Test]
    public function encode_wraps_an_unencodable_payload_as_a_serialization_failure(): void
    {
        // a content value json_encode rejects for invalid UTF-8 surfaces as the contracted SerializationException
        // via cannotEncode, never a raw JsonException leaking out of the transport.
        $serializer = new NeutralMessageSerializer(new DefaultMessageSerializer, new IdentityEventTypeMapper);

        $this->expectException(SerializationException::class);

        $serializer->encode(new Envelope(new SampleEvent("\xB1\x31"), [new MessageIdStamp('evt-1')]));
    }
}
