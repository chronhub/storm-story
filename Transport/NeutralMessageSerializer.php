<?php

declare(strict_types=1);

namespace Storm\Story\Transport;

use InvalidArgumentException;
use JsonException;
use Override;
use Storm\Bureau\Exception\InvalidActor;
use Storm\Chronicler\Evolution\UpcasterChain;
use Storm\Contracts\Chronicler\EventTypeMapper;
use Storm\Contracts\Chronicler\UndecodableRow;
use Storm\Contracts\Chronicler\UnknownEventType;
use Storm\Contracts\Serializer\SerializationExceptionContract;
use Storm\Message\Exception\InvalidMessageException;
use Storm\Message\Header;
use Storm\Message\Message;
use Storm\Serializer\Exception\SerializationException;
use Storm\Serializer\MessageSerializer;
use Storm\Serializer\SerializedMessage;
use Storm\Story\Stamp\ContextStamps;
use Storm\Story\Stamp\StoredHeaderStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\RoutableMessageBus;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * A Messenger transport serializer that puts a neutral `{type, version, header, content}` JSON form on
 * the wire instead of PHP's `serialize()`, so a non-PHP consumer can read it and it stays version-stable.
 *
 * The shape is a faithful event-store row. `header` carries the whole stored header, meaning `occurred_at`,
 * the event's own causation, `aggregate{id,type,version}` and the cross-cutting context, when a
 * `StoredHeaderStamp` is present, which the outbox publisher always attaches; it falls back to context-only
 * via `ContextStamps` for an envelope that has none, such as a command routed async. `type` is the
 * `EventTypeMapper` alias, portable and rename-proof, the source of truth for the class, mirroring the
 * store's `type` column; the FQCN `Header::MessageType` is therefore stripped from the wire header on
 * encoding and re-injected from `type` on decoding. `content` is the event's `toPayload()`. `version` is
 * the payload's schema version, mirroring the store's `event_version` column: the type's current version
 * on encode, and the decoder's upcast starting point on decode. The payload runs through the same
 * `UpcasterChain` as a store read, so `fromPayload()` always sees a current shape whatever the
 * producer's age; a version step with no upcaster, or a version ahead of this runtime, is a loud
 * decoding failure, never a payload trusted as current.
 *
 * Not auto-registered: the bundle builds one instance per `storm.neutral_transports` entry, as
 * `storm.neutral_transport.<name>`, referenced from that transport's `serializer` key in the app
 * messenger config, so it only governs the transports that declared it; the entry fixes the bus and
 * the trust posture, the two things a shared instance could not carry per transport.
 *
 * `decode()` validates the untrusted edge:
 *
 * - The wire shape, via `SerializedMessage::fromWire`;
 * - The `type` alias, resolved via the `EventTypeMapper`;
 * - The reserved `__` header wire types, via `Header::assertWellTyped` inside `deserialize`.
 *
 * A mistyped reserved key, for instance `__aggregate_version` as a string, is rejected here, not silently
 * dropped as a wrong-typed string read back as null, nor thrown deep in a handler. Every such failure
 * surfaces as `MessageDecodingFailedException`, the contract Messenger's receivers catch to reject the
 * message and send it to the failure transport, instead of a storm exception they would let crash the
 * worker. The `__message_id` header is REQUIRED at decode: it is the consumer-side dedup key, and an
 * id-less message would silently VOID the inbox, since the boundary middleware would mint a fresh id per
 * delivery. Still trusted in v1: the event payload shape, since a foreign producer must emit a `content` its
 * `fromPayload()` accepts.
 *
 * What crosses the wire, and what deliberately does not:
 *
 * - `type` is read from the BODY; the `type` transport header is broker routing only, never read back.
 *
 * - The redelivery count IS carried, as the `X-Redelivery-Count` transport header. Every retry re-send
 *   passes through `encode()`, so a wire that dropped the `RedeliveryStamp` would reset the counter on
 *   each attempt, `max_retries` would never be reached, and a deterministic poison would retry without
 *   bound instead of reaching the failure transport. `decode()` re-attaches the stamp, giving the Worker
 *   retry strategy and the batch consumer's redeliver cap the real attempt count; a forged count costs
 *   at most an early trip to the failure transport, never a capability, so the header is read as-is.
 *
 * - `DelayStamp` stays uncarried, being a send-time broker directive consumed by the transport rather
 *   than message state; idempotency stays the inbox's job through `DeduplicateConsumer`.
 *
 * - The failure-capture stamps, `SentToFailureTransportStamp` and `ErrorDetailsStamp`, are not carried
 *   either, a deliberate non-goal: they are rich, PHP-specific objects serving the `messenger:failed:*`
 *   ops tooling of an INTERNAL store, with no cross-runtime value. A deployment rule follows, and it is
 *   the one to remember here: never bind this serializer on a failure transport, or that tooling goes
 *   blind; a failure transport stays on the default `PhpSerializer`, which keeps every stamp.
 *
 * - `__correlation_id`, `__actor_id`/`__actor_type` and `__tenant_id` cross the wire in the body but
 *   become nothing: `decode()` reads them through `ContextStamps::fromMessage` with ambient identity
 *   untrusted, so none becomes a stamp. A foreign producer fixing the correlation that routes a live
 *   saga, or claiming an actor or tenant it never authenticated as, is a capability this edge must not
 *   grant. The declared bag, `storm.context.propagated_keys`, is the legitimate channel and is unaffected.
 *
 * The bus a decoded message routes to is transport configuration, never producer input. Without a bus
 * name the worker's `RoutableMessageBus` falls back to the default bus and an event handler bound to the
 * event bus is never found; so a transport that carries one bus's traffic is given that bus as `$bus`, and
 * `decode()` stamps THAT, ignoring any `self::BUS_HEADER` a producer put on the wire. The header is still
 * emitted on `encode()` as advisory routing metadata for a foreign consumer, but this decoder never trusts
 * it: letting the producer pick the internal bus is a capability it must not have.
 *
 * Trust model at the untrusted edge. A transport fed by a foreign producer must be given `$allowedTypes`,
 * the wire `type` aliases this channel accepts; `decode()` rejects anything else BEFORE resolving the class
 * or calling `fromPayload()`, so a loadable-but-unregistered FQCN can neither be instantiated nor dispatched.
 * Left null, the channel trusts every resolvable type, correct only for an in-process or same-trust-domain
 * transport, never for an integration edge. Pair `$allowedTypes` with a strict `EventTypeMapper`, one whose
 * `toClass` has no FQCN fallback, for defense in depth; the allowlist alone already gates instantiation.
 *
 * @see \Storm\Message\Header::MessageType
 * @see \Symfony\Component\Messenger\RoutableMessageBus
 */
final readonly class NeutralMessageSerializer implements SerializerInterface
{
    /** Transport header carrying the originating bus name, advisory for a foreign consumer; never read back here. */
    private const string BUS_HEADER = 'X-Message-Bus';

    /** Transport header carrying the Messenger redelivery count, re-attached as a `RedeliveryStamp` on decode so retry caps survive the neutral wire. */
    private const string REDELIVERY_HEADER = 'X-Redelivery-Count';

    /**
     * @param  UpcasterChain  $upcasters  the store's chain, migrating a wire payload from its carried
     *                                    version to the type's current one before the event is rebuilt
     * @param  string|null  $bus  the bus a decoded message routes to, fixed by the transport config, never
     *                            read from the wire; null attaches no bus stamp, Messenger's default bus
     * @param  list<string>|null  $allowedTypes  the wire `type` aliases this channel accepts; null trusts
     *                                           every resolvable type, for in-process or same-trust transports only
     */
    public function __construct(
        private MessageSerializer $serializer,
        private EventTypeMapper $eventTypeMapper,
        private UpcasterChain $upcasters = new UpcasterChain,
        private ?string $bus = null,
        private ?array $allowedTypes = null,
        /**
         * The bag keys re-stamped on decode, declared under `storm.context.propagated_keys`; the bundle owns the list.
         *
         * @var list<string>
         */
        private array $propagatedKeys = [],
    ) {}

    /**
     * {@inheritDoc}
     *
     * Serializes the envelope's message into a neutral wire message, then encodes that as JSON.
     *
     * @return array{body: string, headers: array<string, string>}
     *
     * @throws SerializationExceptionContract when the wrapped message is not a serializable payload, or the
     *                                        neutral JSON body cannot be encoded
     * @throws UnknownEventType when the message class is not a declared event type, so neither a portable
     *                          alias nor a schema version exists for it
     * @throws InvalidMessageException never in practice, since the envelope wraps a domain event, not a
     *                                 Message; declared because `new Message()` may throw it
     */
    #[Override]
    public function encode(Envelope $envelope): array
    {
        $class = $envelope->getMessage()::class;

        $data = $this->serializer->serialize($this->messageFrom($envelope));
        $type = $this->eventTypeMapper->toType($class);

        $header = SerializedMessage::externalizeType($data['header']); // the FQCN leaves the header; the shape's `type` field names the class

        $headers = ['type' => $type, 'Content-Type' => 'application/json'];

        if (($busName = $envelope->last(BusNameStamp::class)?->getBusName()) !== null) {
            $headers[self::BUS_HEADER] = $busName;
        }

        if (($retryCount = $envelope->last(RedeliveryStamp::class)?->getRetryCount()) !== null) {
            $headers[self::REDELIVERY_HEADER] = (string) $retryCount;
        }

        try {
            $body = json_encode(
                SerializedMessage::fromTriple($type, $this->eventTypeMapper->versionOf($class), $header, $data['content'])->toArray(),
                JSON_THROW_ON_ERROR,
            );
        } catch (JsonException $e) {
            throw SerializationException::cannotEncode($e);
        }

        return ['body' => $body, 'headers' => $headers];
    }

    /**
     * {@inheritDoc}
     *
     * Rebuilds the envelope from the neutral wire body, validating the untrusted edge, then re-attaches
     * the context stamps, the stored header, the carried redelivery count and the transport-configured bus.
     *
     * @throws MessageDecodingFailedException when the body, a reserved header, the message id, the actor
     *                                        pair or the redelivery count is malformed, the wire type is
     *                                        unknown or not allowed on this transport, or the payload
     *                                        cannot be upcast from its wire version
     */
    #[Override]
    public function decode(array $encodedEnvelope): Envelope
    {
        try {
            // the key check is part of the untrusted edge: a transport handing no body must land on
            // the decoding-failure contract like every other malformed input, never on a warning
            // plus an empty-string parse
            // @phpstan-ignore booleanNot.alwaysFalse, isset.offset, booleanOr.alwaysFalse (the @param shape guarantees body; the check is the runtime belt for a shape-violating transport)
            if (! isset($encodedEnvelope['body']) || ! is_string($encodedEnvelope['body'])) {
                throw new MessageDecodingFailedException('The encoded envelope carries no string body.');
            }

            $serialized = SerializedMessage::fromWire($encodedEnvelope['body']);

            $type = $serialized->requireType();

            if ($this->allowedTypes !== null && ! in_array($type, $this->allowedTypes, true)) {
                // a wire type this channel does not accept; reject BEFORE resolving the class or calling
                // fromPayload(), so a loadable-but-unregistered payload is never instantiated nor dispatched.
                throw new MessageDecodingFailedException(sprintf('The neutral wire type [%s] is not allowed on this transport.', $type));
            }

            $class = $this->eventTypeMapper->toClass($type);
            $header = SerializedMessage::internalizeType($serialized->header, $class);

            // the same chain as a store read: the payload is migrated from its wire version to the
            // type's current one before the event is rebuilt, so fromPayload always sees a current shape
            $content = $this->upcasters->upcast($type, $serialized->requireVersion(), $this->eventTypeMapper->versionOf($class), $serialized->content);

            $message = $this->serializer->deserialize(['header' => $header, 'content' => $content]);
        } catch (SerializationExceptionContract|UndecodableRow $e) {
            // when the body is malformed, the type alias is unknown, a reserved header has the wrong
            // wire type, or the payload cannot be upcast from its wire version; the contract
            // Messenger's receivers catch to reject the message, sending it to the failure transport,
            // rather than a storm exception they don't recognize, which crashes the worker
            throw new MessageDecodingFailedException('Cannot decode the neutral wire message: '.$e->getMessage(), previous: $e);
        }

        $messageId = $message->messageId();

        if ($messageId === null || trim($messageId) === '') {
            // No id, no inbox key: the boundary middleware would mint a FRESH one per delivery, so every
            // broker redelivery would re-run its handlers. Rejected at the untrusted edge, where the
            // producer's bug is visible and the receiver's decoding-failure path captures it.
            //
            // What actually arrives here is the ABSENT id. A blank one is WORSE, since every blank-id
            // message would share ONE dedup entry per transport, the first handled and every later
            // DIFFERENT one silently skipped-acked; that case is already refused a few lines up, inside
            // deserialize, where the header vocabulary rejects a blank framework header wholesale. The
            // blank arm below is the net under that gate, kept because the two live in different modules
            // and only one of them is this class's own contract.
            throw new MessageDecodingFailedException('The neutral wire message carries no usable __message_id header — a producer must stamp a stable, non-blank id, it is the consumer-side dedup key.');
        }

        if (($message->actorId() === null) !== ($message->actorType() === null)) {
            // wire-shape sanity, independent of whether ambient identity is later trusted: __actor_id
            // and __actor_type are one header pair in this vocabulary, and a producer sending half of
            // it is sending a malformed message, rejected here like any other malformed reserved header.
            throw new MessageDecodingFailedException('The neutral wire message carries half an actor identity — __actor_id and __actor_type travel together or not at all.');
        }

        try {
            // untrusted producer, so ambient identity is not: __correlation_id, __actor_id/__actor_type
            // and __tenant_id read from nothing here, since honoring them would let a foreign producer
            // fix the correlation that steers a live saga, or claim an actor or tenant it never
            // authenticated as. The declared bag is unaffected, an app-level opt-in with no such claim.
            //
            // Deferred, not built: a trusted-producer allowlist that would honor these identifiers for a
            // declared, verified internal service sharing this bus. Revisit only if one appears; nothing
            // today needs it, and a speculative seam here would be unexercised surface.
            $stamps = [
                ...ContextStamps::fromMessage($message, $this->propagatedKeys, trustAmbientIdentity: false),
                new StoredHeaderStamp($message->headers()),
            ];
        } catch (InvalidArgumentException|InvalidActor $e) {
            // with ambient identity untrusted, the only reachable throw here is the declared bag's own
            // validation, a blank, padded or reserved-prefixed key; still the untrusted edge, so it must
            // reject-to-failure-transport, not crash the worker with a type Messenger's receivers don't
            // recognize.
            //
            // `InvalidActor` stays in the union for the contract `fromMessage` still declares; dropping
            // it is an EQUIVALENT mutant regardless, by the type hierarchy: it extends
            // `InvalidArgumentException`, so the sibling arm would catch it just the same were it ever
            // reachable here. Left UN-ignored on purpose: dropping the OTHER type, the one this bag
            // validation actually throws, is killed by a test, and a line-level ignore would mask that
            // sibling.
            throw new MessageDecodingFailedException('Cannot decode the neutral wire message: '.$e->getMessage(), previous: $e);
        }

        if (($redelivery = $encodedEnvelope['headers'][self::REDELIVERY_HEADER] ?? null) !== null) {
            if (! ctype_digit((string) $redelivery)) {
                // a mistyped retry count is rejected like any other malformed wire value: dropping it
                // silently would reset the retry cap and re-arm the unbounded-retry poison this header closes
                throw new MessageDecodingFailedException('The neutral wire message carries a non-integer X-Redelivery-Count header.');
            }

            $stamps[] = new RedeliveryStamp((int) $redelivery);
        }

        // the bus is transport config, never the producer's `X-Message-Bus`: a foreign producer must not
        // choose which internal bus its message is dispatched to.
        if ($this->bus !== null) {
            $stamps[] = new BusNameStamp($this->bus);
        }

        return new Envelope($message->message(), $stamps);
    }

    /**
     * The faithful stored header when the envelope carries a `StoredHeaderStamp`, attached by the outbox
     * publisher, else the cross-cutting context only, such as a command routed async.
     *
     * @throws InvalidMessageException never in practice, since the envelope wraps a domain event, not a
     *                                 Message; declared because `new Message()` may throw it
     */
    private function messageFrom(Envelope $envelope): Message
    {
        $stored = $envelope->last(StoredHeaderStamp::class);

        if ($stored !== null) {
            // fromStored: the stamp carries a DURABLE header; a republished row may hold a reserved
            // key from another framework version; the strict write gate would brick its re-encode
            return Message::fromStored($envelope->getMessage(), $stored->header);
        }

        return ContextStamps::toMessage($envelope);
    }
}
