<?php

declare(strict_types=1);

namespace Storm\Story\Stamp;

use Storm\Bureau\Actor;
use Storm\Bureau\Exception\InvalidActor;
use Storm\Message\Exception\InvalidMessageException;
use Storm\Message\Header;
use Storm\Message\Message;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Single owner of the "which stamp corresponds to which header" mapping for the cross-cutting context.
 *
 * Used by the outbox publisher, turning `Message` headers into stamps to dispatch a stored event, and by
 * the transport serializer, mapping both ways across the wire. Keeping it here makes adding a context stamp
 * a one-place edit. Not a service but a pure, stateless mapping; it lives under `Stamp/`, excluded from DI.
 *
 * The propagation rule across hops:
 *
 * - Correlation travels end-to-end, as both trace and saga routing identity;
 *
 * - Actor and tenant travel end-to-end as the chain's root principal; the saga's `HopProtocol`
 *   copies them from the ambient `MessageContext` onto its outgoing commands, and this mapper re-stamps them;
 *
 * - The DECLARED bag keys, listed under `storm.context.propagated_keys`, travel end-to-end as one
 *   `ContextBagStamp`: the caller passes the declared list to `fromMessage`, which lifts only
 *   those headers; an undeclared application header stays a one-message annotation and never
 *   re-stamps;
 *
 * - The message id is per-message, stable per outbox row, for dedup;
 *
 * - Causation is re-derived at each hop from the handled message's id, never transported.
 *
 * The three above travel this way only from a TRUSTED producer, an internal republish or an
 * outgoing saga command. A message decoded off an untrusted wire calls `fromMessage` with
 * `$trustAmbientIdentity: false`: the wire's `__correlation_id`, `__actor_id`/`__actor_type` and
 * `__tenant_id` are read by nothing downstream, since a foreign producer fixing the correlation
 * that steers a live saga, or claiming an actor or tenant it never authenticated as, is exactly the
 * capability an untrusted edge must not have. The declared bag is unaffected: it is an app-level
 * opt-in, never a framework identity.
 *
 * Adding a context identifier touches many sites, and nothing structural catches partial wiring: a missed
 * site reads as a silent null downstream. The sites are:
 *
 * - The `Header` enum case, plus `assertValue` when non-string;
 * - The `Message`-typed accessor;
 * - Contracts `MessageContext`, a BC-break for implementors;
 * - `ContextValues`;
 * - The `CurrentMessageContext` delegate;
 * - A new `Stamp` class;
 * - Both directions here;
 * - `BindMessageContext`;
 * - `ContextEnricher`'s pairs;
 * - `HopProtocol`, the saga hop.
 */
final class ContextStamps
{
    /**
     * Maps Message headers to bus stamps.
     *
     * @param  list<string>  $propagatedKeys  the bag keys declared under `storm.context.propagated_keys`;
     *                                        only these application headers are lifted into a
     *                                        ContextBagStamp, and only when present with a scalar value
     * @param  bool  $trustAmbientIdentity  false for a message decoded off an untrusted wire: correlation,
     *                                      actor and tenant are read from nothing then, so a foreign
     *                                      producer's claim never becomes ambient identity. The declared
     *                                      bag is unaffected either way
     * @return list<StampInterface>
     *
     * @throws InvalidActor when actor id or type is empty, meaning the headers were malformed
     * @throws InvalidMessageException when the message carries exactly one half of the actor pair; a
     *                                 corruption refused at the CENTRAL normalizer, since silently dropping
     *                                 the half would erase provenance on the next hop. The outbox publishers
     *                                 hydrate stored rows tolerantly, so a corrupt row reaches here
     *                                 constructible
     */
    public static function fromMessage(Message $message, array $propagatedKeys = [], bool $trustAmbientIdentity = true): array
    {
        $stamps = [];

        if (($id = $message->messageId()) !== null) {
            $stamps[] = new MessageIdStamp($id);
        }

        if ($trustAmbientIdentity) {
            if (($correlationId = $message->correlationId()) !== null) {
                $stamps[] = new CorrelationStamp($correlationId);
            }

            $actorId = $message->actorId();
            $actorType = $message->actorType();

            if (($actorId === null) !== ($actorType === null)) {
                throw InvalidMessageException::halfActorIdentity(
                    $actorId !== null ? Header::ActorId->value : Header::ActorType->value,
                    $actorId !== null ? Header::ActorType->value : Header::ActorId->value,
                );
            }

            if ($actorId !== null && $actorType !== null) {
                $stamps[] = new ActorStamp(new Actor($actorId, $actorType));
            }

            if (($tenantId = $message->tenantId()) !== null) {
                $stamps[] = new TenantStamp($tenantId);
            }
        }

        if ($propagatedKeys !== []) {
            $bag = [];
            $headers = $message->headers();

            foreach ($propagatedKeys as $key) {
                if (is_scalar($headers[$key] ?? null)) {
                    $bag[$key] = $headers[$key];
                }
            }

            if ($bag !== []) {
                $stamps[] = new ContextBagStamp($bag);
            }
        }

        return $stamps;
    }

    /**
     * Maps a bus envelope, its event and stamps, to a neutral Message carrying the context as headers. The
     * event type header is not set here: it is not a stamp, so the serializer derives it from the event
     * class.
     *
     * @throws InvalidMessageException never in practice, since the envelope wraps a domain event, not a
     *                                 Message; declared because `new Message()` may throw it
     */
    public static function toMessage(Envelope $envelope): Message
    {
        $headers = [];

        if (($stamp = $envelope->last(MessageIdStamp::class)) !== null) {
            $headers[Header::MessageId->value] = $stamp->id;
        }

        if (($stamp = $envelope->last(CorrelationStamp::class)) !== null) {
            $headers[Header::CorrelationId->value] = $stamp->id;
        }

        if (($stamp = $envelope->last(ActorStamp::class)) !== null) {
            $headers[Header::ActorId->value] = $stamp->actor->id;
            $headers[Header::ActorType->value] = $stamp->actor->type;
        }

        if (($stamp = $envelope->last(TenantStamp::class)) !== null) {
            $headers[Header::TenantId->value] = $stamp->id;
        }

        if (($stamp = $envelope->last(ContextBagStamp::class)) !== null) {
            foreach ($stamp->values as $key => $value) {
                $headers[$key] = $value;
            }
        }

        return new Message($envelope->getMessage(), $headers);
    }
}
