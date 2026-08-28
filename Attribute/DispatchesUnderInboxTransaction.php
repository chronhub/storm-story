<?php

declare(strict_types=1);

namespace Storm\Story\Attribute;

use Attribute;
use Storm\Story\Consume\InboxTransactionContext;
use Storm\Story\Middleware\DeduplicateConsumer;
use Storm\Story\Transport\InboxGuardedSendersLocator;

/**
 * Signs the dual-write contract for a message handler that dispatches an async-routed message while the
 * inbox transaction is open.
 *
 * A broker send inside an inbox-owned transaction cannot be rolled back with it: on a later failure the
 * transaction and the inbox row are undone, but the sent message stands, and the replay's nested dispatch
 * is a fresh envelope the downstream inbox cannot recognize as a duplicate. Without this attribute the
 * guarded senders locator REFUSES such a send, loud, at the send site; the accidental dual-write is
 * impossible to write.
 *
 * Declaring the attribute states, per handler class, that the handler OWNS the at-least-once consequence,
 * through one of the two working defenses:
 *
 * - A deterministic message id on the nested dispatch, `MessageIdStamp` derived from the handled
 *   message, its causation or a settlement id, so a replayed duplicate dedups on the downstream
 *   inbox;
 *
 * - A domain-idempotent target, where re-dispatching the same intent is a no-op, such as an
 *   idempotent `Engine::start` or a saga outbox keyed on the issued id.
 *
 * The grant is compiled per HANDLED message type and read AMBIENTLY: only a consume path opens a
 * context frame, a received message or a batched dispatch, and a nested in-process dispatch opens
 * none. While a granted class is being handled, broker sends therefore pass for the ENTIRE
 * synchronous call tree beneath it: every co-handler of the same class, and every handler a nested
 * sync dispatch reaches. Signing the contract means owning the at-least-once consequence for that
 * whole tree, so keep the granted handler's synchronous fan-out small and deliberate.
 *
 * Alternatives weighed and not retained:
 *
 * - A generic transactional command outbox, the mechanical closure, is a full design for what is
 *   today a handful of declared sites; re-evaluate when a second consumer needs it or a declared
 *   site cannot provide either defense;
 *
 * - Framework-derived deterministic child ids, a parent id plus a dispatch ordinal, would close the
 *   replay hole at the root but require deterministic handlers; a hardening that would sit BEHIND
 *   this attribute, not a default.
 *
 * @see InboxGuardedSendersLocator the enforcement point
 * @see InboxTransactionContext the ambient transaction marker
 * @see DeduplicateConsumer the contract this attribute signs
 */
#[Attribute(Attribute::TARGET_CLASS)]
final readonly class DispatchesUnderInboxTransaction {}
