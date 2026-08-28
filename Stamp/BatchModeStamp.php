<?php

declare(strict_types=1);

namespace Storm\Story\Stamp;

use Storm\Story\Middleware\DeduplicateConsumer;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * Marks an envelope dispatched from inside a batched inbox transaction.
 *
 * The batch consumer owns the dedup `INSERT` and the shared transaction for the whole batch, then dispatches
 * each message on the bus carrying this stamp. `DeduplicateConsumer` sees it and runs the handlers directly;
 * it does NOT open its own per-message `once()` transaction, which would nest inside and fight the batch
 * transaction.
 *
 * Non-sendable, deliberately: this stamp is an in-process execution mode, never wire state. Sendable, a
 * producer-forged or accidentally-carried copy on a PhpSerializer transport would ride into the Worker path
 * and switch the inbox OFF for that delivery, the handlers running outside any inbox transaction. The
 * senders strip it, so only the batch consumer, constructing it in the same process, can ever set it.
 *
 * @see DeduplicateConsumer the middleware that honors this stamp
 * @see \Storm\Chronicler\Inbox\BatchInboxStore
 */
final readonly class BatchModeStamp implements NonSendableStampInterface {}
