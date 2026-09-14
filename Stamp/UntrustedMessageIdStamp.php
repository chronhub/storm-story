<?php

declare(strict_types=1);

namespace Storm\Story\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Marks a producer-supplied message id that may deduplicate deliveries but must not seed ambient correlation.
 *
 * The neutral decoder attaches this marker on every delivery. Internal PHP serialization preserves it
 * so failure capture before bus enrichment cannot restore trust in the producer id. The neutral wire
 * does not carry the marker; decoding reconstructs it.
 */
final readonly class UntrustedMessageIdStamp implements StampInterface {}
