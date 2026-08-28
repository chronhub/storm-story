<?php

declare(strict_types=1);

namespace Storm\Story\Stamp;

use InvalidArgumentException;
use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * The declared transverse bag in transit: the propagated application headers of the message,
 * carried as one stamp between the dispatch boundary and the ambient context.
 *
 * Which keys ride here is DECLARED wiring under `storm.context.propagated_keys`, applied by the
 * re-stamping publishers through `ContextStamps::fromMessage`. A dispatch-time producer constructs
 * the stamp directly: an HTTP channel posing its provenance, a CLI entry posing a trace id. The
 * framework never reads the values. Maps to plain application headers on recorded events.
 *
 * The same rules as ContextValues, enforced at construction so a malformed bag fails at its
 * source: keys are non-blank and never `__`-reserved. Value shape is the phpdoc contract; the
 * runtime net is Message's JSON-tree gate, held by BOTH write doors, constructor and
 * `withHeader()` alike, so an unjsonable value refuses whichever path carries the bag onto an
 * envelope. An empty bag is refused for the reason a blank tenant is: absence is carried by NO
 * stamp at all.
 */
final readonly class ContextBagStamp implements StampInterface
{
    /**
     * @param  array<string, bool|float|int|string>  $values
     *
     * @throws InvalidArgumentException when the bag is empty, or a key is blank, padded or `__`-reserved
     */
    public function __construct(
        public array $values,
    ) {
        if ($values === []) {
            throw new InvalidArgumentException('A context bag stamp cannot be empty: an absent bag is carried by no stamp at all, and an empty one would only add a phantom hop to inspect.');
        }

        foreach (array_keys($values) as $key) {
            $key = (string) $key; // a runtime bag may carry int keys despite the phpdoc shape

            // the same refusal Message's own gate applies, at the dispatch boundary: a key this
            // stamp accepts must never blow up mid-enrichment when withHeader() replays it
            if ($key === '' || $key !== trim($key)) {
                throw new InvalidArgumentException(sprintf('A context bag key must be non-blank and unpadded, "%s" is not: "x", " x" and "x " would ride as three silently distinct headers.', $key));
            }

            if (str_starts_with($key, '__')) {
                throw new InvalidArgumentException(sprintf('The context bag refuses the reserved framework prefix "__": "%s". Framework identifiers travel as their own stamps, never through the bag.', $key));
            }
        }
    }
}
