<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Fixture;

/**
 * The concrete message behind `SampleContract`: the class the inbox-transaction context reports
 * while an interface-granted handler runs.
 */
final readonly class ContractedCommand implements SampleContract {}
