<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Fixture;

/**
 * A message contract fixture: what an interface-typed `#[AsMessageHandler]` declares, so the
 * compiled inbox-dispatch grant may carry an interface while the runtime context holds a concrete
 * implementing class.
 */
interface SampleContract {}
