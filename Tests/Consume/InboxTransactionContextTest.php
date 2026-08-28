<?php

declare(strict_types=1);

namespace Storm\Story\Tests\Consume;

use LogicException;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use stdClass;
use Storm\Story\Consume\InboxTransactionContext;

final class InboxTransactionContextTest extends TestCase
{
    #[Test]
    public function starts_inactive_with_no_current_message(): void
    {
        $context = new InboxTransactionContext;

        $this->assertFalse($context->active());
        $this->assertNull($context->current());
    }

    #[Test]
    public function an_entry_is_active_and_names_the_handled_message(): void
    {
        $context = new InboxTransactionContext;

        $context->enter(stdClass::class);

        $this->assertTrue($context->active());
        $this->assertSame(stdClass::class, $context->current());

        $context->leave();

        $this->assertFalse($context->active());
        $this->assertNull($context->current());
    }

    #[Test]
    public function a_nested_entry_restores_the_parent_on_leave(): void
    {
        // a stack, not a flag: a same-process re-entry must restore the parent's entry, not clear it
        $context = new InboxTransactionContext;

        $context->enter(stdClass::class);
        $context->enter(self::class);

        $this->assertSame(self::class, $context->current());

        $context->leave();

        $this->assertTrue($context->active());
        $this->assertSame(stdClass::class, $context->current());
    }

    #[Test]
    public function leaving_without_a_matching_enter_refuses_loud(): void
    {
        // the silent no-op this replaces was the dangerous half: an unbalanced leave() would later
        // pop a PARENT frame, disarming the dual-write guard on the outer transaction
        $context = new InboxTransactionContext;

        $this->expectException(LogicException::class);

        $context->leave();
    }
}
