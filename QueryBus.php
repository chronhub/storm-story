<?php

declare(strict_types=1);

namespace Storm\Story;

use LogicException;
use Storm\Story\Query\QueryResults;
use Storm\Story\Query\Settled;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\HandleTrait;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Throwable;

/**
 * Thin query bus: dispatches on `storm.query.bus` and returns the single handler's result.
 *
 * A query, unlike a command, has a return value. Messenger's HandleTrait dispatches the
 * message and reads the lone HandledStamp, throwing if zero or several handlers ran,
 * which enforces the "exactly one handler per query" rule for free.
 * Queries are always synchronous, the result must come back in-process, so this bus is
 * never routed to transport.
 *
 * @see \Symfony\Component\Messenger\Stamp\HandledStamp
 */
final class QueryBus
{
    use HandleTrait;

    public function __construct(
        #[Autowire(service: 'storm.query.bus')]
        MessageBusInterface $queryBus,
    ) {
        $this->messageBus = $queryBus;
    }

    /**
     * Ask the query bus to handle the query and return the result.
     *
     * @throws LogicException when zero or more than one handler ran; surfaced by Messenger's HandleTrait
     * @throws Throwable propagated from the handler, wrapped by Messenger's HandlerFailedException
     */
    public function ask(object $query): mixed
    {
        return $this->handle($query);
    }

    /**
     * Run every query, keyed by the input key; raise at the first failure, like `Promise.all`. Sequential
     * and short-circuiting: a failing query stops the run and the rest are not executed.
     *
     * @param  array<array-key, object>  $queries
     * @return array<array-key, mixed>
     *
     * @throws LogicException when a query ran zero or more than one handler
     * @throws Throwable propagated from the first failing handler
     */
    public function askAll(array $queries): array
    {
        return array_map($this->ask(...), $queries);
    }

    /**
     * Run every query, capturing each outcome, like `Promise.allSettled`; the caller picks the read mode on
     * the returned `QueryResults`. Sequential, no short-circuit: every query runs. A handler failure arrives
     * wrapped in `HandlerFailedException`; the wrapper is unwrapped so the captured error is the handler's
     * own exception, not the envelope.
     *
     * @param  array<array-key, object>  $queries
     */
    public function askAllSettled(array $queries): QueryResults
    {
        $settled = [];

        foreach ($queries as $key => $query) {
            try {
                $settled[$key] = Settled::ok($this->ask($query));
            } catch (HandlerFailedException $e) {
                // the recursive leaves, never one level: a handler asking another query
                // synchronously nests a second wrapper, and the captured error must be the type a
                // consumer branches on, not an envelope of envelopes
                $leaves = $e->getWrappedExceptions(recursive: true);
                $settled[$key] = Settled::err($leaves === [] ? $e : $leaves[array_key_first($leaves)]);
            } catch (Throwable $e) {
                $settled[$key] = Settled::err($e);
            }
        }

        return QueryResults::from($settled);
    }
}
