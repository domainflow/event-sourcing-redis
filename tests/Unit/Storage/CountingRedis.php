<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Unit\Storage;

use Redis;

/**
 * A real Redis that counts how many round trips it was actually asked for.
 *
 * The property under test is not speed — wall-clock timing in a test
 * measures the machine — it is the *number of calls*. A page of a hundred
 * events must cost one resolution, not a hundred and one, and the only honest
 * way to assert that is to count.
 *
 * A pipelined call is queued rather than sent, so `xRange()` inside `multi()`
 * is not a round trip; that is exactly the distinction being measured, which
 * is why the pipeline state is tracked rather than every call being counted
 * alike.
 */
final class CountingRedis extends Redis
{
    /** Calls that each cost their own round trip. */
    public int $directXRangeCalls = 0;

    /** Pipelines opened — one of these resolves any number of pointers. */
    public int $pipelinesOpened = 0;

    private bool $inPipeline = false;

    public function multi(
        int $value = Redis::MULTI
    ): Redis|bool {
        if ($value === Redis::PIPELINE) {
            $this->inPipeline = true;
            $this->pipelinesOpened++;
        }

        return parent::multi($value);
    }

    public function exec(): Redis|array|false
    {
        $this->inPipeline = false;

        return parent::exec();
    }

    public function xRange(
        string $key,
        string $start,
        string $end,
        int $count = -1
    ): Redis|array|bool {
        if (!$this->inPipeline) {
            $this->directXRangeCalls++;
        }

        return parent::xRange($key, $start, $end, $count);
    }
}
