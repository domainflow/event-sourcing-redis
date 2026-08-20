<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration\Repository;

use DomainFlow\EventSourcingCore\Provider\Integration\CounterProjectionRepositoryInterface;
use Redis;

/**
 * Test-only fixture, mirroring the MySQL adapter's MySqlCounterProjectionRepository:
 * a single Hash (field = aggregate ID, value = counter) instead of a dedicated SQL table.
 */
final readonly class RedisCounterProjectionRepository implements CounterProjectionRepositoryInterface
{
    private const string KEY = 'counter_projection';

    public function __construct(
        private Redis $redis
    ) {
    }

    public function getCounter(
        string $aggregateId
    ): ?int {
        $value = $this->redis->hGet(self::KEY, $aggregateId);

        return is_string($value) && is_numeric($value) ? (int) $value : null;
    }

    public function saveCounter(
        string $aggregateId,
        int $counter
    ): void {
        $this->redis->hSet(self::KEY, $aggregateId, (string) $counter);
    }

    public function reset(): void
    {
        $this->redis->del(self::KEY);
    }

    /**
     * @return array<string, mixed>[]
     */
    public function all(): array
    {
        /** @var array<string, string> $rows */
        $rows = $this->redis->hGetAll(self::KEY);

        return array_map(
            static fn (string $aggregateId, string $counter): array => [
                'aggregate_id' => $aggregateId,
                'counter' => (int) $counter,
            ],
            array_keys($rows),
            array_values($rows)
        );
    }
}
