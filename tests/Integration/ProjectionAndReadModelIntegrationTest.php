<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration;

use DomainFlow\EventSourcingCore\Provider\Integration\CounterProjectionRepositoryInterface;
use DomainFlow\EventSourcingCore\Provider\Integration\ProjectionAndReadModelIntegrationTestCase;
use DomainFlow\EventSourcingRedis\Tests\Integration\Repository\RedisCounterProjectionRepository;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class ProjectionAndReadModelIntegrationTest extends ProjectionAndReadModelIntegrationTestCase
{
    use RedisSetup;

    protected function setupCounterProjections(): void
    {
        $this->flushRedis();
    }

    protected function getCounterProjectionRepository(): CounterProjectionRepositoryInterface
    {
        return new RedisCounterProjectionRepository($this->getRedis());
    }

    protected function getCounterFromProjection(string $aggregateId): ?int
    {
        return $this->getCounterProjectionRepository()->getCounter($aggregateId);
    }

    protected function getProjectionCounter(string $aggregateId): ?int
    {
        return $this->getCounterProjectionRepository()->getCounter($aggregateId);
    }

    protected function projectionRowExists(string $aggregateId): bool
    {
        return $this->getCounterProjectionRepository()->getCounter($aggregateId) !== null;
    }
}
