<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration;

use DomainFlow\EventSourcing\Interface\ProjectorInterface;
use DomainFlow\EventSourcingCore\Provider\Integration\ProjectionReplayIntegrationTestCase;
use DomainFlow\EventSourcingRedis\Tests\Integration\Repository\RedisCounterProjectionRepository;
use DomainFlow\EventSourcingRedis\Tests\Integration\Repository\RedisCounterProjector;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class ProjectionReplayIntegrationTest extends ProjectionReplayIntegrationTestCase
{
    use RedisSetup;

    protected function setupCounterProjections(): void
    {
        $this->flushRedis();
    }

    protected function getCounterProjectionRepository(): ProjectorInterface
    {
        return new RedisCounterProjector($this->getRedis());
    }

    protected function getProjectionCounter(string $aggregateId): ?int
    {
        return (new RedisCounterProjectionRepository($this->getRedis()))->getCounter($aggregateId);
    }

    protected function projectionRowExists(string $aggregateId): bool
    {
        return $this->getProjectionCounter($aggregateId) !== null;
    }

    protected function getAllProjectionRows(): array
    {
        return (new RedisCounterProjectionRepository($this->getRedis()))->all();
    }
}
