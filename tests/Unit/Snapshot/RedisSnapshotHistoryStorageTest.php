<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Unit\Snapshot;

use DomainFlow\EventSourcing\Interface\SnapshotHistoryStorageInterface;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractSnapshotHistoryStorageTestCase;
use DomainFlow\EventSourcingRedis\Snapshot\RedisSnapshotHistoryStorage;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisHelper;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(RedisSnapshotHistoryStorage::class)]
final class RedisSnapshotHistoryStorageTest extends AbstractSnapshotHistoryStorageTestCase
{
    use RedisHelper;

    protected function setUp(): void
    {
        parent::setUp();
        $this->flushRedis();

        $redis = $this->getRedis();

        $redis->zAdd('snapshot_history:corrupt-agg', 1, 'not-json');
        $redis->zAdd(
            'snapshot_history:invalid-date-agg',
            1,
            (string) json_encode(['occurred_on' => '2024-01-01 00:00:00.000000', 'state' => ['foo' => 'bar']])
        );
    }

    protected function tearDown(): void
    {
        $this->flushRedis();
    }

    protected function getSnapshotHistoryStorage(): SnapshotHistoryStorageInterface
    {
        return new RedisSnapshotHistoryStorage($this->getRedis());
    }
}
