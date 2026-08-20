<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Unit\Snapshot;

use DomainFlow\EventSourcing\Interface\SnapshotStorageInterface;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractSnapshotStorageTestCase;
use DomainFlow\EventSourcingRedis\Snapshot\RedisSnapshotStorage;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisHelper;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(RedisSnapshotStorage::class)]
final class RedisSnapshotStorageTest extends AbstractSnapshotStorageTestCase
{
    use RedisHelper;

    protected function setUp(): void
    {
        parent::setUp();
        $this->flushRedis();

        $redis = $this->getRedis();

        $redis->hMSet('snapshots:json-corrupt-id', [
            'aggregate_id' => 'json-corrupt-id',
            'version' => '1',
            'occurred_on' => '2024-01-01 00:00:00.000000',
            'state' => 'INVALID_JSON',
            'snapshot_class' => 'DomainFlow\\EventSourcing\\Snapshot\\GenericSnapshot',
        ]);

        $redis->hMSet('snapshots:bad-occurred-id', [
            'aggregate_id' => 'bad-occurred-id',
            'version' => '1',
            'occurred_on' => '2024-01-01 00:00:00.000000',
            'state' => '{"x":"y"}',
            'snapshot_class' => 'DomainFlow\\EventSourcing\\Snapshot\\GenericSnapshot',
        ]);
    }

    protected function tearDown(): void
    {
        $this->flushRedis();
    }

    protected function getSnapshotStorage(): SnapshotStorageInterface
    {
        return new RedisSnapshotStorage($this->getRedis());
    }
}
