<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Setup;

use DomainFlow\EventSourcing\Interface\EventStorageInterface;
use DomainFlow\EventSourcing\Interface\ProcessManagerStorageInterface;
use DomainFlow\EventSourcing\Interface\SnapshotHistoryStorageInterface;
use DomainFlow\EventSourcing\Interface\SnapshotStorageInterface;
use DomainFlow\EventSourcingRedis\ProcessManager\RedisProcessManagerStorage;
use DomainFlow\EventSourcingRedis\Snapshot\RedisSnapshotHistoryStorage;
use DomainFlow\EventSourcingRedis\Snapshot\RedisSnapshotStorage;
use DomainFlow\EventSourcingRedis\Storage\RedisEventStorage;

trait RedisSetup
{
    use RedisHelper;

    protected RedisEventStorage $eventStorage;
    protected RedisSnapshotStorage $snapshotStorage;
    protected RedisSnapshotHistoryStorage $snapshotHistoryStorage;
    protected RedisProcessManagerStorage $processManagerStorage;

    public function setUp(): void
    {
        parent::setUp();
        $this->flushRedis();
    }

    public function tearDown(): void
    {
        $this->flushRedis();
    }

    protected function getStorage(): EventStorageInterface
    {
        return new RedisEventStorage($this->getRedis());
    }

    protected function getSnapshotStorage(): SnapshotStorageInterface
    {
        return new RedisSnapshotStorage($this->getRedis());
    }

    protected function getSnapshotHistoryStorage(): SnapshotHistoryStorageInterface
    {
        return new RedisSnapshotHistoryStorage($this->getRedis());
    }

    protected function getProcessManagerStorage(): ProcessManagerStorageInterface
    {
        return new RedisProcessManagerStorage($this->getRedis());
    }
}
