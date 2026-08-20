<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration;

use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcingCore\Provider\Integration\CustomFieldsStorageTestCase;
use DomainFlow\EventSourcingRedis\Storage\RedisEventStorage;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

/**
 * Unlike the MySQL adapter's equivalent, this needs no schema setup: RedisEventStorage
 * writes whatever fields the factory's EventPersistenceRecord produces, schemaless.
 */
#[CoversNothing]
final class CustomFieldsStorageTest extends CustomFieldsStorageTestCase
{
    use RedisSetup;

    public function getStorageWithFactory(
        EventEntryFactoryInterface $factory
    ): RedisEventStorage {
        return new RedisEventStorage($this->getRedis(), $factory);
    }
}
