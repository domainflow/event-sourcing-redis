<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventEntry;
use DomainFlow\EventSourcingCore\Provider\Integration\EventMigrationAndSerializationIntegrationTestCase;
use DomainFlow\EventSourcingCore\Provider\Integration\MigratableDummyEvent;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

/**
 * Bypasses RedisEventStorage entirely (a raw XADD, not storeEvents()) to simulate
 * pre-existing legacy-shaped data, mirroring the MySQL adapter's raw INSERT fixture.
 * Only exercises retrieveEvents() on a single aggregate, so the global index / versions
 * hash / maxversion keys don't need seeding here.
 */
#[CoversNothing]
final class EventMigrationAndSerializationIntegrationTest extends EventMigrationAndSerializationIntegrationTestCase
{
    use RedisSetup;

    /**
     * @param array<string, mixed> $payload
     */
    protected function insertEvent(
        string $eventId,
        EntityIdentifier $aggregateId,
        string $eventClass,
        int $version,
        string $occurredOn,
        array $payload
    ): void {
        $this->getRedis()->xAdd('events:aggregate:' . (string) $aggregateId, '*', [
            'event_id' => $eventId,
            'aggregate_id' => (string) $aggregateId,
            'event_class' => $eventClass,
            'version' => (string) $version,
            'occurred_on' => $occurredOn,
            'payload' => (string) json_encode($payload),
        ]);
    }

    protected function insertLegacyEvent(
        EntityIdentifier $aggregateId,
        string $eventId,
        string $occurredOn
    ): void {
        $this->insertEvent(
            $eventId,
            $aggregateId,
            MigratableDummyEvent::class,
            1,
            $occurredOn,
            [
                'aggregateId' => (string) $aggregateId,
                'eventId' => $eventId,
                'occurredOn' => $occurredOn,
                'version' => 1,
                'delta' => 3,
            ]
        );
    }

    protected function insertNewEventData(
        EntityIdentifier $aggregateId,
        string $eventId,
        string $occurredOn,
        string $description
    ): void {
        $this->insertEvent(
            $eventId,
            $aggregateId,
            MigratableDummyEvent::class,
            2,
            $occurredOn,
            [
                'aggregateId' => (string) $aggregateId,
                'eventId' => $eventId,
                'occurredOn' => $occurredOn,
                'version' => 2,
                'delta' => 7,
                'description' => $description,
                EventEntry::SCHEMA_VERSION_KEY => 2,
            ]
        );
    }
}
