<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Snapshot;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\OccurredOn;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\SnapshotInterface;
use DomainFlow\EventSourcing\Interface\SnapshotStorageInterface;
use DomainFlow\EventSourcing\Snapshot\GenericSnapshot;
use JsonException;
use Redis;
use RuntimeException;

/**
 * Key scheme: `snapshots:{aggregateId}` — a Hash holding the latest snapshot's fields
 * (aggregate_id, version, occurred_on, state, snapshot_class), overwritten on every store.
 */
final readonly class RedisSnapshotStorage implements SnapshotStorageInterface
{
    private const string KEY_PREFIX = 'snapshots:';

    public function __construct(
        private Redis $redis
    ) {
    }

    public function storeSnapshot(
        SnapshotInterface $snapshot
    ): void {
        $this->redis->hMSet($this->key((string) $snapshot->getAggregateId()), [
            'aggregate_id' => (string) $snapshot->getAggregateId(),
            'version' => (string) $snapshot->getVersion()->toInt(),
            'occurred_on' => (string) $snapshot->getOccurredOn(),
            'state' => json_encode($snapshot->getState(), JSON_THROW_ON_ERROR),
            'snapshot_class' => $snapshot::class,
        ]);
    }

    public function retrieveSnapshot(
        EntityIdentifierInterface $aggregateId
    ): ?SnapshotInterface {
        /** @var array<string, string> $row */
        $row = $this->redis->hGetAll($this->key((string) $aggregateId));

        if ($row === []) {
            return null;
        }

        try {
            /** @var array<string, mixed> $state */
            $state = json_decode($row['state'] ?? '{}', true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new RuntimeException(
                sprintf('Failed to decode snapshot state for aggregate "%s": %s', $aggregateId, $e->getMessage()),
                0,
                $e
            );
        }

        return new GenericSnapshot(
            EntityIdentifier::fromString($row['aggregate_id'] ?? (string) $aggregateId),
            EventVersion::fromInt(isset($row['version']) && is_numeric($row['version']) ? (int) $row['version'] : 0),
            $state,
            OccurredOn::fromString($row['occurred_on'] ?? 'now')
        );
    }

    public function deleteSnapshot(
        EntityIdentifierInterface $aggregateId
    ): void {
        $this->redis->del($this->key((string) $aggregateId));
    }

    private function key(
        string $aggregateId
    ): string {
        return self::KEY_PREFIX . $aggregateId;
    }
}
