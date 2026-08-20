<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Snapshot;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\OccurredOn;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\SnapshotHistoryStorageInterface;
use DomainFlow\EventSourcing\Interface\SnapshotInterface;
use DomainFlow\EventSourcing\Snapshot\GenericSnapshot;
use JsonException;
use Redis;
use RuntimeException;

/**
 * Key scheme: `snapshot_history:{aggregateId}` — a Sorted Set scored by version, so
 * deleteSingle(aggregateId, version) is a native ZREMRANGEBYSCORE rather than a
 * read-modify-write of the whole history. Each member is a JSON blob of {occurred_on, state};
 * aggregate_id/version live outside the blob (score + read context), not duplicated in it.
 */
final readonly class RedisSnapshotHistoryStorage implements SnapshotHistoryStorageInterface
{
    private const string KEY_PREFIX = 'snapshot_history:';

    public function __construct(
        private Redis $redis
    ) {
    }

    public function persistVersioned(
        SnapshotInterface $snapshot
    ): void {
        // JSON_THROW_ON_ERROR, because the read path already throws on bad
        // JSON: without it a failed encode was stored as an empty string and
        // surfaced on load instead of on save, as far from the cause as it
        // could be.
        $member = json_encode([
            'occurred_on' => (string) $snapshot->getOccurredOn(),
            'state' => $snapshot->getState(),
        ], JSON_THROW_ON_ERROR);

        $this->redis->zAdd(
            $this->key((string) $snapshot->getAggregateId()),
            $snapshot->getVersion()->toInt(),
            $member
        );
    }

    public function deleteSingle(
        EntityIdentifierInterface $aggregateId,
        int $version
    ): void {
        $this->redis->zRemRangeByScore($this->key((string) $aggregateId), (string) $version, (string) $version);
    }

    public function deleteAll(
        EntityIdentifierInterface $aggregateId
    ): void {
        $this->redis->del($this->key((string) $aggregateId));
    }

    /**
     * @return array<SnapshotInterface>
     */
    public function retrieveAll(
        EntityIdentifierInterface $aggregateId
    ): array {
        /** @var array<string, float> $membersWithScores */
        $membersWithScores = $this->redis->zRange($this->key((string) $aggregateId), 0, -1, true);

        $snapshots = [];
        foreach ($membersWithScores as $member => $version) {
            $snapshots[] = $this->hydrate($aggregateId, (int) $version, $member);
        }

        return $snapshots;
    }

    private function hydrate(
        EntityIdentifierInterface $aggregateId,
        int $version,
        string $member
    ): SnapshotInterface {
        try {
            /** @var array{occurred_on: string, state: array<string, mixed>} $record */
            $record = json_decode($member, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new RuntimeException(
                sprintf(
                    'Failed to decode snapshot history state for aggregate "%s" at version %d',
                    $aggregateId,
                    $version
                ),
                0,
                $e
            );
        }

        return new GenericSnapshot(
            EntityIdentifier::fromString((string) $aggregateId),
            EventVersion::fromInt($version),
            $record['state'] ?? [],
            OccurredOn::fromString($record['occurred_on'] ?? 'now')
        );
    }

    private function key(
        string $aggregateId
    ): string {
        return self::KEY_PREFIX . $aggregateId;
    }
}
