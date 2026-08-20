<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Storage;

use DomainFlow\EventSourcing\Event\DefaultEventEntryFactory;
use DomainFlow\EventSourcing\Event\EventPersistenceRecord;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\GlobalEventPage;
use DomainFlow\EventSourcing\Exception\ConcurrencyException;
use DomainFlow\EventSourcing\Exception\EventSourcingException;
use DomainFlow\EventSourcing\Interface\DomainEventInterface;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcing\Interface\EventFactoryInterface;
use DomainFlow\EventSourcing\Interface\EventStorageInterface;
use DomainFlow\EventSourcing\Interface\OutboxBackedStorageInterface;
use DomainFlow\EventSourcingRedis\Outbox\RedisOutboxStorage;
use DomainFlow\EventSourcingRedis\Support\ConvertsToRedisString;
use DomainFlow\EventSourcingRedis\Support\ReadsRedisReplies;
use Redis;
use RuntimeException;
use Throwable;

/**
 * Key scheme (see docs/ARCHITECTURE.md):
 *  - events:aggregate:{aggregateId}  Redis Stream, one entry per event, entry id "1-{version}".
 *  - events:global                   Sorted set, score = monotonic sequence, member = pointer.
 *  - events:global:seq               String counter feeding the sorted set score.
 *
 * The aggregate's version lives in the sequence half of the stream entry id, which makes the stream
 * the single source of truth for three things at once: XADD refuses any id that is not strictly
 * greater than the stream's top entry, so the version sequence is the concurrency guard; XRANGE with
 * an exclusive lower bound reads "everything after version N" server-side; and the stream's own order
 * is the version order by construction. That is why there is no longer a :versions hash and no
 * :maxversion key — both stated a fact the stream already states, and a second source of truth for
 * the same fact can only ever drift.
 *
 * The version is also kept as a field on the entry, because that is what the persistence record is
 * hydrated from. The id is the index; the field is the data.
 *
 * Pointer members in events:global are "{aggregateId}\x1f{streamEntryId}" so a global read can resolve
 * back into the source stream. \x1f (ASCII unit separator) is used because it cannot occur in a normal
 * aggregate id string.
 */
final class RedisEventStorage implements EventStorageInterface, OutboxBackedStorageInterface
{
    use ReadsRedisReplies;

    use ConvertsToRedisString;

    private const string STREAM_KEY_PREFIX = 'events:aggregate:';
    private const string GLOBAL_INDEX_KEY = 'events:global';
    private const string GLOBAL_SEQ_KEY = 'events:global:seq';
    private const string POINTER_DELIMITER = "\x1f";

    /**
     * Appends a whole call atomically, across any number of aggregates.
     *
     * KEYS: the global index, the global sequence, the two outbox keys, then
     * one stream key per aggregate, in the order the aggregates appear in ARGV.
     * ARGV: aggregateCount, outboxEnabled, outboxEntryPrefix, then per
     * aggregate: id, eventCount, and per event: version, fieldCount, followed
     * by fieldCount name/value pairs.
     *
     * Every aggregate's versions are reserved and verified before the first
     * XADD, so a rejected call never writes anything and needs no undo. That
     * is possible because the version is the entry id: an aggregate's current
     * version is simply the last entry of its stream.
     *
     * One script rather than one per aggregate: a Lua script runs to
     * completion without interleaving and may touch any number of keys, so the
     * whole call is the unit. Redis Cluster would refuse the multi-slot KEYS,
     * and Redis Cluster is not supported by this adapter.
     */
    private const string STORE_EVENTS_SCRIPT = <<<'LUA'
        local globalIndexKey = KEYS[1]
        local globalSeqKey = KEYS[2]
        local outboxPendingKey = KEYS[3]
        local outboxSeqKey = KEYS[4]
        local firstStreamKey = 5

        local aggregateCount = tonumber(ARGV[1])
        local outboxEnabled = ARGV[2] == '1'
        local outboxEntryPrefix = ARGV[3]

        local cursor = 4
        local aggregates = {}

        for a = 1, aggregateCount do
            local aggregate = {
                id = ARGV[cursor],
                streamKey = KEYS[firstStreamKey + a - 1],
                versions = {},
                fieldSets = {},
            }
            cursor = cursor + 1

            aggregate.eventCount = tonumber(ARGV[cursor])
            cursor = cursor + 1

            for e = 1, aggregate.eventCount do
                aggregate.versions[e] = ARGV[cursor]
                cursor = cursor + 1

                local fieldCount = tonumber(ARGV[cursor]) * 2
                cursor = cursor + 1

                local fields = {}
                for f = 1, fieldCount do
                    fields[f] = ARGV[cursor]
                    cursor = cursor + 1
                end
                aggregate.fieldSets[e] = fields
            end

            aggregates[a] = aggregate
        end

        -- Every aggregate is checked before any aggregate is written, so the
        -- last event of the call can still stop the first one from landing.
        for a = 1, aggregateCount do
            local aggregate = aggregates[a]
            local previous = 0
            local top = redis.call('XREVRANGE', aggregate.streamKey, '+', '-', 'COUNT', 1)
            if top[1] ~= nil then
                previous = tonumber(string.match(top[1][1], '%-(%d+)$'))
            end

            for e = 1, aggregate.eventCount do
                local version = tonumber(aggregate.versions[e])
                if version == nil or version <= previous then
                    return 'CONFLICT:' .. aggregate.id .. '\31' .. aggregate.versions[e]
                end
                previous = version
            end
        end

        for a = 1, aggregateCount do
            local aggregate = aggregates[a]

            for e = 1, aggregate.eventCount do
                local entryId = redis.call('XADD', aggregate.streamKey, '1-' .. aggregate.versions[e], unpack(aggregate.fieldSets[e]))

                local seq = redis.call('INCR', globalSeqKey)
                redis.call('ZADD', globalIndexKey, seq, aggregate.id .. '\31' .. entryId)

                -- Inside this script, so the pending delivery and the event it
                -- describes are one atomic act. A script is the only place in
                -- Redis where that is true of two writes.
                if outboxEnabled then
                    local outboxId = redis.call('INCR', outboxSeqKey)
                    redis.call('HSET', outboxEntryPrefix .. outboxId, 'attempts', '0', unpack(aggregate.fieldSets[e]))
                    redis.call('ZADD', outboxPendingKey, 0, outboxId)
                end
            end
        end

        return 'OK'
        LUA;

    private const string DELETE_EVENTS_SCRIPT = <<<'LUA'
        local streamKey = KEYS[1]
        local globalIndexKey = KEYS[2]

        local aggregateId = ARGV[1]

        local entries = redis.call('XRANGE', streamKey, '-', '+')
        for _, entry in ipairs(entries) do
            redis.call('ZREM', globalIndexKey, aggregateId .. '\31' .. entry[1])
        end

        redis.call('DEL', streamKey)

        return true
        LUA;

    private readonly EventEntryFactoryInterface $entryFactory;

    /**
     * @param bool $outboxEnabled Whether to write a pending delivery for every
     *        event, inside the same Lua script. A flag rather than an injected
     *        RedisOutboxStorage: the entries are written by the script, not by
     *        that class, so passing the object would suggest a collaboration
     *        that does not happen. Use RedisOutboxStorage for the relay side.
     * @param bool $assertDurableConfiguration Whether to refuse a Redis whose
     *        configuration can lose events. Pass false only when the
     *        configuration has been verified some other way — the check itself
     *        needs CONFIG GET, which some managed Redis offerings forbid.
     */
    public function __construct(
        private readonly Redis $redis,
        ?EventEntryFactoryInterface $entryFactory = null,
        ?EventFactoryInterface $eventFactory = null,
        bool $assertDurableConfiguration = true,
        private readonly bool $outboxEnabled = false
    ) {
        // The event factory goes to the entry factory that will use it, not
        // into a process-wide static.
        $this->entryFactory = $entryFactory ?? new DefaultEventEntryFactory($eventFactory);

        if ($assertDurableConfiguration) {
            $this->assertDurableConfiguration();
        }
    }

    /**
     * Refuses a Redis configured in a way that loses events.
     *
     * Checked here rather than lazily on first write, unlike the MongoDB
     * adapter's index creation: this class is handed an already-connected
     * Redis, so asking it two questions costs a round trip and no more. And a
     * misconfiguration found at construction is a five-second fix, while the
     * same misconfiguration found in production is missing history.
     *
     * Both settings are checked unconditionally, including maxmemory-policy on
     * an instance with no maxmemory limit. Eviction only happens once a limit
     * exists, but the policy is what decides *what* gets evicted when someone
     * sets one later — and by then nothing announces it.
     *
     * @throws EventSourcingException
     * @return void
     */
    private function assertDurableConfiguration(): void
    {
        $policy = $this->readConfiguration('maxmemory-policy');

        if ($policy !== 'noeviction') {
            throw new EventSourcingException(sprintf(
                'Redis is configured with maxmemory-policy "%s". An event store is a system of record and '
                . 'its data must never be evicted; any other policy lets Redis silently discard event '
                . 'streams to reclaim memory, with no error anywhere. Set "noeviction", or point this '
                . 'adapter at an instance that is not shared with a cache.',
                $policy
            ));
        }

        $appendOnly = $this->readConfiguration('appendonly');

        if ($appendOnly !== 'yes') {
            throw new EventSourcingException(
                'Redis is configured with appendonly "no". RDB snapshotting alone loses every event '
                . 'written since the last snapshot when the process dies. Set "appendonly yes", and '
                . '"appendfsync always" if no event may ever be lost.'
            );
        }
    }

    /**
     * @param string $setting
     * @throws EventSourcingException
     * @return string
     */
    private function readConfiguration(
        string $setting
    ): string {
        try {
            $config = $this->redis->config('GET', $setting);
        } catch (Throwable $throwable) {
            throw new EventSourcingException(sprintf(
                'Cannot read the Redis setting "%s" to verify it is safe for an event store. Some managed '
                . 'Redis offerings forbid CONFIG GET; if this is one of them, verify the configuration '
                . 'yourself and construct this adapter with $assertDurableConfiguration set to false.',
                $setting
            ), 0, $throwable);
        }

        $value = is_array($config) ? ($config[$setting] ?? null) : null;

        if (!is_string($value)) {
            throw new EventSourcingException(sprintf(
                'Redis did not report a value for the setting "%s", so it cannot be verified as safe for '
                . 'an event store. Verify the configuration yourself and construct this adapter with '
                . '$assertDurableConfiguration set to false.',
                $setting
            ));
        }

        return $value;
    }

    /**
     * Appends the whole call atomically: either every event lands, or none
     * does, across every aggregate the batch touches.
     *
     * One eval rather than one per aggregate group. The grouping
     * survives only to lay out the script's arguments — it no longer decides
     * what a failure rolls back.
     *
     * @param array<DomainEventInterface> $events
     */
    public function storeEvents(
        array $events
    ): void {
        $grouped = $this->groupByAggregate($events);

        if ($grouped === []) {
            return;
        }

        $this->storeBatch($grouped);
    }

    /**
     * @param array<DomainEventInterface> $events
     * @return array<string, array<DomainEventInterface>>
     */
    private function groupByAggregate(
        array $events
    ): array {
        $grouped = [];

        foreach ($events as $event) {
            $grouped[(string) $event->getAggregateId()][] = $event;
        }

        return $grouped;
    }

    /**
     * @param array<string, array<DomainEventInterface>> $grouped
     */
    private function storeBatch(
        array $grouped
    ): void {
        $streamKeys = [];
        $argv = [
            (string) count($grouped),
            $this->outboxEnabled ? '1' : '0',
            RedisOutboxStorage::ENTRY_KEY_PREFIX,
        ];

        foreach ($grouped as $aggregateIdString => $group) {
            $streamKeys[] = $this->streamKey((string) $aggregateIdString);

            $argv[] = (string) $aggregateIdString;
            $argv[] = (string) count($group);

            foreach ($group as $event) {
                $values = $this->entryFactory->createFromDomainEvent($event)->getValues();

                $argv[] = $this->toRedisString($values['version'] ?? 0);
                $argv[] = (string) count($values);

                foreach ($values as $field => $value) {
                    $argv[] = $field;
                    $argv[] = $this->toRedisString($value);
                }
            }
        }

        $result = $this->redis->eval(
            self::STORE_EVENTS_SCRIPT,
            [
                self::GLOBAL_INDEX_KEY,
                self::GLOBAL_SEQ_KEY,
                RedisOutboxStorage::PENDING_KEY,
                RedisOutboxStorage::SEQUENCE_KEY,
                ...$streamKeys,
                ...$argv,
            ],
            4 + count($streamKeys)
        );

        if (is_string($result) && str_starts_with($result, 'CONFLICT:')) {
            [$aggregateIdString, $version] = explode(
                self::POINTER_DELIMITER,
                substr($result, strlen('CONFLICT:')),
                2
            );

            throw new ConcurrencyException(sprintf(
                'Event version %s for aggregate %s already exists.',
                $version,
                $aggregateIdString
            ));
        }

        // Anything other than the script's own success sentinel is a failed
        // write. phpredis reports a Lua error by returning false rather than
        // throwing, so without this a script that could not run — a key of the
        // wrong type where the stream belongs, an out-of-memory refusal — was
        // indistinguishable from a successful append. The events were simply
        // never written and nobody was told.
        if ($result !== 'OK') {
            throw new RuntimeException(sprintf(
                'Storing events for aggregate %s failed: Redis did not complete the append script. %s',
                implode(', ', array_keys($grouped)),
                (string) $this->redis->getLastError()
            ));
        }
    }

    /**
     * @return array<DomainEventInterface>
     */
    public function retrieveEvents(
        EntityIdentifierInterface $aggregateId
    ): array {
        $streamKey = $this->streamKey((string) $aggregateId);

        return $this->hydrateEntries($this->toStreamEntries($this->redis->xRange($streamKey, '-', '+')));
    }

    /**
     * Retrieve an aggregate's events newer than a given version.
     *
     * Server-side, because the version is the entry id: an exclusive XRANGE
     * lower bound is exactly "everything after version N". Reading the whole
     * stream and filtering in PHP would defeat the reason the snapshot load
     * path asks for a tail at all.
     *
     * @param EntityIdentifierInterface $aggregateId
     * @param EventVersion $afterVersion
     * @return array<DomainEventInterface>
     */
    public function retrieveEventsFromVersion(
        EntityIdentifierInterface $aggregateId,
        EventVersion $afterVersion
    ): array {
        return $this->hydrateEntries($this->toStreamEntries($this->redis->xRange(
            $this->streamKey((string) $aggregateId),
            '(' . $this->entryId($afterVersion->toInt()),
            '+'
        )));
    }

    /**
     * Read the global stream from a position.
     *
     * The position is the score in `events:global`, handed out by INCR inside
     * the same Lua script that appends the event, so it only ever moves
     * forward. A reader resuming from its last score therefore cannot see an
     * event twice, and cannot step over one that was appended in between.
     *
     * @param string|null $afterPosition
     * @param int $limit
     * @return GlobalEventPage
     */
    public function retrieveEventsFromPosition(
        ?string $afterPosition,
        int $limit
    ): GlobalEventPage {
        $limit = max(0, $limit);

        if ($limit === 0) {
            return new GlobalEventPage([], $afterPosition);
        }

        /** @var array<string, float|int> $pointers */
        $pointers = $this->redis->zRangeByScore(
            self::GLOBAL_INDEX_KEY,
            $afterPosition === null ? '-inf' : '(' . $afterPosition,
            '+inf',
            ['withscores' => true, 'limit' => [0, $limit]]
        );

        // One pipelined resolution for the whole page. This used to
        // call XRANGE per pointer, so a hundred-event page cost a hundred and
        // one round trips — and CatchUpReader pays that on every cycle, which
        // is precisely what this path is intended to avoid.
        $position = $afterPosition;

        foreach ($pointers as $score) {
            // From the index, not from the records: recordsForPointers()
            // returns records without their scores, and a position derived
            // from an event rather than from the sorted set is a different
            // number that happens to look right on a small store.
            $position = (string) (int) $score;
        }

        $records = $this->recordsForPointers(array_map(strval(...), array_keys($pointers)));

        return new GlobalEventPage($this->hydrateRecords($records), $position);
    }

    /**
     * Resolves a global-index pointer back into its stream entry.
     *
     * One round trip per pointer, which is why only retrieveAllEvents() uses
     * it — see the note there.
     *
     * @param string $pointer
     * @return array<string, array<string, string>>
     */
    private function entriesFor(
        string $pointer
    ): array {
        [$aggregateIdString, $entryId] = explode(self::POINTER_DELIMITER, $pointer, 2);

        return $this->toStreamEntries($this->redis->xRange($this->streamKey($aggregateIdString), $entryId, $entryId));
    }

    /**
     * Ordered by global position, which is the order the events were appended.
     *
     * Not routed through retrievePaginatedEvents(): that one is deprecated for
     * a reason, and a healthy method should not be built on it.
     *
     * @return iterable<DomainEventInterface>
     */
    public function retrieveAllEvents(): iterable
    {
        // The index itself is read in one go — a sorted set of pointers is
        // small next to the events they point at — but the events are resolved
        // and hydrated one at a time, so a full sweep no longer holds
        // the whole store in memory at once.
        //
        // Deliberately *not* pipelined the way retrieveEventsFromPosition() is
        // This is a generator, and resolving the whole store in one
        // pipeline would materialise every event before yielding the first,
        // which is exactly what the generator is designed to prevent. Resolving in
        // pipelined chunks would beat both and is a separate change; leaving
        // this note so the next reader does not "fix" it into an OOM.
        /** @var array<string> $pointers */
        $pointers = $this->redis->zRange(self::GLOBAL_INDEX_KEY, 0, -1);

        foreach ($pointers as $pointer) {
            foreach ($this->entriesFor($pointer) as $fields) {
                yield $this->entryFactory->recordToDomainEvent(EventPersistenceRecord::fromArray($fields));
            }
        }
    }

    /**
     * @param array<string> $pointers
     * @return array<EventPersistenceRecord>
     */
    private function recordsForPointers(
        array $pointers
    ): array {
        if ($pointers === []) {
            return [];
        }

        // One round trip instead of one per pointer. A thousand
        // events used to mean a thousand and one calls, which is the kind of
        // cost that only shows up once a store is big enough for it to hurt.
        $pipeline = $this->redis->multi(Redis::PIPELINE);

        foreach ($pointers as $pointer) {
            [$aggregateIdString, $entryId] = explode(self::POINTER_DELIMITER, $pointer, 2);
            $pipeline->xRange($this->streamKey($aggregateIdString), $entryId, $entryId);
        }

        $replies = $pipeline->exec();
        $records = [];

        foreach (is_array($replies) ? $replies : [] as $entries) {
            foreach (is_array($entries) ? $entries : [] as $fields) {
                $records[] = EventPersistenceRecord::fromArray($this->toFields($fields));
            }
        }

        return $records;
    }

    public function deleteEvents(
        EntityIdentifierInterface $aggregateId
    ): void {
        $aggregateIdString = (string) $aggregateId;

        $this->redis->eval(
            self::DELETE_EVENTS_SCRIPT,
            [
                $this->streamKey($aggregateIdString),
                self::GLOBAL_INDEX_KEY,
                $aggregateIdString,
            ],
            2
        );
    }

    /**
     * @return array<DomainEventInterface>
     */
    public function retrievePaginatedEvents(
        ?int $offset,
        ?int $limit
    ): array {
        $start = $offset ?? 0;

        if ($limit !== null) {
            $stop = $start + $limit - 1;

            if ($stop < $start) {
                return [];
            }
        } else {
            $stop = -1;
        }

        /** @var array<string> $pointers */
        $pointers = $this->redis->zRange(self::GLOBAL_INDEX_KEY, $start, $stop);

        return $this->hydrateRecords($this->recordsForPointers($pointers));
    }

    /**
     * The stream's last entry is the aggregate's current version, so there is
     * nothing to keep in sync alongside it.
     */
    public function getCurrentMaxVersion(
        EntityIdentifierInterface $aggregateId
    ): EventVersion {
        $entries = $this->redis->xRevRange($this->streamKey((string) $aggregateId), '+', '-', 1);

        if (!is_array($entries) || $entries === []) {
            return EventVersion::unassigned();
        }

        $entryId = (string) array_key_first($entries);

        return EventVersion::fromInt($this->versionFromEntryId($entryId));
    }

    /**
     * The stream entry id carrying a version.
     */
    private function entryId(
        int $version
    ): string {
        return '1-' . $version;
    }

    /**
     * The version encoded in a stream entry id.
     *
     * A Redis stream id is always "{ms}-{seq}", so the sequence half is always
     * there; the numeric guard covers the case of an id this adapter did not
     * write, which resolves to the unassigned sentinel rather than a guess.
     */
    private function versionFromEntryId(
        string $entryId
    ): int {
        $parts = explode('-', $entryId);
        $version = end($parts);

        return is_numeric($version) ? (int) $version : 0;
    }

    /**
     * @param array<string, array<string, string>> $entries
     * @return array<DomainEventInterface>
     */
    private function hydrateEntries(
        array $entries
    ): array {
        $records = array_map(
            static fn (array $fields): EventPersistenceRecord => EventPersistenceRecord::fromArray($fields),
            array_values($entries)
        );

        return $this->hydrateRecords($records);
    }

    /**
     * @param array<EventPersistenceRecord> $records
     * @return array<DomainEventInterface>
     */
    private function hydrateRecords(
        array $records
    ): array {
        return array_map(
            fn (EventPersistenceRecord $record): DomainEventInterface => $this->entryFactory->recordToDomainEvent($record),
            $records
        );
    }

    private function streamKey(
        string $aggregateId
    ): string {
        return self::STREAM_KEY_PREFIX . $aggregateId;
    }

    /**
     * Whether a relay, rather than this process, will deliver what is written
     * here.
     *
     * Read from the $outboxEnabled flag the write script reads: it is the
     * configuration in force, not the classes installed. `EventSourcingFacade`
     * needs the answer because the second delivery path — a dispatcher — is
     * given to *it*, and with both in place every event goes out twice with
     * nothing reporting it.
     *
     * @return bool
     */
    public function deliversThroughOutbox(): bool
    {
        return $this->outboxEnabled;
    }
}
