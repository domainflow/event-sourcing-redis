<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\ProcessManager;

use DateTimeImmutable;
use DateTimeZone;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Exception\ProcessManagerConcurrencyException;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\ProcessManagerStorageInterface;
use DomainFlow\EventSourcing\ProcessManager\ProcessManagerState;
use DomainFlow\EventSourcing\ProcessManager\ProcessManagerStateEnum;
use JsonException;
use Redis;
use RuntimeException;

/**
 * Key scheme: `process_manager:{processId}` — a Hash holding the current state's fields
 * (process_id, status, data, timeout, version). `timeout` is only present as a field when set,
 * so a previously-set timeout doesn't linger after a store() without one.
 *
 * Alongside it, `process_manager_timeouts` — a Sorted Set scored by the timeout, holding only
 * processes that are still running and have one. It is written by the same script that writes the
 * state, so the two cannot disagree: there is no window in which a process is overdue according to
 * one and not the other, and no pass in which a cleared timeout is still findable.
 *
 * store() is one Lua script. It used to be three round trips — hDel, hMSet, hSet — so a
 * crash between them left a state that never existed: new status, old data, or a timeout belonging
 * to neither. The script also makes the version check and the write one act, which is what stops
 * two workers handling events for the same saga from overwriting each other.
 */
final readonly class RedisProcessManagerStorage implements ProcessManagerStorageInterface
{
    private const string KEY_PREFIX = 'process_manager:';

    private const string TIMEOUT_INDEX_KEY = 'process_manager_timeouts';

    /**
     * ARGV: expected version, next version, process id, status, data, timeout
     * ('' for none).
     *
     * Rewrites the hash rather than merging into it, so a field that is no
     * longer part of the state — a timeout that was cleared — cannot survive
     * the write.
     */
    private const string STORE_SCRIPT = <<<'LUA'
        local key = KEYS[1]
        local indexKey = KEYS[2]

        local expected = tonumber(ARGV[1])
        local nextVersion = ARGV[2]
        local processId = ARGV[3]
        local status = ARGV[4]
        local data = ARGV[5]
        local timeout = ARGV[6]
        local timeoutScore = ARGV[7]
        local stillRunning = ARGV[8]

        local stored = tonumber(redis.call('HGET', key, 'version') or '0')

        if stored ~= expected then
            return 'CONFLICT:' .. stored
        end

        redis.call('DEL', key)
        redis.call('HSET', key,
            'process_id', processId,
            'status', status,
            'data', data,
            'version', nextVersion
        )

        if timeout ~= '' then
            redis.call('HSET', key, 'timeout', timeout)
        end

        -- Same act as the write above. A cleared timeout, or one belonging to a
        -- process that has finished, leaves the index here rather than being
        -- filtered out on every later read.
        if timeout ~= '' and stillRunning == '1' then
            redis.call('ZADD', indexKey, timeoutScore, processId)
        else
            redis.call('ZREM', indexKey, processId)
        end

        return 'OK'
        LUA;

    /**
     * ARGV: cutoff score, page size, key prefix.
     *
     * One round trip for the page rather than one per process: the
     * ids come out of the index in score order and the states are resolved
     * inside the same script.
     */
    private const string FIND_TIMED_OUT_SCRIPT = <<<'LUA'
        local indexKey = KEYS[1]

        local cutoff = ARGV[1]
        local pageSize = tonumber(ARGV[2])
        local prefix = ARGV[3]

        local ids = redis.call('ZRANGEBYSCORE', indexKey, '-inf', cutoff, 'LIMIT', 0, pageSize)
        local states = {}

        for i = 1, #ids do
            states[i] = redis.call('HGETALL', prefix .. ids[i])
        end

        return states
        LUA;

    /**
     * Removing the state without removing its index entry would leave a process
     * that is overdue forever and has nothing behind it.
     */
    private const string DELETE_SCRIPT = <<<'LUA'
        redis.call('DEL', KEYS[1])
        redis.call('ZREM', KEYS[2], ARGV[1])

        return 'OK'
        LUA;

    public function __construct(
        private Redis $redis
    ) {
    }

    /**
     * Compares the stored version and writes the whole state in one script, so
     * there is no window in which a crash leaves a half-updated state and no
     * window in which two workers both pass the check.
     *
     * @param ProcessManagerState $state
     * @throws JsonException|ProcessManagerConcurrencyException
     * @return void
     */
    public function store(
        ProcessManagerState $state
    ): void {
        $expected = $state->getVersion();
        $next = $expected + 1;
        $timeout = $state->getTimeout();

        $status = $state->getStatus();

        $result = $this->redis->eval(
            self::STORE_SCRIPT,
            [
                $this->key((string) $state->getProcessId()),
                self::TIMEOUT_INDEX_KEY,
                (string) $expected,
                (string) $next,
                (string) $state->getProcessId(),
                $status->value,
                json_encode($state->getData(), JSON_THROW_ON_ERROR),
                $timeout?->format('Y-m-d H:i:s.u') ?? '',
                $timeout === null ? '0' : $this->score($timeout),
                $status->isCompleted() || $status->isFailed() ? '' : '1',
            ],
            2
        );

        if (is_string($result) && str_starts_with($result, 'CONFLICT:')) {
            throw ProcessManagerConcurrencyException::versionMoved(
                $state->getProcessId(),
                $expected,
                (int) substr($result, strlen('CONFLICT:'))
            );
        }

        $state->markPersisted($next);
    }

    public function retrieve(
        EntityIdentifierInterface $processId
    ): ?ProcessManagerState {
        /** @var array<string, string> $row */
        $row = $this->redis->hGetAll($this->key((string) $processId));

        if ($row === []) {
            return null;
        }

        return $this->hydrate($row, (string) $processId);
    }

    /**
     * @param array<string, string> $row
     * @param string|null $requestedProcessId The id this state was looked up by,
     *        where there was one. A find by timeout has none.
     * @throws RuntimeException
     * @return ProcessManagerState
     */
    private function hydrate(
        array $row,
        ?string $requestedProcessId = null
    ): ProcessManagerState {
        $processId = $row['process_id'] ?? $requestedProcessId ?? '';

        try {
            /** @var array<string, mixed> $data */
            $data = json_decode($row['data'] ?? '{}', true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new RuntimeException(
                sprintf('Failed to decode process manager data for process "%s": %s', $processId, $e->getMessage()),
                0,
                $e
            );
        }

        $state = new ProcessManagerState(
            EntityIdentifier::fromString($processId),
            ProcessManagerStateEnum::from($row['status'] ?? ProcessManagerStateEnum::WAITING->value),
            is_numeric($row['version'] ?? null) ? (int) $row['version'] : 0
        );
        $state->setData($data);

        if (isset($row['timeout'])) {
            // Stated, not inferred. The stored string carries no offset, and
            // everything written here is UTC, so a runtime in another zone
            // would otherwise read it as local time and move it.
            $state->setTimeout(new DateTimeImmutable($row['timeout'], new DateTimeZone('UTC')));
        }

        return $state;
    }

    public function delete(
        EntityIdentifierInterface $processId
    ): void {
        $this->redis->eval(
            self::DELETE_SCRIPT,
            [
                $this->key((string) $processId),
                self::TIMEOUT_INDEX_KEY,
                (string) $processId,
            ],
            2
        );
    }

    /**
     * Overdue processes, oldest first, still running.
     *
     * Nothing is filtered on the way out: the index holds exactly the processes
     * that qualify, because the script that writes a state is the one that
     * decides whether it belongs there. A read-side filter would be a second
     * opinion about the same question, and the two would eventually disagree.
     *
     * @param DateTimeImmutable $asOf
     * @param int $limit
     * @return list<ProcessManagerState>
     */
    public function findTimedOut(
        DateTimeImmutable $asOf,
        int $limit
    ): array {
        /** @var list<array<int, string>> $rows */
        $rows = $this->redis->eval(
            self::FIND_TIMED_OUT_SCRIPT,
            [
                self::TIMEOUT_INDEX_KEY,
                $this->score($asOf),
                (string) max(0, $limit),
                self::KEY_PREFIX,
            ],
            1
        );

        $states = [];

        foreach ($rows as $fields) {
            $states[] = $this->hydrate($this->toRow($fields));
        }

        return $states;
    }

    /**
     * A timeout as a sorted-set score: whole microseconds since the epoch.
     *
     * An integer rather than seconds-with-a-fraction, because a Redis score is
     * a double and the fractional form spends its precision on the decimal
     * expansion. Microseconds since the epoch are around 1.8e15 and land well
     * inside what a double represents exactly, so two timeouts a microsecond
     * apart stay a microsecond apart — including the one that falls exactly on
     * a cutoff.
     *
     * Computed in native integers, which are 64-bit on every platform this
     * package supports. Reaching for bcmath here would add an extension this
     * adapter does not require and consumers would not know to install.
     *
     * @param DateTimeImmutable $moment
     * @return string
     */
    private function score(
        DateTimeImmutable $moment
    ): string {
        $utc = $moment->setTimezone(new DateTimeZone('UTC'));

        return (string) ((int) $utc->format('U') * 1_000_000 + (int) $utc->format('u'));
    }

    /**
     * HGETALL comes back from Lua as a flat field/value list.
     *
     * @param array<int, string> $fields
     * @return array<string, string>
     */
    private function toRow(
        array $fields
    ): array {
        $row = [];

        for ($i = 0; $i + 1 < count($fields); $i += 2) {
            $row[$fields[$i]] = $fields[$i + 1];
        }

        return $row;
    }

    private function key(
        string $processId
    ): string {
        return self::KEY_PREFIX . $processId;
    }
}
