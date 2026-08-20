<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Outbox;

use DomainFlow\EventSourcing\Clock\ClockInterface;
use DomainFlow\EventSourcing\Clock\SystemClock;
use DomainFlow\EventSourcing\Event\DefaultEventEntryFactory;
use DomainFlow\EventSourcing\Event\EventPersistenceRecord;
use DomainFlow\EventSourcing\Interface\DomainEventInterface;
use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcing\Interface\OutboxStorageInterface;
use DomainFlow\EventSourcing\Outbox\OutboxEntry;
use DomainFlow\EventSourcingRedis\Support\ConvertsToRedisString;
use Redis;

/**
 * The outbox, as a claimable set beside one hash per entry.
 *
 * Key scheme:
 *  - outbox:pending          Sorted set, member = entry id, score = the instant
 *                            the entry becomes claimable, as the server's clock
 *                            reads it (0 means "now").
 *  - outbox:dead             Sorted set, member = entry id, score = the instant
 *                            the relay gave up on it.
 *  - outbox:entry:{id}       Hash, the persistence record plus an attempt count.
 *  - outbox:seq              Counter handing out entry ids.
 *
 * A sorted set keyed on "claimable after" rather than a Redis Stream with a
 * consumer group: streams carry leases and delivery counts of their own, but
 * releasing an entry early — which `markFailed()` has to do, so a consumer that
 * was briefly down is retried on the next pass rather than after the lease —
 * has no direct equivalent there. A score of 0 says "free" and a score in the
 * future says "claimed until then", which is the whole lease mechanism in one
 * number.
 *
 * Enqueuing is *not* done here when events are being stored: `RedisEventStorage`
 * writes the entries inside its own Lua script, because that script is the only
 * place in Redis where two writes are one atomic act. This class owns the key
 * scheme and the relay side of it.
 */
final class RedisOutboxStorage implements OutboxStorageInterface
{
    use ConvertsToRedisString;

    public const string PENDING_KEY = 'outbox:pending';
    public const string DEAD_KEY = 'outbox:dead';
    public const string ENTRY_KEY_PREFIX = 'outbox:entry:';
    public const string SEQUENCE_KEY = 'outbox:seq';

    /**
     * Moves an entry from the pending set to the dead-letter set, keeping its
     * hash so an operator can still read what could not be delivered.
     *
     * One script rather than ZREM plus ZADD, so an entry can never be in both
     * sets or in neither. The ZREM's own return value is the guard: it is 1
     * only if this call is the one that took the entry out of pending, which
     * makes a repeated call harmless and — the case that matters — stops an
     * already-delivered entry, whose hash is gone, from being resurrected as
     * a dead letter with nothing in it.
     */
    private const string ABANDON_SCRIPT = <<<'LUA'
        local pendingKey = KEYS[1]
        local deadKey = KEYS[2]

        local id = ARGV[1]
        local at = tonumber(ARGV[2])

        if redis.call('ZREM', pendingKey, id) == 0 then
            return 0
        end

        redis.call('ZADD', deadKey, at, id)

        return 1
        LUA;

    /**
     * Claims the entries that are free and pushes their next claimable instant
     * out by the lease, returning the ids it took.
     *
     * One script, so two relays cannot claim the same entry: the second sees
     * the scores the first already moved. Only the ids come back — the entries
     * themselves are read in one pipelined round trip afterwards, which keeps
     * the script short and the return shape trivial to validate.
     *
     * `TIME` rather than an instant passed in, so both halves of the lease —
     * the score that says "claimed until" and the threshold that decides what
     * is free — are read from the server's clock. A relay fleet is the
     * normal deployment and the lease is the one number all of it has to agree
     * on; with the instant supplied by the caller, two hosts a minute apart
     * disagreed about when a claim had lapsed, and the fast one took entries
     * the slow one was still delivering.
     *
     * Redis marks a script that calls `TIME` non-deterministic, which matters
     * only for replicating the *script*: since Redis 5 the effects are
     * replicated instead, so a replica sees the scores this run wrote rather
     * than re-running the clock. Fractional seconds because `TIME` has
     * microsecond resolution and a sorted-set score is a double — rounding to
     * whole seconds would make every claim inside one second indistinguishable.
     */
    private const string RESERVE_SCRIPT = <<<'LUA'
        local pendingKey = KEYS[1]

        local limit = tonumber(ARGV[1])
        local lease = tonumber(ARGV[2])

        local time = redis.call('TIME')
        local now = tonumber(time[1]) + (tonumber(time[2]) / 1000000)

        local ids = redis.call('ZRANGEBYSCORE', pendingKey, '-inf', now, 'LIMIT', 0, limit)

        for _, id in ipairs(ids) do
            redis.call('ZADD', pendingKey, now + lease, id)
        end

        return ids
        LUA;

    private readonly EventEntryFactoryInterface $entryFactory;

    /**
     * Two clocks are in play, and each has a distinct role.
     *
     * **The lease is the server's**: both the claim's score and the threshold
     * it is compared against come from `TIME` inside the reserve script, so
     * every relay is measured against one clock however many hosts they run on.
     *
     * **`$clock` is this relay's own**, and it decides nothing: it stamps the
     * dead-letter score, which records when *this* relay gave up and is never
     * compared against anything. Injectable so a test can pin that instant.
     *
     * @param int $leaseSeconds How long a claim holds before another relay may
     *        take the entry. Without it, a relay dying between claiming and
     *        marking strands its entries and the queue stops draining with
     *        nothing reported anywhere. Measured by the server, not the caller.
     * @param ClockInterface $clock The relay's clock. Stamps the dead-letter
     *        score only — see above.
     */
    public function __construct(
        private readonly Redis $redis,
        ?EventEntryFactoryInterface $entryFactory = null,
        private readonly int $leaseSeconds = 300,
        private readonly ClockInterface $clock = new SystemClock()
    ) {
        $this->entryFactory = $entryFactory ?? new DefaultEventEntryFactory();
    }

    /**
     * Records deliveries outside any event write.
     *
     * The atomic path is RedisEventStorage's Lua script; this exists because
     * the interface promises it, and for a consumer replaying events into the
     * outbox by hand.
     *
     * @param array<DomainEventInterface> $events
     * @return void
     */
    public function enqueue(
        array $events
    ): void {
        foreach ($events as $event) {
            $id = (string) $this->redis->incr(self::SEQUENCE_KEY);
            $fields = $this->entryFactory->createFromDomainEvent($event)->getValues();

            $flat = ['attempts' => '0'];
            foreach ($fields as $name => $value) {
                $flat[$name] = $this->toRedisString($value);
            }

            $this->redis->hMSet(self::ENTRY_KEY_PREFIX . $id, $flat);
            $this->redis->zAdd(self::PENDING_KEY, 0, $id);
        }
    }

    /**
     * @param int $limit
     * @return list<OutboxEntry>
     */
    public function reserve(
        int $limit
    ): array {
        if ($limit <= 0) {
            return [];
        }

        // No instant is passed in: the script reads the server's clock for
        // both sides of the lease.
        /** @var mixed $claimed */
        $claimed = $this->redis->eval(
            self::RESERVE_SCRIPT,
            [
                self::PENDING_KEY,
                (string) $limit,
                (string) max(0, $this->leaseSeconds),
            ],
            1
        );

        // One round trip for every claimed entry rather than one per entry:
        // a relay claiming a batch of a hundred should not cost a hundred
        // round trips to read them.
        return $this->entriesFor(
            array_values(array_filter(is_array($claimed) ? $claimed : [], 'is_string'))
        );
    }

    public function markDelivered(
        OutboxEntry $entry
    ): void {
        $this->redis->zRem(self::PENDING_KEY, $entry->getId());
        $this->redis->del(self::ENTRY_KEY_PREFIX . $entry->getId());
    }

    public function markFailed(
        OutboxEntry $entry
    ): void {
        if ($this->redis->zScore(self::PENDING_KEY, $entry->getId()) === false) {
            // Already delivered. A relay that died between delivering and
            // marking retries the whole step, so this has to be harmless.
            return;
        }

        $this->redis->hIncrBy(self::ENTRY_KEY_PREFIX . $entry->getId(), 'attempts', 1);
        $this->redis->zAdd(self::PENDING_KEY, 0, $entry->getId());
    }

    /**
     * The dead-letter score is the relay's own clock, not the server's, and
     * that is the one place the distinction does not matter: it records
     * when this relay gave up and nothing is ever compared against it. The
     * lease is the opposite case, which is why it moved into the script.
     *
     * @param OutboxEntry $entry
     * @return void
     */
    public function markAbandoned(
        OutboxEntry $entry
    ): void {
        $this->redis->eval(
            self::ABANDON_SCRIPT,
            [
                self::PENDING_KEY,
                self::DEAD_KEY,
                $entry->getId(),
                (string) $this->clock->now()->getTimestamp(),
            ],
            2
        );
    }

    /**
     * @param int $limit
     * @return list<OutboxEntry>
     */
    public function retrieveAbandoned(
        int $limit
    ): array {
        if ($limit <= 0) {
            return [];
        }

        // Ranked, not scored: the score is the moment the relay gave up, and
        // two entries abandoned in the same second share it. Rank keeps the
        // set's own order, which is that score plus the id as a tie-break.
        $ids = $this->redis->zRange(self::DEAD_KEY, 0, $limit - 1);

        return $this->entriesFor(array_values(array_filter(is_array($ids) ? $ids : [], 'is_string')));
    }

    public function countPending(): int
    {
        $count = $this->redis->zCard(self::PENDING_KEY);

        return is_int($count) ? $count : 0;
    }

    public function countAbandoned(): int
    {
        $count = $this->redis->zCard(self::DEAD_KEY);

        return is_int($count) ? $count : 0;
    }

    /**
     * Reads a list of entry hashes in one pipelined round trip.
     *
     * @param list<string> $ids
     * @return list<OutboxEntry>
     */
    private function entriesFor(
        array $ids
    ): array {
        if ($ids === []) {
            return [];
        }

        $pipeline = $this->redis->multi(Redis::PIPELINE);

        foreach ($ids as $id) {
            $pipeline->hGetAll(self::ENTRY_KEY_PREFIX . $id);
        }

        $hashes = $pipeline->exec();
        $hashes = is_array($hashes) ? array_values($hashes) : [];

        $entries = [];

        foreach ($ids as $index => $id) {
            $fields = $hashes[$index] ?? [];
            $entries[] = $this->toEntry($id, $this->stringMap(is_array($fields) ? $fields : []));
        }

        return $entries;
    }

    /**
     * Keeps only the string-to-string pairs, which is all a persistence record
     * ever consists of.
     *
     * @param array<mixed> $fields
     * @return array<string, string>
     */
    private function stringMap(
        array $fields
    ): array {
        $map = [];

        foreach ($fields as $name => $value) {
            if (is_string($name) && is_string($value)) {
                $map[$name] = $value;
            }
        }

        return $map;
    }

    /**
     * @param string $id
     * @param array<string, string> $fields
     * @return OutboxEntry
     */
    private function toEntry(
        string $id,
        array $fields
    ): OutboxEntry {
        $attempts = $fields['attempts'] ?? '0';
        unset($fields['attempts']);

        return new OutboxEntry(
            $id,
            $this->entryFactory->recordToDomainEvent(EventPersistenceRecord::fromArray($fields)),
            (int) $attempts
        );
    }
}
