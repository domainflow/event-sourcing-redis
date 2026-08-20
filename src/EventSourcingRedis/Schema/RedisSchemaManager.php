<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Schema;

use DomainFlow\EventSourcing\Interface\SchemaManagerInterface;
use DomainFlow\EventSourcingRedis\Storage\RedisEventStorage;
use Redis;

/**
 * Redis has no schema, and that is worth a call rather than a silence.
 *
 * Nothing here creates a structure: a stream, a sorted set and a hash all come
 * into being when something is written to them. The other two adapters needed
 * setup and this one did not, and "nothing to do" is the one answer a consumer
 * cannot distinguish from "nobody documented it".
 *
 * What `ensureSchema()` does do is check that this Redis is fit to be an event
 * store at all. That check already existed, in `RedisEventStorage`'s
 * constructor, where it stays — it protects the consumer who never ran setup.
 * Having it here as well puts it where it belongs: "is this deployment
 * suitable" is a setup question, and finding the answer at deploy time beats
 * finding it when the first aggregate is saved.
 */
final readonly class RedisSchemaManager implements SchemaManagerInterface
{
    /**
     * Every key namespace this adapter owns.
     *
     * Listed rather than derived from a single prefix, because the keys were
     * not designed under one — and a `dropSchema()` that guessed the pattern
     * would leave whatever it did not think of behind.
     *
     * @var list<string>
     */
    private const array KEY_PATTERNS = [
        'events:aggregate:*',
        'events:global',
        'events:global:seq',
        'snapshots:*',
        'snapshot_history:*',
        'process_manager:*',
        'process_manager_timeouts',
        'outbox:pending',
        'outbox:dead',
        'outbox:entry:*',
        'outbox:seq',
    ];

    public function __construct(
        private Redis $redis
    ) {
    }

    /**
     * Verifies the deployment; creates nothing, because there is nothing to
     * create.
     *
     * Idempotent for the same reason it is empty.
     *
     * @return void
     */
    public function ensureSchema(): void
    {
        // Constructed only to run the check the constructor runs — the one
        // place that knows what "fit for an event store" means, rather than a
        // second copy of the same two settings drifting beside it.
        new RedisEventStorage($this->redis);
    }

    /**
     * Deletes everything this adapter owns.
     *
     * `SCAN` rather than `KEYS`: this runs against a live server, and `KEYS`
     * blocks it for the length of the keyspace.
     *
     * @return void
     */
    public function dropSchema(): void
    {
        foreach (self::KEY_PATTERNS as $pattern) {
            $iterator = null;

            do {
                $keys = $this->redis->scan($iterator, $pattern, 1000);

                if (is_array($keys) && $keys !== []) {
                    $this->redis->del($keys);
                }
            } while ($iterator > 0);
        }
    }

    /**
     * @return list<string>
     */
    public function describeSchema(): array
    {
        return array_merge(
            [
                'REQUIRE maxmemory-policy noeviction (an event store is a system of record; anything else lets Redis discard streams silently)',
                'REQUIRE appendonly yes (RDB snapshotting alone loses every event written since the last snapshot)',
            ],
            array_map(
                static fn (string $pattern): string => sprintf('USES KEYS %s', $pattern),
                self::KEY_PATTERNS
            )
        );
    }
}
