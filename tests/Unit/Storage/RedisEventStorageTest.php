<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Unit\Storage;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Exception\ConcurrencyException;
use DomainFlow\EventSourcing\Exception\EventSourcingException;
use DomainFlow\EventSourcing\Interface\DomainEventInterface;
use DomainFlow\EventSourcing\Interface\EventStorageInterface;
use DomainFlow\EventSourcing\Upcaster\ReflectionEventFactory;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractEventStorageTestCase;
use DomainFlow\EventSourcingCore\Provider\Unit\AnotherDummyEvent;
use DomainFlow\EventSourcingRedis\Outbox\RedisOutboxStorage;
use DomainFlow\EventSourcingRedis\Storage\RedisEventStorage;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisHelper;
use DomainFlow\Uuid\UuidV6;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use RuntimeException;

#[CoversClass(RedisEventStorage::class)]
#[UsesClass(RedisOutboxStorage::class)]
class RedisEventStorageTest extends AbstractEventStorageTestCase
{
    use RedisHelper;

    protected function setUp(): void
    {
        parent::setUp();
        $this->flushRedis();
    }

    protected function tearDown(): void
    {
        $this->flushRedis();
    }

    protected function getStorage(): EventStorageInterface
    {
        return new RedisEventStorage($this->getRedis());
    }

    protected function getStorageWhoseWritesFailWithoutConflict(): EventStorageInterface
    {
        // A plain string where the aggregate's stream belongs, so XADD fails
        // with WRONGTYPE. Redis rarely refuses a write at all — this is its
        // realistic "the store said no, and not about versions" case.
        $this->getRedis()->set(
            'events:aggregate:NonConflictingFailureAggregate',
            'this is not a stream'
        );

        return $this->getStorage();
    }

    protected function getStorageWithFactory(): EventStorageInterface
    {
        return new RedisEventStorage(
            $this->getRedis(),
            null,
            new ReflectionEventFactory(),
        );
    }

    public function test_storeEventsThrowsConcurrencyExceptionOnDuplicateVersion(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('ConcurrentAggregate');

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);

        $this->expectException(ConcurrencyException::class);

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
    }

    public function test_storeEventsDoesNotAdvanceMaxVersionOrGlobalIndexOnConflict(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('ConcurrentAggregate2');

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);

        try {
            $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
            $this->fail('Expected ConcurrencyException was not thrown.');
        } catch (ConcurrencyException) {
            // expected
        }

        $this->assertEquals(1, $storage->getCurrentMaxVersion($aggregateId)->toInt());
        $this->assertCount(1, iterator_to_array($storage->retrieveAllEvents(), false));
    }

    public function test_retrieveEventsThrowsOnMalformedPayload(): void
    {
        $redis = $this->getRedis();
        $aggregateId = EntityIdentifier::fromString('CorruptPayloadAggregate');

        $redis->xAdd('events:aggregate:CorruptPayloadAggregate', '*', [
            'event_id' => (string) UuidV6::generate(),
            'aggregate_id' => 'CorruptPayloadAggregate',
            'event_class' => AnotherDummyEvent::class,
            'version' => '1',
            'occurred_on' => '2026-01-01 00:00:00.000000',
            'payload' => '{not valid json',
        ]);

        $storage = $this->getStorage();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Missing required payload field');

        $storage->retrieveEvents($aggregateId);
    }

    public function test_retrievePaginatedEventsReturnsEmptyArrayForZeroLimit(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('ZeroLimitAggregate');

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);

        $this->assertSame([], $storage->retrievePaginatedEvents(0, 0));
    }

    public function test_deleteEventsRemovesAggregateFromGlobalIndex(): void
    {
        $storage = $this->getStorage();
        $keepId = EntityIdentifier::fromString('KeepAggregate');
        $deleteId = EntityIdentifier::fromString('DeleteAggregate');

        $storage->storeEvents([new AnotherDummyEvent($keepId, 1)]);
        $storage->storeEvents([new AnotherDummyEvent($deleteId, 1)]);

        $storage->deleteEvents($deleteId);

        $allEvents = iterator_to_array($storage->retrieveAllEvents(), false);
        $this->assertCount(1, $allEvents);
        $this->assertEquals((string) $keepId, (string) $allEvents[0]->getAggregateId());
    }
    /**
     * retrieveAllEvents() used to route through this, which covered the
     * "null means no limit" branch by accident. It no longer does — a healthy
     * method should not be built on a deprecated one — so the branch needs a
     * test of its own for as long as the method is still shipped.
     */
    public function test_retrievePaginatedEventsWithoutALimitReturnsTheWholeStream(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('UnboundedPageAggregate');

        $storage->storeEvents([
            new AnotherDummyEvent($aggregateId, 1),
            new AnotherDummyEvent($aggregateId, 2),
            new AnotherDummyEvent($aggregateId, 3),
        ]);

        $this->assertCount(3, $storage->retrievePaginatedEvents(null, null));
    }

    /**
     * The failure this guard exists for is invisible: Redis evicts an event
     * stream to reclaim memory, reports nothing, and the aggregate simply
     * replays to a state that never existed. Refusing to start is the only
     * moment at which anyone can still notice.
     */
    public function test_anEvictingRedisIsRefused(): void
    {
        $redis = $this->getRedis();
        $original = $this->configuredValue('maxmemory-policy');

        $redis->config('SET', 'maxmemory-policy', 'allkeys-lru');

        try {
            new RedisEventStorage($redis);
            $this->fail('An event store must not run against a Redis that evicts its data.');
        } catch (EventSourcingException $exception) {
            $this->assertStringContainsString('noeviction', $exception->getMessage(), 'The message has to say what to set.');
        } finally {
            $redis->config('SET', 'maxmemory-policy', $original);
        }
    }

    public function test_aRedisWithoutAppendOnlyPersistenceIsRefused(): void
    {
        $redis = $this->getRedis();
        $original = $this->configuredValue('appendonly');

        $redis->config('SET', 'appendonly', 'no');

        try {
            new RedisEventStorage($redis);
            $this->fail('RDB snapshotting alone loses every event since the last snapshot.');
        } catch (EventSourcingException $exception) {
            $this->assertStringContainsString('appendonly', $exception->getMessage());
        } finally {
            $redis->config('SET', 'appendonly', $original);
        }
    }

    public function test_theDurabilityCheckCanBeWaived(): void
    {
        $redis = $this->getRedis();
        $original = $this->configuredValue('maxmemory-policy');

        $redis->config('SET', 'maxmemory-policy', 'allkeys-lru');

        try {
            $storage = new RedisEventStorage($redis, null, null, false);
            $this->assertInstanceOf(RedisEventStorage::class, $storage, 'An operator who has verified the configuration another way must be able to proceed.');
        } finally {
            $redis->config('SET', 'maxmemory-policy', $original);
        }
    }

    public function test_aRedisThatForbidsConfigGetIsRefusedRatherThanAssumedSafe(): void
    {
        $this->expectException(EventSourcingException::class);
        $this->expectExceptionMessage('CONFIG GET');

        new RedisEventStorage(new UnreadableConfigRedis(throwOnConfig: true));
    }

    public function test_aRedisThatReportsNoValueIsRefusedRatherThanAssumedSafe(): void
    {
        $this->expectException(EventSourcingException::class);
        $this->expectExceptionMessage('did not report a value');

        new RedisEventStorage(new UnreadableConfigRedis(throwOnConfig: false, configResult: []));
    }

    private function configuredValue(
        string $setting
    ): string {
        /** @var array<string, string> $config */
        $config = $this->getRedis()->config('GET', $setting);

        return $config[$setting];
    }

    public function test_aStoredBatchQueuesOneDeliveryPerEvent(): void
    {
        $storage = new RedisEventStorage($this->getRedis(), null, null, true, true);
        $outbox = new RedisOutboxStorage($this->getRedis());
        $aggregateId = EntityIdentifier::fromString('outbox-happy-path');

        $storage->storeEvents([
            new AnotherDummyEvent($aggregateId, 1),
            new AnotherDummyEvent($aggregateId, 2),
        ]);

        $this->assertSame(2, $outbox->countPending());

        $reserved = $outbox->reserve(10);
        $this->assertCount(2, $reserved);
        $this->assertSame('outbox-happy-path', (string) $reserved[0]->getEvent()->getAggregateId());
    }

    /**
     * The reason the entries are written by the Lua script rather than by a
     * second call: a script is the only place in Redis where two writes are one
     * atomic act. A batch rejected by the version guard must leave no pending
     * delivery, or the relay ships an event the store never kept.
     */
    public function test_aRejectedBatchLeavesNoPendingDeliveryBehind(): void
    {
        $storage = new RedisEventStorage($this->getRedis(), null, null, true, true);
        $outbox = new RedisOutboxStorage($this->getRedis());
        $aggregateId = EntityIdentifier::fromString('outbox-atomicity');

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
        $this->assertSame(1, $outbox->countPending(), 'Precondition: one event stored, one delivery queued.');

        try {
            $storage->storeEvents([
                new AnotherDummyEvent($aggregateId, 2),
                new AnotherDummyEvent($aggregateId, 1),
            ]);
            $this->fail('A batch reusing version 1 must be rejected.');
        } catch (ConcurrencyException) {
            // expected
        }

        $this->assertSame(1, $outbox->countPending(), 'The rejected batch must not have queued anything.');
        $this->assertCount(1, $storage->retrieveEvents($aggregateId), 'And must not have stored anything either.');
    }

    public function test_storageWithoutTheOutboxEnabledQueuesNothing(): void
    {
        $storage = new RedisEventStorage($this->getRedis());
        $outbox = new RedisOutboxStorage($this->getRedis());

        $storage->storeEvents([new AnotherDummyEvent(EntityIdentifier::fromString('outbox-absent'), 1)]);

        $this->assertSame(0, $outbox->countPending(), 'The outbox is opt-in.');
    }

    public function test_retrievePaginatedEventsOnAnEmptyStoreAsksRedisNothing(): void
    {
        $this->assertSame(
            [],
            $this->getStorage()->retrievePaginatedEvents(0, 10),
            'With no pointers to resolve there is nothing to pipeline.'
        );
    }

    /**
     * The pointer resolution is pipelined, and applied to
     * `retrievePaginatedEvents()` — the method the same release deprecated.
     * `retrieveEventsFromPosition()`, the one that replaced it and the one
     * `CatchUpReader` runs in a loop, still resolved one pointer per round
     * trip: a page of a hundred events cost a hundred and one calls.
     *
     * Asserted as a count rather than a duration, because the property is the
     * number of round trips and a stopwatch in a test measures the machine.
     */
    public function test_a_position_read_resolves_its_whole_page_in_one_round_trip(): void
    {
        $redis = new CountingRedis();
        $redis->connect(getenv('REDIS_HOST') ?: '127.0.0.1', (int) (getenv('REDIS_PORT') ?: 6379));

        $storage = new RedisEventStorage($redis);

        // Two aggregates, so the page spans more than one stream and the
        // pipeline has to resolve pointers that do not share a key.
        $first = EntityIdentifier::fromString('PipelinedPositionA');
        $second = EntityIdentifier::fromString('PipelinedPositionB');

        $storage->storeEvents([
            new AnotherDummyEvent($first, 1),
            new AnotherDummyEvent($first, 2),
            new AnotherDummyEvent($first, 3),
            new AnotherDummyEvent($second, 1),
            new AnotherDummyEvent($second, 2),
        ]);

        $redis->directXRangeCalls = 0;
        $redis->pipelinesOpened = 0;

        $page = $storage->retrieveEventsFromPosition(null, 5);

        $this->assertCount(5, $page->getEvents(), 'Precondition: the page really did carry five events.');
        $this->assertSame(0, $redis->directXRangeCalls, 'Every pointer must be resolved inside the pipeline.');
        $this->assertSame(1, $redis->pipelinesOpened, 'And one pipeline, not one per pointer.');
    }

    /**
     * The other half of the same property: the page and the position it hands
     * back must be exactly what they were. Pipelining returns records without
     * their scores, and taking the next position from the records rather than
     * from the index is the way to get this wrong.
     */
    public function test_a_pipelined_position_read_returns_the_same_page_and_position(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('PipelinedPositionOrder');

        $storage->storeEvents([
            new AnotherDummyEvent($aggregateId, 1),
            new AnotherDummyEvent($aggregateId, 2),
            new AnotherDummyEvent($aggregateId, 3),
        ]);

        $firstPage = $storage->retrieveEventsFromPosition(null, 2);

        $this->assertSame(
            [1, 2],
            array_map(
                static fn (DomainEventInterface $event): int => $event->getVersion()->toInt(),
                $firstPage->getEvents()
            )
        );

        $secondPage = $storage->retrieveEventsFromPosition($firstPage->getNextPosition(), 2);

        $this->assertSame(
            [3],
            array_map(
                static fn (DomainEventInterface $event): int => $event->getVersion()->toInt(),
                $secondPage->getEvents()
            ),
            'The next position must resume after the page, not inside it.'
        );
    }

    /**
     * The same storage, built so its writes are enqueued for a relay.
     *
     * @return EventStorageInterface|null
     */
    protected function getStorageDeliveringThroughOutbox(): ?EventStorageInterface
    {
        return new RedisEventStorage($this->getRedis(), outboxEnabled: true);
    }
}
