<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Unit\ProcessManager;

use DateTimeImmutable;
use DateTimeZone;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Interface\ProcessManagerStorageInterface;
use DomainFlow\EventSourcing\ProcessManager\ProcessManagerState;
use DomainFlow\EventSourcing\ProcessManager\ProcessManagerStateEnum;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractProcessManagerStorageTestCase;
use DomainFlow\EventSourcingRedis\ProcessManager\RedisProcessManagerStorage;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisHelper;
use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;

#[CoversClass(RedisProcessManagerStorage::class)]
final class RedisProcessManagerStorageTest extends AbstractProcessManagerStorageTestCase
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

    protected function getProcessManagerStorage(): ProcessManagerStorageInterface
    {
        return new RedisProcessManagerStorage($this->getRedis());
    }

    public function test_storeRoundtripsTimeout(): void
    {
        $storage = $this->getProcessManagerStorage();
        $processId = EntityIdentifier::fromString('process-with-timeout');
        $timeout = new DateTimeImmutable('2026-01-01 12:00:00.000000');

        $state = new ProcessManagerState($processId, ProcessManagerStateEnum::WAITING);
        $state->setTimeout($timeout);
        $storage->store($state);

        $retrieved = $storage->retrieve($processId);

        $this->assertNotNull($retrieved);
        $this->assertSame($timeout->format('Y-m-d H:i:s'), $retrieved->getTimeout()?->format('Y-m-d H:i:s'));
    }

    /**
     * The index is a second key holding a claim about the first one, and the
     * contract cannot see it: a stale entry would point at a hash that no
     * longer exists, and the worker would be handed a state with nothing behind
     * it. Deleting a process therefore has to leave the index, in the same act
     * — which is why delete() is a script rather than a DEL.
     */
    public function test_deleting_a_process_takes_its_timeout_out_of_the_index(): void
    {
        $storage = $this->getProcessManagerStorage();
        $processId = EntityIdentifier::fromString('process-deleted-with-timeout');

        $state = new ProcessManagerState($processId, ProcessManagerStateEnum::WAITING);
        $state->setTimeout(new DateTimeImmutable('2026-01-01 12:00:00.000000', new DateTimeZone('UTC')));
        $storage->store($state);

        $this->assertSame(1, $this->getRedis()->zCard('process_manager_timeouts'), 'Precondition: the timeout is indexed.');

        $storage->delete($processId);

        $this->assertSame(0, $this->getRedis()->zCard('process_manager_timeouts'));
        $this->assertSame([], $storage->findTimedOut(new DateTimeImmutable('2099-01-01 00:00:00.000000', new DateTimeZone('UTC')), 10));
    }

    /**
     * A timeout that no longer applies leaves the index rather than being
     * filtered out on every later read — there is only ever one answer to
     * "is this process overdue", and it lives in one place.
     */
    public function test_a_process_that_finishes_leaves_the_timeout_index(): void
    {
        $storage = $this->getProcessManagerStorage();
        $processId = EntityIdentifier::fromString('process-finished-with-timeout');

        $state = new ProcessManagerState($processId, ProcessManagerStateEnum::WAITING);
        $state->setTimeout(new DateTimeImmutable('2026-01-01 12:00:00.000000', new DateTimeZone('UTC')));
        $storage->store($state);

        $loaded = $storage->retrieve($processId);
        $this->assertNotNull($loaded);
        $loaded->setStatus(ProcessManagerStateEnum::COMPLETED);
        $storage->store($loaded);

        $this->assertSame(0, $this->getRedis()->zCard('process_manager_timeouts'));
        $this->assertNotNull($storage->retrieve($processId)?->getTimeout(), 'The state keeps its timeout for auditing.');
    }

    public function test_retrieveThrowsOnMalformedData(): void
    {
        $redis = $this->getRedis();
        $processId = EntityIdentifier::fromString('corrupt-process');

        $redis->hMSet('process_manager:corrupt-process', [
            'process_id' => 'corrupt-process',
            'status' => ProcessManagerStateEnum::WAITING->value,
            'data' => 'NOT_VALID_JSON',
        ]);

        $storage = $this->getProcessManagerStorage();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Failed to decode process manager data for process "corrupt-process"');

        $storage->retrieve($processId);
    }
}
