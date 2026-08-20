<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Unit\Schema;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Interface\SchemaManagerInterface;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractSchemaManagerTestCase;
use DomainFlow\EventSourcingCore\Provider\Unit\AnotherDummyEvent;
use DomainFlow\EventSourcingRedis\Schema\RedisSchemaManager;
use DomainFlow\EventSourcingRedis\Storage\RedisEventStorage;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisHelper;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;

#[CoversClass(RedisSchemaManager::class)]
#[UsesClass(RedisEventStorage::class)]
final class RedisSchemaManagerTest extends AbstractSchemaManagerTestCase
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

    protected function getSchemaManager(): SchemaManagerInterface
    {
        return new RedisSchemaManager($this->getRedis());
    }

    protected function writeAnEvent(): void
    {
        (new RedisEventStorage($this->getRedis()))
            ->storeEvents([new AnotherDummyEvent(EntityIdentifier::fromString('schema-probe'), 1)]);
    }

    protected function schemaExists(): bool
    {
        return $this->getRedis()->exists('events:global') > 0;
    }

    /**
     * The patterns are listed rather than derived from one prefix, because the
     * keys were never designed under one. A `dropSchema()` matching a guessed
     * pattern would leave whatever it did not think of behind — so write
     * through every storage and check that nothing survives.
     */
    public function test_drop_schema_leaves_nothing_this_adapter_owns(): void
    {
        $this->writeAnEvent();
        $this->getRedis()->set('process_manager_timeouts', 'x');
        $this->getRedis()->set('outbox:seq', '7');
        $this->getRedis()->set('snapshots:probe', 'x');
        $this->getRedis()->set('snapshot_history:probe', 'x');
        $this->getRedis()->set('outbox:entry:1', 'x');

        $this->getSchemaManager()->dropSchema();

        $this->assertSame([], $this->getRedis()->keys('*'));
    }

    /**
     * A key that is not this adapter's is not this adapter's to delete.
     */
    public function test_drop_schema_leaves_everything_else_alone(): void
    {
        $this->getRedis()->set('someone:elses:cache', 'keep me');

        $this->getSchemaManager()->dropSchema();

        $this->assertSame('keep me', $this->getRedis()->get('someone:elses:cache'));
    }

    /**
     * "Nothing to do" is the one answer a consumer cannot tell apart from
     * "nobody documented it", so the description says what this adapter needs
     * of the deployment and which keys it will occupy.
     */
    public function test_describe_schema_states_the_requirements_and_the_key_space(): void
    {
        $description = $this->getSchemaManager()->describeSchema();

        $this->assertStringContainsString('noeviction', $description[0]);
        $this->assertStringContainsString('appendonly yes', $description[1]);
        $this->assertContains('USES KEYS events:aggregate:*', $description);
    }
}
