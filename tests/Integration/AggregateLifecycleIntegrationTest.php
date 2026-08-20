<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration;

use DomainFlow\EventSourcingCore\Provider\Integration\AggregateLifecycleIntegrationTestCase;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class AggregateLifecycleIntegrationTest extends AggregateLifecycleIntegrationTestCase
{
    use RedisSetup;
}
