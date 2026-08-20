<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration;

use DomainFlow\EventSourcingCore\Provider\Integration\AutomaticSnapshottingIntegrationTestCase;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class AutomaticSnapshottingIntegrationTest extends AutomaticSnapshottingIntegrationTestCase
{
    use RedisSetup;
}
