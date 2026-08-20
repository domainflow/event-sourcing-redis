<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration;

use DomainFlow\EventSourcingCore\Provider\Integration\ConcurrencyIntegrationTestCase;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class ConcurrencyIntegrationTest extends ConcurrencyIntegrationTestCase
{
    use RedisSetup;
}
