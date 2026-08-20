<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration;

use DomainFlow\EventSourcingCore\Provider\Integration\ConcurrencyAttributeIntegrationTestCase;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class ConcurrencyAttributeIntegrationTest extends ConcurrencyAttributeIntegrationTestCase
{
    use RedisSetup;
}
