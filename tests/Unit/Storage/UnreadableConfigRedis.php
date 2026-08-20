<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Unit\Storage;

use Redis;
use RedisException;

/**
 * A Redis whose CONFIG GET cannot be trusted.
 *
 * Managed Redis offerings routinely forbid CONFIG GET, and a server that
 * answers with something unexpected is the same situation from the adapter's
 * side: the configuration is unverified. Neither can be produced against a
 * real server, and both have to fail loudly rather than be read as "fine".
 */
final class UnreadableConfigRedis extends Redis
{
    public function __construct(
        private readonly bool $throwOnConfig,
        private readonly mixed $configResult = []
    ) {
        parent::__construct();
    }

    /**
     * @param mixed $operation
     * @param mixed ...$args
     * @return mixed
     */
    public function config(
        mixed $operation,
        mixed ...$args
    ): mixed {
        if ($this->throwOnConfig) {
            throw new RedisException('CONFIG is disabled for this deployment.');
        }

        return $this->configResult;
    }
}
