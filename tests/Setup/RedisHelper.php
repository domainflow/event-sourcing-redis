<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Setup;

use Redis;

trait RedisHelper
{
    private static ?Redis $redisClient = null;

    public function getRedis(): Redis
    {
        if (self::$redisClient === null) {
            $host = getenv('REDIS_HOST') ?: '127.0.0.1';
            $port = getenv('REDIS_PORT') ?: 6379;

            $redis = new Redis();
            $redis->connect($host, (int) $port);

            self::$redisClient = $redis;
        }

        return self::$redisClient;
    }

    protected function flushRedis(): void
    {
        $this->getRedis()->flushAll();
    }
}
