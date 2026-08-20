# DomainFlow EventSourcing Redis

[![Tests](https://github.com/domainflow/event-sourcing-redis/actions/workflows/tests.yml/badge.svg)](https://github.com/domainflow/event-sourcing-redis/actions/workflows/tests.yml)
![Packagist Version](https://img.shields.io/packagist/v/domainflow/event-sourcing-redis)
![PHP Version](https://img.shields.io/packagist/php-v/domainflow/event-sourcing-redis)
![License](https://img.shields.io/github/license/domainflow/event-sourcing-redis)
![PHPStan](https://img.shields.io/badge/PHPStan-Level%210-brightgreen.svg)

A Redis storage adapter for [`domainflow/event-sourcing-core`](https://github.com/domainflow/event-sourcing-core) 
— implements `EventStorageInterface`, `SnapshotStorageInterface`, `SnapshotHistoryStorageInterface`, and `ProcessManagerStorageInterface` against Redis. No domain logic of its own — no aggregate modeling, no business rules, just translation between Core's interfaces and Redis.

## Requirements

- PHP 8.4+
- `ext-redis`
- A reachable Redis 7+ instance, configured as below

## Installation

```bash
composer require domainflow/event-sourcing-redis
```

## Production requirements

An event store is a system of record, and Redis' defaults are not built for one. Two settings are mandatory, and `RedisEventStorage` refuses to start without them rather than let you find out later:

```
appendonly yes
appendfsync always        # everysec is acceptable if losing up to a second of events is
maxmemory-policy noeviction
```

- **`appendonly yes`.** With RDB snapshotting alone, a crash loses every event written since the last snapshot. `appendfsync always` is what "no event is ever lost" actually costs; `everysec` trades a one-second window for throughput.
- **`maxmemory-policy noeviction`.** Any `allkeys-*` policy lets Redis discard event streams to reclaim memory. There is no error and no log line — an aggregate simply replays to a state it was never in. This is the realistic failure, because it is what happens when the adapter is pointed at an instance that is already serving as a cache.

The check runs once, when the storage is constructed. If your host forbids `CONFIG GET` — several managed Redis offerings do — verify the settings yourself and pass `assertDurableConfiguration: false`:

```php
$storage = new RedisEventStorage($redis, assertDurableConfiguration: false);
```

**`storeEvents()` is all-or-nothing for the whole call.** One Lua script appends every event of the call, across every aggregate it touches, and verifies all their versions before writing any of them. If it throws, nothing was written and the identical batch can be retried.

**Redis Cluster is not supported.** That script touches every aggregate's stream plus the global index in one atomic call, and those keys cannot share a hash slot unless every key in the store shares one — at which point the cluster is a single node with extra steps. Use a single instance, or Sentinel for failover.


## Development

```bash
docker compose up -d          # start a local Redis instance
composer install
composer quality              # lint + static analysis + full test suite (100% coverage required) + audit
```

## License

MIT — see [LICENSE](LICENSE).
