<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Integration\Repository;

use DomainFlow\EventSourcing\Interface\DomainEventInterface;
use DomainFlow\EventSourcing\Interface\ProjectorInterface;
use DomainFlow\EventSourcingCore\Provider\Integration\ProjectorDummyEvent;
use Redis;

/**
 * Test-only fixture, mirroring the MySQL adapter's AnotherCounterProjector, for
 * ProjectionReplayIntegrationTestCase (which needs a ProjectorInterface, not the
 * CounterProjectionRepositoryInterface used by ProjectionAndReadModelIntegrationTestCase).
 */
final readonly class RedisCounterProjector implements ProjectorInterface
{
    private RedisCounterProjectionRepository $repository;

    public function __construct(
        Redis $redis
    ) {
        $this->repository = new RedisCounterProjectionRepository($redis);
    }

    public static function getSubscribedTo(): array
    {
        return [ProjectorDummyEvent::class];
    }

    public function handle(
        DomainEventInterface $event
    ): void {
        if (!$this->supports($event::class)) {
            return;
        }

        /** @var ProjectorDummyEvent $event */
        $aggregateId = (string) $event->getAggregateId();
        $current = $this->repository->getCounter($aggregateId) ?? 0;

        $this->repository->saveCounter($aggregateId, $current + $event->getDelta());
    }

    public function reset(): void
    {
        $this->repository->reset();
    }

    public function replay(
        DomainEventInterface ...$events
    ): void {
        foreach ($events as $event) {
            if ($this->supports($event::class)) {
                $this->handle($event);
            }
        }
    }

    public function supports(
        string $eventClass
    ): bool {
        return in_array($eventClass, self::getSubscribedTo(), true);
    }

    public function getName(): string
    {
        return 'RedisCounterProjector';
    }
}
