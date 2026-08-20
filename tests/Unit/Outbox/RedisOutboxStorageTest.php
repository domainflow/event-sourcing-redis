<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Unit\Outbox;

use DateTimeImmutable;
use DomainFlow\EventSourcing\Clock\FrozenClock;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Interface\OutboxStorageInterface;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractOutboxStorageTestCase;
use DomainFlow\EventSourcingCore\Provider\Unit\AnotherDummyEvent;
use DomainFlow\EventSourcingRedis\Outbox\RedisOutboxStorage;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisHelper;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(RedisOutboxStorage::class)]
final class RedisOutboxStorageTest extends AbstractOutboxStorageTestCase
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

    protected function getOutbox(): OutboxStorageInterface
    {
        return new RedisOutboxStorage($this->getRedis());
    }

    /**
     * Two relays over one key space, each with its own clock.
     *
     * @param int $leaseSeconds
     * @param int $skewSeconds
     * @return array{0: OutboxStorageInterface, 1: OutboxStorageInterface}
     */
    protected function getRelaysWithSkewedClocks(
        int $leaseSeconds,
        int $skewSeconds
    ): array {
        $now = new DateTimeImmutable('now');

        return [
            new RedisOutboxStorage($this->getRedis(), null, $leaseSeconds, new FrozenClock($now)),
            new RedisOutboxStorage(
                $this->getRedis(),
                null,
                $leaseSeconds,
                new FrozenClock($now->modify(sprintf('+%d seconds', $skewSeconds)))
            ),
        ];
    }

    /**
     * A relay that dies between claiming and marking would otherwise strand
     * its entries: a queue that stops draining while reporting nothing.
     *
     * The score is aged rather than a clock advanced, which is a deliberate
     * step back from the frozen-clock version added here. Since the lease
     * boundary is read from `TIME` inside the script, so moving a `FrozenClock`
     * no longer moves it and a test that tried would assert nothing. The lapse
     * is asserted this way and the skew directions are asserted by the two
     * contract cases; the counter-check records which mutation each
     * of the three catches.
     */
    public function test_anExpiredClaimIsPickedUpByTheNextRelay(): void
    {
        $outbox = $this->getOutbox();
        $outbox->enqueue([new AnotherDummyEvent(EntityIdentifier::fromString('OutboxStranded'), 1)]);

        $claimed = $outbox->reserve(1);
        $this->assertCount(1, $claimed, 'Precondition: the first relay claims it.');
        $this->assertSame([], $outbox->reserve(1), 'And holds it while the lease is live.');

        // Age the claim rather than sleeping: the point is the lease boundary,
        // not how fast the test machine is. A score in the past means "free".
        $this->getRedis()->zAdd(RedisOutboxStorage::PENDING_KEY, time() - 3600, $claimed[0]->getId());

        $this->assertCount(1, $outbox->reserve(1), 'With the lease lapsed it has to become claimable again.');
    }

    /**
     * The one instant this storage still takes from the relay's clock, and the
     * reason the parameter is not decorative.
     *
     * The dead-letter score records when *this* relay gave up. Nothing compares
     * it to anything, so it needs no fleet-wide agreement — unlike the lease,
     * which is why the lease moved into the script and this did not.
     */
    public function test_theDeadLetterScoreComesFromTheRelaysOwnClock(): void
    {
        $clock = new FrozenClock('2026-01-01 12:00:00.000000');
        $outbox = new RedisOutboxStorage($this->getRedis(), null, 300, $clock);

        $outbox->enqueue([new AnotherDummyEvent(EntityIdentifier::fromString('OutboxRelayStamp'), 1)]);
        $outbox->markAbandoned($outbox->reserve(1)[0]);

        $entries = $outbox->retrieveAbandoned(1);
        $this->assertCount(1, $entries);

        $this->assertSame(
            (float) $clock->now()->getTimestamp(),
            $this->getRedis()->zScore(RedisOutboxStorage::DEAD_KEY, $entries[0]->getId()),
            'The relay stamps its own giving-up; the store owns the lease and nothing else.'
        );
    }

    public function test_reservingNothingAsksRedisNothing(): void
    {
        $this->assertSame([], $this->getOutbox()->reserve(0));
    }
}
