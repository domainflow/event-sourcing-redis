<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Tests\Unit\Crypto;

use DateTimeImmutable;
use DomainFlow\EventSourcing\Attribute\DataSubjectId;
use DomainFlow\EventSourcing\Attribute\PersonalData;
use DomainFlow\EventSourcing\Crypto\EncryptingEventEntryFactory;
use DomainFlow\EventSourcing\Crypto\InMemoryPersonalDataKeyStore;
use DomainFlow\EventSourcing\Crypto\RedactedValue;
use DomainFlow\EventSourcing\Crypto\SodiumCipher;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\DefaultEventEntryFactory;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\SourceEvent;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcing\Interface\EventStorageInterface;
use DomainFlow\EventSourcing\Upcaster\ReflectionEventFactory;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractEventStorageTestCase;
use DomainFlow\EventSourcingRedis\Storage\RedisEventStorage;
use DomainFlow\EventSourcingRedis\Tests\Setup\RedisHelper;
use PHPUnit\Framework\Attributes\CoversClass;

/**
 * The whole storage contract, run through the crypto-shredding decorator.
 *
 * Here the payload travels as a field of a stream entry, written by a Lua
 * script that never looks inside it — which is exactly why this adapter needs
 * no changes, and worth asserting rather than assuming.
 */
#[CoversClass(RedisEventStorage::class)]
final class EncryptedRedisEventStorageTest extends AbstractEventStorageTestCase
{
    use RedisHelper;

    private ?InMemoryPersonalDataKeyStore $keys = null;

    protected function setUp(): void
    {
        parent::setUp();
        $this->flushRedis();
    }

    protected function tearDown(): void
    {
        $this->flushRedis();
    }

    private function keys(): InMemoryPersonalDataKeyStore
    {
        return $this->keys ??= new InMemoryPersonalDataKeyStore();
    }

    private function encrypting(
        EventEntryFactoryInterface $inner
    ): EncryptingEventEntryFactory {
        return new EncryptingEventEntryFactory($inner, $this->keys(), new SodiumCipher());
    }

    protected function getStorage(): EventStorageInterface
    {
        return new RedisEventStorage(
            $this->getRedis(),
            $this->encrypting(new DefaultEventEntryFactory(new ReflectionEventFactory()))
        );
    }

    protected function getStorageWithFactory(): EventStorageInterface
    {
        return $this->getStorage();
    }

    protected function getStorageWhoseWritesFailWithoutConflict(): EventStorageInterface
    {
        $this->getRedis()->set('events:aggregate:NonConflictingFailureAggregate', 'this is not a stream');

        return $this->getStorage();
    }

    public function test_an_erased_subject_is_redacted_when_the_stream_is_replayed(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('order-erased');

        $event = new RedisEncryptedCustomerRegistered($aggregateId, null, 'customer-1', 'ada@example.com', 'ORD-42');
        $event->setVersion(EventVersion::fromInt(1));
        $storage->storeEvents([$event]);

        $entries = $this->getRedis()->xRange('events:aggregate:order-erased', '-', '+');
        $this->assertStringNotContainsString(
            'ada@example.com',
            json_encode($entries, JSON_THROW_ON_ERROR),
            'The personal data reached the stream in the clear.'
        );

        $this->keys()->forget('customer-1');

        $replayed = $storage->retrieveEvents($aggregateId);

        $this->assertCount(1, $replayed);
        $this->assertInstanceOf(RedisEncryptedCustomerRegistered::class, $replayed[0]);
        $this->assertTrue(RedactedValue::isRedacted($replayed[0]->email));
        $this->assertSame('ORD-42', $replayed[0]->orderReference);
    }

    /**
     * The same storage, built so its writes are enqueued for a relay.
     *
     * @return EventStorageInterface|null
     */
    protected function getStorageDeliveringThroughOutbox(): ?EventStorageInterface
    {
        return new RedisEventStorage($this->getRedis(), outboxEnabled: true);
    }
}

final class RedisEncryptedCustomerRegistered extends SourceEvent
{
    public function __construct(
        ?EntityIdentifierInterface $aggregateId,
        ?EntityIdentifierInterface $eventId,
        #[DataSubjectId]
        public string $customerId = '',
        #[PersonalData]
        public string $email = '',
        public string $orderReference = '',
        ?DateTimeImmutable $occurredOn = null,
        ?EventVersion $version = null
    ) {
        parent::__construct($aggregateId, $eventId, $occurredOn, $version);
    }

    public function toArray(): array
    {
        return parent::toArray() + [
            'customerId' => $this->customerId,
            'email' => $this->email,
            'orderReference' => $this->orderReference,
        ];
    }
}
