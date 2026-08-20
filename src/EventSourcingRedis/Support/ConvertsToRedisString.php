<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Support;

use Stringable;

/**
 * Redis stores strings, so every persistence-record field has to become one.
 *
 * Shared between the event storage and the outbox rather than written twice.
 * Both write the same record shape, and the interesting case is exactly the one
 * a second implementation gets wrong: a value object such as EventId is
 * Stringable but not scalar, so falling through to json_encode() wraps it in
 * quotes and the value then fails to parse on the way back out.
 */
trait ConvertsToRedisString
{
    private function toRedisString(
        mixed $value
    ): string {
        return match (true) {
            is_string($value) => $value,
            is_scalar($value) || $value instanceof Stringable => (string) $value,
            default => (string) json_encode($value),
        };
    }
}
