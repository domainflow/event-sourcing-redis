<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingRedis\Support;

/**
 * One place where a phpredis reply stops being untyped.
 *
 * The extension declares the stream commands as returning `array|bool|Redis`,
 * because the same method call means "give me the entries" against a plain
 * connection and "queue this command" against one in pipeline or multi mode.
 * A caller in one mode can never see the other's return, but the signature
 * cannot say which mode it is in, so level 10 asks the reader to.
 *
 * Conversions rather than assertions: this adapter only calls these outside
 * pipeline mode, so a guard that threw would be a branch nothing could reach.
 */
trait ReadsRedisReplies
{
    /**
     * A stream range as entry id => fields.
     *
     * @param mixed $reply
     * @return array<string, array<string, string>>
     */
    private function toStreamEntries(
        mixed $reply
    ): array {
        $entries = [];

        foreach (is_array($reply) ? $reply : [] as $entryId => $fields) {
            $entries[(string) $entryId] = $this->toFields($fields);
        }

        return $entries;
    }

    /**
     * A hash reply as field => value.
     *
     * Everything Redis stores is a string on the way out; a value that is not
     * scalar cannot have been written by this adapter and is read as absent
     * rather than turned into a warning at the point it is concatenated.
     *
     * @param mixed $reply
     * @return array<string, string>
     */
    private function toFields(
        mixed $reply
    ): array {
        $fields = [];

        foreach (is_array($reply) ? $reply : [] as $name => $value) {
            $fields[(string) $name] = is_scalar($value) ? (string) $value : '';
        }

        return $fields;
    }
}
