# ADR-004: tolerant reader, strict writer, poison-on-desync

**Статус:** принято.

## Решение

Extra fields и unknown notifications допускаются и журналируются. Malformed framing, oversized frames, unknown response IDs и невозможные terminal transitions poison connection и вызывают controlled restart. Writer отправляет только locked stable schema.
