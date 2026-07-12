# ADR-002: stdio JSONL transport

**Статус:** принято.

## Решение

Foundation MVP использует только официальный `codex app-server --listen stdio://`. WebSocket не используется, поскольку официально experimental/unsupported для production.

## Последствия

Runner обязан владеть child lifecycle, framing, stderr drain, process tree и graceful shutdown. Взамен transport локален, прост и не требует отдельной network auth границы между Runner и app-server.
