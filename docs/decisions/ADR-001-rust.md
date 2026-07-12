# ADR-001: Rust как production runtime

**Статус:** принято.

## Контекст

Runner — долгоживущий process supervisor и bidirectional protocol bridge. Приоритеты: предсказуемость, статическая проверка, bounded concurrency, один deployable binary и compiler-driven AI-assisted development.

## Решение

Production core пишется на Rust/Tokio. Python SDK используется только как oracle; TypeScript не входит в ядро.

## Последствия

Плюсы: строгие state machines, no-GC runtime, качественный tooling. Минусы: более дорогой первый implementation и необходимость собственного app-server client layer.
