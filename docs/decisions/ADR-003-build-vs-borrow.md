# ADR-003: own narrow core + selective borrowing

**Статус:** принято.

## Решение

Не выбирать один community SDK как безусловное ядро. Создать собственный narrow transport/runtime за `CodexBackend`; готовые crates проходят одинаковый bake-off и могут использоваться только как replaceable adapters.

## Причины

Process hardening, protocol completeness, fixtures и licensing лучше всего представлены в разных проектах. Public contract должен принадлежать нам.
