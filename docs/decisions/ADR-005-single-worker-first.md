# ADR-005: один worker и один active turn

**Статус:** принято для Foundation MVP.

## Решение

Один app-server process обслуживает максимум один active turn. Параллельность позже реализуется несколькими изолированными workers после измерения latency/quota/resource behavior.

## Причина

Упрощаются approvals, correlation, recovery, deterministic tests и измерение Spark quota.
