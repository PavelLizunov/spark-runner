# Независимый технический аудит spark-runner

- Дата: 2026-07-17
- Аудитор: независимый reviewer (Claude, senior Rust/backend review), вне команды миссии
- Проверенный commit: `6c2b9146707716159cf6462b0800b26bd690802c` (main; совпадает с ожидаемым HEAD, сверен с GitHub API)
- Режим: только safe offline-проверки; live UAT, реальные credentials и платные вызовы не выполнялись

## 1. Выполненные команды

Выполнено аудитором самостоятельно (не заимствовано из CI):

| Команда | Окружение | Результат |
|---|---|---|
| `cargo fmt --all -- --check` | Windows-хост (rustc 1.96.0) и Linux-контейнер `rust:1` (rustc 1.97.1), чистый LF-чекаут проверяемого commit | PASS |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | Windows | **FAIL — не компилируется** (E0560, см. F-4) |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | Linux-контейнер | PASS |
| `cargo test --locked --all-targets --all-features` | Linux-контейнер | **76 passed, 0 failed** (30 unit + 46 integration; совпадает с заявленными «76 offline tests») |
| `cargo build --locked --release` | Linux-контейнер | PASS |
| Сверка `schema_hash` из `codex.lock` с git-blob схемы | локально | Совпадает (`ff44dca1…`) |
| GitHub API: protection ветки `main`, список PR | публичное API | Protection **выключена** |

Не выполнено: `cargo audit` / `cargo deny` (требуют сетевых advisory-запросов — вне разрешённого offline-объёма; отмечено как непроверенное), live UAT (запрещён условиями аудита).

## 2. Краткий вердикт

**Реально готово:** offline-ядро. Протокольный клиент (poison-on-desync, bounded frames, один controlled restart), fail-closed approvals с явной schema-матрицей 0.144.3, journal + идемпотентное восстановление, loopback HTTP/SSE с bearer-auth и жёсткими лимитами. Все 76 тестов проходят, fmt/clippy/release-build чистые на Linux. README/PROGRESS.json/evidence согласованы между собой и с кодом: всё честно помечено «partial», CP7 «pending».

**Частично готово:** live-путь. Весь live-код (verify-by-inode, subscription-auth provisioning, admission) существует и покрыт fake-canary тестами, но с момента апгрейда пина 0.142.0 → 0.144.3 ни одного live-запуска не было — CP1/CP2 live-evidence относятся к 0.142.0.

**Отсутствует:** CP7 целиком (systemd unit, soak, release-артефакты, SBOM, notices), branch protection, release-гейты в CI, и — главное — сервис не переживёт soak из-за F-1/F-6.

## 3. Findings (по убыванию severity)

### P1

**F-1. Нет пути повторной admission: одна ошибка live-turn необратимо переводит API в 503.**
Подтверждено кодом. `src/api.rs` (`owner_finished`, ~строка 1114): любой `Err` активного исполнения → `snapshot.ready = false`; при этом `OwnerCommand::Bootstrap` отправляется ровно один раз при старте `serve()` (~строка 1926) и больше нигде. Комментарий в коде обещает «until a later bounded bootstrap succeeds», но повторного bootstrap не существует.
Сценарий: `AmbiguousNonIdempotent`, `cancellation_timeout`, ошибка journal или временно исчерпанная квота → все последующие `POST /turns` навсегда получают `RUNTIME_NOT_READY` до рестарта процесса.
Последствие: 24/72h soak гарантированно не проходится. Fail-closed, безопасно, но availability сломана.
Минимальный fix: при `CreateTurn` с `!ready` (или по таймеру) переотправлять `Bootstrap`.
Статус: подтверждённый дефект.

**F-2. Default branch `main` не защищена.**
Подтверждено: GitHub API `protection.enabled = false`, `protected = false`. MISSION.md требует protected branch + required green CI, запрет прямого push. Все 8 PR мержились через PR, но текущее состояние допускает прямой push/force-push.
Fix: включить protection с required check `validate`.
Статус: подтверждённое нарушение контракта миссии (организационная мера).

**F-3. CI не содержит release-гейтов, требуемых миссией.**
Подтверждено: `.github/workflows/ci.yml` — только fmt/clippy/test + JSON-валидация. MISSION.md («CI and security») требует также locked release build, dependency/license checks (audit/deny), secret scan; SBOM/notices нужны к релизу. Ничего из этого нет ни в CI, ни в репозитории.
Статус: подтверждённый пробел release readiness.

### P2

**F-4. Крейт компилируется только на Linux; все `#[cfg(not(unix))]`-ветки — фикция.**
Подтверждено запуском на Windows: E0560 в `src/config.rs:322` — `VerifiedExecutable { path, file }` безусловно инициализирует поле `file`, объявленное только под `#[cfg(target_os = "linux")]`; плюс unused imports под не-unix cfg. Код содержит десяток «переносимых» заглушек (`owner_only`, `is_executable`, non-linux `program()`), которые никогда не компилировались; CI (ubuntu-latest) это не поймает.
Fix: `compile_error!` для не-Linux, либо починить cfg на конструкторе и удалить мёртвые заглушки.
Статус: подтверждённый дефект (для linux-only миссии — не блокер, но код декларирует несуществующую переносимость).

**F-5. API принимает `timeout_seconds` до 300, но транспорт ждёт сообщение максимум 120 s.**
Подтверждено: `src/api.rs` (валидация 1..=300, ~строка 1580); `CodexClient` всегда строится через `JsonlClient::new` (`src/client.rs:757`) с `DEFAULT_WAIT_TIMEOUT = 120 s` (`src/jsonl.rs:29`); `wait_turn_completed` → `next_message()` с этим же таймаутом.
Сценарий: live-turn с паузой >120 s между notifications при запрошенном таймауте 300 s → `Timeout` → `runtime_failure`, а по F-1 ещё и перманентный 503.
Fix: пробрасывать turn-таймаут в `JsonlClient::with_timeout`.
Статус: подтверждённый дефект.

**F-6. `threads` никогда не освобождаются — API отказывает после 128 созданных thread.**
Подтверждено: `MAX_THREADS = 128`; `prune_terminal_records` (`src/api.rs`, ~строка 1856) сознательно не трогает threads. Клиент с паттерном «thread на turn» получит перманентный 429 `THREAD_CAPACITY` через 128 turns — на 72h soak это гарантированный отказ.
Fix: прунить threads без живых turn-записей (капасити-семантика сохраняется).
Статус: подтверждённый дефект (осознанное решение с непосчитанным следствием для soak).

**F-7. Одноразовый live-bootstrap ограничен 6 секундами.**
`OWNER_DEADLINE = 6 s` оборачивает `bootstrap_runtime` (`src/api.rs`, ~строка 388), внутри которого: чтение и SHA-256 всего native-бинаря, `--version` exec, spawn, initialize и три RPC. Холодный старт может не уложиться → `/ready` навсегда 503 (см. F-1).
Fix: отдельный бюджет для bootstrap + retry.
Статус: риск (не подтверждён замером — live недоступен).

**F-8. Crash-consistency журнала проверена только «чистым» рестартом.**
Тест `restart_projection_…` (`tests/cp5_journal_recovery.rs:29`) называет `writer.shutdown()` «kill simulation» — это clean shutdown, не kill -9 посреди записи. Повреждённая/недописанная БД не тестируется; при битом payload `project_recovery` вернёт `Err` и `serve` не стартует (fail-closed, но без операторского пути восстановления). SQLite WAL закрывает большинство torn-write сценариев.
Статус: риск.

### P3

**F-9. `codex.lock` непереносим — подтверждено, но это осознанный дизайн.**
Абсолютный путь `/home/uap/.local/lib/node_modules/...`, `platform: linux-x86_64`, требование сегмента `vendor` в пути (`src/config.rs:272`). На любом другом хосте `--live` невозможен без пересоздания lock. Риск: lock воспроизводим только на build-1.

**F-10. Нет `.gitattributes` — CRLF-чекаут ломает верификацию.**
Воспроизведено: клон с `core.autocrlf=true` даёт расхождение `schema_hash` с `codex.lock` (git-blob при этом совпадает); `verify_for_spawn` на таком чекауте fail-closed падает. Fix: `.gitattributes` с `* -text` или `*.json text eol=lf`.

**F-11. `serve` печатает готовность после завершения сервера.**
`src/main.rs:16-21` + `src/api.rs` (`serve` возвращает `addr` только когда `axum::serve` завершился): строка «serve: listening on …» не печатается за всё время работы. Оператор/systemd не имеют stdout-подтверждения старта.

**F-12. Graceful shutdown отсутствует.**
`OwnerCommand::Shutdown` помечен `#[allow(dead_code)]` и ниоткуда не вызывается; обработки SIGTERM нет; journal-writer не закрывается. Смягчено: child умирает по stdin EOF, WAL восстанавливается. Для systemd-сервиса CP7 придётся дописать.

**F-13. `ephemeral_cwd` предсказуем и использует `create_dir_all`.**
`src/config.rs:564-572`: имя из pid+nanos в общем `TMPDIR`, `create_dir_all` не падает на существующем каталоге → на многопользовательском хосте возможна pre-creation чужого каталога (сам `auth.json` создаётся `create_new` + 0600 — это спасает). Fix в одну строку: `create_dir`.

**F-14. Тихая потеря SSE-событий при отставании клиента.**
`BroadcastStream` `Err(Lagged)` фильтруется молча; глобальная eviction в `push_event` может выбросить события ещё активного turn. Replay по `Last-Event-ID` смягчает, но клиент не узнаёт о пропуске. Bounded by design; стоит документировать.

**F-15. Неограниченный рост `journal_events`.**
`prune_expired` чистит только captures — сам журнал append-only навсегда. При низком rate это годы, но для 72h soak стоит зафиксировать ожидаемый объём.

## 4. Матрица CP1–CP7

| CP | Заявлено | Подтверждено кодом | Подтверждено тестом | Недостаёт |
|---|---|---|---|---|
| CP1 Phase 0 | partial (live evidence на **0.142.0**) | schema 0.142.0 в репо; текущий пин — 0.144.3 | нет (historical) | live evidence на текущем пине 0.144.3 |
| CP2 минимальный runner | partial | да: process/jsonl/client/CLI | 2 app_server + unit | live `doctor --live` был только на 0.142.0 |
| CP3 poison/restart | partial | да: jsonl.rs + orchestrator (ровно один restart) | 12 тестов cp3, обе стороны | — (offline закрыт) |
| CP4 approvals fail-closed | partial | да: client.rs, deny по умолчанию, duplicate/timeout/desync-after-approval | 6 тестов cp4 | live-подтверждение реальных approval-форм |
| CP5 journal/recovery | partial | да: journal.rs, идемпотентное recovery, redaction | 6 тестов cp5 + unit | crash-mid-write, повреждённая БД (F-8) |
| CP6 HTTP/SSE | partial: «offline принят, live UAT pending» | да: api.rs полностью | 20 тестов (http_sse + runtime_owner), включая e2e t01 | live UAT; F-1/F-5/F-6 в этом слое |
| CP7 release | pending | ничего (нет unit, workflow, SBOM, notices) | нет | всё: systemd, soak, release-гейты, protection |

Проверка входных гипотез: «CP6 offline есть, live UAT нет» — подтверждена. «CP7 не завершён» — подтверждена (не начат). «PROGRESS.json противоречит evidence/PR» — не подтверждена: после hygiene-коммита всё согласовано (атавизм: CP1.md внутри говорит «green» при заголовке «PARTIAL»; заголовок честнее). «codex.lock непереносим» — подтверждена (F-9). «CI без release/audit/deny/secret scan» — подтверждена (F-3). «main не защищена» — подтверждена (F-2).

## 5. Тестовое покрытие и ложная уверенность

Сильные стороны: тесты проверяют поведение, а не строки — процесс-группа убивается по-настоящему (`kill(pid,0)`), interrupt сверяется по wire-маркеру с оригинальным JSON-RPC id, порядок журнала проверяется по строкам SQLite, канарейки секретов ищутся в выводе `env` ребёнка. Это выше среднего уровня.

Ложная уверенность:

1. **Fake app-server написан тем же автором, что и клиент, и с апгрейда на 0.144.3 ни разу не сверялся с реальным сервером.** Вся admission-логика (`quota_available` с `rateLimitsByLimitId`, `credits`, `rateLimitReachedType`), формы approval-ответов и `model/rerouted` проверены только против фикстуры, воспроизводящей предположения клиента. Петля самоподтверждения; закрывается только controlled live UAT.
2. **Вакуумные assert'ы:** `projection.replayed_turns == 0` (`tests/cp5_journal_recovery.rs:82`) — поле жёстко захардкожено в 0 (`src/journal.rs:600-601`), тест не может упасть.
3. **`model_cannot_self_approve`** тестирует guard (`ApprovalSource::Model`), который production-код никогда не вызывает.
4. **Никакой тест не гоняет turn дольше 120 s** — поэтому F-5 невидим для сьюта.
5. Заявленные «76 offline tests» — подтверждено фактическим прогоном (76 passed).

Минимальные недостающие тесты: (а) turn с `timeout_seconds > 120` и паузой fake-сервера >120 s (ловит F-5); (б) второй `CreateTurn` после `Err`-завершения первого (ловит F-1); (в) 129 созданий thread (ловит F-6); (г) kill -9 писателя журнала посреди append + повторное открытие (F-8).

## 6. Что упростить или удалить

- Мёртвый код: `jsonl::wait_for_notification` + `WaitTarget::Notification` (production-вызовов нет); `state::on_interrupt_requested` + `InternalEventKind::InterruptRequested` (ни одного вызова); `ApprovalSource::Model/System` + `ModelSelfApproval`; поля `replayed_turns`/`replayed_approvals`; карты `executions`/`approvals` в `RecoveryProjection` (используются только тестами); `OwnerCommand::Shutdown` (`#[allow(dead_code)]`).
- `StderrTail` хранит 16 КБ байт, которые никогда не показываются — `snapshot()` отдаёт только счётчик; достаточно счётчика.
- `owner_finished` парсит собственную строку-summary (`summary.contains("turn_status=completed")`) — вернуть типизированный результат вместо строки.
- `run_flow_body`: ветки Doctor и Run дублируют ~60 строк, отличаются одной строкой prompt.
- Рукописный SHA-256 (~80 строк, один тест-вектор) — решение задокументировано, реализация корректна, но `sha2` дешевле в сопровождении. Спорно, не требуется менять.
- Переписывать архитектуру не нужно: actor-owner + один worker соответствуют ADR-005, плотность защитного кода на approval-границе оправдана.

## 7. Следующие шаги (по порядку)

1. Починить soak-блокеры: повторный Bootstrap при `!ready` (F-1), проброс turn-таймаута в `JsonlClient` (F-5), прунинг терминальных threads (F-6) — три маленьких патча + три теста из §5.
2. Включить branch protection на `main` с required check; расширить CI: `cargo build --locked --release`, `cargo audit`, `cargo deny`, secret scan, пин конкретной версии toolchain вместо `channel = "stable"`.
3. Удалить мёртвый код из §6 и заменить string-parsing на типизированный результат.
4. Controlled live UAT на uap-build-1 против пина 0.144.3: `doctor --live`, `serve --live` + один turn с реальным approval и interrupt — закрывает CP6 и петлю самоподтверждения fake-сервера.
5. CP7: systemd unit с hardening + graceful shutdown (F-12), 24h → 72h soak, затем release с checksums/SBOM/notices.

## 8. Финальная оценка

**Offline-ready.** До UAT-ready формально не хватает только live-прогона, но запускать длительный UAT до фикса F-1/F-5/F-6 бессмысленно — сервис самоблокируется на первом транзиентном сбое. До production-ready — весь CP7 плюс P1-находки.

Серьёзных дефектов безопасности не найдено: approvals действительно fail-closed на каждом проверенном пути, секреты не утекают в журнал/ошибки/SSE (redaction протестирована), bind строго loopback, auth constant-time. Остаточные непроверяемые offline условия: реальное поведение codex 0.144.3 (формы rate-limit/approval/reroute), латентность live-bootstrap (F-7) и корректность `codex.lock` на build-1.
