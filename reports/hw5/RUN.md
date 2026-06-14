# HW5 Runbook

## 1. Быстрая проверка без Wikipedia

```bash
cd hw5
make demo
make test
make bench-smoke
make compression-stats-synthetic DOCS=5000
```

Synthetic corpus нужен только для smoke-проверки и для стабильных пар `data pipeline`, `new york`, `machine learning`, `distributed systems`. Основной отчёт считаем на Wikipedia.

## 2. Текущий Wikipedia dataset

Сейчас локально используется Wikipedia JSONL размером примерно `6.1 GB`.

Файл:

```text
hw5/data/wiki_sample.jsonl
```

Формат строк:

```json
{"id":1,"title":"Article title","text":"Article text"}
```

В файле `835456` строк, одна строка соответствует одной статье Wikipedia. Поэтому срезы задаются параметром `DOCS`:

- `DOCS=50000` — один segment, малый контрольный прогон;
- `DOCS=417728` — отдельный half-prefix в `9` segment-ах;
- `DOCS=835456` — весь текущий датасет.

Срез выбирается как первые `DOCS` строк из `wiki_sample.jsonl`. Это не случайная выборка, зато такой benchmark легко воспроизвести: один и тот же `DOCS` всегда дает один и тот же корпус.

## 3. Как догружать Wikipedia


DOWNLOAD=1 TARGET_GB=6 scripts/hw5_prepare_wiki_sample.sh

Источник:

```text
https://dumps.wikimedia.org/enwiki/latest/
```

## 4. Основной прогон для отчёта

```bash
cd hw5
make bench-build-wiki DOCS=50000 INDEX=./data/wiki_50k.seg ITERATIONS=5
make build-wiki-shards DOCS=835456 SEGMENT_DOCS=50000
make build-wiki-shards-half-prefix SEGMENT_DOCS=50000
make wiki-report-scale-stats SEGMENT_DOCS=50000
make bench-query-wiki-shards-one-reuse SEGMENT_DOCS=50000 QUERIES_PER_TYPE=50 QUERY_SAMPLE_DOCS=10000 ITERATIONS=5
make bench-query-wiki-shards-half-reuse SEGMENT_DOCS=50000 QUERIES_PER_TYPE=50 QUERY_SAMPLE_DOCS=10000 ITERATIONS=5
make bench-query-wiki-shards DOCS=835456 SHARD_LIMIT=0 SEGMENT_DOCS=50000 QUERIES_PER_TYPE=50 QUERY_SAMPLE_DOCS=10000 ITERATIONS=5
make bench-ranking-wiki DOCS=50000 INDEX=./data/wiki_50k.seg TOPK=10 ITERATIONS=5
make graphs
```

`make wiki-report-scale-stats` пишет `reports/hw5/results/wiki_scale_stats.csv` с тремя точками: `1 seg / 50000 docs`, `9 seg / 417728 docs`, `17 seg / 835456 docs`. Именно этот CSV нужен, чтобы в `Raw vs compressed` появились не только значения для одного segment-а, но и половина и полный датасет.

Для финальных графиков масштабирования нужны три CSV:

```text
reports/hw5/results/wiki_sharded_query_latency_docs50000_shards1.csv
reports/hw5/results/wiki_sharded_query_latency_docs417728_shards9.csv
reports/hw5/results/wiki_sharded_query_latency_docs835456_shards17.csv
```

Первый файл уже снят. Если нужно доснять половину и полный датасет, запускай:

```bash
make build-wiki-shards-half-prefix SEGMENT_DOCS=50000
make wiki-report-scale-stats SEGMENT_DOCS=50000
make bench-query-wiki-shards-half-reuse SEGMENT_DOCS=50000 QUERIES_PER_TYPE=50 QUERY_SAMPLE_DOCS=10000 ITERATIONS=5
make bench-query-wiki-shards DOCS=835456 SHARD_LIMIT=0 SEGMENT_DOCS=50000 QUERIES_PER_TYPE=50 QUERY_SAMPLE_DOCS=10000 ITERATIONS=5
make graphs
```

После каждого sharded-прогона код сохраняет общий `wiki_sharded_query_latency.csv` и отдельный snapshot с `docs` и числом shard-ов в имени. Именно snapshot-файлы нужны для графика сравнения `1 / 9 / 17` segment-ов.

В query benchmark перед пятью измерениями делается один дополнительный прогревочный запуск. Он нужен, чтобы первый доступ к mmap/декодированию не портил среднее.

Эти команды используют разные shard-индексы:

- `data/wiki_shards/manifest.json` для полного прогона на `835456` документах;
- `data/wiki_shards/prefix-417728/manifest.json` для точной половины датасета;
- один segment `50000` документов для малого контрольного прогона.

`QUERIES_PER_TYPE=50` означает, что генератор попробует собрать до `50` запросов на каждый тип оператора. Это уже сотни запросов и нормально подходит для отчета. Если нужен стресс-тест на несколько тысяч запросов, можно поднять до `300`, но такой прогон будет заметно дольше.

Если нужны характеристики корпуса именно для трех срезов, основной путь для отчёта - это `make wiki-report-scale-stats SEGMENT_DOCS=50000`. Он пишет итоговый сравнительный CSV для `50000 / 417728 / 835456`.

## 5. Консольный поиск

```bash
make search-women-science TOPK=10
make search-women-rights TOPK=10
make search-feminism TOPK=10
make search-scientists TOPK=10
make search-marie-curie TOPK=10
make search-ada-lovelace TOPK=10

go run ./cmd/searchdemo --mode search --index ./data/wiki_50k.seg --query "women AND science" --topK 10 --rank bm25
go run ./cmd/searchdemo --mode search --index ./data/wiki_50k.seg --query "(women OR feminism) AND rights" --topK 10 --rank bm25
go run ./cmd/searchdemo --mode search --index ./data/wiki_50k.seg --query '"marie curie"' --topK 10 --rank bm25
```

Если нужен интерактивный режим, можно открыть REPL так:

```bash
go run ./cmd/searchdemo --mode repl --index ./data/wiki_50k.seg
```

REPL команды:

```text
:help
:stats
:mode bm25
:mode tfidf
:mode none
:topk 10
:explain women NEAR/3 rights
:exit
```

## 6. Go benchmarks

Обычные тесты не требуют Wikipedia:

```bash
go test ./...
```

Большие wiki benchmarks включаются явно:

```bash
RUN_WIKI_BENCH=1 WIKI=./data/wiki_sample.jsonl DOCS=5000 go test ./... -bench=Wiki -benchmem
```

## 7. Профилирование

Профилирование удобнее снимать на одном базовом segment-е `50000` документов: так проще открыть flame graph в браузере и не ждать полный sharded-прогон. Сейчас все profiling-скрипты используют один и тот же путь через `cmd/searchdemo --mode profile-workload`.

Базовые команды:

```bash
cd hw5
make profile-cpu
make profile-mem
make profile-interesting
```

- `make profile-cpu` снимает только CPU profiles
- `make profile-mem` снимает только memory profiles
- `make profile-interesting` просто запускает оба скрипта подряд

Снимаются такие сценарии:

- `wiki-near-query` - только `query.Execute` для proximity-запроса на memory backend
- `wiki-complex-query` - только `query.Execute` для сложного boolean-запроса
- `wiki-bm25-topk` - только `TopK` после уже готового boolean filter
- `wiki-mmap-materialize-near` - `Materialize + query.Execute` для mmap backend
- `wiki-mmap-lookup` - чистый `LookupTerm` без ranking и boolean merge

Результаты сохраняются в:

```text
reports/hw5/profiles/cpu/clean_*.out
reports/hw5/profiles/cpu/clean_*_top.txt
reports/hw5/profiles/mem/clean_*.out
reports/hw5/profiles/mem/clean_*_top.txt
reports/hw5/profiles/bin/searchdemo
```

По умолчанию используется базовый `50k`-срез:

```text
WIKI=./data/wiki_sample.jsonl
DOCS=50000
INDEX=./data/wiki_50k.seg
CPU_ITERATIONS=200
MEM_ITERATIONS=50
```

Для быстрого прогона можно уменьшить число итераций:

```bash
cd hw5
CPU_ITERATIONS=50 make profile-cpu
MEM_ITERATIONS=10 make profile-mem
```

Для Web UI:

```bash
scripts/hw5_pprof_ui.sh cpu wiki-near-query
scripts/hw5_pprof_ui.sh mem wiki-bm25-topk
```

Или через `make`:

```bash
cd hw5
make pprof-ui-cpu PROFILE=wiki-near-query
make pprof-ui-mem PROFILE=wiki-bm25-topk PPROF_HTTP=:8081
```

В `PROFILE` можно передать и полное имя файла, например `clean_wiki-near-query.out`, или полный путь до конкретного `.out`.
