# HW5 Runbook

## 1. Быстрая проверка без Wikipedia

```bash
cd hw5
make demo
make test
make bench-smoke
make compression-stats-synthetic DOCS=5000
```

Synthetic corpus нужен для smoke-проверки и для стабильных пар `data pipeline`, `new york`, `machine learning`, `distributed systems`.

## 2. Текущий Wikipedia dataset

Сейчас локально используется не полный 6GB subset, а частично скачанный Wikipedia JSONL примерно на `1.4-1.5 GB`.

Файл:

```text
hw5/data/wiki_sample.jsonl
```

Формат строк:

```json
{"id":1,"title":"Article title","text":"Article text"}
```

Это нормальный рабочий вариант для текущего отчёта: все CSV и графики пересчитаны на первых `5000` статьях из этого файла. Когда сеть позволит, можно догрузить до 5-8GB той же командой.

## 3. Как догружать Wikipedia

```bash
cd ..
DOWNLOAD=1 TARGET_GB=6 scripts/hw5_prepare_wiki_sample.sh
```

Если Python на macOS ругается на сертификаты:

```bash
open "/Applications/Python 3.10/Install Certificates.command"
```

Или для одного скачивания публичного Wikimedia dump:

```bash
WIKI_INSECURE_SSL=1 DOWNLOAD=1 TARGET_GB=6 scripts/hw5_prepare_wiki_sample.sh
```

Источник:

```text
https://dumps.wikimedia.org/enwiki/latest/
```

## 4. Основной прогон для отчёта

```bash
cd hw5
make wiki-stats DOCS=5000 WIKI=./data/wiki_sample.jsonl
make build-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl
make prepare-wiki-queries DOCS=5000 WIKI=./data/wiki_sample.jsonl
make compression-stats-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl
make bench-query-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl ITERATIONS=3
make bench-mmap-vs-memory DOCS=5000 WIKI=./data/wiki_sample.jsonl
make bench-ranking-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl
make graphs
```

Одна команда:

```bash
make bench-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl ITERATIONS=3
make graphs
```

Для более крупного прогона после догрузки:

```bash
make bench-wiki DOCS=10000 WIKI=./data/wiki_sample.jsonl ITERATIONS=3
make bench-wiki DOCS=25000 WIKI=./data/wiki_sample.jsonl ITERATIONS=3
```

Удобные aliases:

```bash
make report-wiki-5k
make report-wiki-10k
```

## 5. Консольный поиск

```bash
make run-cli-wiki
make search-theme TOPK=5
make search-women-science TOPK=10
make search-women-rights TOPK=10
make search-feminism TOPK=10
make search-marie-curie TOPK=10
make search-ada-lovelace TOPK=10

go run ./cmd/searchdemo --mode search --index ./data/wiki.seg --query "women AND science" --topK 10 --rank bm25
go run ./cmd/searchdemo --mode search --index ./data/wiki.seg --query "(women OR feminism) AND rights" --topK 10 --rank bm25
go run ./cmd/searchdemo --mode search --index ./data/wiki.seg --query '"marie curie"' --topK 10 --rank bm25
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

```bash
../scripts/hw5_profile_cpu.sh
../scripts/hw5_profile_mem.sh
```

Файлы:

```text
reports/hw5/profiles/cpu_and.out
reports/hw5/profiles/cpu_or.out
reports/hw5/profiles/cpu_near.out
reports/hw5/profiles/cpu_bm25.out
reports/hw5/profiles/mem_query.out
```

Просмотр:

```bash
go tool pprof reports/hw5/profiles/cpu_and.out
go tool pprof -http=:8080 reports/hw5/profiles/cpu_and.out
```

## 8. Где лежат результаты

```text
reports/hw5/results/
graphs/hw5/
```

Сырые большие данные и сегменты не коммитятся:

```text
hw5/data/wiki_sample.jsonl
hw5/data/wiki_dump/
hw5/data/*.seg
```
