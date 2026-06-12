# Лабораторная работа 5: Inverted Index

Учебная реализация координатного обратного индекса на Go.

## Что реализовано

* tokenizer с Unicode-разбиением и позициями токенов;
* in-memory индекс `term -> posting list`, где posting содержит `docId`, `tf`, `positions`;
* `AND`, `OR`, `AND NOT`, `ADJ`, `NEAR/k`, фразы и скобки;
* TF-IDF и BM25 (`k1=1.2`, `b=0.75`);
* topK через min-heap;
* skip pointers с шагом `sqrt(df)`;
* сегмент на файловой системе с mmap-reader;
* delta-encoding, PForDelta блоками по 128 значений и bitpacking;
* synthetic benchmark data и JSONL-reader для wiki subset.

## Быстрый запуск

```bash
make demo
make test
make bench
make bench-smoke
```

## Wikipedia benchmark

Основной защитный benchmark рассчитан на Wikipedia JSONL. В текущем отчете использован частично скачанный subset примерно `1.4-1.5 GB`; целевой расширенный прогон можно догрузить до 5-8 GB:

```bash
cd ..
DOWNLOAD=1 TARGET_GB=6 scripts/hw5_prepare_wiki_sample.sh

cd hw5
make bench-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl
make bench-wiki DOCS=10000 WIKI=./data/wiki_sample.jsonl
make bench-wiki DOCS=25000 WIKI=./data/wiki_sample.jsonl
make graphs
```

Для текущего прогона на 5000 документах:

```bash
make report-wiki-5k
```

Когда будет возможность прогнать на более сильном компьютере:

```bash
make report-wiki-10k
make bench-wiki-25k
make graphs
```

Если `wiki_sample.jsonl` уже подготовлен, он должен лежать в `hw5/data/` и иметь формат:

```json
{"id":1,"title":"Article title","text":"Article text"}
```

## Тематические запросы

Для удобной проверки через консоль добавлены запросы ближе к теме женщин, науки, феминизма и прав:

```bash
make search-theme TOPK=5
make search-women-science TOPK=10
make search-women-rights TOPK=10
make search-feminism TOPK=10
make search-suffrage TOPK=10
make search-scientists TOPK=10
make search-voting-rights TOPK=10
make search-marie-curie TOPK=10
make search-ada-lovelace TOPK=10
```

Можно передать любой запрос:

```bash
make search QUERY="women AND science" TOPK=10
make explain QUERY="women NEAR/3 rights"
```

## CLI

```bash
go run ./cmd/searchdemo --mode demo
go run ./cmd/searchdemo --mode build --docs 5000 --index ./data/index.seg
go run ./cmd/searchdemo --mode search --index ./data/index.seg --query "data AND engineer" --topK 10 --rank bm25
go run ./cmd/searchdemo --mode repl --index ./data/index.seg
go run ./cmd/searchdemo --mode bench-smoke --docs 5000
go run ./cmd/searchdemo --mode build-wiki --input ./data/wiki_sample.jsonl --limit 5000 --index ./data/wiki.seg
```

## REPL

Команды:

* `:help`
* `:stats`
* `:mode bm25`
* `:mode tfidf`
* `:topk 10`
* `:exit`
