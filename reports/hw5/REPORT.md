# HW5 - Позиционный Inverted Index

Выполнила Дусаева Элина.

## Общая идея решения

В этой лабораторной работе я реализовала свой обратный индекс для текстового поиска.

Основная идея такая:

- текст разбивается на токены;
- для каждого терма строится posting list;
- в posting хранится `docId`, частота терма в документе и позиции;
- boolean-запросы выполняются через итераторы по posting list-ам;
- позиционные запросы `ADJ` и `NEAR/k` дополнительно проверяют positions;
- индекс можно записать на диск в бинарный segment и потом открыть через `mmap`;
- posting list-ы сжимаются через delta-encoding, PForDelta и bitpacking;
- поверх boolean-результата считается ранжирование через TF-IDF или BM25.

То есть это не Lucene/Elasticsearch и не готовый движок, а небольшая учебная реализация основных частей поискового индекса.

## Что реализовано

- tokenizer с unicode-буквами и цифрами;
- координатный индекс `term -> []Posting`;
- `AND`, `OR`, `AND NOT`, `ADJ`, `NEAR/k`;
- фразы, например `"united states"`;
- сложные запросы со скобками;
- skip pointers с шагом `sqrt(df)`;
- TF-IDF;
- BM25;
- topK через min-heap;
- disk segment;
- mmap-reader;
- delta-encoding;
- PForDelta блоками по 128 значений;
- bitpacking;
- synthetic benchmark;
- Wikipedia benchmark;
- query suite по операторам;
- raw vs compressed size;
- memory vs mmap сравнение;
- CPU/memory profile scripts.

## Как устроено решение

Основная логика лежит в `hw5/internal`.

| пакет | что делает |
| --- | --- |
| `analyzer` | приводит текст к lower-case, режет его на токены и сохраняет позиции |
| `index` | строит `MemoryIndex`, posting list-ы, df/ttf, skip pointers |
| `query` | парсит запрос и выполняет `AND/OR/NOT/ADJ/NEAR` |
| `scoring` | считает TF-IDF, BM25 и topK |
| `codec` | сжимает числа через delta, bitpacking и PForDelta |
| `storage` | пишет `.seg` файл и открывает его через mmap |
| `benchdata` | synthetic corpus, wiki JSONL reader, query suite |
| `benchmark` | собирает метрики и пишет CSV |

При построении индекса каждый документ сначала токенизируется. Потом для каждого токена сохраняется позиция внутри документа. После добавления всех документов builder сортирует posting list-ы по `docId`, считает `df`, `ttf`, длины документов и среднюю длину документа.

## Формат индекса на диске

На диске индекс хранится в одном `.seg` файле.

Сегмент устроен так:

1. `Header` - magic, version, codec, количество документов, количество терминов и offset-ы секций.
2. `Dictionary` - для каждого терма хранится `df`, `ttf`, offset и длина posting list-а.
3. `Postings` - сжатые docId gaps, freqs, positions и skip metadata.
4. `Doc norms` - длины документов.
5. `Titles` - заголовки документов для вывода результата.

`MmapSegmentReader` открывает файл через `mmap`. В память сразу читаются header, dictionary, doc norms и titles, а сами posting list-ы декодируются лениво только для терминов, которые реально встретились в запросе.

## Сжатие

Raw baseline я считаю так:

- `docId` = `uint32` на каждый posting;
- `freq` = `uint32` на каждый posting;
- `position` = `uint32` на каждую позицию;
- `raw_total = raw_docids + raw_freqs + raw_positions`.

Дальше применяются:

- delta-encoding для `docId` и positions;
- PForDelta блоками по 128 значений;
- bitpacking для базовых значений блока;
- exceptions отдельно;
- VarInt только для metadata/freqs, не как замена PForDelta.

Почему это должно помогать: `docId` внутри posting list-а отсортированы, поэтому gaps обычно меньше абсолютных значений. Positions внутри одного документа тоже отсортированы, поэтому gaps между позициями часто маленькие.

### Raw vs compressed

Замер сделан на текущем Wikipedia subset. Полностью 6 GB скачать не получилось из-за сетевых таймаутов, поэтому сейчас основной локальный файл `wiki_sample.jsonl` имеет размер примерно `1.4 GB`. В таблицах ниже индексировались первые `5000` статей из этого файла.

| corpus | docs | raw total bytes | compressed total bytes | segment file bytes | compression ratio | saving |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| wiki | 5000 | 110100548 | 68679954 | 74883697 | 1.6031 | 37.62% |
| synthetic | 5000 | 8484396 | 6106927 | 6745154 | 1.3893 | 28.02% |

![Raw vs compressed](../../graphs/hw5/raw_vs_compressed_size.png)

![Compression ratio](../../graphs/hw5/compression_ratio_by_corpus.png)

На Wikipedia сжатие получилось лучше, чем на synthetic. Это ожидаемо: в реальных текстах много повторяющихся слов и много позиционных gaps, которые хорошо ложатся на delta + PForDelta.

## Query processing

### AND

`AND` сначала сортирует children по estimated cost (`df`), чтобы начинать с самого короткого posting list-а. Дальше используется iterator + `Advance`.

### OR

`OR` сделан через min-heap по текущему `docId`. Это позволяет объединять posting list-ы без полной сортировки всех результатов.

### NOT

`NOT` не выполняется как самостоятельный запрос по всему universe. Поддерживается только форма с положительной частью:

```text
termA AND NOT termB
```

Это важно, потому что иначе пришлось бы строить огромный список всех документов.

### ADJ и NEAR/k

`ADJ` и `NEAR/k` сначала пересекают документы, а потом уже проверяют positions внутри совпавших документов.

- `ADJ` проверяет соседние позиции;
- `NEAR/k` проверяет расстояние между позициями не больше `k`.

## Набор запросов для benchmark

Изначально query suite получался плохим, потому что самые частотные термы Wikipedia - это `the`, `of`, `and`, `to`. Такие запросы дают огромные posting list-ы и плохо показывают работу поисковых операторов.

Поэтому я скорректировала набор:

- стоп-слова не используются как основные термы;
- сначала пробуются тематические запросы про женщин, феминизм, права, образование и науку;
- термы берутся из средней частоты, а не из самого верха словаря;
- для `ADJ`, `NEAR/3` и phrase берутся пары, реально встречающиеся рядом в Wikipedia;
- для каждого типа берется по 10 запросов;
- все запросы дают ненулевой результат.

Примеры запросов:

```text
women AND science
women NEAR/3 rights
(women OR feminism) AND rights
female AND scientists
feminist ADJ movement
"marie curie"
"ada lovelace"
"voting rights"
```

Всего в текущем `wiki_query_suite.json` — `100` запросов.

## На чем тестировалось

### Synthetic

Synthetic corpus используется как быстрый smoke:

- 1000, 5000, 10000 документов;
- длина документа 100-300 токенов;
- словарь около 10000 терминов;
- high-frequency и low-frequency термы;
- контролируемые фразы `data pipeline`, `new york`, `machine learning`, `distributed systems`.

Он нужен, чтобы стабильно проверять `ADJ` и `NEAR`, потому что в случайном тексте такие пары могут просто не встретиться.

### Wikipedia

Основной benchmark сделан на Wikipedia JSONL:

```json
{"id":1,"title":"Article title","text":"Article text"}
```

Скачивание делалось из официального Wikimedia dump:

```text
https://dumps.wikimedia.org/enwiki/latest/
```

Полный план был взять 5-8 GB, но локально удалось скачать около `1.4 GB` JSONL. Поэтому текущие результаты честно считаются на этом файле. Для самого benchmark-а в таблицах ниже используется `DOCS=5000`, то есть первые 5000 статей.

Характеристики корпуса:

| docs | total tokens | unique terms | avg doc len | min doc len | max doc len | total postings |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5000 | 17319971 | 313566 | 3463.99 | 28 | 21518 | 5102583 |

## Конфигурация замеров

Часть benchmark-ов снималась через Go benchmark:

```text
goos: darwin
goarch: amd64
cpu: Intel(R) Core(TM) i5-8279U CPU @ 2.40GHz
```

Основные команды:

```bash
cd hw5
make test
make bench-smoke
make wiki-stats DOCS=5000 WIKI=./data/wiki_sample.jsonl
make build-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl
make prepare-wiki-queries DOCS=5000 WIKI=./data/wiki_sample.jsonl
make compression-stats-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl
make bench-query-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl ITERATIONS=3
make bench-mmap-vs-memory DOCS=5000 WIKI=./data/wiki_sample.jsonl
make bench-ranking-wiki DOCS=5000 WIKI=./data/wiki_sample.jsonl
make graphs
```

## Построение индекса

| docs | unique terms | total postings | build time ms | segment write ms | total time ms | segment size |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5000 | 313566 | 5102583 | 11510.44 | 5858.80 | 17369.24 | 74883697 |

Построение индекса занимает примерно `17.4s`, из них около `5.9s` уходит на запись сжатого segment-а. Это нормально для текущей реализации, потому что во время записи заново кодируются docId gaps и positions.

![Build time](../../graphs/hw5/build_time_by_docs.png)

## Задержка запросов

В таблице ниже среднее время по 10 запросам каждого типа. Backend — `mmap`, segment открывается один раз, а posting list-ы для терминов запроса декодируются лениво.

| operator | queries | avg latency ms | min ms | max ms |
| --- | ---: | ---: | ---: | ---: |
| TERM | 10 | 1.266 | 0.701 | 2.206 |
| AND | 10 | 2.197 | 1.339 | 4.106 |
| NOT | 10 | 3.793 | 1.079 | 5.286 |
| OR | 10 | 4.957 | 1.467 | 7.870 |
| ADJ | 10 | 6.506 | 1.405 | 9.627 |
| NEAR/3 | 10 | 5.374 | 1.620 | 12.189 |
| PHRASE | 10 | 5.685 | 1.191 | 11.382 |
| COMPLEX | 10 | 6.998 | 1.702 | 10.635 |

![Query latency](../../graphs/hw5/query_latency_by_operator.png)

![QPS](../../graphs/hw5/qps_by_operator.png)

Самые дешевые запросы — одиночный term и простые boolean-пересечения. `OR`, `ADJ`, `NEAR/3` и phrase дороже, потому что им нужно либо объединять несколько posting list-ов, либо дополнительно проверять позиции.

## Memory index vs mmap segment

Для проверки я сравнила результаты на одних и тех же 100 запросах.

Все результаты совпали:

```text
results_equal = true для 100/100 запросов
```

Важно: memory index уже держит все posting list-ы в памяти, а mmap backend каждый раз лениво декодирует нужные posting list-ы из segment-а. Поэтому mmap ожидаемо медленнее, зато не требует держать весь postings section как готовые Go-структуры.

Примеры:

| query | memory ms | mmap ms | hits | equal |
| --- | ---: | ---: | ---: | --- |
| `women AND science` | 0.1503 | 2.3977 | 329 | true |
| `women NEAR/3 rights` | 0.1423 | 2.4697 | 62 | true |
| `(women OR feminism) AND rights` | 0.5700 | 2.9063 | 410 | true |
| `"ada lovelace"` | 0.0293 | 0.7420 | 15 | true |
| `"marie curie"` | 0.0137 | 0.8973 | 13 | true |

![Memory vs mmap](../../graphs/hw5/mmap_vs_memory_latency.png)

## Ранжирование

Для ранжирования я сравнила boolean-only, TF-IDF и BM25. TopK считается через min-heap, а не через сортировку всех результатов.

Средние значения по `30` запросам:

| mode | avg latency ms |
| --- | ---: |
| TF-IDF topK | 0.245 |
| BM25 topK | 0.486 |

BM25 дороже TF-IDF, потому что дополнительно учитывает длину документа и среднюю длину корпуса.

На тематических запросах в top-результатах появляются ожидаемые статьи: `A Vindication of the Rights of Woman`, `Ada Lovelace`, `Dava Sobel`, `Egalitarianism`, `Dianic Wicca`.

![Ranking latency](../../graphs/hw5/ranking_latency.png)

## Память и аллокации

Для query benchmark сохраняются `alloc_bytes_per_query` и `allocs_per_query`. По графику видно, что позиционные операции и сложные запросы создают больше временных структур, потому что им нужно декодировать positions и проверять расстояния между позициями.

![Allocs by operator](../../graphs/hw5/allocs_by_operator.png)

## Профилирование

Скрипты:

```bash
../scripts/hw5_profile_cpu.sh
../scripts/hw5_profile_mem.sh
```

Они сохраняют:

```text
reports/hw5/profiles/cpu_and.out
reports/hw5/profiles/cpu_or.out
reports/hw5/profiles/cpu_near.out
reports/hw5/profiles/cpu_bm25.out
reports/hw5/profiles/mem_query.out
```

Ожидаемые bottleneck-и:

- tokenizer и построение map-ов на этапе build;
- PForDelta encode/decode;
- декодирование positions;
- проверка positions для `ADJ` и `NEAR`;
- BM25 scoring;
- heap merge для `OR`.

## Где была сложность

Самая неприятная часть здесь — не сам `AND`, а сочетание трех вещей:

1. нужно сохранить positions, иначе `ADJ` и `NEAR` невозможны;
2. positions резко увеличивают размер postings;
3. после сжатия нужно уметь читать только нужный term из mmap segment-а.

Из-за этого пришлось отдельно считать offsets в dictionary, отдельно кодировать docIds/freqs/positions и отдельно проверять, что memory и mmap дают одинаковые docId.

Еще одна проблема была в benchmark-наборе. Если брать самые частотные слова Wikipedia, запросы становятся слишком шумными. Поэтому query suite теперь фильтрует стоп-слова и выбирает более нормальные термы средней частоты.

## Валидация корректности

Покрыто тестами:

- tokenizer;
- delta encode/decode;
- bitpack encode/decode;
- PForDelta encode/decode;
- index builder;
- `AND`;
- `OR`;
- `NOT`;
- `ADJ`;
- `NEAR/k`;
- TF-IDF;
- BM25;
- segment write/read;
- mmap lookup;
- query parser.

Проверка:

```bash
go test ./...
```

Большие wiki benchmark-и не запускаются в обычном `go test`, потому что требуют локальный `wiki_sample.jsonl`.

## Что еще можно улучшить

- добавить нормальный stop-word/stemming analyzer;
- кешировать декодированные posting list-ы для mmap backend;
- хранить skip offsets прямо внутрь compressed stream;
- сделать несколько segment-ов вместо одного;
- добавить snippets;
- отдельно сравнить холодный mmap и теплый mmap;
- догрузить Wikipedia до 5-8 GB и повторить те же замеры на 10000 и 25000 документах.

## Выводы

Получился рабочий позиционный inverted index: он строит координатные posting list-ы, поддерживает boolean и proximity-запросы, сохраняет индекс на диск, открывает его через mmap и сжимает postings.

На текущем Wikipedia subset сжатие уменьшило raw postings примерно на `37.62%`. Самые быстрые запросы — одиночный term и простые boolean-операции. Позиционные запросы дороже, потому что после пересечения документов нужно проверять positions. Mmap backend медленнее memory index, но при этом он читает posting list-ы из segment-а лениво и не держит весь postings section как готовые Go-структуры.

Главная доработка перед финальной защитой — повторить этот же пайплайн на большем wiki-файле, когда получится скачать 5-8 GB без сетевых обрывов.
