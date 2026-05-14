# HW1

Структура:

- `hw1/extendible/` — extendible hashing;
- `hw1/perfect/` — perfect hash;
- `hw1/lsh/` — lsh для точек в 3D.
- `hw1/<algo>/artifacts/metrics/{raw,summary}/` — benchmark-артефакты;
- `hw1/<algo>/artifacts/profiles/` — `cpu/mem` профили;
- `graphs/hw1/<algo>/` — графики для отчета;
- `reports/hw1/report_hashing.md` — отчет по первой лабораторной.

Основные команды:

```bash
make test
make test-short
make bench ALGO=extendible
make warmup ALGO=extendible
make bench-save ALGO=extendible
make collect ALGO=extendible
make metrics ALGO=extendible
make profile ALGO=extendible
make cpu-web ALGO=extendible
make mem-web ALGO=extendible
make framegraph-paths ALGO=extendible
```

Кратко:

- `ALGO=extendible|perfect|lsh`;
- `warmup` делает разогрев benchmark-кода перед серией финальных прогонов;
- `bench` запускает benchmark'и без сохранения артефактов;
- `bench-save` сначала делает `warmup`, потом несколько прогонов и сохраняет raw-логи и `*_runs.tsv`;
- `collect` агрегирует raw-таблицы в summary с `mean` и `95% CI`;
- `plot` строит графики в `graphs/hw1/<algo>/`;
- `metrics` делает полный цикл `bench-save + plot`;
- `profile` сохраняет `cpu.prof`, `mem.prof` и `png`-визуализации;
- `cpu-web` поднимает web UI `pprof` для CPU profile;
- `mem-web` поднимает web UI `pprof` для memory profile;
- `framegraph-paths` печатает пути к профилям и будущим framegraph-файлам.

Параметры финального прогона:

- `extendible`: `SIZES=10000,50000,100000`, `WARMUP_SIZES=10000,50000`;
- `perfect`: `SIZES=10000,100000,500000,1000000`, `WARMUP_SIZES=10000,100000`;
- `lsh`: `SIZES=10000,100000,500000,1000000`, `WARMUP_SIZES=10000,100000`;
- `BENCH_RUNS=5`;
- `BENCH_TIME=3x`;
- `WARMUP_TIME=1x`;
- `PROFILE_SIZE=100000`.
