# HW1

Структура:

- `hw1/extendible/` — extendible hashing;
- `hw1/perfect/` — perfect hash;
- `hw1/lsh/` — lsh для точек в 3D.

Основные команды:

```bash
make test
make test-short
make bench-refresh ALGO=extendible
make bench ALGO=extendible
make collect ALGO=extendible
make metrics ALGO=extendible
make profile ALGO=extendible
```

Кратко:

- `ALGO=extendible|perfect|lsh`;
- `bench-refresh` пересчитывает benchmark и сохраняет `benchmarks.txt`;
- `bench` просто показывает уже готовый raw log;
- `collect` делает `benchmarks.csv` из готового raw log;
- `metrics` делает `collect` и `plot`;
- `profile` сохраняет `cpu.prof` и `mem.prof`.

