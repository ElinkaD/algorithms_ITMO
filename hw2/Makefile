ARTIFACTS_DIR := artifacts
METRICS_DIR := $(ARTIFACTS_DIR)/metrics
RAW_DIR := $(METRICS_DIR)/raw
SUMMARY_DIR := $(METRICS_DIR)/summary
PROFILES_DIR := $(ARTIFACTS_DIR)/profiles
GRAPHS_DIR := ../graphs/hw2
BENCH_RUNS ?= 4
PROFILE_BENCH ?= BenchmarkInsert/size=1000000$$
EXACT_PROFILE_BENCH ?= BenchmarkSearchExact/size=1000000$$
GOMAXPROCS ?= 2
CPU_PPROF_HTTP ?= :12312
MEM_PPROF_HTTP ?= :12311
EXACT_CPU_PPROF_HTTP ?= :12314
EXACT_MEM_PPROF_HTTP ?= :12313

.PHONY: test bench bench-save plot clean dirs cpuprofile memprofile cpu-framegraph mem-framegraph exact-cpuprofile exact-memprofile exact-cpu-framegraph exact-mem-framegraph

test:
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test ./...

bench:
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench . -benchmem

dirs:
	mkdir -p $(RAW_DIR) $(SUMMARY_DIR) $(PROFILES_DIR)

bench-save: dirs
	rm -f $(RAW_DIR)/*.txt $(RAW_DIR)/*_runs.tsv $(SUMMARY_DIR)/*.tsv
	for run in $$(seq 1 $(BENCH_RUNS)); do \
		echo "run $$run/$(BENCH_RUNS)"; \
		env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench . -benchmem | tee $(RAW_DIR)/bench_run_$${run}.txt; \
		./scripts/bench_to_tsv.sh $$run $(RAW_DIR)/bench_run_$${run}.txt $(RAW_DIR); \
	done
	./scripts/aggregate_bench.sh $(RAW_DIR) $(SUMMARY_DIR)

plot: dirs
	../scripts/ensure_graphs_venv.sh
	mkdir -p ../.mplconfig $(GRAPHS_DIR)
	env MPLCONFIGDIR=$(CURDIR)/../.mplconfig ../.venv-graphs/bin/python ../scripts/visualize_hw2_benchmarks.py

cpuprofile: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench '$(PROFILE_BENCH)' -cpuprofile $(PROFILES_DIR)/insert.cpu.prof
	mkdir -p $(GRAPHS_DIR)
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go tool pprof -png -output $(GRAPHS_DIR)/insert_cpu.png $(PROFILES_DIR)/insert.cpu.prof

memprofile: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench '$(PROFILE_BENCH)' -memprofile $(PROFILES_DIR)/insert.mem.prof
	mkdir -p $(GRAPHS_DIR)
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go tool pprof -png -output $(GRAPHS_DIR)/insert_mem.png $(PROFILES_DIR)/insert.mem.prof

cpu-framegraph: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench '$(PROFILE_BENCH)' -cpuprofile $(PROFILES_DIR)/insert.cpu.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go tool pprof -http=$(CPU_PPROF_HTTP) $(PROFILES_DIR)/insert.cpu.prof

mem-framegraph: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench '$(PROFILE_BENCH)' -memprofile $(PROFILES_DIR)/insert.mem.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go tool pprof -http=$(MEM_PPROF_HTTP) $(PROFILES_DIR)/insert.mem.prof

exact-cpuprofile: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench '$(EXACT_PROFILE_BENCH)' -cpuprofile $(PROFILES_DIR)/search_exact.cpu.prof
	mkdir -p $(GRAPHS_DIR)
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go tool pprof -png -output $(GRAPHS_DIR)/search_exact_cpu.png $(PROFILES_DIR)/search_exact.cpu.prof

exact-memprofile: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench '$(EXACT_PROFILE_BENCH)' -memprofile $(PROFILES_DIR)/search_exact.mem.prof
	mkdir -p $(GRAPHS_DIR)
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go tool pprof -png -output $(GRAPHS_DIR)/search_exact_mem.png $(PROFILES_DIR)/search_exact.mem.prof

exact-cpu-framegraph: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench '$(EXACT_PROFILE_BENCH)' -cpuprofile $(PROFILES_DIR)/search_exact.cpu.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go tool pprof -http=$(EXACT_CPU_PPROF_HTTP) $(PROFILES_DIR)/search_exact.cpu.prof

exact-mem-framegraph: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench '$(EXACT_PROFILE_BENCH)' -memprofile $(PROFILES_DIR)/search_exact.mem.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=/tmp/go-build-hw2 go tool pprof -http=$(EXACT_MEM_PPROF_HTTP) $(PROFILES_DIR)/search_exact.mem.prof

clean:
	rm -rf $(ARTIFACTS_DIR)
