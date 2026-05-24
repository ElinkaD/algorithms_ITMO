GO ?= go
GOCACHE ?= /tmp/go-build-hw4
GOMODCACHE ?= /tmp/go-mod-cache
GOMAXPROCS ?= 4
ARTIFACTS_DIR := artifacts
METRICS_DIR := $(ARTIFACTS_DIR)/metrics
RAW_DIR := $(METRICS_DIR)/raw
SUMMARY_DIR := $(METRICS_DIR)/summary
PROFILES_DIR := $(ARTIFACTS_DIR)/profiles
GRAPHS_DIR := ../graphs/hw4
BENCH_RUNS ?= 4
PUT_PROFILE_BENCH ?= ^BenchmarkMapPut/concurrent/size=100000$$
GET_PROFILE_BENCH ?= ^BenchmarkMapGetHit/concurrent/size=100000$$
PUT_CPU_PPROF_HTTP ?= :12331
PUT_MEM_PPROF_HTTP ?= :12332
GET_CPU_PPROF_HTTP ?= :12333
GET_MEM_PPROF_HTTP ?= :12334

.PHONY: test test-race bench dirs bench-save plot cpuprofile memprofile cpu-framegraph mem-framegraph get-cpuprofile get-memprofile get-cpu-framegraph get-mem-framegraph clean

test:
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test ./...

test-race:
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -race ./...

bench:
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench . -benchmem

dirs:
	mkdir -p $(RAW_DIR) $(SUMMARY_DIR) $(PROFILES_DIR) $(GRAPHS_DIR)

bench-save: dirs
	../scripts/ensure_graphs_venv.sh
	rm -f $(RAW_DIR)/*.txt $(RAW_DIR)/*_runs.tsv $(SUMMARY_DIR)/*.tsv
	for run in $$(seq 1 $(BENCH_RUNS)); do \
		echo "run $$run/$(BENCH_RUNS)"; \
		env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench . -benchmem | tee $(RAW_DIR)/bench_run_$${run}.txt; \
		./scripts/bench_to_tsv.sh $$run $(RAW_DIR)/bench_run_$${run}.txt $(RAW_DIR); \
	done
	../.venv-graphs/bin/python ./scripts/aggregate_bench.py $(RAW_DIR) $(SUMMARY_DIR)

plot: dirs
	../scripts/ensure_graphs_venv.sh
	mkdir -p ../.mplconfig $(GRAPHS_DIR)
	env MPLCONFIGDIR=$(CURDIR)/../.mplconfig ../.venv-graphs/bin/python ../scripts/visualize_hw4_benchmarks.py

cpuprofile: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench '$(PUT_PROFILE_BENCH)' -cpuprofile $(PROFILES_DIR)/put.cpu.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) tool pprof -png -output $(GRAPHS_DIR)/put_cpu.png $(PROFILES_DIR)/put.cpu.prof

memprofile: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench '$(PUT_PROFILE_BENCH)' -memprofile $(PROFILES_DIR)/put.mem.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) tool pprof -png -output $(GRAPHS_DIR)/put_mem.png $(PROFILES_DIR)/put.mem.prof

cpu-framegraph: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench '$(PUT_PROFILE_BENCH)' -cpuprofile $(PROFILES_DIR)/put.cpu.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) tool pprof -http=$(PUT_CPU_PPROF_HTTP) $(PROFILES_DIR)/put.cpu.prof

mem-framegraph: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench '$(PUT_PROFILE_BENCH)' -memprofile $(PROFILES_DIR)/put.mem.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) tool pprof -http=$(PUT_MEM_PPROF_HTTP) $(PROFILES_DIR)/put.mem.prof

get-cpuprofile: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench '$(GET_PROFILE_BENCH)' -cpuprofile $(PROFILES_DIR)/get.cpu.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) tool pprof -png -output $(GRAPHS_DIR)/get_cpu.png $(PROFILES_DIR)/get.cpu.prof

get-memprofile: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench '$(GET_PROFILE_BENCH)' -memprofile $(PROFILES_DIR)/get.mem.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) tool pprof -png -output $(GRAPHS_DIR)/get_mem.png $(PROFILES_DIR)/get.mem.prof

get-cpu-framegraph: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench '$(GET_PROFILE_BENCH)' -cpuprofile $(PROFILES_DIR)/get.cpu.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) tool pprof -http=$(GET_CPU_PPROF_HTTP) $(PROFILES_DIR)/get.cpu.prof

get-mem-framegraph: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) test -run '^$$' -bench '$(GET_PROFILE_BENCH)' -memprofile $(PROFILES_DIR)/get.mem.prof
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) $(GO) tool pprof -http=$(GET_MEM_PPROF_HTTP) $(PROFILES_DIR)/get.mem.prof

clean:
	rm -rf $(ARTIFACTS_DIR)
