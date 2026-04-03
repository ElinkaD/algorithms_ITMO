GO ?= go
GOCACHE ?= /tmp/go-cache
ALGO ?= extendible
PKG ?= ./$(ALGO)

ifeq ($(ALGO),extendible)
SIZES ?= 10000,50000,100000
WARMUP_SIZES ?= 10000,50000
else ifeq ($(ALGO),perfect)
SIZES ?= 10000,100000,500000,1000000
WARMUP_SIZES ?= 10000,100000
else ifeq ($(ALGO),lsh)
SIZES ?= 10000,100000,500000,1000000
WARMUP_SIZES ?= 10000,100000
else
SIZES ?= 10000,50000,100000
WARMUP_SIZES ?= 10000,50000
endif

BENCH_RUNS ?= 4
BENCH_TIME ?= 3x
PROFILE_SIZE ?= 100000
GOMAXPROCS ?= 2
WARMUP_TIME ?= 1x
CPU_PPROF_HTTP ?= :8089
MEM_PPROF_HTTP ?= :8090

ARTIFACTS_DIR := $(ALGO)/artifacts
METRICS_DIR := $(ARTIFACTS_DIR)/metrics
RAW_DIR := $(METRICS_DIR)/raw
SUMMARY_DIR := $(METRICS_DIR)/summary
PLOT_DIR := ../graphs/hw1/$(ALGO)
PROFILE_DIR := $(ARTIFACTS_DIR)/profiles
TESTBIN := $(PROFILE_DIR)/$(ALGO).test

CPU_PROFILE := $(PROFILE_DIR)/$(ALGO)_$(PROFILE_SIZE).cpu.prof
MEM_PROFILE := $(PROFILE_DIR)/$(ALGO)_$(PROFILE_SIZE).mem.prof
CPU_PROFILE_PNG := $(PLOT_DIR)/$(ALGO)_$(PROFILE_SIZE)_cpu.png
MEM_PROFILE_PNG := $(PLOT_DIR)/$(ALGO)_$(PROFILE_SIZE)_mem.png
CPU_FRAMEGRAPH_OUT := $(PLOT_DIR)/$(ALGO)_$(PROFILE_SIZE)_cpu_framegraph.svg
MEM_FRAMEGRAPH_OUT := $(PLOT_DIR)/$(ALGO)_$(PROFILE_SIZE)_mem_framegraph.svg

ifeq ($(ALGO),perfect)
PROFILE_BENCH ?= ^BenchmarkTableBuild$$
else ifeq ($(ALGO),lsh)
PROFILE_BENCH ?= ^BenchmarkTableSearch$$
else
PROFILE_BENCH ?= ^BenchmarkTableInsert$$
endif

.PHONY: test test-short bench warmup bench-save collect plot metrics profile cpuprofile memprofile testbin profile-web cpu-web mem-web framegraph-paths clean dirs

test:
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) $(GO) test ./...

test-short:
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) $(GO) test -short ./...

dirs:
	mkdir -p $(RAW_DIR) $(SUMMARY_DIR) $(PROFILE_DIR) $(PLOT_DIR)

bench:
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) SIZES='$(SIZES)' $(GO) test $(PKG) -run '^$$' -bench '^BenchmarkTable' -benchmem -benchtime $(BENCH_TIME)

warmup:
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) SIZES='$(WARMUP_SIZES)' $(GO) test $(PKG) -run '^$$' -bench '^BenchmarkTable' -benchmem -benchtime $(WARMUP_TIME) > /dev/null

bench-save: dirs
	rm -f $(RAW_DIR)/*.txt $(RAW_DIR)/*_runs.tsv $(SUMMARY_DIR)/*.tsv
	$(MAKE) warmup ALGO=$(ALGO) WARMUP_SIZES='$(WARMUP_SIZES)' GOMAXPROCS='$(GOMAXPROCS)' GOCACHE='$(GOCACHE)' WARMUP_TIME='$(WARMUP_TIME)'
	for run in $$(seq 1 $(BENCH_RUNS)); do \
		echo "run $$run/$(BENCH_RUNS)"; \
		env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) SIZES='$(SIZES)' $(GO) test $(PKG) -run '^$$' -bench '^BenchmarkTable' -benchmem -benchtime $(BENCH_TIME) | tee $(RAW_DIR)/bench_run_$${run}.txt; \
		../scripts/hw1_bench_to_tsv.sh $(ALGO) $$run $(RAW_DIR)/bench_run_$${run}.txt $(RAW_DIR); \
	done
	../scripts/hw1_aggregate_bench.sh $(RAW_DIR) $(SUMMARY_DIR)

collect:
	../scripts/hw1_aggregate_bench.sh $(RAW_DIR) $(SUMMARY_DIR)

plot: dirs
	../scripts/ensure_graphs_venv.sh
	mkdir -p ../.mplconfig $(PLOT_DIR)
	env MPLCONFIGDIR=$(CURDIR)/../.mplconfig ALGO=$(ALGO) HW1_SUMMARY_DIR=$(CURDIR)/$(SUMMARY_DIR) HW1_RAW_DIR=$(CURDIR)/$(RAW_DIR) HW1_OUT_DIR=$(CURDIR)/$(PLOT_DIR) ../.venv-graphs/bin/python ../scripts/visualize_hw1_benchmarks.py

metrics: bench-save plot

profile: cpuprofile memprofile

cpuprofile: testbin
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) SIZES='$(PROFILE_SIZE)' $(GO) test $(PKG) -run '^$$' -bench '$(PROFILE_BENCH)' -benchmem -benchtime $(BENCH_TIME) -count=1 -cpuprofile $(CPU_PROFILE)
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) $(GO) tool pprof -png -output $(CPU_PROFILE_PNG) $(TESTBIN) $(CPU_PROFILE)

memprofile: testbin
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) SIZES='$(PROFILE_SIZE)' $(GO) test $(PKG) -run '^$$' -bench '$(PROFILE_BENCH)' -benchmem -benchtime $(BENCH_TIME) -count=1 -memprofile $(MEM_PROFILE)
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) $(GO) tool pprof -png -output $(MEM_PROFILE_PNG) $(TESTBIN) $(MEM_PROFILE)

testbin: dirs
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) $(GO) test -c -o $(TESTBIN) $(PKG)

profile-web: cpu-web

cpu-web: testbin cpuprofile
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) $(GO) tool pprof -http=$(CPU_PPROF_HTTP) $(TESTBIN) $(CPU_PROFILE)

mem-web: testbin memprofile
	env GOMAXPROCS=$(GOMAXPROCS) GOCACHE=$(GOCACHE) $(GO) tool pprof -http=$(MEM_PPROF_HTTP) $(TESTBIN) $(MEM_PROFILE)

framegraph-paths: dirs
	@printf 'cpu profile: %s\n' "$(CPU_PROFILE)"
	@printf 'mem profile: %s\n' "$(MEM_PROFILE)"
	@printf 'cpu framegraph target: %s\n' "$(CPU_FRAMEGRAPH_OUT)"
	@printf 'mem framegraph target: %s\n' "$(MEM_FRAMEGRAPH_OUT)"

clean:
	rm -rf extendible/artifacts perfect/artifacts lsh/artifacts ../graphs/hw1
