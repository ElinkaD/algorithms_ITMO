ARTIFACTS_DIR := artifacts
METRICS_DIR := $(ARTIFACTS_DIR)/metrics
RAW_DIR := $(METRICS_DIR)/raw
SUMMARY_DIR := $(METRICS_DIR)/summary
PROFILES_DIR := $(ARTIFACTS_DIR)/profiles
BENCH_RUNS ?= 5

.PHONY: test bench bench-save plot clean dirs flamegraph memprofile

test:
	env GOCACHE=/tmp/go-build-hw2 go test ./...

bench:
	env GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench . -benchmem

dirs:
	mkdir -p $(RAW_DIR) $(SUMMARY_DIR) $(PROFILES_DIR)

bench-save: dirs
	rm -f $(RAW_DIR)/*.txt $(RAW_DIR)/*_runs.tsv $(SUMMARY_DIR)/*.tsv
	for run in $$(seq 1 $(BENCH_RUNS)); do \
		echo "run $$run/$(BENCH_RUNS)"; \
		env GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench . -benchmem | tee $(RAW_DIR)/bench_run_$${run}.txt; \
		./scripts/bench_to_tsv.sh $$run $(RAW_DIR)/bench_run_$${run}.txt $(RAW_DIR); \
	done
	./scripts/aggregate_bench.sh $(RAW_DIR) $(SUMMARY_DIR)

plot: dirs
	../scripts/ensure_graphs_venv.sh
	mkdir -p ../.mplconfig ../graphs/hw2
	env MPLCONFIGDIR=$(CURDIR)/../.mplconfig ../.venv-graphs/bin/python ../scripts/visualize_hw2_benchmarks.py

flamegraph: dirs
	env GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench 'BenchmarkSearchNearby/size=100000/precision=5/radius=50000$$' -cpuprofile $(PROFILES_DIR)/search_nearby.cpu.prof
	mkdir -p ../graphs/hw2
	env GOCACHE=/tmp/go-build-hw2 go tool pprof -png -output ../graphs/hw2/search_nearby_flame.png $(PROFILES_DIR)/search_nearby.cpu.prof

memprofile: dirs
	env GOCACHE=/tmp/go-build-hw2 go test -run '^$$' -bench 'BenchmarkSearchNearby/size=100000/precision=5/radius=50000$$' -memprofile $(PROFILES_DIR)/search_nearby.mem.prof
	mkdir -p ../graphs/hw2
	env GOCACHE=/tmp/go-build-hw2 go tool pprof -png -output ../graphs/hw2/search_nearby_mem.png $(PROFILES_DIR)/search_nearby.mem.prof

clean:
	rm -rf $(ARTIFACTS_DIR)
