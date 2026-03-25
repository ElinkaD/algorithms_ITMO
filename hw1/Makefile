GO ?= go
GOCACHE ?= /tmp/go-cache
ALGO ?= extendible
PKG ?= ./$(ALGO)

SIZES ?= 1000,10000,50000,100000
METRICS_OUT ?= $(ALGO)/artifacts/metrics
BENCH_FILTER ?= ^BenchmarkTable
BENCH_TIME ?= 3x
PROFILE_SIZE ?= 10000
RAW_DIR := $(METRICS_OUT)/raw
PLOT_DIR := $(METRICS_OUT)/plots
PROFILE_DIR := $(ALGO)/artifacts/profiles
TESTBIN := $(PROFILE_DIR)/$(ALGO).test
PPROF ?= go tool pprof
RAW_BENCH := $(RAW_DIR)/benchmarks.txt
CSV_BENCH := $(RAW_DIR)/benchmarks.csv

ifeq ($(ALGO),perfect)
PROFILE_BENCH ?= ^BenchmarkTableBuild$$
else ifeq ($(ALGO),lsh)
PROFILE_BENCH ?= ^BenchmarkTableSearch$$
else
PROFILE_BENCH ?= ^BenchmarkTableInsert$$
endif

.PHONY: test test-short bench bench-refresh collect metrics plot profile testbin profile-web profile-png clean

test:
	GOCACHE=$(GOCACHE) $(GO) test ./...

test-short:
	GOCACHE=$(GOCACHE) $(GO) test -short ./...

bench: $(RAW_BENCH)
	cat $(RAW_BENCH)

bench-refresh:
	rm -f $(RAW_BENCH) $(CSV_BENCH)
	$(MAKE) bench ALGO=$(ALGO) SIZES='$(SIZES)' BENCH_TIME='$(BENCH_TIME)' PROFILE_SIZE='$(PROFILE_SIZE)'

$(RAW_BENCH):
	mkdir -p $(RAW_DIR) $(PLOT_DIR)
	GOCACHE=$(GOCACHE) SIZES='$(SIZES)' $(GO) test $(PKG) -run '^$$' -bench '$(BENCH_FILTER)' -benchmem -benchtime $(BENCH_TIME) > $(RAW_BENCH)

collect:
	test -f $(RAW_BENCH) || (echo "run 'make bench-refresh ALGO=$(ALGO)' first"; exit 1)
	awk 'BEGIN { OFS=","; print "operation,size,iterations,ns_per_op,ns_per_item,ops_per_sec,bytes_per_op,allocs_per_op" } \
	/^BenchmarkTable[A-Za-z0-9]+\/size=[0-9]+-[0-9]+/ { \
		name=$$1; iters=$$2; \
		split(name, a, "/"); split(a[1], b, "BenchmarkTable"); op=tolower(b[2]); \
		split(a[2], c, "="); split(c[2], d, "-"); size=d[1]; \
		delete m; \
		for (i=3; i+1<=NF; i+=2) m[$$(i+1)]=$$i; \
		nsop=("ns/op" in m)?m["ns/op"]:0; \
		nsitem=("ns/item" in m)?m["ns/item"]:0; \
		ops=("ops/s" in m)?m["ops/s"]:0; \
		bop=("B/op" in m)?m["B/op"]:0; \
		alloc=("allocs/op" in m)?m["allocs/op"]:0; \
		print op,size,iters,nsop,nsitem,ops,bop,alloc; \
	}' $(RAW_BENCH) > $(CSV_BENCH)

metrics: collect plot

plot:
	test -f $(CSV_BENCH) || (echo "run 'make collect ALGO=$(ALGO)' first"; exit 1)
	mkdir -p $(PLOT_DIR)
	gnuplot -e "raw_dir='$(RAW_DIR)'; plot_dir='$(PLOT_DIR)'" $(ALGO)/plot_metrics.gnuplot

profile:
	mkdir -p $(PROFILE_DIR)
	GOCACHE=$(GOCACHE) SIZES='$(PROFILE_SIZE)' $(GO) test $(PKG) -run '^$$' -bench '$(PROFILE_BENCH)' -benchmem -benchtime $(BENCH_TIME) -count=1 -cpuprofile=$(PROFILE_DIR)/$(ALGO)_$(PROFILE_SIZE).cpu.prof -memprofile=$(PROFILE_DIR)/$(ALGO)_$(PROFILE_SIZE).mem.prof

testbin:
	mkdir -p $(PROFILE_DIR)
	GOCACHE=$(GOCACHE) $(GO) test -c -o $(TESTBIN) $(PKG)

profile-web: testbin
	$(PPROF) -http=:8089 $(TESTBIN) $(PROFILE_DIR)/$(ALGO)_$(PROFILE_SIZE).cpu.prof

profile-png: testbin
	$(PPROF) -png $(TESTBIN) $(PROFILE_DIR)/$(ALGO)_$(PROFILE_SIZE).cpu.prof > $(PROFILE_DIR)/$(ALGO)_$(PROFILE_SIZE).cpu.png

clean:
	rm -rf extendible/artifacts perfect/artifacts lsh/artifacts
