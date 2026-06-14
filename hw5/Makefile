DOCS ?= 50000
WIKI ?= ./data/wiki_sample.jsonl
INDEX ?= ./data/wiki_50k.seg
ITERATIONS ?= 5
TOPK ?= 10
QUERY ?= women AND science
WIKI_TOTAL_DOCS ?= 835456
WIKI_HALF_DOCS ?= 417728
SEGMENT_DOCS ?= 50000
SHARD_LIMIT ?= 0
QUERIES_PER_TYPE ?= 50
QUERY_SAMPLE_DOCS ?= 10000
SHARDS_INDEX ?= ./data/wiki_shards
WIKI_HALF_INDEX ?= ./data/wiki_shards/prefix-$(WIKI_HALF_DOCS)
PYTHON ?= .venv-graphs/bin/python

test:
	go test ./...

demo:
	go run ./cmd/searchdemo --mode demo

bench-smoke:
	go run ./cmd/searchdemo --mode bench-smoke --docs 5000

profile-cpu:
	mkdir -p ../reports/hw5/profiles
	cd .. && scripts/hw5_profile_cpu.sh

profile-mem:
	mkdir -p ../reports/hw5/profiles
	cd .. && scripts/hw5_profile_mem.sh

profile-interesting:
	mkdir -p ../reports/hw5/profiles
	cd .. && scripts/hw5_profile_interesting.sh

pprof-ui-cpu:
	cd .. && PPROF_HTTP=$${PPROF_HTTP:-:8080} scripts/hw5_pprof_ui.sh cpu $(PROFILE)

pprof-ui-mem:
	cd .. && PPROF_HTTP=$${PPROF_HTTP:-:8080} scripts/hw5_pprof_ui.sh mem $(PROFILE)

build-wiki-shards:
	go run ./cmd/searchdemo --mode build-wiki-shards --input $(WIKI) --limit $(DOCS) --index $(SHARDS_INDEX) --segment-docs $(SEGMENT_DOCS)

search:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query "$(QUERY)" --topK $(TOPK) --rank bm25

explain:
	printf ':explain $(QUERY)\n:exit\n' | go run ./cmd/searchdemo --mode repl --index $(INDEX)

search-women-science:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query "women AND science" --topK $(TOPK) --rank bm25

search-women-rights:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query "women NEAR/3 rights" --topK $(TOPK) --rank bm25

search-feminism:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query "(women OR feminism) AND rights" --topK $(TOPK) --rank bm25

search-scientists:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query "(women OR female) AND scientists" --topK $(TOPK) --rank bm25

search-marie-curie:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query '"marie curie"' --topK $(TOPK) --rank bm25

search-ada-lovelace:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query '"ada lovelace"' --topK $(TOPK) --rank bm25

search-theme:
	$(MAKE) search-women-science TOPK=$(TOPK) INDEX=$(INDEX)
	$(MAKE) search-women-rights TOPK=$(TOPK) INDEX=$(INDEX)
	$(MAKE) search-feminism TOPK=$(TOPK) INDEX=$(INDEX)
	$(MAKE) search-scientists TOPK=$(TOPK) INDEX=$(INDEX)
	$(MAKE) search-marie-curie TOPK=$(TOPK) INDEX=$(INDEX)
	$(MAKE) search-ada-lovelace TOPK=$(TOPK) INDEX=$(INDEX)

wiki-stats:
	go run ./cmd/searchdemo --mode wiki-stats --input $(WIKI) --limit $(DOCS)

wiki-report-scale-stats:
	go run ./cmd/searchdemo --mode wiki-report-scale-stats --input $(WIKI) --limit $(WIKI_TOTAL_DOCS) --half-limit $(WIKI_HALF_DOCS) --index $(SHARDS_INDEX) --half-index $(WIKI_HALF_INDEX) --segment-docs $(SEGMENT_DOCS)

prepare-wiki-queries:
	go run ./cmd/searchdemo --mode prepare-wiki-queries --input $(WIKI) --limit $(DOCS) --index $(INDEX) --queries-per-type $(QUERIES_PER_TYPE)

bench-build-wiki:
	go run ./cmd/searchdemo --mode bench-build-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX)

bench-query-wiki:
	go run ./cmd/searchdemo --mode bench-query-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX) --iterations $(ITERATIONS)

bench-query-wiki-shards:
	go run ./cmd/searchdemo --mode bench-query-wiki-shards --input $(WIKI) --limit $(DOCS) --index $(SHARDS_INDEX) --segment-docs $(SEGMENT_DOCS) --shard-limit $(SHARD_LIMIT) --queries-per-type $(QUERIES_PER_TYPE) --query-sample-docs $(QUERY_SAMPLE_DOCS) --iterations $(ITERATIONS)

bench-query-wiki-shards-one-reuse:
	$(MAKE) bench-query-wiki-shards DOCS=$(SEGMENT_DOCS) WIKI=$(WIKI) SEGMENT_DOCS=$(SEGMENT_DOCS) SHARD_LIMIT=1 QUERIES_PER_TYPE=$(QUERIES_PER_TYPE) QUERY_SAMPLE_DOCS=$(QUERY_SAMPLE_DOCS) ITERATIONS=$(ITERATIONS) SHARDS_INDEX=$(SHARDS_INDEX)

bench-query-wiki-shards-half-reuse:
	$(MAKE) bench-query-wiki-shards DOCS=$(WIKI_HALF_DOCS) WIKI=$(WIKI) SEGMENT_DOCS=$(SEGMENT_DOCS) QUERIES_PER_TYPE=$(QUERIES_PER_TYPE) QUERY_SAMPLE_DOCS=$(QUERY_SAMPLE_DOCS) ITERATIONS=$(ITERATIONS) SHARDS_INDEX=$(WIKI_HALF_INDEX)

build-wiki-shards-half-prefix:
	$(MAKE) build-wiki-shards DOCS=$(WIKI_HALF_DOCS) WIKI=$(WIKI) SEGMENT_DOCS=$(SEGMENT_DOCS) SHARDS_INDEX=$(WIKI_HALF_INDEX)

compression-stats-wiki:
	go run ./cmd/searchdemo --mode compression-stats-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX)

compression-stats-synthetic:
	go run ./cmd/searchdemo --mode compression-stats-synthetic --docs $(DOCS)

bench-ranking-wiki:
	go run ./cmd/searchdemo --mode bench-ranking-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX) --topK $(TOPK) --iterations $(ITERATIONS)

run-cli:
	go run ./cmd/searchdemo --mode repl --index ./data/index.seg

graphs:
	cd .. && $(PYTHON) scripts/visualize_hw5_benchmarks.py

clean-results:
	rm -f ../reports/hw5/results/*.csv ../reports/hw5/results/*.txt ../graphs/hw5/*.png
