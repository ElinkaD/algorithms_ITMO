DOCS ?= 5000
WIKI ?= ./data/wiki_sample.jsonl
INDEX ?= ./data/wiki.seg
ITERATIONS ?= 5
TOPK ?= 10
QUERY ?= women AND science

test:
	go test ./...

bench:
	go test ./... -bench=. -benchmem

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

build-wiki:
	go run ./cmd/searchdemo --mode build-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX)

search-wiki:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query "information AND retrieval" --topK $(TOPK) --rank bm25

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

search-suffrage:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query "women AND suffrage" --topK $(TOPK) --rank bm25

search-scientists:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query "(women OR female) AND scientists" --topK $(TOPK) --rank bm25

search-voting-rights:
	go run ./cmd/searchdemo --mode search --index $(INDEX) --query '"voting rights"' --topK $(TOPK) --rank bm25

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

prepare-wiki-queries:
	go run ./cmd/searchdemo --mode prepare-wiki-queries --input $(WIKI) --limit $(DOCS) --index $(INDEX)

bench-wiki:
	go run ./cmd/searchdemo --mode bench-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX) --iterations $(ITERATIONS) --topK $(TOPK)

bench-wiki-5k:
	$(MAKE) bench-wiki DOCS=5000 WIKI=$(WIKI) INDEX=$(INDEX) ITERATIONS=3 TOPK=$(TOPK)

bench-wiki-10k:
	$(MAKE) bench-wiki DOCS=10000 WIKI=$(WIKI) INDEX=$(INDEX) ITERATIONS=3 TOPK=$(TOPK)

bench-wiki-25k:
	$(MAKE) bench-wiki DOCS=25000 WIKI=$(WIKI) INDEX=$(INDEX) ITERATIONS=3 TOPK=$(TOPK)

report-wiki-5k:
	$(MAKE) bench-wiki-5k WIKI=$(WIKI) INDEX=$(INDEX) TOPK=$(TOPK)
	$(MAKE) graphs

report-wiki-10k:
	$(MAKE) bench-wiki-10k WIKI=$(WIKI) INDEX=$(INDEX) TOPK=$(TOPK)
	$(MAKE) graphs

bench-build-wiki:
	go run ./cmd/searchdemo --mode bench-build-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX)

bench-query-wiki:
	go run ./cmd/searchdemo --mode bench-query-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX) --iterations $(ITERATIONS)

compression-stats-wiki:
	go run ./cmd/searchdemo --mode compression-stats-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX)

compression-stats-synthetic:
	go run ./cmd/searchdemo --mode compression-stats-synthetic --docs $(DOCS)

bench-mmap-vs-memory:
	go run ./cmd/searchdemo --mode bench-mmap-vs-memory --input $(WIKI) --limit $(DOCS) --index $(INDEX)

bench-ranking-wiki:
	go run ./cmd/searchdemo --mode bench-ranking-wiki --input $(WIKI) --limit $(DOCS) --index $(INDEX) --topK $(TOPK)

run-cli:
	go run ./cmd/searchdemo --mode repl --index ./data/index.seg

run-cli-wiki:
	go run ./cmd/searchdemo --mode repl --index $(INDEX)

graphs:
	cd .. && python3 scripts/visualize_hw5_benchmarks.py

clean-results:
	rm -f ../reports/hw5/results/*.csv ../reports/hw5/results/*.txt ../graphs/hw5/*.png
