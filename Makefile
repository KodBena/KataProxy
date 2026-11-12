.PHONY: test-stencil-corpus-integration

# Starts an isolated temporary Redis server, executes the real-Redis
# integration tests and benchmark, and writes reports under .artifacts/.
test-stencil-corpus-integration:
	python scripts/run_stencil_corpus_integration.py
