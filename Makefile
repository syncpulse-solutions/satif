.PHONY: setup format lint test clean help release-sdif release-mcp release-core release-sdk release-ai generate-api-docs

.DEFAULT_GOAL := help

.ONESHELL:
SHELL := /bin/bash

# Colors for terminal output
BOLD := $(shell tput bold)
GREEN := $(shell tput setaf 2)
YELLOW := $(shell tput setaf 3)
RESET := $(shell tput sgr0)
RED := $(shell tput setaf 1)

# Default branch to check against
MAIN_BRANCH ?= main

# Library directory names
SDIF_LIB := sdif
MCP_LIB := mcp
CORE_LIB := core
SDK_LIB := sdk
AI_LIB := ai
XLSX_LIB := xlsx-to-sdif

help: ## Show this help message
	@echo "$(BOLD)Synthetic Data Generator Makefile$(RESET)"
	@echo ""
	@echo "$(BOLD)Usage:$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(RESET) %s\n", $$1, $$2}'

setup: ## Set up the development environment
	@echo "$(BOLD)$(GREEN)Setting up development environment...$(RESET)"
	poetry install
	poetry run pre-commit install
	@echo "$(BOLD)$(GREEN)Setup complete!$(RESET)"

format: ## Format code using ruff
	@echo "$(BOLD)$(GREEN)Formatting code...$(RESET)"
	poetry run ruff format src tests
	@echo "$(BOLD)$(GREEN)Formatting complete!$(RESET)"

lint: ## Lint code using ruff
	@echo "$(BOLD)$(GREEN)Linting code...$(RESET)"
	poetry run ruff check src tests
	poetry run mypy src
	@echo "$(BOLD)$(GREEN)Linting complete!$(RESET)"

test: ## Run tests
	@echo "$(BOLD)$(GREEN)Running tests...$(RESET)"
	poetry run pytest -v
	@echo "$(BOLD)$(GREEN)Tests complete!$(RESET)"

clean: ## Clean up temporary files
	@echo "$(BOLD)$(GREEN)Cleaning up...$(RESET)"
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf .mypy_cache
	rm -rf __pycache__
	rm -rf src/__pycache__
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	@echo "$(BOLD)$(GREEN)Cleanup complete!$(RESET)"

# --- Release Targets ---

release-sdif: ## Bump version, commit, tag, and push 'sdif' lib (Set BUMP=patch/minor/...)
	@echo "$(YELLOW)Triggering release script for library: $(SDIF_LIB) with bump: $(BUMP)$(RESET)"
	@scripts/release.sh $(SDIF_LIB) $(BUMP)

release-mcp: ## Bump version, commit, tag, and push 'mcp' lib (Set BUMP=patch/minor/...)
	@echo "$(YELLOW)Triggering release script for library: $(MCP_LIB) with bump: $(BUMP)$(RESET)"
	@scripts/release.sh $(MCP_LIB) $(BUMP)

release-core: ## Bump version, commit, tag, and push 'core' lib (Set BUMP=patch/minor/...)
	@echo "$(YELLOW)Triggering release script for library: $(CORE_LIB) with bump: $(BUMP)$(RESET)"
	@scripts/release.sh $(CORE_LIB) $(BUMP)

release-sdk: ## Bump version, commit, tag, and push 'sdk' lib (Set BUMP=patch/minor/...)
	@echo "$(YELLOW)Triggering release script for library: $(SDK_LIB) with bump: $(BUMP)$(RESET)"
	@scripts/release.sh $(SDK_LIB) $(BUMP)

release-ai: ## Bump version, commit, tag, and push 'ai' lib (Set BUMP=patch/minor/...)
	@echo "$(YELLOW)Triggering release script for library: $(AI_LIB) with bump: $(BUMP)$(RESET)"
	@scripts/release.sh $(AI_LIB) $(BUMP)

release-xlsx: ## Bump version, commit, tag, and push 'xlsx-to-sdif' lib (Set BUMP=patch/minor/...)
	@echo "$(YELLOW)Triggering release script for library: $(XLSX_LIB) with bump: $(BUMP)$(RESET)"
	@scripts/release.sh $(XLSX_LIB) $(BUMP)

# --- Documentation Targets ---

## Generate Markdown API reference (requires `pydoc-markdown`)
# Usage: `make generate-api-docs`
# It will read the configuration in pydoc-markdown.yml and write docs under
# satif/docs/docs/api_reference.

generate-api-docs: ## Generate API reference docs with pydoc-markdown
	@echo "$(BOLD)$(GREEN)Generating API reference documentation...$(RESET)"
	pydoc-markdown
	@echo "$(BOLD)$(GREEN)API reference documentation generated!$(RESET)"

.PHONY: git-count-lines
git-count-lines:          ## Count lines of code.
	git ls-files '*.py' | grep -v 'test_' | xargs wc -l
