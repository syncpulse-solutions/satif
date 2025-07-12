.PHONY: help install test coverage lint format typecheck build clean docs

# Default target executed when no arguments are given to make.
help:
	@echo "Makefile for satif-sdk library"
	@echo ""
	@echo "Usage:"
	@echo "  make install    Install dependencies using Poetry"
	@echo "  make test       Run tests using pytest"
	@echo "  make coverage   Run tests and generate coverage report"
	@echo "  make lint       Lint code using Ruff"
	@echo "  make format     Format code using Ruff"
	@echo "  make typecheck  Run static type checking using Mypy"
	@echo "  make build      Build the distributable package using Poetry"
	@echo "  make docs       Generate API documentation and copy to Docusaurus"
	@echo "  make clean      Remove build artifacts and cache directories"
	@echo ""

# Install dependencies
install:
	poetry install

# Run tests
test:
	poetry run pytest

# Run tests and generate coverage report
coverage:
	poetry run pytest --cov=satif_sdk --cov-report=term-missing --cov-report=html

# Lint code
lint:
	poetry run ruff check .
	poetry run mypy satif_sdk tests

# Format code
format:
	poetry run ruff format .

# Build package
build:
	poetry build

# Generate API documentation and copy to Docusaurus
docs:
	@echo "🔨 Building Sphinx documentation in Markdown format..."
	cd docs && poetry run make markdown
	@echo "📋 Copying documentation to Docusaurus structure..."
	poetry run python scripts/copy_docs_to_docusaurus.py
	@echo "✅ Documentation pipeline completed!"

# Clean build artifacts and cache
clean:
	@echo "Cleaning up..."
	@rm -rf dist/
	@rm -rf .pytest_cache/
	@rm -rf htmlcov/
	@rm -rf docs/build/
	@rm -rf .coverage
	@find . -name '*.pyc' -exec rm -f {} +
	@find . -name '*.pyo' -exec rm -f {} +
	@find . -name '__pycache__' -exec rm -rf {} +
	@echo "Cleaned."

# Alias for convenience
cov: coverage
check: format lint test
