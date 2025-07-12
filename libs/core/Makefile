.PHONY: help install test coverage lint format typecheck build clean

# Default target executed when no arguments are given to make.
help:
	@echo "Makefile for satif-core library"
	@echo ""
	@echo "Usage:"
	@echo "  make install    Install dependencies using Poetry"
	@echo "  make test       Run tests using pytest"
	@echo "  make coverage   Run tests and generate coverage report"
	@echo "  make lint       Lint code using Ruff"
	@echo "  make format     Format code using Ruff"
	@echo "  make typecheck  Run static type checking using Mypy"
	@echo "  make build      Build the distributable package using Poetry"
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
	poetry run pytest --cov=satif_core --cov-report=term-missing --cov-report=html

# Lint code
lint:
	poetry run ruff check .
	poetry run mypy satif_core tests

# Format code
format:
	poetry run ruff format .

# Build package
build:
	poetry build

# Clean build artifacts and cache
clean:
	@echo "Cleaning up..."
	@rm -rf dist/
	@rm -rf .pytest_cache/
	@rm -rf htmlcov/
	@rm -rf .coverage
	@find . -name '*.pyc' -exec rm -f {} +
	@find . -name '*.pyo' -exec rm -f {} +
	@find . -name '__pycache__' -exec rm -rf {} +
	@echo "Cleaned."

# Alias for convenience
cov: coverage
check: format lint test
