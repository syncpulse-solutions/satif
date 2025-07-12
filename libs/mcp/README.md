# SDIF MCP

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![Status: Experimental](https://img.shields.io/badge/Status-Experimental-orange.svg)](https://github.com/syncpulse-solutions/satif)
d
MCP server implementation for working with SDIF (SQL Data Interoperable Format) files.

## Overview

`sdif_mcp` provides a [Model Context Protocol](https://modelcontextprotocol.io/) server implementation to enable AI agents to easily interact with and manipulate SDIF files.

> **Note:** This library is highly experimental. The API may change significantly in the near future.

## Features

- MCP server implementation for SDIF files
- Direct SQL querying capabilities for AI agents
- Predefined prompts for common SDIF operations:
  - Standardization
  - Schema adaptation
  - Transformation

## Installation

```bash
pip install sdif-mcp
```

## Contributing

As this is an experimental package, we welcome contributions and feedback to help stabilize and improve the API.

Please follow the general contribution guidelines for the SATIF project:

1. **Fork the repository** on GitHub.
2. **Clone your fork** locally.
3. **Create a new branch** for your feature or bug fix.
4. **Set up the development environment** (refer to the main SATIF project or relevant sub-project for build/dev environment details).
5. **Make your changes.** Ensure your code adheres to PEP 8 and includes comprehensive docstrings and type hints.
6. **Add or update tests** for your changes.
7. **Run linters, type checkers, and tests** to ensure code quality and correctness.
8. **Commit your changes** with a clear and descriptive commit message.
9. **Push your changes** to your fork.
10. **Submit a Pull Request (PR)** to the `main` branch of the original `syncpulse-solutions/satif` repository.

## License

This project is licensed under the MIT License.

## Contact

Maintainer: Bryan Djafer (bryan.djafer@syncpulse.fr)
