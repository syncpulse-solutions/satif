# SATIF Core

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)

Core abstractions and types for the SATIF toolkit.

## Table of Contents

- [Overview](#overview)
- [Intended Audience &amp; Usage](#intended-audience--usage)
- [Contributing](#contributing)
- [License](#license)

## Overview

The `satif_core` package serves as the foundational library for the SATIF ecosystem. It provides a collection of essential base classes, abstract interfaces, custom data types, and common exceptions that are utilized by other SATIF components to ensure consistency and facilitate interoperability within the system.

This package does **not** contain concrete implementations or executable logic for end-users. Instead, it defines the contracts and building blocks upon which the rest of the SATIF system is built.

## Intended Audience & Usage

`satif_core` is intended for **internal development** within the SATIF project. It is **not designed for direct usage** by end-users or applications seeking to perform data transformations.

Other SATIF libraries (e.g., satif-sdk) will use the abstractions and types defined in `satif_core` to build their respective components.

## Contributing

Contributions to `satif_core` are welcome, especially those that refine existing abstractions or propose new fundamental components beneficial to the SATIF ecosystem. As this is a core library, changes will be reviewed carefully for their impact on the overall architecture.

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

This project is licensed under the MIT License - see the `LICENSE` file in the SATIF project root for details.
