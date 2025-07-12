# sdif-db

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)

SDIF Database Manager.

This library provides tools to manage and interact with SDIF (Standardized Data Interoperable Format) databases.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [Command Line Interface (CLI)](#command-line-interface-cli)
  - [As a Library](#as-a-library)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Installation

### Prerequisites

- Python 3.9 or higher.

### From PyPI (Recommended)

To install the latest stable version of `sdif-db` from PyPI:

```bash
pip install sdif-db
```

### From Source (for Development)

If you want to contribute to `sdif-db` or need the latest development version:

1. **Clone the repository:**

   ```bash
   git clone https://github.com/syncpulse-solutions/satif.git
   cd satif/libs/sdif
   ```
2. **Install dependencies using Poetry:**
   Make sure you have [Poetry](https://python-poetry.org/docs/#installation) installed.

   ```bash
   poetry install
   ```

   This will create a virtual environment and install all necessary dependencies, including development tools. Alternatively, you can use the Makefile command:

   ```bash
   make install
   ```

## Usage

`sdif-db` can be used both as a Python library in your projects.

### As a Library

You can import and use `sdif-db` components in your Python code.

```python
from sdif_db import SDIFDatabase

try:
    with SDIFDatabase(path_to_your_sqlite_file) as db:
        # Perform operations
        data = db.query("SELECT * FROM your_table")
        print(data)
except Exception as e:
    print(f"An error occurred: {e}")

```

*(More detailed examples and API documentation will be provided as the library evolves.)*

## Contributing

Contributions are welcome! Whether it's bug reports, feature requests, or code contributions, please feel free to get involved.

### Contribution Workflow

1. **Fork the repository** on GitHub.
2. **Clone your fork** locally:

   ```bash
   git clone https://github.com/syncpulse-solutions/satif.git
   cd satif/libs/sdif
   ```
3. **Create a new branch** for your feature or bug fix:

   ```bash
   git checkout -b feature/your-feature-name
   ```

   or

   ```bash
   git checkout -b fix/your-bug-fix-name
   ```
4. **Set up the development environment** as described in the [From Source (for Development)](#from-source-for-development) section:

   ```bash
   make install  # or poetry install
   ```
5. **Make your changes.** Ensure your code follows the project's style guidelines.
6. **Format and lint your code:**

   ```bash
   make format
   make lint
   ```
7. **Run type checks:**

   ```bash
   make typecheck
   ```
8. **Run tests** to ensure your changes don't break existing functionality:

   ```bash
   make test
   ```

   To also generate a coverage report:

   ```bash
   make coverage
   ```
9. **Commit your changes** with a clear and descriptive commit message.
10. **Push your changes** to your fork on GitHub:

    ```bash
    git push origin feature/your-feature-name
    ```
11. **Submit a Pull Request (PR)** to the `main` branch of the original `syncpulse-solutions/satif` repository.

### Coding Standards

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) for Python code.
- Use [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html) for docstrings (as configured in `pyproject.toml`).
- Ensure code is well-documented, especially public APIs.

### Issue Reporting

- For bug reports, please include steps to reproduce the issue.
- For feature requests, describe the desired functionality and its use case.
- Check existing issues before creating a new one to avoid duplicates.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

For questions or support, please open an issue on the [GitHub repository](https://github.com/syncpulse-solutions/satif/issues).

Maintainer: Bryan Djafer (bryan.djafer@syncpulse.fr)
