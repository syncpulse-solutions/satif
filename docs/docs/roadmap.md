---
sidebar_position: 100
---
# Roadmap

-> making the SATIF production-ready

- Async API
  - remove any blocking I/O
    - use aiosqlite ?
    - use aiofiles ?
  - propose Async API for all components
- Adapters: SDIF Schema Adapter
- CodeExecutor: E2B Sandbox Executor
- XLSXRepresenter: Use Libreoffice instead of aspose-cells

-> improving the SATIF reliability & success rate

- Improve the XLSX-to-SDIF standardizer
  - Implement guardrails (ensure not altered data + ensure data rows are complete)
  - Prompting techniques and tools
- Implement AI Comparator
- Implement Better Representer
- R&D on the Transformation Builder
