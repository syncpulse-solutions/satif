---
sidebar_position: 1
---

# Building a transformation pipeline

This guide walks you through the simplest possible SATIF AI pipeline:

• **Build Phase**  : generate the transformation code once by feeding SATIF **both** the standardized data and an *example* of the desired output.
• **Run Phase**  : apply that code to any number of new datasources to get fresh output files.

Everything below fits in ±50 lines of Python.

---

## 1  Install the dependencies

```bash
pip install "satif-ai>=0.1" fastmcp
```

*Python 3.10+ is required.*

---

## 2  Project layout

```
my_project/
├── input_files/
│   └── sales.csv
├── output_examples/
│   └── expected_sales.json
└── run_satif.py
```

* `sales.csv` is the raw source file.
* `expected_sales.json` is a single example of the desired result.

SATIF will learn the transformation by comparing the generated output with the output example file.

---

## 3  The script (both phases)

```python
# run_satif.py
"""Minimal two-phase SATIF pipeline in explicit functions."""

import asyncio
from pathlib import Path

from fastmcp import FastMCP, Client
from fastmcp.client.transports import FastMCPTransport

from satif_ai.standardizers.ai import AIStandardizer
from satif_ai.transformation_builders.syncpulse import (
    SyncpulseTransformationBuilder,
)
from satif_ai.utils.openai_mcp import OpenAICompatibleMCP
from satif_sdk.code_executors.local_executor import LocalCodeExecutor
from satif_sdk.transformers.code import CodeTransformer


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INPUT_FILE = "input_files/sales.csv"
OUTPUT_EXAMPLE = "output_examples/expected_sales.json"
MODEL = "o4-mini"

SDIF_PATH = Path("input.sdif")
CODE_PATH = Path("transform.py")
OUTPUT_DIR = Path("generated_output")


# ---------------------------------------------------------------------------
# Phase A – BUILD (one-off)
# ---------------------------------------------------------------------------


async def build_transformation() -> None:
    """Standardize *INPUT_FILE* and generate *CODE_PATH*."""

    mcp_server = FastMCP()
    mcp_transport = FastMCPTransport(mcp=mcp_server)

    async with Client(mcp_transport) as mcp_client:
        openai_mcp = OpenAICompatibleMCP(mcp=mcp_server)
        await openai_mcp.connect()

        # 1  Standardize datasource → SDIF
        standardizer = AIStandardizer(
            mcp_server=openai_mcp,
            mcp_session=mcp_client.session,
            llm_model=MODEL,
        )
        await standardizer.standardize(
            datasource=INPUT_FILE,
            output_path=SDIF_PATH,
            overwrite=True,
        )

        # 2  Generate Python transformation code
        builder = SyncpulseTransformationBuilder(
            mcp_server=openai_mcp,
            mcp_session=mcp_client.session,
            llm_model=MODEL,
        )
        code_str = await builder.build(
            sdif=SDIF_PATH,
            output_target_files={OUTPUT_EXAMPLE: Path(OUTPUT_EXAMPLE).name},
            instructions=(
                "For every customer in sales.csv, compute total_amount and "
                "output JSON with fields: customer_id and total_amount."
            ),
        )
        CODE_PATH.write_text(code_str)


# ---------------------------------------------------------------------------
# Phase B – RUN (repeatable)
# ---------------------------------------------------------------------------


async def run_transformation() -> None:
    """Apply *CODE_PATH* to *SDIF_PATH* to produce files in *OUTPUT_DIR*."""

    transformer = CodeTransformer(
        function=CODE_PATH,
        code_executor=LocalCodeExecutor(disable_security_warning=True),
    )
    transformer.export(
        sdif=SDIF_PATH,
        output_path=OUTPUT_DIR,
    )


# ---------------------------------------------------------------------------
# Entrypoint (dev convenience): build then run.
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    asyncio.run(build_transformation())
    asyncio.run(run_transformation())
```

---

## 4  Run it

```bash
python run_satif.py
```

You will obtain:

* `input.sdif` – the standardized SQLite database.
* `transform.py` – the AI-generated transformation script.
* `generated_output/expected_sales.json` – the file produced by the script.

Compare the generated file with `output_examples/expected_sales.json`. If they differ, tweak the `instructions` string or provide additional example files.

---

## 5  Where to go next

* Pass a list of paths to `datasource` to merge multiple inputs into one SDIF.
* Map several example outputs in `output_target_files` to generate multi-file transformations.
* Tune `llm_model` for different speed/quality trade-offs.
* Use the higher-level helpers `astandardize()` and `atransform()` when you don't need fine-grained control over the builder.

That's all – you now have a working SATIF transformation pipeline in two explicit steps.
