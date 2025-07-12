#!/usr/bin/env python3
"""
Script to copy Sphinx-generated markdown documentation to Docusaurus structure.

This script:
1. Copies markdown files from Sphinx build output to Docusaurus docs
2. Adds Docusaurus front matter to markdown files
3. Cleans up file structure for better Docusaurus integration
4. Ensures proper navigation and linking
"""

import re
import shutil
import sys
from pathlib import Path


def add_docusaurus_frontmatter(
    content: str, title: str, sidebar_position: int = None
) -> str:
    """Add Docusaurus front matter to markdown content."""
    # Ensure title is valid
    if not title or title.strip() == "":
        title = "Untitled"

    # Clean up title
    title = title.strip()

    frontmatter = f"""---
title: {title}
description: Auto-generated API documentation for {title}
"""

    if sidebar_position:
        frontmatter += f"sidebar_position: {sidebar_position}\n"

    frontmatter += "---\n\n"

    return frontmatter + content


def clean_sphinx_markdown(content: str, is_main_index: bool = False) -> str:
    """Clean up Sphinx-specific markdown for better Docusaurus compatibility."""
    # Remove Sphinx-specific HTML anchors that might interfere with Docusaurus
    content = re.sub(r'<a id="[^"]*"></a>\s*', "", content)

    # Convert Sphinx cross-references to simple links
    content = re.sub(r"\[(.*?)\]\(#(.*?)\)", r"[\1](#\2)", content)

    # Clean up redundant "satif_sdk." prefixes in titles and headers
    content = re.sub(r"(#{1,6})\s*satif_sdk\.([a-zA-Z_]+)\s+module", r"\1 \2", content)
    content = re.sub(
        r"(#{1,6})\s*satif_sdk\.([a-zA-Z_]+)\.([a-zA-Z_]+)\s+module",
        r"\1 \2.\3",
        content,
    )

    # Clean up class and function signatures
    content = re.sub(
        r"\*class\* satif_sdk\.([a-zA-Z_]+)\.([a-zA-Z_]+)\(", r"**class** \2(", content
    )
    content = re.sub(r"satif_sdk\.([a-zA-Z_]+)\.([a-zA-Z_]+)\(", r"\2(", content)
    content = re.sub(r"### \*class\* ([^(]+)\(", r"### class \1(", content)

    # Clean up function signatures - remove module prefixes
    content = re.sub(
        r"### satif_sdk\.([a-zA-Z_]+)\.([a-zA-Z_]+)\(", r"### \2(", content
    )

    # Clean up type annotations in signatures for better readability
    content = re.sub(
        r"(\*args: Any, \*\*kwargs: Any)", r"`*args: Any, **kwargs: Any`", content
    )
    content = re.sub(r"(\*args: [^,)]+)", r"`*\1`", content)
    content = re.sub(r"(\*\*kwargs: [^,)]+)", r"`**\1`", content)

    # Improve parameter formatting
    content = re.sub(
        r"^\s*\*\s+\*\*Parameters:\*\*", "**Parameters:**", content, flags=re.MULTILINE
    )
    content = re.sub(
        r"^\s*\*\s+\*\*Returns:\*\*", "**Returns:**", content, flags=re.MULTILINE
    )
    content = re.sub(
        r"^\s*\*\s+\*\*Raises:\*\*", "**Raises:**", content, flags=re.MULTILINE
    )

    # Clean up parameter descriptions for better formatting
    content = re.sub(
        r"^\s*\*\s+\*\*([^*]+)\*\*\s+–", r"- **\1** –", content, flags=re.MULTILINE
    )

    # Clean up "Bases:" lines to be more concise
    content = re.sub(r"Bases:\s+`([^`]+)`", r"*Inherits from:* `\1`", content)

    # Clean up angle brackets and curly braces for MDX compatibility
    content = re.sub(r"<([^>]+)>", r"`<\1>`", content)
    content = re.sub(r"\{([^}]+)\}", r"\\{\1\\}", content)

    # Fix broken Sphinx links
    content = re.sub(r"\[([^\]]+)\]\(genindex\)", r"\1", content)
    content = re.sub(r"\[([^\]]+)\]\(py-modindex\)", r"\1", content)
    content = re.sub(r"\[([^\]]+)\]\(search\)", r"\1", content)

    # Fix links to removed overview.md
    content = re.sub(r"\]\(\.\.\/\.\.\/overview\.md\)", r"](../index.md)", content)

    # Fix autosummary links to point to modules directory
    content = re.sub(r"\]\(api/_autosummary/([^)#]+)\)", r"](modules/\1.md)", content)
    content = re.sub(
        r"\]\(api/_autosummary/([^)#]+)#([^)]+)\)", r"](modules/\1.md#\2)", content
    )
    content = re.sub(r"\]\(_autosummary/([^)#]+)\)", r"](modules/\1.md)", content)
    content = re.sub(
        r"\]\(_autosummary/([^)#]+)#([^)]+)\)", r"](modules/\1.md#\2)", content
    )

    # Fix cross-references to other modules within the same modules directory
    content = re.sub(
        r"\]\(satif_sdk\.([^)#]+)#([^)]+)\)", r"](satif_sdk.\1.md#\2)", content
    )
    content = re.sub(r"\]\(satif_sdk\.([^)#]+)\)", r"](satif_sdk.\1.md)", content)

    # Fix cross-references to main index anchors
    content = re.sub(
        r"\]\(\.\.\/\.\.\/index#module-([^)]+)\)", r"](../index.md)", content
    )
    content = re.sub(r"\]\(\.\.\/\.\.\/index#([^)]+)\)", r"](../index.md)", content)

    # Clean up excessive whitespace and normalize formatting
    content = re.sub(r"\n{3,}", "\n\n", content)
    content = re.sub(r"^(#{1,6})\s*\n", r"\1 \n", content, flags=re.MULTILINE)
    content = re.sub(r"^(\s*[^:\n]+):\s*$", r"\1", content, flags=re.MULTILINE)

    return content


def organize_module_content(content: str, module_name: str) -> str:
    """Organize module content into cleaner sections like LangChain docs."""
    # Create a cleaner module header
    clean_module_name = module_name.replace("satif_sdk.", "").replace("_", " ").title()

    # Split content into sections
    lines = content.split("\n")
    organized_lines = []
    current_section = []

    # Group functions and classes
    classes = []
    functions = []
    current_item = []
    in_class = False
    in_function = False

    for line in lines:
        # Detect class definitions
        if line.startswith("### class ") or line.startswith("### *class*"):
            if current_item:
                if in_class:
                    classes.append("\n".join(current_item))
                elif in_function:
                    functions.append("\n".join(current_item))
                current_item = []
            current_item.append(line)
            in_class = True
            in_function = False
        # Detect function definitions
        elif (
            line.startswith("### ") and "(" in line and not line.startswith("### class")
        ):
            if current_item:
                if in_class:
                    classes.append("\n".join(current_item))
                elif in_function:
                    functions.append("\n".join(current_item))
                current_item = []
            current_item.append(line)
            in_class = False
            in_function = True
        # Continue current item
        elif in_class or in_function:
            current_item.append(line)
        # Regular content
        else:
            if current_item:
                if in_class:
                    classes.append("\n".join(current_item))
                elif in_function:
                    functions.append("\n".join(current_item))
                current_item = []
                in_class = False
                in_function = False
            organized_lines.append(line)

    # Add the last item
    if current_item:
        if in_class:
            classes.append("\n".join(current_item))
        elif in_function:
            functions.append("\n".join(current_item))

    # Rebuild content with better organization
    result = "\n".join(organized_lines)

    if classes:
        result += "\n\n## Classes\n\n"
        result += "\n\n".join(classes)

    if functions:
        result += "\n\n## Functions\n\n"
        result += "\n\n".join(functions)

    return result


def copy_docs_to_docusaurus():
    """Main function to copy and process documentation."""
    # Paths
    script_dir = Path(__file__).parent
    sdk_root = script_dir.parent
    project_root = sdk_root.parent.parent

    sphinx_build_dir = sdk_root / "docs" / "build" / "markdown"
    docusaurus_api_dir = project_root / "docs" / "docs" / "api_reference" / "satif_sdk"

    print(f"📁 Source: {sphinx_build_dir}")
    print(f"📁 Target: {docusaurus_api_dir}")

    if not sphinx_build_dir.exists():
        print(f"❌ Error: Sphinx build directory not found: {sphinx_build_dir}")
        print("   Run 'make docs' first to generate the documentation.")
        sys.exit(1)

    # Create target directory
    docusaurus_api_dir.mkdir(parents=True, exist_ok=True)

    # Clear existing files in target directory
    for item in docusaurus_api_dir.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)

    print("🧹 Cleared existing documentation files")

    # Copy and process files
    files_copied = 0

    # Process main index file to create overview
    index_file = sphinx_build_dir / "index.md"
    if index_file.exists():
        content = index_file.read_text(encoding="utf-8")
        content = clean_sphinx_markdown(content, True)

        # Create a clean main overview
        overview_content = """# SATIF SDK

The SATIF SDK provides a comprehensive data processing and AI agent toolkit for transforming, standardizing, and analyzing data.

## Core Modules

### Data Processing
- [**Utils**](modules/utils.md) - Utility functions and helpers
- [**Adapters**](modules/adapters.md) - Data source connectors and loaders
- [**Standardizers**](modules/standardizers.md) - Data standardization tools

### Data Transformation
- [**Transformers**](modules/transformers.md) - Data transformation engines
- [**Representers**](modules/representers.md) - Data visualization and representation

### Analysis & Comparison
- [**Comparators**](modules/comparators.md) - Data comparison and validation utilities

### Code Execution
- [**Code Executors**](modules/code_executors.md) - Code execution environments

## Detailed Module Reference

For comprehensive class and function documentation, explore the individual module pages above.

---

*This documentation is automatically generated from source code and updated with each release.*
"""

        content = add_docusaurus_frontmatter(overview_content, "SATIF SDK", 1)
        target_file = docusaurus_api_dir / "index.md"
        target_file.write_text(content, encoding="utf-8")
        files_copied += 1
        print("✅ Created: index.md")

    # Create modules directory
    modules_dir = docusaurus_api_dir / "modules"
    modules_dir.mkdir(exist_ok=True)

    # Define module organization
    module_groups = {
        "utils": {"title": "Utils", "description": "Utility functions and helpers"},
        "adapters": {
            "title": "Adapters",
            "description": "Data source connectors and loaders",
        },
        "standardizers": {
            "title": "Standardizers",
            "description": "Data standardization tools",
        },
        "transformers": {
            "title": "Transformers",
            "description": "Data transformation engines",
        },
        "representers": {
            "title": "Representers",
            "description": "Data visualization and representation",
        },
        "comparators": {
            "title": "Comparators",
            "description": "Data comparison and validation utilities",
        },
        "code_executors": {
            "title": "Code Executors",
            "description": "Code execution environments",
        },
    }

    # Process API directory for main module files
    api_dir = sphinx_build_dir / "api"
    if api_dir.exists():
        for module_name, module_info in module_groups.items():
            module_file = api_dir / f"{module_name}.md"
            if module_file.exists():
                content = module_file.read_text(encoding="utf-8")
                content = clean_sphinx_markdown(content)
                content = organize_module_content(content, module_name)

                # Add module description
                enhanced_content = f"""# {module_info["title"]}

{module_info["description"]}

{content}
"""

                content = add_docusaurus_frontmatter(
                    enhanced_content, module_info["title"]
                )
                target_file = modules_dir / f"{module_name}.md"
                target_file.write_text(content, encoding="utf-8")
                files_copied += 1
                print(f"✅ Copied: modules/{module_name}.md")

    # Process detailed _autosummary modules
    autosummary_dir = api_dir / "_autosummary" if api_dir.exists() else None
    if autosummary_dir and autosummary_dir.exists():
        # Group detailed modules by category
        for md_file in autosummary_dir.glob("*.md"):
            content = md_file.read_text(encoding="utf-8")
            content = clean_sphinx_markdown(content)

            module_path = md_file.stem
            if module_path.startswith("satif_sdk."):
                clean_title = module_path.replace("satif_sdk.", "").replace("_", ".")
            else:
                clean_title = module_path.replace("_", ".")

            # Ensure title is not empty
            if not clean_title.strip():
                clean_title = module_path

            # Organize content for better structure
            content = organize_module_content(content, module_path)
            content = add_docusaurus_frontmatter(content, clean_title.strip())

            # Use clean filename without satif_sdk prefix
            filename = module_path.replace("satif_sdk.", "") + ".md"
            target_file = modules_dir / filename
            target_file.write_text(content, encoding="utf-8")
            files_copied += 1
            print(f"✅ Copied: modules/{filename}")

    print(f"\n🎉 Successfully copied {files_copied} documentation files to Docusaurus!")
    print(f"📂 Documentation available at: {docusaurus_api_dir}")
    print("\n💡 Next steps:")
    print("   1. Run 'npm run start' in the docs directory to preview")
    print("   2. Check the API Reference section in your Docusaurus site")


if __name__ == "__main__":
    copy_docs_to_docusaurus()
