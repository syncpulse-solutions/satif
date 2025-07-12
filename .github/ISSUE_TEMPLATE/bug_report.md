---
name: "🐛 Bug Report"
description: "Create a report to help us improve the SDK. Please provide detailed steps to reproduce."
labels: ["bug", "needs-triage"]
assignees: '' # Optional: assign to a specific user/team for triage
---
<!-- Thank you for reporting a bug!

Please provide as much detail as possible. Vague reports may be closed if actionable information is missing.

Before submitting:
- Have you checked the documentation (link in config.yml)?
- Have you searched for existing similar issues?
- Are you using the latest version of the SDK? If not, please try upgrading first.
-->

**1. Describe the Bug**
A clear and concise description of what the bug is.

**2. Steps to Reproduce**
Please provide detailed steps to reproduce the behavior:

1. '...'
2. '...'
3. '...'
4. Error occurs here.

**3. Expected Behavior**
A clear and concise description of what you expected to happen.

**4. Actual Behavior**
A clear and concise description of what actually happened. Please include:

* Full error messages (if any).
* Complete stack traces (if applicable). Use code blocks (```) for readability.

**5. Environment (REQUIRED)**
Please provide the following details:

* **SDK Version:** `pip show your-sdk-name` -> [e.g., 1.2.3]
* **Python Version:** `python --version` -> [e.g., 3.10.4]
* **Operating System:** [e.g., Ubuntu 22.04, macOS 13.1, Windows 11]
* **Relevant Dependencies (if applicable):** [e.g., requests 2.28.1, Django 4.1]

**6. Minimal Reproducible Example (MRE)**

<!--
REQUIRED: Please provide a *minimal*, *self-contained*, and *runnable* code snippet that demonstrates the bug.
This is the most helpful piece of information for debugging!
- Minimal: Use the smallest amount of code possible. Remove anything unrelated.
- Self-contained: Should run on its own without external files or services (unless essential to the bug). Mock external dependencies if possible.
- Runnable: Ensure the code actually runs and produces the error.
Learn more about MREs: https://stackoverflow.com/help/minimal-reproducible-example
-->

```python
# Your MRE code here
import your_sdk_name

# ... code demonstrating the bug ...
```
