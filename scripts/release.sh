#!/bin/bash

set -e # Exit immediately if a command exits with a non-zero status.

# Colors for terminal output
BOLD=$(tput bold 2>/dev/null || echo)
GREEN=$(tput setaf 2 2>/dev/null || echo)
YELLOW=$(tput setaf 3 2>/dev/null || echo)
RED=$(tput setaf 1 2>/dev/null || echo)
RESET=$(tput sgr0 2>/dev/null || echo)

# --- Configuration ---
MAIN_BRANCH="main" # Default main branch name

# --- Argument Parsing ---
LIB_DIR_NAME=$1
BUMP_LEVEL=$2

if [ -z "$LIB_DIR_NAME" ]; then
  echo "${RED}Error: Library directory name not provided.${RESET}" >&2
  echo "Usage: $0 <library_dir_name> [bump_level]" >&2
  exit 1
fi

# Check if BUMP_LEVEL is provided, otherwise prompt
if [ -z "$BUMP_LEVEL" ]; then
  # Check if stdin is a TTY (interactive terminal)
  if [ -t 0 ]; then
    options=("patch" "minor" "major" "prepatch" "preminor" "premajor" "prerelease")
    echo "Please select the version bump level:"
    # Simple menu using select (bash specific)
    select opt in "${options[@]}"; do
      if [[ " ${options[*]} " =~ " ${opt} " ]]; then
        BUMP_LEVEL=$opt
        echo "Selected bump level: $BUMP_LEVEL"
        break
      elif [[ "$REPLY" == "q" ]]; then
          echo "${RED}Release cancelled by user.${RESET}"
          exit 1
      else
        echo "${YELLOW}Invalid selection. Please enter the number corresponding to the desired level, or 'q' to quit.${RESET}"
      fi
    done
  else
    # Non-interactive environment (e.g., CI) - require bump level
    echo "${RED}Error: Bump level must be provided as the second argument in non-interactive environments.${RESET}" >&2
    echo "Usage: $0 <library_dir_name> <bump_level>" >&2
    exit 1
  fi
fi

# Validate BUMP_LEVEL (optional - basic check if it looks like a valid word)
if ! [[ "$BUMP_LEVEL" =~ ^(patch|minor|major|prepatch|preminor|premajor|prerelease)$ ]]; then
    echo "${RED}Error: Invalid bump level '$BUMP_LEVEL'. Must be one of: patch, minor, major, prepatch, preminor, premajor, prerelease.${RESET}" >&2
    exit 1
fi

LIB_PATH="libs/$LIB_DIR_NAME"
PYPROJECT_PATH="$LIB_PATH/pyproject.toml"

if [ ! -d "$LIB_PATH" ]; then
    echo "${RED}Error: Library path "$LIB_PATH" does not exist.${RESET}" >&2
    exit 1
fi

if [ ! -f "$PYPROJECT_PATH" ]; then
    echo "${RED}Error: Cannot find "$PYPROJECT_PATH".${RESET}" >&2
    exit 1
fi

echo "${YELLOW}Starting release process for library in \"$LIB_PATH\" with bump level: \"$BUMP_LEVEL\"${RESET}"

# --- Derive Package Name ---
echo "Deriving package name from "$PYPROJECT_PATH"..."
# Use grep and sed for potentially better portability than awk field splitting with complex delimiters
PKG_NAME=$(grep -E '^[[:space:]]*name[[:space:]]*=[[:space:]]*"' "$PYPROJECT_PATH" | head -n 1 | sed -E 's/^[[:space:]]*name[[:space:]]*=[[:space:]]*"([^"]*)".*/\1/')

if [ -z "$PKG_NAME" ]; then
    echo "${RED}Error: Could not derive package name from "$PYPROJECT_PATH". Check the 'name = ...' line.${RESET}" >&2
    exit 1
fi
echo "Package name: \"$PKG_NAME\""

# --- Git Checks ---
echo "Checking for uncommitted changes in \"$LIB_PATH\"..."
git diff --quiet --exit-code -- "$LIB_PATH" || { echo "${RED}Error: Uncommitted changes detected in \"$LIB_PATH\". Please commit or stash them.${RESET}"; exit 1; }
echo "No uncommitted changes found."

echo "Checking for untracked files in \"$LIB_PATH\"..."
if [ -n "$(git ls-files --others --exclude-standard "$LIB_PATH")" ]; then
    echo "${RED}Error: Untracked files present in \"$LIB_PATH\". Please commit or stash them.${RESET}" >&2; exit 1;
fi
echo "No untracked files found."

echo "Checking current branch..."
current_branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$current_branch" != "$MAIN_BRANCH" ]; then
    echo "${RED}Error: Not on the \"$MAIN_BRANCH\" branch (currently \"$current_branch\").${RESET}" >&2; exit 1;
fi
echo "On branch \"$MAIN_BRANCH\"."

echo "Pulling latest from origin \"$MAIN_BRANCH\"..."
git pull origin "$MAIN_BRANCH" --ff-only || { echo "${RED}Error: Unable to pull latest changes from origin \"$MAIN_BRANCH\".${RESET}"; exit 1; }
echo "Pull complete."

# --- Version Bump ---
echo "Bumping version in \"$LIB_PATH\" using \"poetry version $BUMP_LEVEL\"..."
poetry --directory "$LIB_PATH" version "$BUMP_LEVEL" || { echo "${RED}Error: poetry version command failed for \"$LIB_PATH\".${RESET}"; exit 1; }

# --- Capture New Version ---
echo "Capturing new version..."
NEW_VERSION=$(poetry --directory "$LIB_PATH" version --short)
if [ -z "$NEW_VERSION" ]; then
    echo "${RED}Error: Could not determine new version in \"$LIB_PATH\" after bump.${RESET}" >&2; exit 1;
fi
echo "New version for \"$PKG_NAME\": $NEW_VERSION"

# --- Git Release Steps ---
COMMIT_MSG="chore($LIB_DIR_NAME): bump version to $NEW_VERSION"
TAG_NAME="$PKG_NAME/v$NEW_VERSION"

echo "Adding changes: \"$PYPROJECT_PATH\"..."
git add "$PYPROJECT_PATH" || { echo "${RED}Error: git add failed.${RESET}"; exit 1; }

echo "Committing version bump (message: \"$COMMIT_MSG\")..."
git commit -m "$COMMIT_MSG" || { echo "${RED}Error: git commit failed.${RESET}"; exit 1; }
echo "Commit successful."

echo "Creating tag \"$TAG_NAME\"..."
# Check if tag exists locally first
if git rev-parse "$TAG_NAME" >/dev/null 2>&1; then
    echo "${RED}Error: Tag \"$TAG_NAME\" already exists locally.${RESET}" >&2
    # Consider instructions for cleanup or just exit
    exit 1
fi
git tag "$TAG_NAME" || { echo "${RED}Error: Failed to create git tag \"$TAG_NAME\".${RESET}"; exit 1; }
echo "Tag created."

echo "Pushing commit to origin \"$MAIN_BRANCH\"..."
git push origin "$MAIN_BRANCH" || { echo "${RED}Error: git push origin \"$MAIN_BRANCH\" failed.${RESET}"; exit 1; }
echo "Commit pushed."

echo "Pushing tag \"$TAG_NAME\" to origin..."
git push origin "$TAG_NAME" || { echo "${RED}Error: git push origin \"$TAG_NAME\" failed.${RESET}"; exit 1; }
echo "Tag pushed."

# --- Finish ---
echo "${BOLD}${GREEN}$PKG_NAME release $NEW_VERSION successfully initiated.${RESET}"

exit 0
