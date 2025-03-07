#!/bin/bash

# Ensure VERSION is set
if [[ -z "$VERSION" ]]; then
  echo "Error: VERSION environment variable is not set."
  exit 1
fi

# Extract major, minor, and patch numbers using regex
if [[ "$VERSION" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  MAJOR=${BASH_REMATCH[1]}
  MINOR=${BASH_REMATCH[2]}
  PATCH=${BASH_REMATCH[3]}
else
  echo "Error: VERSION does not follow semantic versioning (MAJOR.MINOR.PATCH)."
  exit 1
fi

# Determine which version component to increment
case "$1" in
  major)
    ((MAJOR++))
    MINOR=0
    PATCH=0
    ;;
  minor)
    ((MINOR++))
    PATCH=0
    ;;
  patch|"" )
    ((PATCH++))
    ;;
  *)
    echo "Usage: $0 [major|minor|patch]"
    exit 1
    ;;
esac

# Export the new version
NEW_VERSION="$MAJOR.$MINOR.$PATCH"
export VERSION=$NEW_VERSION

echo "Updated VERSION: $VERSION"
