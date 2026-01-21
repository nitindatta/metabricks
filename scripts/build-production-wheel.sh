#!/bin/bash
# Build production wheel without dev dependencies
# Usage: ./scripts/build-production-wheel.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "============================================"
echo "🔨 Building Production Wheel"
echo "============================================"
echo ""

# Clean previous builds
if [ -d dist ]; then
    echo "🗑️  Cleaning previous builds..."
    rm -rf dist/ build/ *.egg-info
fi

# Build wheel. Note: extras (like "dev") may still be present in METADATA,
# but they are NOT installed unless explicitly requested (e.g. metabricks[dev]).
echo "📦 Building wheel..."
uv build --wheel

if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi

# Find the wheel file
WHEEL_FILE=$(ls -t dist/*.whl | head -1)

if [ -z "$WHEEL_FILE" ]; then
    echo "❌ No wheel file found"
    exit 1
fi

# Get wheel size
WHEEL_SIZE=$(du -h "$WHEEL_FILE" | cut -f1)

echo ""
echo "✅ Build successful!"
echo ""
echo "📊 Wheel Information:"
echo "   File: $(basename "$WHEEL_FILE")"
echo "   Size: $WHEEL_SIZE"
echo "   Path: $WHEEL_FILE"
echo ""

echo "🔍 Verifying wheel does not bundle dev-only deps..."

# 1) Ensure pytest isn't shipped as code inside the wheel (it shouldn't be).
if unzip -l "$WHEEL_FILE" | grep -q "pytest"; then
    echo "⚠️  WARNING: a file path containing 'pytest' was found inside the wheel."
    echo "   This likely indicates accidental vendoring/bundling."
    exit 1
else
    echo "✅ No pytest-related files bundled in wheel"
fi

# 2) Ensure pytest isn't an unconditional install dependency.
# It is OK for pytest to exist only behind the optional 'dev' extra.
echo "🔍 Verifying wheel METADATA (install-time dependencies)..."
python - "$WHEEL_FILE" <<'PY'
import sys
import zipfile
from pathlib import Path

whl = Path(sys.argv[1])
with zipfile.ZipFile(whl, "r") as z:
    meta_path = next(n for n in z.namelist() if n.endswith(".dist-info/METADATA"))
    meta = z.read(meta_path).decode("utf-8", errors="replace")

reqs = [line[len("Requires-Dist: "):] for line in meta.splitlines() if line.startswith("Requires-Dist:")]
base_reqs = [r for r in reqs if "; extra ==" not in r]

print("  Base Requires-Dist:")
for r in base_reqs:
    print("   -", r)

bad = [r for r in base_reqs if "pytest" in r.lower()]
if bad:
    raise SystemExit(f"ERROR: pytest is an unconditional dependency: {bad}")

print("  ✅ OK: no unconditional pytest dependency")
PY

echo ""
echo "📋 Top-level packages in wheel:"
unzip -l "$WHEEL_FILE" | grep -E "\.py$" | head -20

echo ""
echo "✅ Ready to deploy to Databricks!"
echo ""
echo "Next steps:"
echo "  1. Upload to Databricks:"
echo "     databricks fs cp $WHEEL_FILE dbfs:/Volumes/main/shared/"
echo ""
echo "  2. Install in notebook:"
echo "     %pip install /Volumes/main/shared/$(basename "$WHEEL_FILE")"
echo ""
echo "  3. Use in code:"
echo "     from metabricks import MetadataOrchestrator"
echo "     orchestrator = MetadataOrchestrator(config)"
echo "     result = orchestrator.run()"
echo ""
