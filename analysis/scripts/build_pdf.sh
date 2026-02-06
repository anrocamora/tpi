#!/usr/bin/env bash
set -euo pipefail

# Build a PDF from a Markdown specification using Pandoc + Eisvogel.
# - Keeps the manual TOC block (between <!-- TOC --> markers) for IDE preview
#   but removes it from the generated PDF via a pandoc Lua filter.
#
# Usage:
#   ./scripts/build_pdf.sh \
#     specifications/subida_de_datos.md \
#     _build/subida_de_datos.pdf

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <input.md> <output.pdf>" >&2
  exit 2
fi

INPUT_MD="$1"
OUTPUT_PDF="$2"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_ABS="$ROOT_DIR/$INPUT_MD"
OUTPUT_ABS="$ROOT_DIR/$OUTPUT_PDF"

# Ensure output directory exists
mkdir -p "$(dirname "$OUTPUT_ABS")"

# Run pandoc from the directory of the input file so relative image links work.
INPUT_DIR="$(dirname "$INPUT_ABS")"
INPUT_FILE="$(basename "$INPUT_ABS")"

cd "$INPUT_DIR"

pandoc "$INPUT_FILE" \
  --from markdown \
  --template eisvogel \
  --pdf-engine=xelatex \
  --variable graphics=true \
  --toc \
  --strip-comments \
  --lua-filter="$ROOT_DIR/filters/drop_manual_toc.lua" \
  -o "$OUTPUT_ABS"

echo "Wrote: $OUTPUT_ABS"
