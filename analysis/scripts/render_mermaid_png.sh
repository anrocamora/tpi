#!/usr/bin/env bash
set -euo pipefail

# Render Mermaid diagrams to high-quality PNG for Pandoc/LaTeX.
# Uses mermaid-cli via npx (headless Chromium), which correctly handles Mermaid HTML labels.
#
# Output strategy:
# - render at a high scale (hi-res)
# - constrain the rendering viewport to avoid extremely wide diagrams in PDF

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

IN_MMD="$ROOT_DIR/specifications/subida de datos/diagrama_usuarios_y_almacenamiento.mmd"
OUT_DIR="$ROOT_DIR/specifications/subida de datos/_rendered"

mkdir -p "$OUT_DIR"

# Keep the output filename stable (referenced from the Markdown).
OUT_PNG="$OUT_DIR/diagrama_usuarios_y_almacenamiento.png"

# Tuning knobs (override via env vars if needed):
# - MERMAID_SCALE: resolution multiplier. 2-4 is typical for PDF.
# - MERMAID_WIDTH / MERMAID_HEIGHT: viewport size used by headless Chromium.
#   This helps prevent super-wide diagrams when using LR orientation.
MERMAID_SCALE="${MERMAID_SCALE:-3}"
MERMAID_WIDTH="${MERMAID_WIDTH:-1600}"
MERMAID_HEIGHT="${MERMAID_HEIGHT:-1000}"

# Notes:
# - -s controls pixel density. Larger => less pixelation.
# - -w/-H constrain the viewport; the diagram will be laid out to fit.
#   If content still goes very wide, consider switching flowchart direction
#   to TB, or inserting <br/> in the longest node labels.

npx -y @mermaid-js/mermaid-cli@11.4.2 \
  -i "$IN_MMD" \
  -o "$OUT_PNG" \
  -b white \
  -t default \
  -s "$MERMAID_SCALE" \
  -w "$MERMAID_WIDTH" \
  -H "$MERMAID_HEIGHT"

echo "Rendered Mermaid PNG: $OUT_PNG"

echo "Tip: check image size with: identify -verbose '$OUT_PNG' | head"
