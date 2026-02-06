-- Pandoc Lua filter: remove the Markdown block between "<!-- TOC -->" markers.
-- Keeps the manual TOC in the source for IDE preview, but excludes it from PDF generation.
--
-- Robust behavior:
-- - If the markers survive parsing (as RawBlock/RawInline), drop content between them.
-- - If the markers are stripped (common for HTML comments), drop a leading list block
--   (BulletList / OrderedList) before the first header, which matches the manual TOC.
--
-- Usage:
--   pandoc ... --lua-filter=../filters/drop_manual_toc.lua ...

local IN_MARKER = "<!-- TOC -->"

local function is_marker_block(block)
  if block.t == "RawBlock" and block.format == "html" then
    local txt = (block.text or ""):gsub("%s+", " ")
    return txt:find(IN_MARKER, 1, true) ~= nil
  end
  if block.t == "Para" then
    for _, inl in ipairs(block.content) do
      if inl.t == "RawInline" and inl.format == "html" then
        local txt = (inl.text or ""):gsub("%s+", " ")
        if txt:find(IN_MARKER, 1, true) ~= nil then
          return true
        end
      end
      if inl.t == "Str" and inl.text == IN_MARKER then
        return true
      end
    end
  end
  return false
end

local function is_list_block(block)
  return block.t == "BulletList" or block.t == "OrderedList"
end

function Pandoc(doc)
  local blocks = doc.blocks

  -- 1) Try marker-based removal when markers exist in the parsed AST.
  local marker_idxs = {}
  for i, b in ipairs(blocks) do
    if is_marker_block(b) then
      table.insert(marker_idxs, i)
    end
  end

  if #marker_idxs >= 2 then
    local start_i = marker_idxs[1]
    local end_i = marker_idxs[2]
    local out = {}
    for i, b in ipairs(blocks) do
      if i < start_i or i > end_i then
        table.insert(out, b)
      end
    end
    doc.blocks = out
    return doc
  end

  -- 2) Fallback: HTML comments are often stripped, so remove the manual TOC
  --    by structure.
  --    In this repo's docs the manual TOC is the first list right after the
  --    document title (# ...).

  local title_idx = nil
  for i, b in ipairs(blocks) do
    if b.t == "Header" then
      title_idx = i
      break
    end
  end

  if not title_idx then
    return doc
  end

  -- Search for a list within a small window after the title header.
  local max_lookahead = 6
  local list_idx = nil
  for i = title_idx + 1, math.min(#blocks, title_idx + max_lookahead) do
    if is_list_block(blocks[i]) then
      list_idx = i
      break
    end
  end

  if not list_idx then
    return doc
  end

  local out = {}
  for i, b in ipairs(blocks) do
    if i ~= list_idx then
      table.insert(out, b)
    end
  end

  doc.blocks = out
  return doc
end
