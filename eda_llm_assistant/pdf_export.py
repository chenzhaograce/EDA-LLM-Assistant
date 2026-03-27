from __future__ import annotations

from io import BytesIO
from pathlib import Path
import re
from textwrap import wrap


def markdown_to_pdf_bytes(
    markdown_text: str,
    *,
    title: str = "EDA Report",
    base_dir: str | Path | None = None,
) -> bytes:
    """Render markdown-like text to a simple PDF.

    Tables are rendered using fpdf2's native table layout (not monospace text).
    """
    try:
        from fpdf import FPDF
    except Exception as e:
        raise RuntimeError(
            "Missing optional dependency `fpdf2`. Install with: pip install fpdf2"
        ) from e

    def _safe(s: str) -> str:
        s = (s or "")
        s = (
            s.replace("—", "-")
            .replace("–", "-")
            .replace("“", '"')
            .replace("”", '"')
            .replace("‘", "'")
            .replace("’", "'")
            .replace("→", "->")
        )
        return s.encode("latin-1", errors="replace").decode("latin-1")

    def _clean_inline_markdown(s: str) -> str:
        s = re.sub(r"`([^`]+)`", r"\1", s)
        s = re.sub(r"\*\*([^*]+)\*\*", r"\1", s)
        s = re.sub(r"\*([^*]+)\*", r"\1", s)
        return s

    def _is_separator_row(cells: list[str]) -> bool:
        if not cells:
            return False
        for c in cells:
            t = (c or "").strip().replace(" ", "")
            if not re.fullmatch(r":?-{3,}:?", t):
                return False
        return True

    def _parse_md_table_block(block: list[str]) -> list[list[str]]:
        rows: list[list[str]] = []
        for raw in block:
            inner = raw.strip()
            if not inner.startswith("|"):
                continue
            cells = [c.strip() for c in inner.strip("|").split("|")]
            if _is_separator_row(cells):
                continue
            rows.append([_safe(_clean_inline_markdown(c)) for c in cells])
        if not rows:
            return []
        n = max(len(r) for r in rows)
        return [r + [""] * (n - len(r)) for r in rows]

    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    def _usable_width() -> float:
        return pdf.w - pdf.l_margin - pdf.r_margin

    def _write(text: str, h: float, width: int = 110) -> None:
        safe = _safe(text)
        for seg in wrap(safe, width=width, break_long_words=True, break_on_hyphens=False) or [""]:
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(0, h, seg)

    pdf.set_font("Helvetica", "B", 16)
    _write(title, 8)
    pdf.ln(1)

    base_path = Path(base_dir) if base_dir else None
    lines_list = markdown_text.splitlines()
    i = 0

    while i < len(lines_list):
        line = lines_list[i].rstrip()
        if not line:
            pdf.ln(1.5)
            i += 1
            continue

        stripped = line.strip()
        # Markdown pipe table: consecutive lines starting with |
        if stripped.startswith("|"):
            block: list[str] = []
            j = i
            while j < len(lines_list):
                s = lines_list[j].strip()
                if not s:
                    break
                if not s.startswith("|"):
                    break
                block.append(lines_list[j])
                j += 1
            rows = _parse_md_table_block(block)
            i = j
            if rows:
                pdf.set_font("Helvetica", "", 9)
                with pdf.table(
                    rows=rows,
                    width=_usable_width(),
                    line_height=6,
                    text_align="LEFT",
                    first_row_as_headings=True,
                    repeat_headings=True,
                    borders_layout="ALL",
                    padding=2,
                    wrapmode="WORD",
                    gutter_height=0,
                    gutter_width=0,
                ):
                    pass
                pdf.ln(2)
            continue

        # Image syntax: ![alt](path)
        m_img = re.match(r"!\[[^\]]*\]\(([^)]+)\)", stripped)
        if m_img and base_path:
            img_rel = m_img.group(1).split("?", 1)[0].split("#", 1)[0]
            img_path = (base_path / img_rel).resolve()
            try:
                img_path.relative_to(base_path.resolve())
            except Exception:
                img_path = None
            if img_path and img_path.is_file():
                try:
                    pdf.set_x(pdf.l_margin)
                    pdf.image(str(img_path), w=_usable_width())
                    pdf.ln(2)
                    i += 1
                    continue
                except Exception:
                    pass

        m_head = re.match(r"^(#{1,6})\s+(.*)$", line)
        if m_head:
            level = len(m_head.group(1))
            txt = _clean_inline_markdown(m_head.group(2).strip())
            size = {1: 14, 2: 13, 3: 12}.get(level, 11)
            pdf.set_font("Helvetica", "B", size)
            _write(txt, 7 if level <= 2 else 6.5)
            i += 1
            continue
        if stripped.startswith("- ") or stripped.startswith("* "):
            pdf.set_font("Helvetica", "", 10.5)
            _write("- " + _clean_inline_markdown(stripped[2:].strip()), 5.5)
            i += 1
            continue
        if stripped.startswith(">"):
            pdf.set_font("Helvetica", "I", 10)
            _write(_clean_inline_markdown(stripped.lstrip("> ").strip()), 5.5)
            i += 1
            continue

        pdf.set_font("Helvetica", "", 10.5)
        _write(_clean_inline_markdown(line), 5.5)
        i += 1

    out = BytesIO()
    raw = pdf.output(dest="S")
    if isinstance(raw, str):
        out.write(raw.encode("latin-1"))
    else:
        out.write(bytes(raw))
    return out.getvalue()
