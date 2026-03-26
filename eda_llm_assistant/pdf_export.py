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

    Keeps formatting intentionally lightweight for portability in Streamlit Cloud.
    """
    try:
        from fpdf import FPDF
    except Exception as e:
        raise RuntimeError(
            "Missing optional dependency `fpdf2`. Install with: pip install fpdf2"
        ) from e

    def _safe(s: str) -> str:
        # Built-in Helvetica supports latin-1. Replace unsupported glyphs.
        s = (s or "")
        # Normalize common unicode punctuation to ASCII for cleaner PDF rendering.
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

    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    def _write(text: str, h: float, width: int = 110) -> None:
        safe = _safe(text)
        # Pre-wrap to avoid fpdf long-token edge cases on very wide lines.
        for seg in (wrap(safe, width=width, break_long_words=True, break_on_hyphens=False) or [""]):
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(0, h, seg)

    pdf.set_font("Helvetica", "B", 16)
    _write(title, 8)
    pdf.ln(1)

    base_path = Path(base_dir) if base_dir else None

    for raw in markdown_text.splitlines():
        line = raw.rstrip()
        if not line:
            pdf.ln(1.5)
            continue

        # Image syntax: ![alt](path)
        m_img = re.match(r"!\[[^\]]*\]\(([^)]+)\)", line.strip())
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
                    usable_w = pdf.w - pdf.l_margin - pdf.r_margin
                    pdf.image(str(img_path), w=usable_w)
                    pdf.ln(2)
                    continue
                except Exception:
                    # Fall back to text if image fails
                    pass

        # Heading syntax: # ... up to ######
        m_head = re.match(r"^(#{1,6})\s+(.*)$", line)
        if m_head:
            level = len(m_head.group(1))
            txt = _clean_inline_markdown(m_head.group(2).strip())
            size = {1: 14, 2: 13, 3: 12}.get(level, 11)
            pdf.set_font("Helvetica", "B", size)
            _write(txt, 7 if level <= 2 else 6.5)
            continue
        if line.startswith("|"):
            # Keep tables as monospaced rows for readability in plain PDF.
            pdf.set_font("Courier", "", 9)
            _write(line, 4.5, width=130)
            continue
        if line.startswith("- ") or line.startswith("* "):
            pdf.set_font("Helvetica", "", 10.5)
            _write("- " + _clean_inline_markdown(line[2:].strip()), 5.5)
            continue
        if line.startswith(">"):
            pdf.set_font("Helvetica", "I", 10)
            _write(_clean_inline_markdown(line.lstrip("> ").strip()), 5.5)
            continue

        pdf.set_font("Helvetica", "", 10.5)
        _write(_clean_inline_markdown(line), 5.5)

    out = BytesIO()
    raw = pdf.output(dest="S")
    if isinstance(raw, str):
        out.write(raw.encode("latin-1"))
    else:
        out.write(bytes(raw))
    return out.getvalue()

