from __future__ import annotations

from io import BytesIO
from textwrap import wrap


def markdown_to_pdf_bytes(markdown_text: str, *, title: str = "EDA Report") -> bytes:
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
        return (s or "").encode("latin-1", errors="replace").decode("latin-1")

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

    for raw in markdown_text.splitlines():
        line = raw.rstrip()
        if not line:
            pdf.ln(1.5)
            continue

        if line.startswith("# "):
            pdf.set_font("Helvetica", "B", 14)
            _write(line[2:].strip(), 7)
            continue
        if line.startswith("## "):
            pdf.set_font("Helvetica", "B", 13)
            _write(line[3:].strip(), 7)
            continue
        if line.startswith("### "):
            pdf.set_font("Helvetica", "B", 12)
            _write(line[4:].strip(), 6.5)
            continue
        if line.startswith("|"):
            # Keep tables as monospaced rows for readability in plain PDF.
            pdf.set_font("Courier", "", 9)
            _write(line, 4.5, width=130)
            continue
        if line.startswith("- ") or line.startswith("* "):
            pdf.set_font("Helvetica", "", 10.5)
            _write("• " + line[2:].strip(), 5.5)
            continue
        if line.startswith(">"):
            pdf.set_font("Helvetica", "I", 10)
            _write(line.lstrip("> ").strip(), 5.5)
            continue

        pdf.set_font("Helvetica", "", 10.5)
        _write(line, 5.5)

    out = BytesIO()
    raw = pdf.output(dest="S")
    if isinstance(raw, str):
        out.write(raw.encode("latin-1"))
    else:
        out.write(bytes(raw))
    return out.getvalue()

