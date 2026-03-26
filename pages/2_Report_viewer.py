"""
Full-page HTML report preview and downloads.

Appears in the sidebar when you run `streamlit run streamlit_app.py` from the repository root.
"""

from __future__ import annotations

import base64
import io
import re
import zipfile
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
from eda_llm_assistant.pdf_export import markdown_to_pdf_bytes

_MAIN_PAGE = "streamlit_app.py"

_LIGHT_SURFACE_INJECTION = """<style id="eda-llm-forced-light">
  html, body { background: #ffffff !important; color: #1a1a1a !important; }
  table td { background: #ffffff !important; color: #1a1a1a !important; }
  table th { background: #f0f3f6 !important; color: #1a1a1a !important; }
  h1, h2, h3, h4, p, li { color: #1a1a1a !important; }
</style>"""


def _inject_light_surface(html: str) -> str:
    """Force light background/text for embedded preview (works for older report.html too)."""
    if not html.strip():
        return html
    low = html.lower()
    if "<head" in low:
        return re.sub(r"(<head[^>]*>)", r"\1" + _LIGHT_SURFACE_INJECTION, html, count=1, flags=re.I)
    return _LIGHT_SURFACE_INJECTION + html


def _mime_for_image(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".png":
        return "image/png"
    if ext in (".jpg", ".jpeg"):
        return "image/jpeg"
    if ext == ".gif":
        return "image/gif"
    if ext == ".webp":
        return "image/webp"
    if ext == ".svg":
        return "image/svg+xml"
    return "application/octet-stream"


def _inline_img_srcs(html: str, report_dir: Path) -> str:
    """Turn relative image paths into data URLs so charts render inside Streamlit's iframe."""

    def replace_one(m: re.Match[str]) -> str:
        quote = m.group(1)
        src = m.group(2)
        if src.startswith(("http://", "https://", "data:")):
            return m.group(0)
        clean = src.split("?", 1)[0].split("#", 1)[0]
        if not clean:
            return m.group(0)
        img_path = (report_dir / clean).resolve()
        try:
            img_path.relative_to(report_dir.resolve())
        except ValueError:
            return m.group(0)
        if not img_path.is_file():
            return m.group(0)
        mime = _mime_for_image(img_path)
        b64 = base64.b64encode(img_path.read_bytes()).decode("ascii")
        return f"src={quote}data:{mime};base64,{b64}{quote}"

    return re.sub(r'src=(["\'])(?!(?:https?:|data:))([^"\']+)\1', replace_one, html)


st.set_page_config(page_title="Report viewer", page_icon="📄", layout="wide")
if st.button("← Back to EDA Report Studio", key="report_viewer_back"):
    st.switch_page(_MAIN_PAGE)

st.title("Report viewer")
st.caption(
    "Embedded preview of the latest HTML report. Images are inlined as data URLs so plots display inside this page."
)

out_root_s = st.session_state.get("last_out_root")
if not out_root_s:
    st.warning(
        "No report in this session yet. Go to **EDA Report Studio**, upload data, and click **Generate report**."
    )
    st.stop()

root = Path(out_root_s)
out_path = root / "outputs"
html_p = out_path / "report.html"
md_p = out_path / "report.md"

if not html_p.is_file():
    st.error("`outputs/report.html` was not found. Return to the studio and generate the report again.")
    st.stop()

html_raw = html_p.read_text(encoding="utf-8")
html_preview = _inline_img_srcs(_inject_light_surface(html_raw), html_p.parent)

st.subheader("Preview")
components.html(html_preview, height=1100, scrolling=True)

st.subheader("Download")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.download_button(
        "Download HTML",
        data=html_p.read_bytes(),
        file_name="eda_report.html",
        mime="text/html",
        use_container_width=True,
    )
with c2:
    if md_p.is_file():
        st.download_button(
            "Download Markdown",
            data=md_p.read_bytes(),
            file_name="eda_report.md",
            mime="text/markdown",
            use_container_width=True,
        )
    else:
        st.caption("Markdown was not generated (enable saving Markdown in `ReportConfig`).")
with c3:
    if md_p.is_file():
        try:
            pdf_bytes = markdown_to_pdf_bytes(md_p.read_text(encoding="utf-8"), title="EDA Report", base_dir=md_p.parent)
            st.download_button(
                "Download PDF",
                data=pdf_bytes,
                file_name="eda_report.pdf",
                mime="application/pdf",
                use_container_width=True,
            )
        except Exception as e:
            st.caption(f"PDF export unavailable: {e}")
with c4:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in out_path.rglob("*"):
            if f.is_file():
                zf.write(f, arcname=f.relative_to(out_path))
    buf.seek(0)
    st.download_button(
        "Download ZIP (HTML + assets)",
        data=buf.getvalue(),
        file_name="eda_outputs.zip",
        mime="application/zip",
        use_container_width=True,
    )

st.info(
    "A standalone HTML file expects an `assets/` folder next to it. To share one download, use the **ZIP**."
)
