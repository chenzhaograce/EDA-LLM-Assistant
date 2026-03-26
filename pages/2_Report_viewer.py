"""
Full-page HTML report preview + downloads.

Streamlit discovers this file automatically under `pages/`.
From the sidebar, open “Report viewer” after generating a report on the home page.
"""

from __future__ import annotations

import base64
import io
import re
import zipfile
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

_ROOT = Path(__file__).resolve().parent.parent


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
    """Replace relative image paths with data URLs so previews work inside Streamlit's iframe."""

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
st.title("Report viewer")
st.caption("内嵌预览最近一次生成的 HTML 报告（图表已内嵌为 data URL，便于在页面里显示）。")

out_root_s = st.session_state.get("last_out_root")
if not out_root_s:
    st.warning("请先在 **EDA Report Studio** 首页上传数据并点击 **Generate report**。")
    st.stop()

root = Path(out_root_s)
out_path = root / "outputs"
html_p = out_path / "report.html"
md_p = out_path / "report.md"

if not html_p.is_file():
    st.error("找不到 `outputs/report.html`。请返回首页重新生成报告。")
    st.stop()

html_raw = html_p.read_text(encoding="utf-8")
html_preview = _inline_img_srcs(html_raw, html_p.parent)

st.subheader("预览")
components.html(html_preview, height=1100, scrolling=True)

st.subheader("下载")
c1, c2, c3 = st.columns(3)
with c1:
    st.download_button(
        "下载 HTML",
        data=html_p.read_bytes(),
        file_name="eda_report.html",
        mime="text/html",
        use_container_width=True,
    )
with c2:
    if md_p.is_file():
        st.download_button(
            "下载 Markdown",
            data=md_p.read_bytes(),
            file_name="eda_report.md",
            mime="text/markdown",
            use_container_width=True,
        )
    else:
        st.caption("未生成 Markdown（可在配置里开启保存）。")
with c3:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in out_path.rglob("*"):
            if f.is_file():
                zf.write(f, arcname=f.relative_to(out_path))
    buf.seek(0)
    st.download_button(
        "下载 ZIP（HTML + 图表等）",
        data=buf.getvalue(),
        file_name="eda_outputs.zip",
        mime="application/zip",
        use_container_width=True,
    )

st.info(
    "单独下载的 HTML 依赖同目录下的 `assets/` 图片；若只发一个文件给别人，**ZIP** 最稳妥。"
)
