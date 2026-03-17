"""
PDF 텍스트 추출 + AI 자동 분석 서비스

AI 키 유무에 따른 동작:
  ANTHROPIC_API_KEY 설정됨  → Claude가 필드 자동 추출 후 반환
  ANTHROPIC_API_KEY 없음   → 추출 텍스트만 반환 (프론트에서 수동 입력)
"""

import json
import os
import re
from datetime import date
from html import escape as html_escape

import fitz  # PyMuPDF

from constants import CATEGORIES, HAZARD_LEVELS
from utils import configure_encoding

configure_encoding()

_CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")


# ---------- PDF 텍스트 추출 ----------

def extract_text(pdf_bytes: bytes) -> str:
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        pages = [page.get_text() for page in doc]
    return "\n".join(pages)


# ---------- 표(table) → HTML 변환 ----------

def _rows_to_html_table(rows: list) -> str:
    if not rows:
        return ""
    html = ['<table class="msds-table">']
    for i, row in enumerate(rows):
        tag = "th" if i == 0 else "td"
        cells = "".join(
            f"<{tag}>{html_escape(str(c).strip())}</{tag}>"
            for c in row
        )
        html.append(f"<tr>{cells}</tr>")
    html.append("</table>")
    return "\n".join(html)


# ---------- PDF 텍스트 + 표 통합 HTML 추출 ----------

def extract_with_tables(pdf_bytes: bytes) -> tuple[str, str]:
    """
    PDF에서 텍스트와 표를 함께 추출하여 (raw_text, content_html) 반환.
    PyMuPDF find_tables() 지원 시 표를 <table> 태그로 변환,
    미지원 시 기존 text_to_html() 방식으로 폴백.
    """
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        raw_pages = []
        html_parts = []

        for page in doc:
            raw_text = page.get_text()
            raw_pages.append(raw_text)

            try:
                detected = page.find_tables()
                if detected:
                    table_bboxes = []

                    for t in detected:
                        rows = t.extract()
                        clean_rows = [
                            [str(c).strip() if c is not None else "" for c in row]
                            for row in rows
                        ]
                        table_bboxes.append((t.bbox, _rows_to_html_table(clean_rows)))

                    blocks = page.get_text("blocks")
                    inserted = set()
                    page_html = []

                    for block in sorted(blocks, key=lambda b: b[1]):
                        bx0, by0, bx1, by1, btext, *_ = block
                        in_table = False
                        for i, (bbox, thtml) in enumerate(table_bboxes):
                            tx0, ty0, tx1, ty1 = bbox
                            if bx0 >= tx0 - 5 and by0 >= ty0 - 5 and bx1 <= tx1 + 5 and by1 <= ty1 + 5:
                                in_table = True
                                if i not in inserted:
                                    page_html.append(thtml)
                                    inserted.add(i)
                                break
                        if not in_table and btext.strip():
                            page_html.append(text_to_html(btext))

                    for i, (_, thtml) in enumerate(table_bboxes):
                        if i not in inserted:
                            page_html.append(thtml)

                    html_parts.append("\n".join(page_html))
                else:
                    html_parts.append(text_to_html(raw_text))

            except Exception:
                html_parts.append(text_to_html(raw_text))

    return "\n".join(raw_pages), "\n".join(html_parts)


# ---------- 텍스트 → HTML 변환 ----------

def text_to_html(raw: str) -> str:
    """PDF 추출 텍스트를 구조화된 HTML로 변환"""
    lines = raw.splitlines()
    html_parts: list[str] = []
    in_ul = False

    section_re = re.compile(r"^(\d{1,2})\s*[\.。]\s*(.+)")

    for line in lines:
        line = line.strip()
        if not line:
            if in_ul:
                html_parts.append("</ul>")
                in_ul = False
            continue

        m = section_re.match(line)
        if m:
            if in_ul:
                html_parts.append("</ul>")
                in_ul = False
            num, title = m.group(1), m.group(2).strip()
            html_parts.append(
                f'<h3 class="msds-section"><span class="section-num">{num}</span>'
                f'{html_escape(title)}</h3>'
            )
        elif line.startswith(("-", "·", "•", "※", "○")):
            if not in_ul:
                html_parts.append("<ul>")
                in_ul = True
            html_parts.append(f"<li>{html_escape(line[1:].strip())}</li>")
        else:
            if in_ul:
                html_parts.append("</ul>")
                in_ul = False
            html_parts.append(f"<p>{html_escape(line)}</p>")

    if in_ul:
        html_parts.append("</ul>")

    return "\n".join(html_parts)


# ---------- Claude AI 분석 ----------

_anthropic_client = None


def _get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        import anthropic  # 런타임에만 import (API 키 없을 때 오류 방지)
        _anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _anthropic_client


def _call_claude(text: str) -> dict:
    client = _get_anthropic_client()

    system_prompt = (
        "당신은 MSDS(물질안전보건자료) 전문 분석 AI입니다. "
        "사용자가 제공하는 MSDS 텍스트를 분석하여 지정된 JSON 형식으로만 응답하세요. "
        "JSON 외에 어떠한 설명도 추가하지 마세요."
    )

    user_content = (
        "아래 MSDS 텍스트를 분석하여 다음 JSON 형식으로 반환하세요:\n\n"
        "{\n"
        '  "product_name": "제품명",\n'
        '  "manufacturer": "제조사명",\n'
        f'  "category": "카테고리 ({json.dumps(CATEGORIES, ensure_ascii=False)} 중 하나)",\n'
        '  "hazard_level": "위험 또는 경고 또는 해당없음",\n'
        '  "revision_date": "YYYY-MM-DD (없으면 오늘 날짜)",\n'
        '  "description": "제품에 대한 한 줄 설명",\n'
        '  "keywords": ["키워드1", "키워드2", "키워드3"]\n'
        "}\n\n"
        "<msds_text>\n"
        f"{text[:6000]}\n"
        "</msds_text>"
    )

    msg = client.messages.create(
        model=_CLAUDE_MODEL,
        max_tokens=1024,
        system=system_prompt,
        messages=[{"role": "user", "content": user_content}],
    )

    raw_json = msg.content[0].text.strip()
    raw_json = re.sub(r"^```[a-z]*\n?", "", raw_json)
    raw_json = re.sub(r"\n?```$", "", raw_json)
    result = json.loads(raw_json)

    # 유효성 검증
    if result.get("category") not in CATEGORIES:
        result["category"] = "기타"
    if result.get("hazard_level") not in HAZARD_LEVELS:
        result["hazard_level"] = "경고"

    return result


# ---------- 메인 진입점 ----------

def analyze(pdf_bytes: bytes) -> dict:
    """
    PDF를 분석하여 결과를 반환합니다.

    반환값:
      mode        : "ai" | "manual"
      extracted   : 추출된 원문 텍스트
      content_html: HTML 변환 내용 (표 포함)
      fields      : AI가 추출한 필드 dict (mode="ai" 일 때만 채워짐)
                    mode="manual" 이면 빈 기본값
    """
    text, content_html = extract_with_tables(pdf_bytes)

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()

    if api_key:
        try:
            fields = _call_claude(text)
            if not fields.get("revision_date"):
                fields["revision_date"] = str(date.today())
            return {
                "mode": "ai",
                "extracted": text,
                "content_html": content_html,
                "fields": fields,
            }
        except Exception as e:
            return {
                "mode": "manual",
                "extracted": text,
                "content_html": content_html,
                "fields": _empty_fields(),
                "ai_error": str(e),
            }
    else:
        return {
            "mode": "manual",
            "extracted": text,
            "content_html": content_html,
            "fields": _empty_fields(),
        }


def _empty_fields() -> dict:
    return {
        "product_name": "",
        "manufacturer": "",
        "category": "",
        "hazard_level": "경고",
        "revision_date": str(date.today()),
        "description": "",
        "keywords": [],
    }
