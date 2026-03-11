"""
PDF 텍스트 추출 + AI 자동 분석 서비스

AI 키 유무에 따른 동작:
  ANTHROPIC_API_KEY 설정됨  → Claude가 필드 자동 추출 후 반환
  ANTHROPIC_API_KEY 없음   → 추출 텍스트만 반환 (프론트에서 수동 입력)
"""

import json
import os
import re
import sys
import textwrap
from datetime import date

# Windows CP949 인코딩 충돌 방지
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

import fitz  # PyMuPDF

CATEGORIES = [
    "용접재료", "절단/연마", "윤활유/그리스", "연료(유류)", "시멘트류",
    "콘크리트혼화제", "콘크리트 응집제", "박리제", "품질시험", "스프레이류",
    "가스류", "요소수", "부동액", "경화제", "몰탈/접착제", "발파/폭약류", "기타",
]

# ---------- PDF 텍스트 추출 ----------

def extract_text(pdf_bytes: bytes) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = [page.get_text() for page in doc]
    doc.close()
    return "\n".join(pages)


# ---------- 표(table) → HTML 변환 ----------

def _rows_to_html_table(rows: list) -> str:
    if not rows:
        return ""
    html = ['<table class="msds-table">']
    for i, row in enumerate(rows):
        tag = "th" if i == 0 else "td"
        cells = "".join(
            f"<{tag}>{str(c).strip().replace('<', '&lt;').replace('>', '&gt;')}</{tag}>"
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
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    raw_pages = []
    html_parts = []

    for page in doc:
        raw_text = page.get_text()
        raw_pages.append(raw_text)

        try:
            detected = page.find_tables()
            if detected:
                # 표가 있는 경우: 표와 텍스트를 위치 기반으로 합성
                table_bboxes = []

                for t in detected:
                    rows = t.extract()
                    clean_rows = [
                        [str(c).strip() if c is not None else "" for c in row]
                        for row in rows
                    ]
                    table_bboxes.append((t.bbox, _rows_to_html_table(clean_rows)))

                # 페이지 텍스트 블록을 y 좌표 순으로 처리 (표 영역 블록 제외)
                blocks = page.get_text("blocks")  # (x0,y0,x1,y1,text,block_no,type)
                inserted = set()
                page_html = []

                for block in sorted(blocks, key=lambda b: b[1]):
                    bx0, by0, bx1, by1, btext, *_ = block
                    # 해당 블록이 표 영역 안에 있으면 스킵
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

                # 삽입 안 된 표 추가
                for i, (_, thtml) in enumerate(table_bboxes):
                    if i not in inserted:
                        page_html.append(thtml)

                html_parts.append("\n".join(page_html))
            else:
                html_parts.append(text_to_html(raw_text))

        except Exception:
            # find_tables 미지원 버전 폴백
            html_parts.append(text_to_html(raw_text))

    doc.close()
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
            html_parts.append(f'<h3 class="msds-section"><span class="section-num">{num}</span>{title}</h3>')
        elif line.startswith(("-", "·", "•", "※", "○")):
            if not in_ul:
                html_parts.append("<ul>")
                in_ul = True
            html_parts.append(f"<li>{line[1:].strip()}</li>")
        else:
            if in_ul:
                html_parts.append("</ul>")
                in_ul = False
            escaped = line.replace("<", "&lt;").replace(">", "&gt;")
            html_parts.append(f"<p>{escaped}</p>")

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

    prompt = textwrap.dedent(f"""
        다음은 MSDS(물질안전보건자료) 문서에서 추출한 텍스트입니다.
        내용을 분석하여 아래 JSON 형식으로 반환하세요.
        JSON 외에 어떠한 설명도 추가하지 마세요.

        반환 형식:
        {{
          "product_name": "제품명",
          "manufacturer": "제조사명",
          "category": "카테고리 (아래 목록 중 하나)",
          "hazard_level": "위험 또는 경고 또는 주의",
          "cas_number": "CAS 번호 (없으면 -)",
          "revision_date": "YYYY-MM-DD (없으면 오늘 날짜)",
          "description": "제품에 대한 한 줄 설명",
          "keywords": ["키워드1", "키워드2", "키워드3"]
        }}

        카테고리 목록: {json.dumps(CATEGORIES, ensure_ascii=False)}

        MSDS 텍스트:
        {text[:6000]}
    """)

    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_json = msg.content[0].text.strip()
    # 마크다운 코드블록 제거
    raw_json = re.sub(r"^```[a-z]*\n?", "", raw_json)
    raw_json = re.sub(r"\n?```$", "", raw_json)
    return json.loads(raw_json)


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
            # 날짜 기본값 보정
            if not fields.get("revision_date"):
                fields["revision_date"] = str(date.today())
            return {
                "mode": "ai",
                "extracted": text,
                "content_html": content_html,
                "fields": fields,
            }
        except Exception as e:
            # AI 실패 시 수동 모드로 graceful fallback
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
        "cas_number": "-",
        "revision_date": str(date.today()),
        "description": "",
        "keywords": [],
    }
