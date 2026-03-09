"""
2단계: analysis_queue/ JSON → content_html 생성 → analysis_results/ 저장

JSON에는 이미 pages[].tables (2D rows)와 pages[].text가 있으므로
HTML 변환만 수행 (추가 API 호출 없음)
"""
import sys
import json
import re
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR    = Path(__file__).parent
QUEUE_DIR   = BASE_DIR / "analysis_queue"
RESULT_DIR  = BASE_DIR / "analysis_results"
RESULT_DIR.mkdir(exist_ok=True)


# ── HTML 변환 함수 ────────────────────────────────────────

def rows_to_html_table(rows: list) -> str:
    if not rows:
        return ""
    html = ['<table class="msds-table">']
    for i, row in enumerate(rows):
        tag = "th" if i == 0 else "td"
        cells = "".join(
            f"<{tag}>{str(c).strip().replace('<','&lt;').replace('>','&gt;').replace(chr(10),' ')}</{tag}>"
            for c in row
        )
        html.append(f"<tr>{cells}</tr>")
    html.append("</table>")
    return "\n".join(html)


def text_to_html(raw: str) -> str:
    lines = raw.splitlines()
    html_parts = []
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
                f'<h3 class="msds-section"><span class="section-num">{num}</span>{title}</h3>'
            )
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


def build_content_html(pages: list) -> str:
    """pages 데이터에서 표 + 텍스트를 합쳐 HTML 생성"""
    html_parts = []

    for page in pages:
        text   = page.get("text", "")
        tables = page.get("tables", [])

        if not tables:
            # 표 없음 → 텍스트만 변환
            if text.strip():
                html_parts.append(text_to_html(text))
            continue

        # 표가 있는 경우: 표의 bbox 기준으로 텍스트 사이에 삽입
        # (JSON에는 bbox 정보 있음)
        table_map = {}  # bbox_y0 → html
        for t in tables:
            bbox  = t.get("bbox", [0, 0, 0, 0])
            rows  = t.get("rows", [])
            ty0   = bbox[1]
            table_map[ty0] = (bbox, rows_to_html_table(rows))

        # 텍스트 라인을 y 좌표 순으로 처리하기 어려우므로
        # 텍스트 전체를 섹션으로 분리하고 표를 적절한 위치에 삽입
        # 단순 전략: 텍스트 블록 후 표 삽입 (순서 유지)
        page_parts = []
        if text.strip():
            page_parts.append(text_to_html(text))
        for ty0 in sorted(table_map.keys()):
            _, thtml = table_map[ty0]
            page_parts.append(thtml)

        html_parts.append("\n".join(page_parts))

    return "\n".join(html_parts)


# ── 메인 ─────────────────────────────────────────────────

def main():
    queue_files = sorted(QUEUE_DIR.glob("*.json"), key=lambda f: int(f.stem))
    total = len(queue_files)
    print(f"처리 대상: {total}개\n")

    ok = skip = fail = 0

    for qf in queue_files:
        msds_id = int(qf.stem)
        result_path = RESULT_DIR / f"{msds_id}.json"

        if result_path.exists():
            print(f"[{msds_id:3d}] SKIP (이미 완료)")
            skip += 1
            continue

        try:
            data = json.loads(qf.read_text(encoding="utf-8"))
            pages = data.get("pages", [])

            content_html = build_content_html(pages)

            table_count = sum(len(p.get("tables", [])) for p in pages)

            result = {
                "id":           msds_id,
                "product_name": data.get("product_name", ""),
                "content_html": content_html,
                "table_count":  table_count,
            }
            result_path.write_text(
                json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
            )

            html_len = len(content_html)
            print(f"[{msds_id:3d}] OK  {data['product_name']}  "
                  f"표 {table_count}개 / HTML {html_len:,}자")
            ok += 1

        except Exception as e:
            print(f"[{msds_id:3d}] FAIL → {e}")
            fail += 1

    print(f"\n완료: 성공 {ok}개 / 스킵 {skip}개 / 실패 {fail}개")
    print(f"→ analysis_results/ 에 {ok + skip}개 JSON 저장됨")


if __name__ == "__main__":
    main()
