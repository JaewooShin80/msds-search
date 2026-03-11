# MSDS 검색 시스템 — 프로젝트 컨텍스트

## 프로젝트 개요

MSDS(물질안전보건자료) 웹 기반 검색 및 관리 시스템.

- **Backend:** FastAPI (Python 3.11) + SQLite3 → Cloud SQL(PostgreSQL) 전환 예정
- **Frontend:** Vanilla HTML5/CSS/JavaScript (프레임워크 없음)
- **Cloud:** Google Cloud Run, Google Cloud Storage (GCS), Anthropic Claude AI
- **배포:** Docker + Cloud Build (asia-northeast3)
- **GCS 버킷:** `msdsdata`
- **Cloud Run 서비스명:** `msds-service`

---

## 아키텍처

```
Frontend (정적 HTML/JS)
    ↓ /api/*
FastAPI (Cloud Run)
    ├── SQLite → Cloud SQL PostgreSQL (마이그레이션 진행 중)
    ├── GCS (PDF 저장: pdfs/{uuid}.pdf)
    ├── Google Drive (PDF 소스 import)
    └── Anthropic Claude Sonnet 4.6 (PDF 자동 분석)
```

---

## 핵심 파일 구조

```
infra-msds-pjt/
├── Dockerfile
├── cloudbuild.yaml               # Cloud Build CI/CD
├── backend/
│   ├── main.py                   # FastAPI 앱 진입점, CORS, 정적파일 마운트
│   ├── requirements.txt
│   ├── routers/
│   │   ├── msds.py               # MSDS CRUD + 업로드 + 일괄 등록 (핵심 라우터)
│   │   └── meta.py               # 통계/카테고리/대시보드 API
│   ├── services/
│   │   ├── analyzer.py           # PyMuPDF 텍스트 추출 + Claude AI 분석
│   │   ├── gcs.py                # GCS 업로드/다운로드
│   │   └── gdrive.py             # Google Drive 파일 접근 (lazy import)
│   └── db/
│       ├── database.py           # DB 연결 (현재 SQLite → PostgreSQL로 전환 예정)
│       ├── schema.sql            # 테이블 정의
│       └── seed.py               # 초기 데이터 10건 삽입
└── frontend/
    ├── index.html
    ├── dashboard.html
    ├── js/
    │   ├── api.js                # fetch 래퍼
    │   ├── app.js                # 메인 앱 로직
    │   └── dashboard.js          # 대시보드 차트
    └── css/style.css
```

---

## DB 스키마 요약

```sql
categories (id SERIAL PK, name TEXT UNIQUE)
msds (
    id SERIAL PK,
    product_name, manufacturer, category, hazard_level CHECK('위험'|'경고'|'주의'),
    cas_number, revision_date, pdf_path (GCS 경로), pdf_url (외부 URL),
    description, keywords (JSON 배열), content_html (PDF → HTML),
    ai_analyzed (0=수동, 1=AI), created_at, updated_at
)
```

---

## Cloud SQL(PostgreSQL) 마이그레이션 플랜

> **배경:** SQLite는 Cloud Run ephemeral 파일시스템에 저장되어 재배포/스케일링 시
> 사용자 등록 데이터가 유실됨. Cloud SQL로 전환하여 영속성 확보.

### GCP 인프라 (1회 실행)

```bash
# 1. Cloud SQL PostgreSQL 15 인스턴스 생성
gcloud sql instances create msds-db \
  --database-version=POSTGRES_15 \
  --tier=db-f1-micro \
  --region=asia-northeast3 \
  --storage-size=10GB \
  --storage-auto-increase \
  --no-assign-ip \
  --enable-google-private-path

# 2. DB 및 사용자 생성
gcloud sql databases create msds --instance=msds-db
gcloud sql users create msds_user --instance=msds-db --password=SECURE_PASSWORD_HERE

# 3. Cloud Run 서비스 계정에 Cloud SQL Client 권한 부여
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:$(gcloud projects describe YOUR_PROJECT_ID \
    --format='value(projectNumber)')-compute@developer.gserviceaccount.com" \
  --role="roles/cloudsql.client"

# 4. 연결 이름 확인
gcloud sql instances describe msds-db --format="value(connectionName)"
# 출력 예: YOUR_PROJECT_ID:asia-northeast3:msds-db

# 5. DATABASE_URL을 Secret Manager에 저장
echo -n "postgresql://msds_user:SECURE_PASSWORD_HERE@/msds?host=/cloudsql/YOUR_PROJECT_ID:asia-northeast3:msds-db" | \
  gcloud secrets create DATABASE_URL --data-file=-

# 6. Secret 접근 권한 부여
gcloud secrets add-iam-policy-binding DATABASE_URL \
  --member="serviceAccount:$(gcloud projects describe YOUR_PROJECT_ID \
    --format='value(projectNumber)')-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### SQLite → PostgreSQL 변환 포인트

| 항목 | SQLite (현재) | PostgreSQL (변경 후) |
|------|--------------|---------------------|
| 파라미터 | `?` | `%s` |
| Auto increment | `INTEGER PRIMARY KEY AUTOINCREMENT` | `SERIAL PRIMARY KEY` |
| Upsert | `INSERT OR IGNORE` | `INSERT ... ON CONFLICT DO NOTHING` |
| 날짜 포맷 | `strftime('%Y-%m', col)` | `TO_CHAR(col, 'YYYY-MM')` |
| lastrowid | `cur.lastrowid` | `INSERT ... RETURNING id` |
| Dict Row | `sqlite3.Row` | `psycopg2.extras.RealDictCursor` |
| PRAGMA | `PRAGMA journal_mode=WAL` | 제거 |

### 코드 변경 대상 파일 (7개)

1. `backend/requirements.txt` — `psycopg2-binary` 추가
2. `backend/db/database.py` — psycopg2 연결 + generator dependency
3. `backend/db/schema.sql` — DDL PostgreSQL 문법으로 변환
4. `backend/db/seed.py` — INSERT OR IGNORE → ON CONFLICT, `?` → `%s`
5. `backend/routers/msds.py` — `?` → `%s` (35곳+), lastrowid → RETURNING id (4곳), cursor 패턴 변경
6. `backend/routers/meta.py` — `strftime` → `TO_CHAR`, cursor 패턴 변경
7. `cloudbuild.yaml` — `--add-cloudsql-instances`, `DATABASE_URL` Secret 주입

### cloudbuild.yaml 추가 인자 (배포 시)

```yaml
- '--add-cloudsql-instances'
- 'YOUR_PROJECT_ID:asia-northeast3:msds-db'
- '--set-secrets'
- 'ANTHROPIC_API_KEY=ANTHROPIC_API_KEY:latest,DATABASE_URL=DATABASE_URL:latest'
```

---

## 알려진 이슈 및 수정 계획

### Critical (즉시 수정 필요)
- **C1. 인증 없음** — Cloud IAP 또는 API key middleware 적용 필요
- **C2. XSS — content_html** — `contentEl.innerHTML = m.content_html` → DOMPurify 적용
- **C3. XSS — 카드/모달 innerHTML** — `escapeHtml()` 유틸리티 작성 후 적용

### High
- **H1. DB 커넥션 누수** — `get_connection()`을 generator dependency로 전환 (Cloud SQL 전환 시 함께 수정)
- **H2. 연산자 우선순위 버그** — `msds.py:64` `and/or` 조건식 괄호 추가
- **H3. SSL verify=False** — `msds.py:201,663` `verify=False` 제거
- **H4. 업로드 크기 무제한** — magic bytes 검증 + 크기 제한 추가
- **H5. API 에러 필드명 불일치** — `api.js:10` `err.error` → `err.detail || err.error`
- **H6. analyze() 이벤트 루프 블로킹** — 7곳 `asyncio.to_thread` 래핑

### Medium
- `.dockerignore` 생성
- GCS 전체 메모리 로드 → lazy iteration
- `update` 엔드포인트 falsy check → `is not None` 통일
- keywords JSON 에러 처리 (JSONDecodeError → 422)
- Anthropic 클라이언트 싱글턴
- GCS input 하드코딩 값 제거 (`index.html:321`)

---

## 환경변수 (로컬 .env)

```
HOST=0.0.0.0
PORT=8000
DB_PATH=./db/msds.db          # Cloud SQL 전환 시 DATABASE_URL로 대체
UPLOAD_DIR=./uploads/pdfs
FRONTEND_DIR=../frontend
PYTHONUTF8=1
PYTHONIOENCODING=utf-8
ANTHROPIC_API_KEY=             # Claude AI 분석용
# DATABASE_URL=postgresql://...  # Cloud SQL 전환 후 추가
```

## 환경변수 (Cloud Run)

```
PYTHONUTF8=1
PYTHONIOENCODING=utf-8
GCS_BUCKET=msdsdata
ANTHROPIC_API_KEY  ← Secret Manager (ANTHROPIC_API_KEY:latest)
DATABASE_URL       ← Secret Manager (DATABASE_URL:latest) — 전환 후 추가
```
