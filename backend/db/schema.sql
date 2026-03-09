-- 카테고리 테이블
CREATE TABLE IF NOT EXISTS categories (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- MSDS 테이블
CREATE TABLE IF NOT EXISTS msds (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name  TEXT    NOT NULL,
    manufacturer  TEXT    NOT NULL,
    category      TEXT    NOT NULL,
    hazard_level  TEXT    NOT NULL CHECK(hazard_level IN ('위험', '경고', '주의')),
    cas_number    TEXT    NOT NULL DEFAULT '-',
    revision_date TEXT    NOT NULL,
    pdf_path      TEXT,
    pdf_url       TEXT,
    description   TEXT,
    keywords      TEXT    DEFAULT '[]',
    content_html  TEXT,              -- PDF 추출 내용 (HTML 형식)
    ai_analyzed   INTEGER DEFAULT 0, -- 0: 수동입력, 1: AI분석
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 검색 성능을 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_msds_category     ON msds(category);
CREATE INDEX IF NOT EXISTS idx_msds_hazard_level ON msds(hazard_level);
CREATE INDEX IF NOT EXISTS idx_msds_manufacturer ON msds(manufacturer);
