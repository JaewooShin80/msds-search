-- 카테고리 테이블
CREATE TABLE IF NOT EXISTS categories (
    id   SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- MSDS 테이블
CREATE TABLE IF NOT EXISTS msds (
    id            SERIAL PRIMARY KEY,
    product_name  TEXT        NOT NULL,
    manufacturer  TEXT        NOT NULL,
    category      TEXT        NOT NULL,
    hazard_level  TEXT        NOT NULL CHECK(hazard_level IN ('위험', '경고', '해당없음')),
    revision_date TEXT        NOT NULL,
    pdf_path      TEXT,
    pdf_url       TEXT,
    description   TEXT,
    keywords      TEXT        DEFAULT '[]',
    content_html  TEXT,
    ai_analyzed   INTEGER     DEFAULT 0,
    created_at    TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 검색 성능을 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_msds_category     ON msds(category);
CREATE INDEX IF NOT EXISTS idx_msds_hazard_level ON msds(hazard_level);
CREATE INDEX IF NOT EXISTS idx_msds_manufacturer ON msds(manufacturer);
