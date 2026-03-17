# 1. 파이썬 환경 설정
FROM python:3.11-slim

# 2. 한글 깨짐 방지 및 환경 변수 설정
ENV PYTHONUTF8=1
ENV PYTHONIOENCODING=utf-8
ENV PORT=8080

# 3. 시스템 패키지 설치 + non-root 사용자 생성
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && adduser --disabled-password --gecos '' appuser

# 4. 작업 디렉토리 설정
WORKDIR /app

# 5. 의존성 설치 (캐시 레이어 분리)
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. 소스 코드 복사 (분리 복사로 캐시 효율화)
COPY backend/ ./backend/
COPY frontend/ ./frontend/

# 7. 업로드 디렉토리 생성 및 소유권 이전 (appuser가 쓸 수 있도록)
RUN mkdir -p /app/backend/uploads/pdfs && chown -R appuser:appuser /app

# 8. non-root 사용자로 전환
USER appuser

# 9. 포트 노출
EXPOSE 8080

# 10. 헬스체크 (liveness — DB 비의존)
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8080/api/health || exit 1

# 11. 실행 (exec-form으로 시그널 정상 전달)
WORKDIR /app/backend
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
