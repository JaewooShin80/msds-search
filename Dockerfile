# 1. 파이썬 환경 설정
FROM python:3.11-slim

# 2. 한글 깨짐 방지 및 환경 변수 설정
ENV PYTHONUTF8=1
ENV PYTHONIOENCODING=utf-8
ENV PORT=8080

# 3. non-root 사용자 생성
RUN adduser --disabled-password --gecos '' appuser

# 4. 작업 디렉토리 설정
WORKDIR /app

# 5. 의존성 설치 (캐시 레이어 분리)
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. 소스 코드 복사 (분리 복사로 캐시 효율화)
COPY backend/ ./backend/
COPY frontend/ ./frontend/

# 7. non-root 사용자로 전환
USER appuser

# 8. 포트 노출
EXPOSE 8080

# 9. 헬스체크
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/health')" || exit 1

# 10. 실행 (exec-form으로 시그널 정상 전달)
WORKDIR /app/backend
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
