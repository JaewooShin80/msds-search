# 1. 파이썬 환경 설정
FROM python:3.11-slim

# 2. 한글 깨짐 방지 및 환경 변수 설정
ENV PYTHONUTF8=1
ENV PYTHONIOENCODING=utf-8
ENV PORT=8080

# 3. 작업 디렉토리 설정
WORKDIR /app

# 4. 필수 라이브러리 설치 (경로: backend/requirements.txt)
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 소스 코드 복사 (전체 복사)
COPY . .

# 6. 실행 (uvicorn)
WORKDIR /app/backend
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
