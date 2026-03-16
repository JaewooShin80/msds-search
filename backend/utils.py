"""공용 유틸리티"""
import sys


def configure_encoding() -> None:
    """Windows CP949 인코딩 충돌 방지 — stdout/stderr를 UTF-8로 강제 지정"""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
