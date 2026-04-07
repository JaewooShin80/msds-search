/**
 * 환경별 백엔드 URL 설정
 * Render 배포 후 RENDER_BACKEND_URL을 실제 URL로 교체하세요.
 * 예) https://msds-backend.onrender.com
 */
(function () {
    const isLocal =
        location.hostname === 'localhost' ||
        location.hostname === '127.0.0.1';

    window.__API_BASE__ = isLocal
        ? '/api'
        : 'https://msds-search.onrender.com/api';
})();
