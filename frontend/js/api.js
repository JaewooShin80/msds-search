/**
 * API 호출 모듈
 */

const BASE = '/api';

/** XSS 방지용 HTML 이스케이프 */
function escapeHtml(str) {
    if (str == null) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function getAdminToken() {
    return sessionStorage.getItem('adminToken') || '';
}

async function request(url, options = {}) {
    const token = getAdminToken();
    const headers = { ...(options.headers || {}) };
    if (token) headers['X-Admin-Token'] = token;

    const res = await fetch(url, { ...options, headers });
    if (!res.ok) {
        const err = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(err.detail || err.error || '서버 오류가 발생했습니다.');
    }
    return res.json();
}

const api = {
    // ===== 관리자 인증 =====
    adminLogin(admin_id, admin_pw) {
        return request(`${BASE}/admin/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ admin_id, admin_pw }),
        });
    },
    adminVerify() {
        return request(`${BASE}/admin/verify`);
    },

    // ===== 메타 =====
    getStats:        () => request(`${BASE}/stats`),
    getCategories:   () => request(`${BASE}/categories`),
    getHazardLevels: () => request(`${BASE}/hazard-levels`),
    getManufacturers:() => request(`${BASE}/manufacturers`),
    getAiStatus:     () => request(`${BASE}/ai-status`),
    getDashboard:    () => request(`${BASE}/dashboard`),

    // ===== MSDS 조회 =====
    getMSDS({ q, category, hazard, manufacturer, page, page_size } = {}) {
        const params = new URLSearchParams();
        if (q)            params.set('q', q);
        if (category)     params.set('category', category);
        if (hazard)       params.set('hazard', hazard);
        if (manufacturer) params.set('manufacturer', manufacturer);
        if (page)         params.set('page', page);
        if (page_size)    params.set('page_size', page_size);
        const qs = params.toString();
        return request(`${BASE}/msds${qs ? '?' + qs : ''}`);
    },
    getMSDSById: (id) => request(`${BASE}/msds/${id}`),
    downloadUrl: (id) => `${BASE}/msds/${id}/download`,

    // ===== 관리자 전용 (쓰기) =====
    analyzePDF(file) {
        const fd = new FormData();
        fd.append('pdf', file);
        return request(`${BASE}/msds/analyze`, { method: 'POST', body: fd });
    },
    createMSDS(formData) {
        return request(`${BASE}/msds`, { method: 'POST', body: formData });
    },
    updateMSDS: (id, fd) => request(`${BASE}/msds/${id}`, { method: 'PUT', body: fd }),
    deleteMSDS: (id)     => request(`${BASE}/msds/${id}`, { method: 'DELETE' }),
    bulkUpload(files) {
        const fd = new FormData();
        for (const file of files) fd.append('pdfs', file);
        return request(`${BASE}/msds/bulk-upload`, { method: 'POST', body: fd });
    },
    reanalyzePending() {
        return request(`${BASE}/msds/reanalyze-pending`, { method: 'POST' });
    },
};
