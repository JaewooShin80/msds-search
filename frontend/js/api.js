/**
 * API 호출 모듈
 */

const BASE = '/api';

async function request(url, options = {}) {
    const res = await fetch(url, options);
    if (!res.ok) {
        const err = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(err.error || '서버 오류가 발생했습니다.');
    }
    return res.json();
}

const api = {
    getStats:        () => request(`${BASE}/stats`),
    getCategories:   () => request(`${BASE}/categories`),
    getHazardLevels: () => request(`${BASE}/hazard-levels`),
    getManufacturers:() => request(`${BASE}/manufacturers`),
    getAiStatus:     () => request(`${BASE}/ai-status`),
    getDashboard:    () => request(`${BASE}/dashboard`),

    getMSDS({ q, category, hazard, manufacturer } = {}) {
        const params = new URLSearchParams();
        if (q)            params.set('q', q);
        if (category)     params.set('category', category);
        if (hazard)       params.set('hazard', hazard);
        if (manufacturer) params.set('manufacturer', manufacturer);
        const qs = params.toString();
        return request(`${BASE}/msds${qs ? '?' + qs : ''}`);
    },

    getMSDSById: (id) => request(`${BASE}/msds/${id}`),

    // PDF 분석 (FormData)
    analyzePDF(file) {
        const fd = new FormData();
        fd.append('pdf', file);
        return request(`${BASE}/msds/analyze`, { method: 'POST', body: fd });
    },

    // Google Drive URL로 PDF 분석
    analyzeGDrive(url) {
        const fd = new FormData();
        fd.append('gdrive_url', url);
        return request(`${BASE}/msds/analyze-gdrive`, { method: 'POST', body: fd });
    },

    // MSDS 등록 (FormData)
    createMSDS(formData) {
        return request(`${BASE}/msds`, { method: 'POST', body: formData });
    },

    // 다운로드 URL
    downloadUrl: (id) => `${BASE}/msds/${id}/download`,

    updateMSDS: (id, fd) => request(`${BASE}/msds/${id}`, { method: 'PUT', body: fd }),
    deleteMSDS: (id)     => request(`${BASE}/msds/${id}`, { method: 'DELETE' }),

    // 로컬 다중 파일 일괄 업로드
    bulkUpload(files) {
        const fd = new FormData();
        for (const file of files) {
            fd.append('pdfs', file);
        }
        return request(`${BASE}/msds/bulk-upload`, { method: 'POST', body: fd });
    },

    // Google Drive 폴더 → GCS 일괄 업로드
    importGDriveFolder(url) {
        const fd = new FormData();
        fd.append('gdrive_folder_url', url);
        return request(`${BASE}/msds/import-gdrive-folder`, { method: 'POST', body: fd });
    },
};
