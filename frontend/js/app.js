/**
 * MSDS 검색 시스템 - 메인 앱
 */

const state = {
    allMSDS: [],
    filteredMSDS: [],
    selectedCategories: new Set(),
    selectedHazards: new Set(),
    selectedManufacturers: new Set(),
    searchQuery: '',
    currentView: 'grid',
    pendingPdfFile: null,    // 업로드 대기 중인 파일
    pendingGDriveUrl: null,  // Google Drive URL
};

// ========== 초기화 ==========
document.addEventListener('DOMContentLoaded', async () => {
    try {
        await initializeApp();
    } catch (err) {
        showError('서버 연결에 실패했습니다. 백엔드 서버를 확인하세요.');
        console.error(err);
    }
});

async function initializeApp() {
    showLoading(true);

    const [stats, msdsData, categories, hazardLevels, manufacturers] = await Promise.all([
        api.getStats(),
        api.getMSDS(),
        api.getCategories(),
        api.getHazardLevels(),
        api.getManufacturers(),
    ]);

    state.allMSDS = msdsData;
    state.filteredMSDS = msdsData;

    document.getElementById('totalCount').textContent = stats.total;
    document.getElementById('categoryCount').textContent = stats.categoryCount;

    initializeFilters(categories, hazardLevels, manufacturers);
    renderCards();
    updateStats();
    registerEventListeners();

    showLoading(false);

    // URL 파라미터로 id가 전달되면 해당 MSDS 모달 자동 오픈
    const urlParams = new URLSearchParams(window.location.search);
    const urlId = urlParams.get('id');
    if (urlId) {
        openPDFModal(parseInt(urlId, 10));
    }
}

// ========== 로딩 / 에러 ==========
function showLoading(show) {
    let el = document.getElementById('loadingOverlay');
    if (!el) {
        el = document.createElement('div');
        el.id = 'loadingOverlay';
        el.innerHTML = `<div class="loading-spinner"><i class="fas fa-spinner fa-spin"></i> 로딩 중...</div>`;
        el.style.cssText = 'position:fixed;inset:0;background:rgba(255,255,255,.75);display:flex;align-items:center;justify-content:center;font-size:1.2rem;color:#1e3a8a;z-index:9999;';
        document.body.appendChild(el);
    }
    el.style.display = show ? 'flex' : 'none';
}

function showError(msg) {
    showLoading(false);
    const c = document.getElementById('cardsContainer');
    if (c) c.innerHTML = `<div style="grid-column:1/-1;text-align:center;padding:3rem;color:#dc2626;"><i class="fas fa-exclamation-circle fa-2x"></i><p style="margin-top:1rem;">${msg}</p></div>`;
}

// ========== 필터 초기화 ==========
function initializeFilters(categories, hazardLevels, manufacturers) {
    const catList = document.getElementById('categoryFilterList');
    categories.forEach(({ name, count }) => catList.appendChild(createFilterCheckbox(name, count, 'category')));

    const hazList = document.getElementById('hazardFilterList');
    hazardLevels.forEach(({ name, count }) => hazList.appendChild(createFilterCheckbox(name, count, 'hazard')));

    const mfrList = document.getElementById('manufacturerFilterList');
    manufacturers.forEach(({ name, count }) => mfrList.appendChild(createFilterCheckbox(name, count, 'manufacturer')));
}

function createFilterCheckbox(label, count, type) {
    const div = document.createElement('div');
    div.className = 'filter-checkbox';
    div.innerHTML = `
        <input type="checkbox" id="${type}-${label}" value="${label}" data-type="${type}">
        <label for="${type}-${label}">${label}</label>
        <span class="filter-count">${count}</span>
    `;
    const cb = div.querySelector('input');
    cb.addEventListener('change', function() {
        handleFilterChange(type, label, this.checked);
        div.classList.toggle('active', this.checked);
    });
    return div;
}

// ========== 이벤트 등록 ==========
function registerEventListeners() {
    // 검색
    document.getElementById('searchInput').addEventListener('input', debounce(handleSearch, 300));
    document.getElementById('clearSearch').addEventListener('click', () => {
        document.getElementById('searchInput').value = '';
        state.searchQuery = '';
        document.getElementById('clearSearch').style.display = 'none';
        applyFilters();
    });
    document.getElementById('resetFilters').addEventListener('click', resetFilters);

    // 필터 토글
    document.querySelectorAll('.toggle-filter').forEach(btn => {
        btn.addEventListener('click', function() {
            document.getElementById(this.dataset.target).classList.toggle('collapsed');
            this.classList.toggle('active');
        });
    });

    // 뷰 전환
    document.querySelectorAll('.view-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.view-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            document.getElementById('cardsContainer').classList.toggle('list-view', this.dataset.view === 'list');
            state.currentView = this.dataset.view;
        });
    });

    // PDF 모달 이벤트 위임
    document.getElementById('cardsContainer').addEventListener('click', e => {
        const btn = e.target.closest('.btn-pdf');
        if (btn) openPDFModal(parseInt(btn.closest('.msds-card').dataset.id, 10));
    });

    // 뷰어 탭 전환
    document.querySelectorAll('.vtab').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.vtab').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            const tab = this.dataset.tab;
            document.getElementById('tabContent').style.display = tab === 'content' ? 'block' : 'none';
            document.getElementById('tabPdf').style.display    = tab === 'pdf'     ? 'block' : 'none';
        });
    });

    // PDF 모달 닫기
    ['closeModal', 'closeModalBtn'].forEach(id => document.getElementById(id).addEventListener('click', closeModal));
    document.querySelector('#pdfModal .modal-overlay').addEventListener('click', closeModal);

    // MSDS 등록 모달
    document.getElementById('uploadBtn').addEventListener('click', openUploadModal);
    document.getElementById('closeUploadModal').addEventListener('click', closeUploadModal);
    document.getElementById('cancelUploadBtn').addEventListener('click', closeUploadModal);
    document.querySelector('#uploadModal .modal-overlay').addEventListener('click', closeUploadModal);
    document.getElementById('submitMsdsBtn').addEventListener('click', submitMsds);

    // 파일 선택
    document.getElementById('pdfFileInput').addEventListener('change', e => {
        if (e.target.files[0]) handleFileSelected(e.target.files[0]);
    });

    // 드래그 앤 드롭
    const dz = document.getElementById('dropZone');
    dz.addEventListener('dragover',  e => { e.preventDefault(); dz.classList.add('dragover'); });
    dz.addEventListener('dragleave', () => dz.classList.remove('dragover'));
    dz.addEventListener('drop', e => {
        e.preventDefault();
        dz.classList.remove('dragover');
        const file = e.dataTransfer.files[0];
        if (file && file.type === 'application/pdf') handleFileSelected(file);
        else alert('PDF 파일만 업로드 가능합니다.');
    });

    // Google Drive URL 가져오기 (개별 파일)
    document.getElementById('gdriveSubmitBtn').addEventListener('click', handleGDriveSubmit);
    document.getElementById('gdriveUrlInput').addEventListener('keydown', e => {
        if (e.key === 'Enter') { e.preventDefault(); handleGDriveSubmit(); }
    });

    // Google Drive 폴더 일괄 업로드
    document.getElementById('gdriveImportBtn').addEventListener('click', openGdriveModal);
    document.getElementById('closeGdriveModal').addEventListener('click', closeGdriveModal);
    document.getElementById('closeGdriveModalBtn').addEventListener('click', closeGdriveModal);
    document.querySelector('#gdriveModal .modal-overlay').addEventListener('click', closeGdriveModal);
    document.getElementById('gdriveStartBtn').addEventListener('click', startGdriveImport);
    document.getElementById('gdriveFolderUrlInput').addEventListener('keydown', e => {
        if (e.key === 'Enter') { e.preventDefault(); startGdriveImport(); }
    });
}

// ========== 검색 / 필터 ==========
function handleSearch(e) {
    state.searchQuery = e.target.value.trim().toLowerCase();
    document.getElementById('clearSearch').style.display = state.searchQuery ? 'block' : 'none';
    applyFilters();
}

function handleFilterChange(type, value, checked) {
    const map = { category: state.selectedCategories, hazard: state.selectedHazards, manufacturer: state.selectedManufacturers };
    const set = map[type];
    if (!set) return;
    checked ? set.add(value) : set.delete(value);
    applyFilters();
}

function applyFilters() {
    let f = [...state.allMSDS];
    if (state.searchQuery) {
        f = f.filter(m => [m.product_name, m.manufacturer, m.cas_number, m.description, ...(m.keywords||[])].join(' ').toLowerCase().includes(state.searchQuery));
    }
    if (state.selectedCategories.size)    f = f.filter(m => state.selectedCategories.has(m.category));
    if (state.selectedHazards.size)       f = f.filter(m => state.selectedHazards.has(m.hazard_level));
    if (state.selectedManufacturers.size) f = f.filter(m => state.selectedManufacturers.has(m.manufacturer));
    state.filteredMSDS = f;
    renderCards();
    updateStats();
}

function resetFilters() {
    state.selectedCategories.clear();
    state.selectedHazards.clear();
    state.selectedManufacturers.clear();
    state.searchQuery = '';
    document.getElementById('searchInput').value = '';
    document.getElementById('clearSearch').style.display = 'none';
    document.querySelectorAll('.filter-checkbox input').forEach(cb => { cb.checked = false; });
    document.querySelectorAll('.filter-checkbox').forEach(el => el.classList.remove('active'));
    applyFilters();
}

// ========== 카드 렌더링 ==========
function renderCards() {
    const container = document.getElementById('cardsContainer');
    const noResults = document.getElementById('noResults');

    if (state.filteredMSDS.length === 0) {
        container.innerHTML = '';
        noResults.style.display = 'block';
        return;
    }
    noResults.style.display = 'none';

    container.innerHTML = state.filteredMSDS.map(m => `
        <div class="msds-card fade-in" data-id="${m.id}">
            <div class="card-header">
                <h3 class="card-title">${m.product_name}</h3>
                <span class="hazard-badge ${m.hazard_level}">${m.hazard_level}</span>
            </div>
            <div class="card-body">
                <div class="card-info">
                    <div class="info-item"><i class="fas fa-industry"></i><span>${m.manufacturer}</span></div>
                    <div class="info-item"><i class="fas fa-calendar"></i><span>개정일: ${m.revision_date}</span></div>
                    ${m.cas_number !== '-' ? `<div class="info-item"><i class="fas fa-flask"></i><span>CAS: ${m.cas_number}</span></div>` : ''}
                </div>
                <span class="category-badge"><i class="fas fa-tag"></i> ${m.category}</span>
            </div>
            <div class="card-footer">
                <button class="btn btn-primary btn-pdf"><i class="fas fa-file-pdf"></i> 상세보기</button>
                <a href="${api.downloadUrl(m.id)}" class="btn btn-secondary" download>
                    <i class="fas fa-download"></i> 다운로드
                </a>
            </div>
        </div>
    `).join('');
}

function updateStats() {
    const info = document.getElementById('searchResultsInfo');
    const active = state.searchQuery || state.selectedCategories.size || state.selectedHazards.size || state.selectedManufacturers.size;
    info.innerHTML = active ? `<i class="fas fa-info-circle"></i> <strong>${state.filteredMSDS.length}개</strong>의 결과 (전체 ${state.allMSDS.length}개)` : '';
}

// ========== PDF 상세 모달 ==========
function openPDFModal(id) {
    const m = state.allMSDS.find(x => x.id === id);
    if (!m) return;

    document.getElementById('modalTitle').textContent = m.product_name;

    document.getElementById('msdsInfo').innerHTML = `
        <div class="msds-info-grid">
            <div class="info-item"><i class="fas fa-industry"></i><strong>제조사:</strong> ${m.manufacturer}</div>
            <div class="info-item"><i class="fas fa-tag"></i><strong>카테고리:</strong> ${m.category}</div>
            <div class="info-item"><i class="fas fa-exclamation-triangle"></i><strong>위험등급:</strong>
                <span class="hazard-badge ${m.hazard_level}">${m.hazard_level}</span>
            </div>
            <div class="info-item"><i class="fas fa-calendar"></i><strong>개정일:</strong> ${m.revision_date}</div>
            ${m.cas_number !== '-' ? `<div class="info-item"><i class="fas fa-flask"></i><strong>CAS:</strong> ${m.cas_number}</div>` : ''}
            ${m.ai_analyzed ? '<div class="info-item"><i class="fas fa-robot"></i><strong>분석:</strong> AI 자동 분석</div>' : ''}
        </div>
        ${m.description ? `<p style="margin-top:10px;"><strong>설명:</strong> ${m.description}</p>` : ''}
    `;

    // 내용 탭
    const contentEl = document.getElementById('msdsContentHtml');
    if (m.content_html) {
        contentEl.innerHTML = m.content_html;
    } else {
        contentEl.innerHTML = '<p class="no-content">추출된 내용이 없습니다. 원본 PDF 탭을 확인하세요.</p>';
    }

    // 원본 PDF 탭
    const pdfUrl = m.pdf_path ? `/uploads/pdfs/${m.pdf_path}` : (m.pdf_url || '');
    document.getElementById('pdfViewer').src = pdfUrl;

    // 다운로드 버튼
    document.getElementById('downloadBtn').href = api.downloadUrl(m.id);

    // 기본 탭: content_html이 있으면 내용 탭, 없으면 PDF 탭
    const showContent = !!m.content_html;
    document.querySelectorAll('.vtab').forEach(b => b.classList.remove('active'));
    document.querySelector(`.vtab[data-tab="${showContent ? 'content' : 'pdf'}"]`).classList.add('active');
    document.getElementById('tabContent').style.display = showContent ? 'block' : 'none';
    document.getElementById('tabPdf').style.display    = showContent ? 'none'  : 'block';

    document.getElementById('pdfModal').classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeModal() {
    document.getElementById('pdfModal').classList.remove('active');
    document.getElementById('pdfViewer').src = '';
    document.body.style.overflow = '';
}

// ========== MSDS 등록 모달 ==========
function openUploadModal() {
    resetUploadModal();
    document.getElementById('uploadModal').classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeUploadModal() {
    document.getElementById('uploadModal').classList.remove('active');
    document.body.style.overflow = '';
    state.pendingPdfFile = null;
}

function resetUploadModal() {
    document.getElementById('uploadStep1').style.display = 'block';
    document.getElementById('uploadStep2').style.display = 'none';
    document.getElementById('uploadStep3').style.display = 'none';
    document.getElementById('uploadFooter').style.display = 'none';
    document.getElementById('pdfFileInput').value = '';
    document.getElementById('gdriveUrlInput').value = '';
    document.getElementById('msdsForm').reset();
    document.getElementById('hiddenContentHtml').value = '';
    state.pendingPdfFile = null;
    state.pendingGDriveUrl = null;
}

async function handleFileSelected(file) {
    state.pendingPdfFile = file;

    // Step2: 분석 중
    document.getElementById('uploadStep1').style.display = 'none';
    document.getElementById('uploadStep2').style.display = 'block';
    document.getElementById('analyzingMsg').textContent = 'PDF 분석 중...';

    try {
        const result = await api.analyzePDF(file);

        // Step3: 폼 표시
        document.getElementById('uploadStep2').style.display = 'none';
        document.getElementById('uploadStep3').style.display = 'block';
        document.getElementById('uploadFooter').style.display = 'flex';

        // 추출 텍스트 표시
        const preview = result.extracted_preview || '';
        const previewEl = document.getElementById('extractedPreview');
        if (preview) {
            previewEl.style.display = 'block';
            document.getElementById('extractedText').textContent = preview;
        } else {
            previewEl.style.display = 'none';
        }

        // AI / 수동 배지
        document.getElementById('aiBadge').style.display    = result.mode === 'ai'     ? 'block' : 'none';
        document.getElementById('manualBadge').style.display = result.mode === 'manual' ? 'block' : 'none';

        // 폼 자동 채우기
        fillForm(result.fields, result.mode === 'ai');
        document.getElementById('hiddenContentHtml').value = result.content_html || '';

    } catch (err) {
        document.getElementById('uploadStep2').style.display = 'none';
        document.getElementById('uploadStep1').style.display = 'block';
        document.getElementById('uploadHint').innerHTML =
            `<span style="color:#dc2626;"><i class="fas fa-exclamation-circle"></i> 분석 실패: ${err.message}</span>`;
    }
}

async function handleGDriveSubmit() {
    const url = document.getElementById('gdriveUrlInput').value.trim();
    if (!url) { alert('Google Drive URL을 입력하세요.'); return; }
    if (!url.includes('drive.google.com')) { alert('유효한 Google Drive URL을 입력하세요.'); return; }

    state.pendingGDriveUrl = url;
    state.pendingPdfFile = null;

    // Step2: 분석 중
    document.getElementById('uploadStep1').style.display = 'none';
    document.getElementById('uploadStep2').style.display = 'block';
    document.getElementById('analyzingMsg').textContent = 'Google Drive에서 PDF 다운로드 및 분석 중...';

    try {
        const result = await api.analyzeGDrive(url);

        document.getElementById('uploadStep2').style.display = 'none';
        document.getElementById('uploadStep3').style.display = 'block';
        document.getElementById('uploadFooter').style.display = 'flex';

        const preview = result.extracted_preview || '';
        const previewEl = document.getElementById('extractedPreview');
        if (preview) {
            previewEl.style.display = 'block';
            document.getElementById('extractedText').textContent = preview;
        } else {
            previewEl.style.display = 'none';
        }

        document.getElementById('aiBadge').style.display    = result.mode === 'ai'     ? 'block' : 'none';
        document.getElementById('manualBadge').style.display = result.mode === 'manual' ? 'block' : 'none';

        fillForm(result.fields, result.mode === 'ai');
        document.getElementById('hiddenContentHtml').value = result.content_html || '';

    } catch (err) {
        document.getElementById('uploadStep2').style.display = 'none';
        document.getElementById('uploadStep1').style.display = 'block';
        document.getElementById('uploadHint').innerHTML =
            `<span style="color:#dc2626;"><i class="fas fa-exclamation-circle"></i> Google Drive 가져오기 실패: ${err.message}</span>`;
    }
}

function fillForm(fields, isAI) {
    const form = document.getElementById('msdsForm');
    if (!fields) return;

    const set = (name, val) => {
        const el = form.elements[name];
        if (el && val !== undefined && val !== null) el.value = val;
    };

    set('product_name',  fields.product_name);
    set('manufacturer',  fields.manufacturer);
    set('category',      fields.category);
    set('hazard_level',  fields.hazard_level);
    set('cas_number',    fields.cas_number);
    set('revision_date', fields.revision_date);
    set('description',   fields.description);

    if (fields.keywords && fields.keywords.length) {
        set('keywords_raw', fields.keywords.join(', '));
    }

    form.elements['ai_analyzed'].value = isAI ? '1' : '0';
}

async function submitMsds() {
    const form = document.getElementById('msdsForm');
    if (!form.checkValidity()) { form.reportValidity(); return; }

    const btn = document.getElementById('submitMsdsBtn');
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 등록 중...';

    try {
        const fd = new FormData();
        const data = Object.fromEntries(new FormData(form));

        // keywords 변환 (쉼표 구분 → JSON 배열)
        const keywords = (data.keywords_raw || '').split(',').map(s => s.trim()).filter(Boolean);
        fd.append('keywords', JSON.stringify(keywords));

        ['product_name','manufacturer','category','hazard_level','cas_number',
         'revision_date','description','ai_analyzed','content_html'].forEach(k => {
            if (data[k] !== undefined) fd.append(k, data[k]);
        });

        if (state.pendingPdfFile) fd.append('pdf', state.pendingPdfFile);
        else if (state.pendingGDriveUrl) fd.append('gdrive_url', state.pendingGDriveUrl);

        await api.createMSDS(fd);

        closeUploadModal();

        // 목록 새로고침
        const [stats, msdsData] = await Promise.all([api.getStats(), api.getMSDS()]);
        state.allMSDS = msdsData;
        state.filteredMSDS = msdsData;
        document.getElementById('totalCount').textContent = stats.total;
        applyFilters();

        alert('MSDS가 등록되었습니다.');
    } catch (err) {
        alert(`등록 실패: ${err.message}`);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-save"></i> 등록하기';
    }
}

function getPdfUrl(m) {
    if (m.pdf_path) return `/uploads/pdfs/${m.pdf_path}`;
    return m.pdf_url || '#';
}

// ========== Google Drive 폴더 일괄 업로드 ==========
function openGdriveModal() {
    document.getElementById('gdriveStep1').style.display = 'block';
    document.getElementById('gdriveStep2').style.display = 'none';
    document.getElementById('gdriveStep3').style.display = 'none';
    document.getElementById('gdriveFolderUrlInput').value = '';
    document.getElementById('gdriveStartBtn').style.display = '';
    document.getElementById('gdriveStartBtn').disabled = false;
    document.getElementById('gdriveStartBtn').innerHTML = '<i class="fas fa-cloud-upload-alt"></i> 업로드 시작';
    document.getElementById('gdriveModal').classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeGdriveModal() {
    document.getElementById('gdriveModal').classList.remove('active');
    document.body.style.overflow = '';
}

async function startGdriveImport() {
    const url = document.getElementById('gdriveFolderUrlInput').value.trim();
    if (!url) { alert('Google Drive 폴더 URL을 입력하세요.'); return; }
    if (!url.includes('drive.google.com')) { alert('유효한 Google Drive URL을 입력하세요.'); return; }

    const btn = document.getElementById('gdriveStartBtn');
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 업로드 중...';

    document.getElementById('gdriveStep1').style.display = 'none';
    document.getElementById('gdriveStep2').style.display = 'block';

    try {
        const result = await api.importGDriveFolder(url);

        document.getElementById('gdriveStep2').style.display = 'none';
        document.getElementById('gdriveStep3').style.display = 'block';
        btn.style.display = 'none';

        let html = `<div class="gdrive-result-summary">
            <i class="fas fa-check-circle" style="color:#10b981;font-size:2rem;"></i>
            <h3>${result.message}</h3>
        </div>`;

        if (result.uploaded.length) {
            html += '<ul class="gdrive-file-list">';
            result.uploaded.forEach(f => {
                html += `<li><i class="fas fa-file-pdf" style="color:#dc2626;"></i> ${f.filename}</li>`;
            });
            html += '</ul>';
        }
        if (result.errors.length) {
            html += '<h4 style="color:#dc2626;margin-top:1rem;">실패 목록:</h4><ul class="gdrive-file-list">';
            result.errors.forEach(f => {
                html += `<li><i class="fas fa-exclamation-circle" style="color:#dc2626;"></i> ${f.filename}: ${f.error}</li>`;
            });
            html += '</ul>';
        }

        document.getElementById('gdriveResult').innerHTML = html;

    } catch (err) {
        document.getElementById('gdriveStep2').style.display = 'none';
        document.getElementById('gdriveStep1').style.display = 'block';
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-cloud-upload-alt"></i> 업로드 시작';
        alert(`업로드 실패: ${err.message}`);
    }
}

function debounce(fn, wait) {
    let t;
    return function(...args) { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
}
