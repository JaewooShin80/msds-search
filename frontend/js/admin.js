/**
 * MSDS 관리자 페이지
 */

const state = {
    allMSDS: [],
    filteredMSDS: [],
    selectedCategories: new Set(),
    selectedHazards: new Set(),
    selectedManufacturers: new Set(),
    searchQuery: '',
    currentView: 'grid',
    pendingPdfFile: null,
    pendingBulkFiles: [],
};

// ========== 로그인 ==========
document.addEventListener('DOMContentLoaded', () => {
    const token = sessionStorage.getItem('adminToken');
    if (token) {
        // 저장된 토큰 유효성 확인
        api.adminVerify()
            .then(() => showAdminUI())
            .catch(() => { sessionStorage.removeItem('adminToken'); showLoginOverlay(); });
    } else {
        showLoginOverlay();
    }

    document.getElementById('loginBtn').addEventListener('click', handleLogin);
    document.getElementById('loginPw').addEventListener('keydown', e => {
        if (e.key === 'Enter') handleLogin();
    });
    document.getElementById('loginId').addEventListener('keydown', e => {
        if (e.key === 'Enter') document.getElementById('loginPw').focus();
    });
    document.getElementById('logoutBtn').addEventListener('click', handleLogout);
});

function showLoginOverlay() {
    document.getElementById('loginOverlay').style.display = 'flex';
}

function hideLoginOverlay() {
    document.getElementById('loginOverlay').style.display = 'none';
}

async function handleLogin() {
    const id = document.getElementById('loginId').value.trim();
    const pw = document.getElementById('loginPw').value;
    const errEl = document.getElementById('loginError');
    const btn = document.getElementById('loginBtn');

    if (!id || !pw) {
        errEl.textContent = '아이디와 비밀번호를 입력하세요.';
        errEl.classList.add('show');
        return;
    }

    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 확인 중...';
    errEl.classList.remove('show');

    try {
        const { token } = await api.adminLogin(id, pw);
        sessionStorage.setItem('adminToken', token);
        hideLoginOverlay();
        showAdminUI();
    } catch (err) {
        errEl.textContent = err.message || '로그인에 실패했습니다.';
        errEl.classList.add('show');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-sign-in-alt"></i> 로그인';
    }
}

function handleLogout() {
    sessionStorage.removeItem('adminToken');
    window.location.href = '/';
}

async function showAdminUI() {
    try {
        await initializeApp();
    } catch (err) {
        showError('서버 연결에 실패했습니다.');
        console.error(err);
    }
}

// ========== 초기화 ==========
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

    api.getAiStatus().then(aiStatus => {
        if (aiStatus.ai_available && aiStatus.pending_count > 0) {
            const btn = document.getElementById('reanalyzeBtn');
            btn.style.display = '';
            document.getElementById('pendingBadge').textContent = aiStatus.pending_count;
        }
    }).catch(() => {});
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
    const safeLabel = escapeHtml(label);
    const safeType  = escapeHtml(type);
    div.innerHTML = `
        <input type="checkbox" id="${safeType}-${safeLabel}" value="${safeLabel}" data-type="${safeType}">
        <label for="${safeType}-${safeLabel}">${safeLabel}</label>
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
    document.getElementById('searchInput').addEventListener('input', debounce(handleSearch, 300));
    document.getElementById('clearSearch').addEventListener('click', () => {
        document.getElementById('searchInput').value = '';
        state.searchQuery = '';
        document.getElementById('clearSearch').style.display = 'none';
        applyFilters();
    });
    document.getElementById('resetFilters').addEventListener('click', resetFilters);

    document.querySelectorAll('.toggle-filter').forEach(btn => {
        btn.addEventListener('click', function() {
            document.getElementById(this.dataset.target).classList.toggle('collapsed');
            this.classList.toggle('active');
        });
    });

    document.querySelectorAll('.view-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.view-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            document.getElementById('cardsContainer').classList.toggle('list-view', this.dataset.view === 'list');
            state.currentView = this.dataset.view;
        });
    });

    // 카드 클릭 이벤트 위임 (상세보기, 수정, 삭제)
    document.getElementById('cardsContainer').addEventListener('click', e => {
        const card = e.target.closest('.msds-card');
        if (!card) return;
        const id = parseInt(card.dataset.id, 10);
        if (e.target.closest('.btn-edit'))     { openEditModal(id); return; }
        if (e.target.closest('.btn-delete'))   { handleDelete(id);  return; }
        if (e.target.closest('.btn-download')) return; // 다운로드 버튼은 기본 동작 유지
        openPDFModal(id); // 나머지 영역 클릭 시 PDF 모달
    });

    document.querySelectorAll('.vtab').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.vtab').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            const tab = this.dataset.tab;
            document.getElementById('tabContent').style.display = tab === 'content' ? 'block' : 'none';
            document.getElementById('tabPdf').style.display    = tab === 'pdf'     ? 'block' : 'none';
        });
    });

    ['closeModal', 'closeModalBtn'].forEach(id => document.getElementById(id).addEventListener('click', closeModal));
    document.querySelector('#pdfModal .modal-overlay').addEventListener('click', closeModal);
    document.getElementById('editFromModalBtn').addEventListener('click', () => {
        closeModal();
        if (state.currentModalId) openEditModal(state.currentModalId);
    });

    // 등록 모달
    document.getElementById('uploadBtn').addEventListener('click', openUploadModal);
    document.getElementById('closeUploadModal').addEventListener('click', closeUploadModal);
    document.getElementById('cancelUploadBtn').addEventListener('click', closeUploadModal);
    document.querySelector('#uploadModal .modal-overlay').addEventListener('click', closeUploadModal);
    document.getElementById('submitMsdsBtn').addEventListener('click', submitMsds);

    document.getElementById('pdfFileInput').addEventListener('change', e => {
        if (e.target.files[0]) handleFileSelected(e.target.files[0]);
    });

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

    // 일괄 등록 모달
    document.getElementById('bulkUploadBtn').addEventListener('click', openBulkModal);
    document.getElementById('closeBulkModal').addEventListener('click', closeBulkModal);
    document.getElementById('closeBulkModalBtn').addEventListener('click', closeBulkModal);
    document.querySelector('#bulkModal .modal-overlay').addEventListener('click', closeBulkModal);
    document.getElementById('bulkStartBtn').addEventListener('click', startBulkUpload);
    document.getElementById('bulkFileInput').addEventListener('change', handleBulkFilesSelected);
    document.getElementById('folderFileInput').addEventListener('change', handleFolderSelected);

    // 탭 전환
    document.querySelectorAll('.bulk-tab').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.bulk-tab').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            const tab = this.dataset.bulkTab;
            document.getElementById('bulkTabFiles').style.display  = tab === 'files'  ? 'block' : 'none';
            document.getElementById('bulkTabFolder').style.display = tab === 'folder' ? 'block' : 'none';
            // 탭 전환 시 파일 목록 초기화
            state.pendingBulkFiles = [];
            document.getElementById('bulkFileList').style.display = 'none';
            document.getElementById('bulkStartBtn').disabled = true;
        });
    });

    const bdz = document.getElementById('bulkDropZone');
    bdz.addEventListener('dragover', e => { e.preventDefault(); bdz.classList.add('dragover'); });
    bdz.addEventListener('dragleave', () => bdz.classList.remove('dragover'));
    bdz.addEventListener('drop', e => {
        e.preventDefault();
        bdz.classList.remove('dragover');
        const files = [...e.dataTransfer.files].filter(f => f.type === 'application/pdf');
        if (files.length === 0) { alert('PDF 파일만 업로드 가능합니다.'); return; }
        state.pendingBulkFiles = files;
        showBulkFileList(files);
    });

    const fdz = document.getElementById('folderDropZone');
    fdz.addEventListener('dragover', e => { e.preventDefault(); fdz.classList.add('dragover'); });
    fdz.addEventListener('dragleave', () => fdz.classList.remove('dragover'));
    fdz.addEventListener('drop', e => {
        e.preventDefault();
        fdz.classList.remove('dragover');
        const files = [...e.dataTransfer.files].filter(f => f.type === 'application/pdf');
        if (files.length === 0) { alert('폴더에 PDF 파일이 없습니다.'); return; }
        state.pendingBulkFiles = files;
        showBulkFileList(files);
    });

    // AI 재분석
    document.getElementById('reanalyzeBtn').addEventListener('click', handleReanalyze);
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

// ========== 카드 렌더링 (관리자: 수정/삭제 버튼 포함) ==========
function renderCards() {
    const container = document.getElementById('cardsContainer');
    const noResults = document.getElementById('noResults');

    if (state.filteredMSDS.length === 0) {
        container.innerHTML = '';
        noResults.style.display = 'block';
        return;
    }
    noResults.style.display = 'none';

    container.innerHTML = state.filteredMSDS.map(m => {
        const name   = escapeHtml(m.product_name);
        const hazard = escapeHtml(m.hazard_level);
        const mfr    = escapeHtml(m.manufacturer);
        const date   = escapeHtml(m.revision_date);
        const cas    = escapeHtml(m.cas_number);
        const cat    = escapeHtml(m.category);
        return `
        <div class="msds-card fade-in" data-id="${m.id}">
            <div class="card-header">
                <h3 class="card-title">${name}</h3>
                <span class="hazard-badge ${hazard}">${hazard}</span>
            </div>
            <div class="card-body">
                <div class="card-info">
                    <div class="info-item"><i class="fas fa-industry"></i><span>${mfr}</span></div>
                    <div class="info-item"><i class="fas fa-calendar"></i><span>개정일: ${date}</span></div>
                    ${m.cas_number !== '-' ? `<div class="info-item"><i class="fas fa-flask"></i><span>CAS: ${cas}</span></div>` : ''}
                </div>
                <span class="category-badge"><i class="fas fa-tag"></i> ${cat}</span>
            </div>
            <div class="card-footer">
                <button class="btn btn-primary btn-pdf"><i class="fas fa-file-pdf"></i> 상세보기</button>
                <a href="${api.downloadUrl(m.id)}" class="btn btn-secondary btn-download" download>
                    <i class="fas fa-download"></i> 다운로드
                </a>
            </div>
            <div class="card-admin-actions">
                <button class="btn-edit"><i class="fas fa-edit"></i> 수정</button>
                <button class="btn-delete"><i class="fas fa-trash"></i> 삭제</button>
            </div>
        </div>
        `;
    }).join('');
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
    state.currentModalId = id;

    document.getElementById('modalTitle').textContent = m.product_name;
    document.getElementById('msdsInfo').innerHTML = `
        <div class="msds-info-grid">
            <div class="info-item"><i class="fas fa-industry"></i><strong>제조사:</strong> ${escapeHtml(m.manufacturer)}</div>
            <div class="info-item"><i class="fas fa-tag"></i><strong>카테고리:</strong> ${escapeHtml(m.category)}</div>
            <div class="info-item"><i class="fas fa-exclamation-triangle"></i><strong>위험등급:</strong>
                <span class="hazard-badge ${escapeHtml(m.hazard_level)}">${escapeHtml(m.hazard_level)}</span>
            </div>
            <div class="info-item"><i class="fas fa-calendar"></i><strong>개정일:</strong> ${escapeHtml(m.revision_date)}</div>
            ${m.cas_number !== '-' ? `<div class="info-item"><i class="fas fa-flask"></i><strong>CAS:</strong> ${escapeHtml(m.cas_number)}</div>` : ''}
            ${m.ai_analyzed ? '<div class="info-item"><i class="fas fa-robot"></i><strong>분석:</strong> AI 자동 분석</div>' : ''}
        </div>
        ${m.description ? `<p style="margin-top:10px;"><strong>설명:</strong> ${escapeHtml(m.description)}</p>` : ''}
    `;

    const contentEl = document.getElementById('msdsContentHtml');
    contentEl.innerHTML = m.content_html
        ? DOMPurify.sanitize(m.content_html)
        : '<p class="no-content">추출된 내용이 없습니다. 원본 PDF 탭을 확인하세요.</p>';

    document.getElementById('pdfViewer').src = (m.pdf_path || m.pdf_url) ? api.downloadUrl(m.id) : '';
    document.getElementById('downloadBtn').href = api.downloadUrl(m.id);

    // 기본: PDF 뷰어 탭 표시
    document.querySelectorAll('.vtab').forEach(b => b.classList.remove('active'));
    document.querySelector('.vtab[data-tab="pdf"]').classList.add('active');
    document.getElementById('tabContent').style.display = 'none';
    document.getElementById('tabPdf').style.display    = 'block';

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
    document.getElementById('uploadModalTitle').textContent = 'MSDS 등록';
    document.getElementById('submitMsdsBtn').innerHTML = '<i class="fas fa-save"></i> 등록하기';
    document.getElementById('uploadModal').classList.add('active');
    document.body.style.overflow = 'hidden';
}

function openEditModal(id) {
    const m = state.allMSDS.find(x => x.id === id);
    if (!m) return;

    resetUploadModal();
    document.getElementById('uploadModalTitle').textContent = 'MSDS 수정';
    document.getElementById('submitMsdsBtn').innerHTML = '<i class="fas fa-save"></i> 수정하기';
    document.getElementById('editId').value = id;

    // Step1 숨기고 바로 폼으로
    document.getElementById('uploadStep1').style.display = 'none';
    document.getElementById('uploadStep3').style.display = 'block';
    document.getElementById('uploadFooter').style.display = 'flex';
    document.getElementById('extractedPreview').style.display = 'none';
    document.getElementById('aiBadge').style.display = 'none';
    document.getElementById('manualBadge').style.display = 'none';

    // 기존 데이터 채우기
    const form = document.getElementById('msdsForm');
    form.elements['product_name'].value  = m.product_name  || '';
    form.elements['manufacturer'].value  = m.manufacturer  || '';
    form.elements['category'].value      = m.category      || '';
    form.elements['hazard_level'].value  = m.hazard_level  || '경고';
    form.elements['cas_number'].value    = m.cas_number    || '-';
    form.elements['revision_date'].value = m.revision_date || '';
    form.elements['description'].value   = m.description   || '';
    form.elements['keywords_raw'].value  = (m.keywords || []).join(', ');
    form.elements['ai_analyzed'].value   = m.ai_analyzed   || '0';
    document.getElementById('hiddenContentHtml').value = m.content_html || '';

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
    document.getElementById('msdsForm').reset();
    document.getElementById('hiddenContentHtml').value = '';
    document.getElementById('editId').value = '';
    document.getElementById('uploadHint').innerHTML = '';
    state.pendingPdfFile = null;
}

async function handleFileSelected(file) {
    state.pendingPdfFile = file;
    document.getElementById('uploadStep1').style.display = 'none';
    document.getElementById('uploadStep2').style.display = 'block';
    document.getElementById('analyzingMsg').textContent = 'PDF 분석 중...';

    try {
        const result = await api.analyzePDF(file);

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
            `<span style="color:#dc2626;"><i class="fas fa-exclamation-circle"></i> 분석 실패: ${err.message}</span>`;
    }
}

function fillForm(fields, isAI) {
    const form = document.getElementById('msdsForm');
    if (!fields) return;
    const set = (name, val) => { const el = form.elements[name]; if (el && val != null) el.value = val; };
    set('product_name',  fields.product_name);
    set('manufacturer',  fields.manufacturer);
    set('category',      fields.category);
    set('hazard_level',  fields.hazard_level);
    set('cas_number',    fields.cas_number);
    set('revision_date', fields.revision_date);
    set('description',   fields.description);
    if (fields.keywords && fields.keywords.length) set('keywords_raw', fields.keywords.join(', '));
    form.elements['ai_analyzed'].value = isAI ? '1' : '0';
}

async function submitMsds() {
    const form = document.getElementById('msdsForm');
    if (!form.checkValidity()) { form.reportValidity(); return; }

    const editId = document.getElementById('editId').value;
    const btn = document.getElementById('submitMsdsBtn');
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 저장 중...';

    try {
        const fd = new FormData();
        const data = Object.fromEntries(new FormData(form));
        const keywords = (data.keywords_raw || '').split(',').map(s => s.trim()).filter(Boolean);
        fd.append('keywords', JSON.stringify(keywords));

        ['product_name','manufacturer','category','hazard_level','cas_number',
         'revision_date','description','ai_analyzed','content_html'].forEach(k => {
            if (data[k] !== undefined) fd.append(k, data[k]);
        });

        if (state.pendingPdfFile) fd.append('pdf', state.pendingPdfFile);

        if (editId) {
            await api.updateMSDS(editId, fd);
        } else {
            await api.createMSDS(fd);
        }

        closeUploadModal();
        await refreshList();
        alert(editId ? 'MSDS가 수정되었습니다.' : 'MSDS가 등록되었습니다.');
    } catch (err) {
        alert(`저장 실패: ${err.message}`);
    } finally {
        btn.disabled = false;
        btn.innerHTML = editId
            ? '<i class="fas fa-save"></i> 수정하기'
            : '<i class="fas fa-save"></i> 등록하기';
    }
}

async function handleDelete(id) {
    const m = state.allMSDS.find(x => x.id === id);
    if (!m) return;
    if (!confirm(`"${m.product_name}"을(를) 삭제하시겠습니까?\n이 작업은 되돌릴 수 없습니다.`)) return;

    try {
        await api.deleteMSDS(id);
        await refreshList();
        alert('삭제되었습니다.');
    } catch (err) {
        alert(`삭제 실패: ${err.message}`);
    }
}

async function refreshList() {
    const [stats, msdsData] = await Promise.all([api.getStats(), api.getMSDS()]);
    state.allMSDS = msdsData;
    state.filteredMSDS = msdsData;
    document.getElementById('totalCount').textContent = stats.total;
    applyFilters();
}

// ========== 일괄 등록 ==========
function openBulkModal() {
    document.getElementById('bulkStep1').style.display = 'block';
    document.getElementById('bulkStep2').style.display = 'none';
    document.getElementById('bulkStep3').style.display = 'none';
    document.getElementById('bulkFileInput').value = '';
    document.getElementById('folderFileInput').value = '';
    document.getElementById('bulkFileList').style.display = 'none';
    document.getElementById('bulkStartBtn').style.display = '';
    document.getElementById('bulkStartBtn').disabled = true;
    document.getElementById('bulkStartBtn').innerHTML = '<i class="fas fa-cloud-upload-alt"></i> 일괄 등록 시작';
    // 탭 초기화: 파일 탭으로
    document.querySelectorAll('.bulk-tab').forEach(b => b.classList.remove('active'));
    document.querySelector('.bulk-tab[data-bulk-tab="files"]').classList.add('active');
    document.getElementById('bulkTabFiles').style.display  = 'block';
    document.getElementById('bulkTabFolder').style.display = 'none';
    state.pendingBulkFiles = [];
    document.getElementById('bulkModal').classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeBulkModal() {
    document.getElementById('bulkModal').classList.remove('active');
    document.body.style.overflow = '';
    state.pendingBulkFiles = [];
}

function handleBulkFilesSelected(e) {
    const files = [...e.target.files].filter(f => f.type === 'application/pdf');
    if (files.length === 0) { alert('PDF 파일만 업로드 가능합니다.'); return; }
    state.pendingBulkFiles = files;
    showBulkFileList(files);
}

function handleFolderSelected(e) {
    const files = [...e.target.files].filter(f => f.type === 'application/pdf');
    if (files.length === 0) { alert('선택한 폴더에 PDF 파일이 없습니다.'); return; }
    state.pendingBulkFiles = files;
    showBulkFileList(files);
}

function showBulkFileList(files) {
    document.getElementById('bulkFileCount').textContent = files.length;
    const ul = document.getElementById('bulkFileNames');
    ul.innerHTML = files.map(f =>
        `<li><i class="fas fa-file-pdf" style="color:#dc2626;"></i> ${f.name} <span style="color:#94a3b8;">(${(f.size/1024/1024).toFixed(1)}MB)</span></li>`
    ).join('');
    document.getElementById('bulkFileList').style.display = 'block';
    document.getElementById('bulkStartBtn').disabled = false;
}

async function startBulkUpload() {
    if (!state.pendingBulkFiles.length) { alert('파일을 선택하세요.'); return; }

    const btn = document.getElementById('bulkStartBtn');
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 업로드 중...';

    document.getElementById('bulkStep1').style.display = 'none';
    document.getElementById('bulkStep2').style.display = 'block';
    document.getElementById('bulkProgressMsg').textContent = `${state.pendingBulkFiles.length}개 PDF 업로드 및 AI 분석 중...`;

    try {
        const result = await api.bulkUpload(state.pendingBulkFiles);
        document.getElementById('bulkStep2').style.display = 'none';
        document.getElementById('bulkStep3').style.display = 'block';
        btn.style.display = 'none';

        let html = `<div class="bulk-result-summary"><i class="fas fa-check-circle" style="color:#10b981;font-size:2rem;"></i><h3>${result.message}</h3></div>`;
        if (result.uploaded.length) {
            html += '<ul class="bulk-file-list">';
            result.uploaded.forEach(f => {
                const badge = f.mode === 'ai'
                    ? '<span style="color:#10b981;font-size:0.8rem;"><i class="fas fa-robot"></i> AI</span>'
                    : '<span style="color:#94a3b8;font-size:0.8rem;"><i class="fas fa-keyboard"></i> 수동</span>';
                html += `<li><i class="fas fa-file-pdf" style="color:#dc2626;"></i><span style="flex:1;">${escapeHtml(f.product_name||f.filename)}</span><span style="color:#64748b;font-size:0.8rem;">${escapeHtml(f.category||'')}</span>${badge}</li>`;
            });
            html += '</ul>';
        }
        if (result.errors && result.errors.length) {
            html += '<h4 style="color:#dc2626;margin-top:1rem;">실패 목록:</h4><ul class="bulk-file-list">';
            result.errors.forEach(f => { html += `<li><i class="fas fa-exclamation-circle" style="color:#dc2626;"></i> ${escapeHtml(f.filename)}: ${escapeHtml(f.error)}</li>`; });
            html += '</ul>';
        }
        document.getElementById('bulkResult').innerHTML = html;

        if (result.uploaded.length) await refreshList();
    } catch (err) {
        document.getElementById('bulkStep2').style.display = 'none';
        document.getElementById('bulkStep1').style.display = 'block';
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-cloud-upload-alt"></i> 일괄 등록 시작';
        alert(`업로드 실패: ${err.message}`);
    }
}

async function handleReanalyze() {
    const btn = document.getElementById('reanalyzeBtn');
    const count = document.getElementById('pendingBadge').textContent;
    if (!confirm(`미분석 항목 ${count}개를 AI로 재분석합니다.\n시간이 걸릴 수 있습니다. 계속하시겠습니까?`)) return;

    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 재분석 중...';

    try {
        const result = await api.reanalyzePending();
        alert(`${result.message}${result.errors.length ? `\n실패: ${result.errors.length}개` : ''}`);

        const [stats, msdsData, aiStatus] = await Promise.all([api.getStats(), api.getMSDS(), api.getAiStatus()]);
        state.allMSDS = msdsData;
        state.filteredMSDS = msdsData;
        document.getElementById('totalCount').textContent = stats.total;
        applyFilters();

        if (!aiStatus.ai_available || aiStatus.pending_count === 0) {
            btn.style.display = 'none';
        } else {
            btn.disabled = false;
            btn.innerHTML = `<i class="fas fa-robot"></i> AI 재분석 <span class="pending-badge">${aiStatus.pending_count}</span>`;
        }
    } catch (err) {
        alert(`재분석 실패: ${err.message}`);
        btn.disabled = false;
        btn.innerHTML = `<i class="fas fa-robot"></i> AI 재분석 <span class="pending-badge">${count}</span>`;
    }
}

function debounce(fn, wait) {
    let t;
    return function(...args) { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
}
