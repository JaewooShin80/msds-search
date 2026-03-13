/**
 * MSDS 검색 시스템 - 사용자 페이지 (읽기 전용)
 */

const state = {
    allMSDS: [],
    filteredMSDS: [],
    selectedCategories: new Set(),
    selectedHazards: new Set(),
    selectedManufacturers: new Set(),
    searchQuery: '',
    currentView: 'grid',
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
    if (urlId) openPDFModal(parseInt(urlId, 10));
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

    document.getElementById('cardsContainer').addEventListener('click', e => {
        if (e.target.closest('.btn-download')) return; // 다운로드 버튼은 기본 동작 유지
        const card = e.target.closest('.msds-card');
        if (card) openPDFModal(parseInt(card.dataset.id, 10));
    });

    document.querySelectorAll('.vtab').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.vtab').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            if (window.innerWidth < 768) {
                const tab = this.dataset.tab;
                document.getElementById('tabContent').style.display = tab === 'content' ? 'block' : 'none';
                document.getElementById('tabPdf').style.display    = tab === 'pdf'     ? 'block' : 'none';
            }
        });
    });

    ['closeModal', 'closeModalBtn'].forEach(id => document.getElementById(id).addEventListener('click', closeModal));
    document.querySelector('#pdfModal .modal-overlay').addEventListener('click', closeModal);
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
        f = f.filter(m => [m.product_name, m.manufacturer, m.description, ...(m.keywords||[])].join(' ').toLowerCase().includes(state.searchQuery));
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
    // 위험등급, 제조사 필터 닫힘 상태로 복원
    ['hazardFilter', 'manufacturerFilter'].forEach(id => {
        document.getElementById(id).classList.add('collapsed');
        document.querySelector(`[data-target="${id}"]`).classList.add('active');
    });
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

    container.innerHTML = state.filteredMSDS.map(m => {
        const name    = escapeHtml(m.product_name);
        const hazard  = escapeHtml(m.hazard_level);
        const mfr     = escapeHtml(m.manufacturer);
        const date    = escapeHtml(m.revision_date);
        const cat     = escapeHtml(m.category);
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
                </div>
                <span class="category-badge"><i class="fas fa-tag"></i> ${cat}</span>
            </div>
            <div class="card-footer">
                <button class="btn btn-primary btn-pdf"><i class="fas fa-file-pdf"></i> 상세보기</button>
                <a href="${api.downloadUrl(m.id)}" class="btn btn-secondary btn-download" download>
                    <i class="fas fa-download"></i> 다운로드
                </a>
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

    document.getElementById('modalTitle').textContent = m.product_name;
    document.getElementById('msdsInfo').innerHTML = `
        <div class="msds-info-grid">
            <div class="info-item"><i class="fas fa-industry"></i><strong>제조사:</strong> ${escapeHtml(m.manufacturer)}</div>
            <div class="info-item"><i class="fas fa-tag"></i><strong>카테고리:</strong> ${escapeHtml(m.category)}</div>
            <div class="info-item"><i class="fas fa-exclamation-triangle"></i><strong>위험등급:</strong>
                <span class="hazard-badge ${escapeHtml(m.hazard_level)}">${escapeHtml(m.hazard_level)}</span>
            </div>
            <div class="info-item"><i class="fas fa-calendar"></i><strong>개정일:</strong> ${escapeHtml(m.revision_date)}</div>
            ${m.ai_analyzed ? '<div class="info-item"><i class="fas fa-robot"></i><strong>분석:</strong> AI 자동 분석</div>' : ''}
        </div>
        ${m.description ? `<p style="margin-top:10px;"><strong>설명:</strong> ${escapeHtml(m.description)}</p>` : ''}
    `;

    const contentEl = document.getElementById('msdsContentHtml');
    contentEl.innerHTML = m.content_html
        ? DOMPurify.sanitize(m.content_html)
        : '<p class="no-content">추출된 내용이 없습니다. 원본 PDF 탭을 확인하세요.</p>';

    const pdfUrl = (m.pdf_path || m.pdf_url) ? api.downloadUrl(m.id) : '';
    document.getElementById('pdfViewer').src = pdfUrl;
    document.getElementById('downloadBtn').href = api.downloadUrl(m.id);

    // 탭 초기화 — 반응형 분기
    document.querySelectorAll('.vtab').forEach(b => b.classList.remove('active'));
    if (window.innerWidth < 768) {
        // 모바일: PDF 탭만 표시
        document.querySelector('.vtab[data-tab="pdf"]').classList.add('active');
        document.getElementById('tabContent').style.display = 'none';
        document.getElementById('tabPdf').style.display    = 'block';
    } else {
        // PC/태블릿: 모두 표시 (CSS가 제어)
        document.getElementById('tabContent').style.display = '';
        document.getElementById('tabPdf').style.display    = '';
    }

    document.getElementById('pdfModal').classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeModal() {
    document.getElementById('pdfModal').classList.remove('active');
    document.getElementById('pdfViewer').src = '';
    document.body.style.overflow = '';
}

function debounce(fn, wait) {
    let t;
    return function(...args) { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
}
