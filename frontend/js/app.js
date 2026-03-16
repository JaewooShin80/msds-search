/**
 * MSDS 검색 시스템 - 사용자 페이지 (읽기 전용)
 */

const state = {
    items: [],              // 현재 페이지 아이템
    total: 0,               // 전체 건수 (서버 제공)
    page: 1,
    page_size: 20,
    selectedCategory: '',
    selectedHazard: '',
    selectedManufacturer: '',
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

    const [stats, result, categories, hazardLevels, manufacturers] = await Promise.all([
        api.getStats(),
        api.getMSDS({ page: 1, page_size: state.page_size }),
        api.getCategories(),
        api.getHazardLevels(),
        api.getManufacturers(),
    ]);

    state.items = result.items;
    state.total = result.total;
    state.page  = result.page;

    document.getElementById('totalCount').textContent = stats.total;
    document.getElementById('categoryCount').textContent = stats.categoryCount;

    initializeFilters(categories, hazardLevels, manufacturers);
    activateTab('category');
    renderCards();
    renderPagination();
    updateStats();
    registerEventListeners();

    showLoading(false);

    // URL 파라미터로 id가 전달되면 해당 MSDS 모달 자동 오픈
    const urlParams = new URLSearchParams(window.location.search);
    const urlId = urlParams.get('id');
    if (urlId) openPDFModal(parseInt(urlId, 10));
}

// ========== 서버 페치 + 렌더 ==========
async function fetchAndRender(page = 1) {
    showLoading(true);
    try {
        const result = await api.getMSDS({
            q:            state.searchQuery    || undefined,
            category:     state.selectedCategory    || undefined,
            hazard:       state.selectedHazard      || undefined,
            manufacturer: state.selectedManufacturer || undefined,
            page,
            page_size: state.page_size,
        });
        state.items = result.items;
        state.total = result.total;
        state.page  = result.page;

        renderCards();
        renderPagination();
        updateStats();
    } finally {
        showLoading(false);
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
    // 드롭다운 옵션 채우기
    const catSel = document.getElementById('selectCategory');
    categories.forEach(({ name, count }) => {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = `${name} (${count})`;
        catSel.appendChild(opt);
    });

    const hazSel = document.getElementById('selectHazard');
    hazardLevels.forEach(({ name, count }) => {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = `${name} (${count})`;
        hazSel.appendChild(opt);
    });

    const mfrSel = document.getElementById('selectManufacturer');
    manufacturers.forEach(({ name, count }) => {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = `${name} (${count})`;
        mfrSel.appendChild(opt);
    });

    // 인기 카테고리 빠른 태그 (상위 6개)
    const quickTags = document.getElementById('quickTags');
    categories.slice(0, 6).forEach(({ name }) => {
        const btn = document.createElement('button');
        btn.className = 'quick-tag';
        btn.textContent = `#${name}`;
        btn.addEventListener('click', () => {
            state.selectedCategory = name;
            catSel.value = name;
            activateTab('category');
            applyFilters();
        });
        quickTags.appendChild(btn);
    });
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

    // 초기화 버튼
    document.getElementById('resetFilters').addEventListener('click', resetFilters);

    // 검색하기 버튼
    document.getElementById('filterSearchBtn').addEventListener('click', () => {
        state.selectedCategory    = document.getElementById('selectCategory').value;
        state.selectedHazard      = document.getElementById('selectHazard').value;
        state.selectedManufacturer = document.getElementById('selectManufacturer').value;
        applyFilters();
    });

    // 탭 클릭
    document.querySelectorAll('.filter-tab-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            activateTab(this.dataset.tab);
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

    ['closeModal', 'closeModalBtn'].forEach(id => document.getElementById(id).addEventListener('click', closeModal));
    document.querySelector('#pdfModal .modal-overlay').addEventListener('click', closeModal);
}

// ========== 탭 활성화 ==========
function activateTab(tab) {
    document.querySelectorAll('.filter-tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelector(`.filter-tab-btn[data-tab="${tab}"]`).classList.add('active');

    // 해당 탭의 select wrap에 primary 스타일 적용
    ['category', 'hazard', 'manufacturer'].forEach(t => {
        document.getElementById(`selectWrap${t.charAt(0).toUpperCase() + t.slice(1)}`)
            .classList.toggle('filter-select-wrap--primary', t === tab);
    });
}

// ========== 검색 / 필터 ==========
function handleSearch(e) {
    state.searchQuery = e.target.value.trim().toLowerCase();
    document.getElementById('clearSearch').style.display = state.searchQuery ? 'block' : 'none';
    applyFilters();
}

function applyFilters() {
    fetchAndRender(1);
}

function resetFilters() {
    state.selectedCategory    = '';
    state.selectedHazard      = '';
    state.selectedManufacturer = '';
    state.searchQuery = '';
    document.getElementById('searchInput').value = '';
    document.getElementById('clearSearch').style.display = 'none';
    document.getElementById('selectCategory').value    = '';
    document.getElementById('selectHazard').value      = '';
    document.getElementById('selectManufacturer').value = '';
    activateTab('category');
    applyFilters();
}

// ========== 카드 렌더링 ==========
function renderCards() {
    const container = document.getElementById('cardsContainer');
    const noResults = document.getElementById('noResults');

    if (state.items.length === 0) {
        container.innerHTML = '';
        noResults.style.display = 'block';
        return;
    }
    noResults.style.display = 'none';

    container.innerHTML = state.items.map(m => {
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
    const active = state.searchQuery || state.selectedCategory || state.selectedHazard || state.selectedManufacturer;
    info.innerHTML = active ? `<i class="fas fa-info-circle"></i> <strong>${state.total}개</strong>의 결과` : '';
}

// ========== 페이지네이션 ==========
function renderPagination() {
    const container = document.getElementById('pagination');
    if (!container) return;
    const totalPages = Math.ceil(state.total / state.page_size);
    if (totalPages <= 1) { container.innerHTML = ''; return; }

    const buttons = [];
    if (state.page > 1) {
        buttons.push(`<button class="page-btn" data-page="${state.page - 1}"><i class="fas fa-chevron-left"></i> 이전</button>`);
    }
    const start = Math.max(1, state.page - 2);
    const end   = Math.min(totalPages, state.page + 2);
    for (let i = start; i <= end; i++) {
        buttons.push(`<button class="page-btn${i === state.page ? ' active' : ''}" data-page="${i}">${i}</button>`);
    }
    if (state.page < totalPages) {
        buttons.push(`<button class="page-btn" data-page="${state.page + 1}">다음 <i class="fas fa-chevron-right"></i></button>`);
    }
    container.innerHTML = buttons.join('');
    container.querySelectorAll('.page-btn').forEach(btn => {
        btn.addEventListener('click', () => fetchAndRender(parseInt(btn.dataset.page, 10)));
    });
}

// ========== PDF 상세 모달 ==========
async function openPDFModal(id) {
    document.getElementById('pdfModal').classList.add('active');
    document.body.style.overflow = 'hidden';
    document.getElementById('modalTitle').textContent = '로딩 중...';
    document.getElementById('msdsInfo').innerHTML = '';
    document.getElementById('msdsContentHtml').innerHTML = '';
    document.getElementById('pdfViewer').src = '';

    try {
        const m = await api.getMSDSById(id);

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
    } catch (err) {
        document.getElementById('modalTitle').textContent = '오류';
        document.getElementById('msdsInfo').innerHTML = `<p style="color:#dc2626;">상세 정보를 불러오지 못했습니다: ${escapeHtml(err.message)}</p>`;
    }
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
