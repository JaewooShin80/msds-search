document.addEventListener('DOMContentLoaded', async () => {
    try {
        const data = await api.getDashboard();
        renderSummary(data);
        renderCategoryChart(data.by_category);
        renderHazardChart(data.by_hazard);
        renderManufacturerChart(data.by_manufacturer);
        renderRecentTable(data.recent);
    } catch (err) {
        document.querySelector('.dashboard-main').innerHTML =
            `<div style="text-align:center;padding:4rem;color:#dc2626;">
                <i class="fas fa-exclamation-circle fa-2x"></i>
                <p style="margin-top:1rem;">데이터를 불러오지 못했습니다: ${escapeHtml(err.message)}</p>
             </div>`;
    }
});

function renderSummary(data) {
    document.getElementById('dTotal').textContent   = data.total;
    document.getElementById('dDanger').textContent  = data.by_hazard['위험'];
    document.getElementById('dWarning').textContent = data.by_hazard['경고'];
    document.getElementById('dCaution').textContent = data.by_hazard['해당없음'];
}

function renderCategoryChart(byCategory) {
    const active = byCategory.filter(c => c.count > 0);
    new Chart(document.getElementById('categoryChart'), {
        type: 'bar',
        data: {
            labels: active.map(c => c.name),
            datasets: [{
                label: '등록 건수',
                data: active.map(c => c.count),
                backgroundColor: '#3b82f6',
                borderRadius: 4,
            }],
        },
        options: {
            indexAxis: 'y',
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { stepSize: 1 }, beginAtZero: true },
            },
        },
    });
}

function renderHazardChart(byHazard) {
    new Chart(document.getElementById('hazardChart'), {
        type: 'doughnut',
        data: {
            labels: ['위험', '경고', '해당없음'],
            datasets: [{
                data: [byHazard['위험'], byHazard['경고'], byHazard['해당없음']],
                backgroundColor: ['#dc2626', '#f59e0b', '#10b981'],
            }],
        },
        options: {
            plugins: {
                legend: { position: 'bottom' },
            },
        },
    });
}

function renderManufacturerChart(byMfr) {
    new Chart(document.getElementById('manufacturerChart'), {
        type: 'bar',
        data: {
            labels: byMfr.map(m => m.name),
            datasets: [{
                label: '등록 건수',
                data: byMfr.map(m => m.count),
                backgroundColor: '#0ea5e9',
                borderRadius: 4,
            }],
        },
        options: {
            indexAxis: 'y',
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { stepSize: 1 }, beginAtZero: true },
            },
        },
    });
}

function renderRecentTable(recent) {
    const tbody = document.getElementById('recentTable');
    tbody.innerHTML = recent.map(r => `
        <tr>
            <td><a href="/?id=${r.id}">${escapeHtml(r.product_name)}</a></td>
            <td>${escapeHtml(r.category)}</td>
            <td><span class="hazard-badge ${escapeHtml(r.hazard_level)}">${escapeHtml(r.hazard_level)}</span></td>
            <td>${r.created_at?.slice(0, 16).replace('T', ' ') ?? '-'}</td>
        </tr>
    `).join('');
}
