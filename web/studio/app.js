// ==========================================================================
// VortexMQ Quantum Studio — Interactive Application Engine
// ==========================================================================

const API_BASE = '/api/v1';

// State
let clusterStats = {
    uptime_seconds: 0,
    total_topics: 0,
    total_published: 0,
    total_consumed: 0,
    total_acked: 0,
    total_nacked: 0,
    total_dlq: 0,
    topics: []
};

// Templates
const SAMPLE_TEMPLATES = {
    order: {
        event: "order.created",
        order_id: "ORD-94821",
        customer_id: "CUST-819",
        amount: 249.99,
        currency: "USD",
        items: [{ sku: "PROD-A", qty: 1 }, { sku: "PROD-B", qty: 2 }],
        timestamp: new Date().toISOString()
    },
    user: {
        event: "user.signup",
        user_id: "USR-3041",
        email: "alex.chen@vortexmq.io",
        tier: "enterprise",
        region: "us-east-1",
        created_at: new Date().toISOString()
    },
    ai: {
        task: "embedding.generate",
        model: "text-embedding-3-large",
        prompt: "Analyze concurrent memory safety and zero-allocation ring buffers in pure Go.",
        max_tokens: 512,
        priority: 1
    },
    iot: {
        device_id: "SENSOR-ALPHA-9",
        reading_type: "temperature_celsius",
        value: 24.6,
        humidity_percent: 48.2,
        battery_level: 0.94
    }
};

document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    initNavigation();
    initPlayground();
    initDLQControls();

    // Start Telemetry Poller
    fetchTelemetry();
    setInterval(fetchTelemetry, 1000);
});

// Theme Management
function initTheme() {
    const savedTheme = localStorage.getItem('vortexmq-theme') || 'dark';
    setTheme(savedTheme);

    const toggleBtn = document.getElementById('theme-toggle');
    toggleBtn.addEventListener('click', () => {
        const current = document.body.getAttribute('data-theme');
        const next = current === 'dark' ? 'light' : 'dark';
        setTheme(next);
    });
}

function setTheme(theme) {
    document.body.setAttribute('data-theme', theme);
    localStorage.setItem('vortexmq-theme', theme);
    const icon = document.getElementById('theme-icon');
    if (icon) {
        icon.textContent = theme === 'dark' ? '☀️' : '🌙';
    }
}

// Navigation Tabs
function initNavigation() {
    const tabs = document.querySelectorAll('.nav-tab');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

            tab.classList.add('active');
            const target = tab.getAttribute('data-tab');
            const targetEl = document.getElementById(`tab-${target}`);
            if (targetEl) targetEl.classList.add('active');

            if (target === 'dlq') fetchDLQ();
            if (target === 'topics') renderTopics();
        });
    });

    const openPublisherBtn = document.getElementById('btn-open-publisher');
    if (openPublisherBtn) {
        openPublisherBtn.addEventListener('click', () => {
            const playTab = document.querySelector('[data-tab="playground"]');
            if (playTab) playTab.click();
        });
    }

    const refreshTopicsBtn = document.getElementById('btn-refresh-topics');
    if (refreshTopicsBtn) {
        refreshTopicsBtn.addEventListener('click', () => {
            fetchTelemetry();
            showToast('Topics refreshed');
        });
    }
}

// Telemetry Poller
async function fetchTelemetry() {
    try {
        const res = await fetch(`${API_BASE}/stats`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        clusterStats = data;
        updateUI();
    } catch (err) {
        const statusEl = document.getElementById('conn-status');
        if (statusEl) {
            statusEl.textContent = 'DISCONNECTED';
            statusEl.parentElement.style.borderColor = 'rgba(255, 51, 102, 0.4)';
            statusEl.parentElement.style.color = '#ff3366';
        }
    }
}

function updateUI() {
    // Header Bar
    document.getElementById('stat-uptime').textContent = formatDuration(clusterStats.uptime_seconds);
    document.getElementById('stat-topics').textContent = clusterStats.total_topics || 0;
    document.getElementById('stat-published').textContent = formatNumber(clusterStats.total_published || 0);
    document.getElementById('stat-consumed').textContent = formatNumber(clusterStats.total_consumed || 0);
    document.getElementById('stat-dlq').textContent = clusterStats.total_dlq || 0;

    const dlqBadge = document.getElementById('dlq-badge');
    if (dlqBadge) {
        dlqBadge.textContent = clusterStats.total_dlq || 0;
        dlqBadge.style.display = clusterStats.total_dlq > 0 ? 'inline-block' : 'none';
    }

    // Overview Big Cards
    document.getElementById('metric-total-pub').textContent = formatNumber(clusterStats.total_published || 0);
    document.getElementById('metric-total-sub').textContent = formatNumber(clusterStats.total_consumed || 0);
    document.getElementById('metric-total-acked').textContent = formatNumber(clusterStats.total_acked || 0);
    document.getElementById('metric-total-dlq').textContent = formatNumber(clusterStats.total_dlq || 0);

    // Flow Topology Nodes
    const flowDlqEl = document.getElementById('flow-dlq-count');
    if (flowDlqEl) flowDlqEl.textContent = `${clusterStats.total_dlq || 0} Poisoned`;

    renderTopics();
}

// Render Topics
function renderTopics() {
    const container = document.getElementById('topics-container');
    if (!container) return;

    if (!clusterStats.topics || clusterStats.topics.length === 0) {
        container.innerHTML = `<div class="empty-state">No active topics found. Publish an event in the Message Playground!</div>`;
        return;
    }

    container.innerHTML = clusterStats.topics.map(t => `
        <div class="topic-card">
            <div class="topic-card-header">
                <span class="topic-name"># ${escapeHTML(t.name)}</span>
                <span class="badge-version">${t.length} queued</span>
            </div>
            <div class="topic-stats-row">
                <div class="stat-item">
                    <span class="label">PUBLISHED</span>
                    <span class="val neon-cyan">${formatNumber(t.published)}</span>
                </div>
                <div class="stat-item">
                    <span class="label">CONSUMED</span>
                    <span class="val neon-green">${formatNumber(t.consumed)}</span>
                </div>
                <div class="stat-item">
                    <span class="label">GROUPS</span>
                    <span class="val">${t.consumer_groups}</span>
                </div>
                <div class="stat-item">
                    <span class="label">DLQ POISONED</span>
                    <span class="val neon-red">${t.dead_letter_count}</span>
                </div>
            </div>
        </div>
    `).join('');
}

// DLQ Management
function initDLQControls() {
    const refreshBtn = document.getElementById('btn-refresh-dlq');
    if (refreshBtn) refreshBtn.addEventListener('click', fetchDLQ);

    const purgeBtn = document.getElementById('btn-purge-dlq');
    if (purgeBtn) {
        purgeBtn.addEventListener('click', async () => {
            if (!confirm('Are you sure you want to permanently purge all Dead Letter Queues?')) return;
            try {
                const res = await fetch(`${API_BASE}/dlq/purge`, { method: 'DELETE' });
                if (res.ok) {
                    showToast('Dead Letter Queues purged successfully');
                    fetchDLQ();
                    fetchTelemetry();
                }
            } catch (err) {
                showToast('Failed to purge DLQ: ' + err.message);
            }
        });
    }
}

async function fetchDLQ() {
    const tbody = document.getElementById('dlq-table-body');
    if (!tbody) return;

    try {
        const res = await fetch(`${API_BASE}/dlq`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        if (!data || data.length === 0) {
            tbody.innerHTML = `<tr><td colspan="7" class="empty-table-cell">No dead-lettered messages! All tasks running healthy.</td></tr>`;
            return;
        }

        tbody.innerHTML = data.map(item => `
            <tr>
                <td><code>${escapeHTML(item.message.id)}</code></td>
                <td><strong>${escapeHTML(item.message.topic)}</strong></td>
                <td><span style="color: var(--accent-red); font-weight: 600;">${escapeHTML(item.failure_cause)}</span></td>
                <td><code>${item.attempts} attempts</code></td>
                <td>${new Date(item.failed_at / 1e6).toLocaleTimeString()}</td>
                <td><div class="payload-preview">${escapeHTML(atob(item.message.payload || '')) || escapeHTML(JSON.stringify(item.message.payload))}</div></td>
                <td>
                    <button class="btn-replay" onclick="replayMessage('${escapeHTML(item.message.topic)}', '${escapeHTML(item.message.id)}')">
                        ⚡ Replay
                    </button>
                </td>
            </tr>
        `).join('');
    } catch (err) {
        tbody.innerHTML = `<tr><td colspan="7" class="empty-table-cell" style="color: var(--accent-red)">Error loading DLQ: ${escapeHTML(err.message)}</td></tr>`;
    }
}

async function replayMessage(topic, id) {
    try {
        const res = await fetch(`${API_BASE}/dlq/replay`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ topic, id })
        });
        if (res.ok) {
            showToast(`Message ${id} re-queued to #${topic}!`);
            fetchDLQ();
            fetchTelemetry();
        } else {
            showToast('Replay failed');
        }
    } catch (err) {
        showToast('Error replaying message: ' + err.message);
    }
}

// Playground
function initPlayground() {
    const topicInput = document.getElementById('pg-topic');
    const delayInput = document.getElementById('pg-delay');
    const payloadInput = document.getElementById('pg-payload');
    const dispatchBtn = document.getElementById('btn-dispatch');
    const resultEl = document.getElementById('dispatch-result');

    // Default template
    if (payloadInput && !payloadInput.value) {
        payloadInput.value = JSON.stringify(SAMPLE_TEMPLATES.order, null, 2);
    }

    // Template Chips
    document.querySelectorAll('.btn-chip').forEach(btn => {
        btn.addEventListener('click', () => {
            const tplKey = btn.getAttribute('data-tpl');
            if (SAMPLE_TEMPLATES[tplKey]) {
                payloadInput.value = JSON.stringify(SAMPLE_TEMPLATES[tplKey], null, 2);
            }
        });
    });

    if (dispatchBtn) {
        dispatchBtn.addEventListener('click', async () => {
            const topic = (topicInput.value || 'events').trim();
            const delayMs = parseInt(delayInput.value, 10) || 0;
            const payload = payloadInput.value.trim();

            if (!payload) {
                showToast('Please enter a payload');
                return;
            }

            dispatchBtn.disabled = true;
            resultEl.textContent = 'Dispatching event...';

            try {
                const res = await fetch(`${API_BASE}/topics/${encodeURIComponent(topic)}/publish`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        payload: payload,
                        delay_ms: delayMs
                    })
                });

                const data = await res.json();
                if (res.ok) {
                    resultEl.innerHTML = `<span style="color: var(--accent-green)">✓ Dispatched successfully! Message ID: <code>${data.id}</code></span>`;
                    showToast(`Event published to #${topic}`);
                    fetchTelemetry();
                } else {
                    resultEl.innerHTML = `<span style="color: var(--accent-red)">✗ Failed: ${data.error || 'Unknown error'}</span>`;
                }
            } catch (err) {
                resultEl.innerHTML = `<span style="color: var(--accent-red)">✗ Network error: ${err.message}</span>`;
            } finally {
                dispatchBtn.disabled = false;
            }
        });
    }
}

// Toast Helper
function showToast(msg) {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.innerHTML = `<span>⚡</span> <span>${escapeHTML(msg)}</span>`;
    container.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(10px)';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// Formatting Utilities
function formatNumber(num) {
    if (num >= 1e6) return (num / 1e6).toFixed(2) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(1) + 'k';
    return num.toLocaleString();
}

function formatDuration(sec) {
    if (sec < 60) return `${sec}s`;
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    if (m < 60) return `${m}m ${s}s`;
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m`;
}

function escapeHTML(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}
