/**
 * PulseStream Live Real-Time Dashboard Client
 * Handles WebSocket connection, metric counter animation, live filtering, and event streams.
 */

(function () {
  'use strict';

  // State Management
  const state = {
    connected: false,
    reconnectAttempts: 0,
    isPaused: false,
    filterSearch: '',
    filterType: 'ALL',
    filterSource: 'ALL',
    displayedEventIds: new Set(),
    rawEvents: [],
    metrics: {
      rate: 0,
      total: 0,
      users: 0,
      anomalies: 0,
    },
  };

  // DOM Elements
  const els = {
    // Status
    wsStatusDot: document.getElementById('ws-status-dot'),
    wsStatusText: document.getElementById('ws-status-text'),
    // KPI Cards
    statRate: document.getElementById('stat-rate'),
    statTotal: document.getElementById('stat-total'),
    statUsers: document.getElementById('stat-users'),
    statAnomalies: document.getElementById('stat-anomalies'),
    badgeAnomaliesCount: document.getElementById('badge-anomalies-count'),
    // Ticker
    tickerWikiVal: document.getElementById('ticker-wiki-val'),
    tickerSimVal: document.getElementById('ticker-sim-val'),
    // Streams & Lists
    eventFeed: document.getElementById('event-feed'),
    feedEmptyState: document.getElementById('feed-empty-state'),
    trendingList: document.getElementById('trending-list'),
    anomaliesList: document.getElementById('anomalies-list'),
    // Controls
    searchInput: document.getElementById('event-search-input'),
    typeFilter: document.getElementById('event-type-filter'),
    sourceFilter: document.getElementById('event-source-filter'),
    pauseBtn: document.getElementById('pause-stream-btn'),
    pauseBtnText: document.getElementById('pause-btn-text'),
    // Distribution Bars
    distPostCount: document.getElementById('dist-count-post'),
    distCommentCount: document.getElementById('dist-count-comment'),
    distReactionCount: document.getElementById('dist-count-reaction'),
    distJoinedCount: document.getElementById('dist-count-joined'),
    distBarPost: document.getElementById('dist-bar-post'),
    distBarComment: document.getElementById('dist-bar-comment'),
    distBarReaction: document.getElementById('dist-bar-reaction'),
    distBarJoined: document.getElementById('dist-bar-joined'),
    // Modal
    modal: document.getElementById('event-modal'),
    modalCloseBtn: document.getElementById('modal-close-btn'),
    modalJson: document.getElementById('modal-json'),
    modalTitle: document.getElementById('modal-title'),
  };

  // Smooth number rolling animation
  function animateNumber(element, start, end, duration = 600, isFloat = false) {
    if (!element) return;
    if (start === end) {
      element.textContent = isFloat ? end.toFixed(1) : end.toLocaleString();
      return;
    }

    const startTime = performance.now();
    function update(time) {
      const elapsed = time - startTime;
      const progress = Math.min(elapsed / duration, 1);
      // Ease out cubic
      const ease = 1 - Math.pow(1 - progress, 3);
      const current = start + (end - start) * ease;

      element.textContent = isFloat
        ? current.toFixed(1)
        : Math.round(current).toLocaleString();

      if (progress < 1) {
        requestAnimationFrame(update);
      } else {
        element.textContent = isFloat ? end.toFixed(1) : end.toLocaleString();
      }
    }
    requestAnimationFrame(update);
  }

  // Format timestamps
  function formatTime(isoOrTimestamp) {
    if (!isoOrTimestamp) return '';
    const date = typeof isoOrTimestamp === 'number'
      ? new Date(isoOrTimestamp)
      : new Date(isoOrTimestamp);
    return date.toTimeString().split(' ')[0] + '.' + String(date.getMilliseconds()).padStart(3, '0');
  }

  // WebSocket Connection
  let ws = null;
  let pingInterval = null;

  function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host || 'localhost:8000';
    const wsUrl = `${protocol}//${host}/api/v1/ws/dashboard`;

    updateStatus('connecting', 'Connecting...');

    try {
      ws = new WebSocket(wsUrl);
    } catch (err) {
      console.error('Failed to instantiate WebSocket:', err);
      scheduleReconnect();
      return;
    }

    ws.onopen = function () {
      console.log('Connected to PulseStream WebSocket');
      state.connected = true;
      state.reconnectAttempts = 0;
      updateStatus('connected', 'Live Connected');

      // Setup ping keepalive
      if (pingInterval) clearInterval(pingInterval);
      pingInterval = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send('ping');
        }
      }, 15000);
    };

    ws.onmessage = function (event) {
      if (event.data === 'pong') return;
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === 'dashboard_update') {
          handleDashboardUpdate(payload);
        }
      } catch (e) {
        console.error('Error handling WebSocket message:', e);
      }
    };

    ws.onerror = function (err) {
      console.warn('WebSocket error encountered:', err);
    };

    ws.onclose = function () {
      state.connected = false;
      if (pingInterval) clearInterval(pingInterval);
      updateStatus('disconnected', 'Disconnected');
      scheduleReconnect();
    };
  }

  function scheduleReconnect() {
    const delay = Math.min(1000 * Math.pow(1.5, state.reconnectAttempts), 10000);
    state.reconnectAttempts++;
    updateStatus('connecting', `Reconnecting (${state.reconnectAttempts})...`);
    setTimeout(connectWebSocket, delay);
  }

  function updateStatus(statusType, message) {
    els.wsStatusText.textContent = message;
    els.wsStatusDot.className = 'status-dot';

    if (statusType === 'connected') {
      els.wsStatusDot.classList.add('connected');
    } else if (statusType === 'disconnected') {
      els.wsStatusDot.classList.add('disconnected');
    } else {
      els.wsStatusDot.classList.add('pulse');
    }
  }

  // Handle incoming dashboard update payload
  function handleDashboardUpdate(data) {
    const metrics = data.metrics || {};

    // 1. Update Throughput (events/sec)
    const newRate = metrics.events_per_second || 0;
    animateNumber(els.statRate, state.metrics.rate, newRate, 500, true);
    state.metrics.rate = newRate;

    // 2. Update Total Events Today
    const newTotal = metrics.total_events_today || 0;
    animateNumber(els.statTotal, state.metrics.total, newTotal, 500, false);
    state.metrics.total = newTotal;

    // 3. Update Active Users
    const newUsers = metrics.active_users || 0;
    animateNumber(els.statUsers, state.metrics.users, newUsers, 500, false);
    state.metrics.users = newUsers;

    // 4. Update Source Tickers
    const bySource = metrics.events_by_source || {};
    const wikiVal = bySource['WIKIPEDIA'] || 0;
    const simVal = bySource['SIMULATOR'] || 0;
    els.tickerWikiVal.textContent = wikiVal.toLocaleString();
    els.tickerSimVal.textContent = simVal.toLocaleString();

    // 5. Update Type Distribution Bars
    const byType = metrics.events_by_type || {};
    const postCount = byType['POST_CREATED'] || 0;
    const commentCount = byType['COMMENT_CREATED'] || 0;
    const reactionCount = byType['REACTION_ADDED'] || 0;
    const joinedCount = byType['USER_JOINED'] || 0;
    const sumTypes = postCount + commentCount + reactionCount + joinedCount || 1;

    els.distPostCount.textContent = postCount.toLocaleString();
    els.distCommentCount.textContent = commentCount.toLocaleString();
    els.distReactionCount.textContent = reactionCount.toLocaleString();
    els.distJoinedCount.textContent = joinedCount.toLocaleString();

    els.distBarPost.style.width = `${Math.min(100, Math.round((postCount / sumTypes) * 100))}%`;
    els.distBarComment.style.width = `${Math.min(100, Math.round((commentCount / sumTypes) * 100))}%`;
    els.distBarReaction.style.width = `${Math.min(100, Math.round((reactionCount / sumTypes) * 100))}%`;
    els.distBarJoined.style.width = `${Math.min(100, Math.round((joinedCount / sumTypes) * 100))}%`;

    // 6. Update Trending Communities
    renderTrending(data.trending || []);

    // 7. Update Recent Anomalies
    renderAnomalies(data.recent_anomalies || []);

    // 8. Update Live Event Feed
    if (!state.isPaused) {
      renderRecentEvents(data.recent_events || []);
    }
  }

  // Render Trending Communities
  function renderTrending(trendingList) {
    if (!trendingList || trendingList.length === 0) {
      els.trendingList.innerHTML = '<div class="empty-list-placeholder">No trending communities yet</div>';
      return;
    }

    els.trendingList.innerHTML = trendingList
      .map(
        (item) => `
        <div class="trending-item">
          <div class="trending-left">
            <span class="trending-rank">#${item.rank}</span>
            <span class="trending-name">${escapeHtml(item.community_id)}</span>
          </div>
          <span class="trending-score">${item.score.toLocaleString()} pts</span>
        </div>
      `
      )
      .join('');
  }

  // Render Anomalies
  function renderAnomalies(anomalies) {
    const count = anomalies.length;
    animateNumber(els.statAnomalies, state.metrics.anomalies, count, 400, false);
    state.metrics.anomalies = count;
    els.badgeAnomaliesCount.textContent = `${count} DETECTED`;

    if (count === 0) {
      els.anomaliesList.innerHTML = '<div class="empty-list-placeholder">No anomalies detected</div>';
      return;
    }

    els.anomaliesList.innerHTML = anomalies
      .map(
        (a) => `
        <div class="anomaly-item">
          <div class="anomaly-top">
            <span class="anomaly-type">${escapeHtml(a.anomaly_type)}</span>
            <span class="anomaly-severity sev-${a.severity}">${a.severity}</span>
          </div>
          <p class="anomaly-desc">${escapeHtml(a.description)}</p>
        </div>
      `
      )
      .join('');
  }

  // Render Recent Events Feed
  function renderRecentEvents(newEvents) {
    if (!newEvents || newEvents.length === 0) return;

    // Filter incoming events
    let hasNewItems = false;
    for (let i = newEvents.length - 1; i >= 0; i--) {
      const ev = newEvents[i];
      if (!ev.event_id || state.displayedEventIds.has(ev.event_id)) {
        continue;
      }

      state.displayedEventIds.add(ev.event_id);
      state.rawEvents.unshift(ev);
      hasNewItems = true;

      // Keep up to 100 in memory
      if (state.rawEvents.length > 100) {
        const removed = state.rawEvents.pop();
        state.displayedEventIds.delete(removed.event_id);
      }
    }

    if (hasNewItems) {
      applyEventFilters();
    }
  }

  // Filter and display events in feed
  function applyEventFilters() {
    const searchLower = state.filterSearch.trim().toLowerCase();

    const filtered = state.rawEvents.filter((ev) => {
      // Type filter
      if (state.filterType !== 'ALL' && ev.event_type !== state.filterType) {
        return false;
      }
      // Source filter
      if (state.filterSource !== 'ALL' && ev.source !== state.filterSource) {
        return false;
      }
      // Search term
      if (searchLower) {
        const comm = (ev.community_id || '').toLowerCase();
        const user = (ev.user_id || '').toLowerCase();
        const payloadStr = typeof ev.payload === 'string' ? ev.payload.toLowerCase() : JSON.stringify(ev.payload).toLowerCase();
        if (!comm.includes(searchLower) && !user.includes(searchLower) && !payloadStr.includes(searchLower)) {
          return false;
        }
      }
      return true;
    });

    if (filtered.length === 0) {
      els.eventFeed.innerHTML = `
        <div class="empty-state">
          <p>No matching events found for current filters.</p>
        </div>
      `;
      return;
    }

    // Build DOM elements
    els.eventFeed.innerHTML = '';
    filtered.slice(0, 40).forEach((ev) => {
      const item = document.createElement('div');
      item.className = 'event-item';
      item.onclick = () => openInspectionModal(ev);

      let payloadSummary = '';
      try {
        const p = typeof ev.payload === 'string' ? JSON.parse(ev.payload) : ev.payload;
        if (p.title) payloadSummary = `Title: ${p.title}`;
        else if (p.comment) payloadSummary = `Comment: ${p.comment}`;
        else if (p.text) payloadSummary = p.text;
        else payloadSummary = JSON.stringify(p);
      } catch (e) {
        payloadSummary = String(ev.payload);
      }

      item.innerHTML = `
        <div class="event-item-top">
          <div class="event-badges">
            <span class="badge-type badge-${ev.event_type}">${ev.event_type}</span>
            <span class="badge-source">${ev.source}</span>
          </div>
          <span class="event-time">${formatTime(ev.timestamp)}</span>
        </div>
        <div class="event-item-body">
          <span class="event-community">${escapeHtml(ev.community_id || 'Global')}</span>
          <span class="event-user">@${escapeHtml(ev.user_id)}</span>
          <span class="event-summary">${escapeHtml(payloadSummary)}</span>
        </div>
      `;

      els.eventFeed.appendChild(item);
    });
  }

  // Modal inspection
  function openInspectionModal(ev) {
    els.modalTitle.textContent = `Event: ${ev.event_id}`;
    els.modalJson.textContent = JSON.stringify(ev, null, 2);
    els.modal.classList.add('active');
  }

  function closeModal() {
    els.modal.classList.remove('active');
  }

  // Utility to escape HTML
  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // Event Listeners for Filters & Controls
  els.searchInput.addEventListener('input', (e) => {
    state.filterSearch = e.target.value;
    applyEventFilters();
  });

  els.typeFilter.addEventListener('change', (e) => {
    state.filterType = e.target.value;
    applyEventFilters();
  });

  els.sourceFilter.addEventListener('change', (e) => {
    state.filterSource = e.target.value;
    applyEventFilters();
  });

  els.pauseBtn.addEventListener('click', () => {
    state.isPaused = !state.isPaused;
    if (state.isPaused) {
      els.pauseBtn.classList.add('paused');
      els.pauseBtnText.textContent = 'Resume';
    } else {
      els.pauseBtn.classList.remove('paused');
      els.pauseBtnText.textContent = 'Pause';
      applyEventFilters();
    }
  });

  els.modalCloseBtn.addEventListener('click', closeModal);
  els.modal.addEventListener('click', (e) => {
    if (e.target === els.modal) closeModal();
  });
  window.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeModal();
  });

  // Initial startup
  connectWebSocket();
})();
