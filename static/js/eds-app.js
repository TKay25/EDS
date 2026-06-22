/* ============================================================
   EDS App JavaScript — React-Inspired SPA Features
   Manages state, transitions, components, and interactions
   ============================================================ */

const EDS = (() => {
  'use strict';

  // ===== STATE MANAGEMENT (React-inspired) =====
  const state = {
    currentTab: null,
    sidebarOpen: true,
    toasts: [],
    modals: [],
    user: null,
    loading: false,
  };

  const listeners = {};

  function setState(key, value) {
    const old = state[key];
    state[key] = value;
    if (listeners[key]) {
      listeners[key].forEach(fn => fn(value, old));
    }
  }

  function useState(key, fn) {
    if (!listeners[key]) listeners[key] = [];
    listeners[key].push(fn);
    return () => {
      listeners[key] = listeners[key].filter(f => f !== fn);
    };
  }

  // ===== DOM UTILITIES =====
  function $(selector, context) {
    return (context || document).querySelector(selector);
  }

  function $$(selector, context) {
    return Array.from((context || document).querySelectorAll(selector));
  }

  function createElement(tag, attrs = {}, children = []) {
    const el = document.createElement(tag);
    Object.entries(attrs).forEach(([k, v]) => {
      if (k === 'className') el.className = v;
      else if (k === 'style' && typeof v === 'object') Object.assign(el.style, v);
      else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
      else if (k === 'innerHTML') el.innerHTML = v;
      else el.setAttribute(k, v);
    });
    children.forEach(c => {
      if (typeof c === 'string') el.appendChild(document.createTextNode(c));
      else if (c instanceof Node) el.appendChild(c);
    });
    return el;
  }

  // ===== TAB COMPONENT =====
  function initTabs() {
    $$('.eds-tab').forEach(tab => {
      tab.addEventListener('click', function (e) {
        e.preventDefault();
        const target = this.dataset.bsTarget || this.getAttribute('data-target');
        const parent = this.closest('.eds-tabs') || this.parentElement;

        // Deactivate all tabs in group
        parent.querySelectorAll('.eds-tab').forEach(t => t.classList.remove('active'));

        // Find the tab content container
        const tabContent = this.closest('.eds-card')?.querySelector('.tab-content') ||
                          document.querySelector(this.getAttribute('data-bs-target'));

        if (tabContent) {
          tabContent.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('show', 'active'));
          const pane = tabContent.querySelector(target);
          if (pane) {
            pane.classList.add('show', 'active');
            // Trigger transition
            pane.style.animation = 'none';
            pane.offsetHeight; // reflow
            pane.style.animation = 'edsFadeIn 0.3s ease';
          }
        }

        this.classList.add('active');
        setState('currentTab', target);
      });
    });
  }

  // ===== TOAST NOTIFICATION SYSTEM =====
  function showToast(message, type = 'info', duration = 4000) {
    const icons = {
      success: 'bi-check-circle-fill',
      error: 'bi-exclamation-circle-fill',
      info: 'bi-info-circle-fill',
      warning: 'bi-exclamation-triangle-fill',
    };

    const toast = createElement('div', {
      className: `eds-toast eds-toast-${type}`,
    }, [
      createElement('i', { className: `bi ${icons[type] || icons.info}` }),
      document.createTextNode(' ' + message),
    ]);

    document.body.appendChild(toast);
    setState('toasts', [...state.toasts, toast]);

    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateX(100px)';
      toast.style.transition = 'all 0.3s ease';
      setTimeout(() => {
        if (toast.parentNode) toast.remove();
        setState('toasts', state.toasts.filter(t => t !== toast));
      }, 300);
    }, duration);
  }

  // ===== MODAL SYSTEM =====
  function openModal(modalId) {
    const modalEl = document.getElementById(modalId);
    if (!modalEl) {
      // Try Bootstrap modal
      const bsModal = bootstrap?.Modal?.getInstance(document.getElementById(modalId)) ||
                     new bootstrap.Modal(document.getElementById(modalId));
      if (bsModal) bsModal.show();
      return;
    }

    modalEl.classList.add('show');
    modalEl.style.display = 'block';
    document.body.classList.add('modal-open');
    const backdrop = createElement('div', { className: 'modal-backdrop fade show' });
    document.body.appendChild(backdrop);
    setState('modals', [...state.modals, modalId]);
  }

  function closeModal(modalId) {
    const modalEl = document.getElementById(modalId);
    if (!modalEl) return;
    modalEl.classList.remove('show');
    modalEl.style.display = 'none';
    document.body.classList.remove('modal-open');
    $$('.modal-backdrop').forEach(b => b.remove());
    setState('modals', state.modals.filter(m => m !== modalId));
  }

  // ===== SIDEBAR TOGGLE =====
  function toggleSidebar() {
    const sidebar = $('.eds-sidebar');
    const main = $('.eds-main');
    if (sidebar && main) {
      const isCollapsed = sidebar.style.width === '60px' || sidebar.classList.contains('collapsed');
      if (isCollapsed) {
        sidebar.style.width = '';
        main.style.marginLeft = '';
        sidebar.classList.remove('collapsed');
      } else {
        sidebar.style.width = '60px';
        sidebar.style.minWidth = '60px';
        main.style.marginLeft = '80px';
        sidebar.classList.add('collapsed');
      }
      setState('sidebarOpen', !isCollapsed);
    }
  }

  // ===== LOADING OVERLAY =====
  function showLoading(message = 'Loading...') {
    hideLoading();
    const overlay = createElement('div', { className: 'eds-loader', id: 'eds-loader' }, [
      createElement('div', { className: 'eds-loader-logo', innerHTML: 'ECHELON DIGITAL SOLUTIONS' }),
      createElement('div', { className: 'eds-loader-tagline', innerHTML: message }),
      createElement('div', { className: 'eds-loader-bar' }, [
        createElement('div', { className: 'eds-loader-progress' })
      ]),
      createElement('div', { className: 'eds-loader-grid' }, [
        createElement('div', { className: 'eds-loader-grid-item' }),
        createElement('div', { className: 'eds-loader-grid-item' }),
        createElement('div', { className: 'eds-loader-grid-item' }),
        createElement('div', { className: 'eds-loader-grid-item' }),
      ])
    ]);
    document.body.appendChild(overlay);
    setState('loading', true);
  }

  function hideLoading() {
    const existing = document.getElementById('eds-loader');
    if (existing) {
      existing.style.opacity = '0';
      setTimeout(() => { if (existing.parentNode) existing.remove(); }, 300);
    }
    setState('loading', false);
  }

  // ===== SMOOTH SCROLL =====
  function initSmoothScroll() {
    $$('a[href^="#"]').forEach(anchor => {
      anchor.addEventListener('click', function (e) {
        const target = this.getAttribute('href');
        if (target === '#') return;
        const el = document.querySelector(target);
        if (el) {
          e.preventDefault();
          el.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      });
    });
  }

  // ===== BACK TO TOP =====
  function initBackToTop() {
    const btn = document.getElementById('back-to-top');
    if (!btn) return;
    window.addEventListener('scroll', () => {
      if (window.scrollY > 300) btn.classList.add('active');
      else btn.classList.remove('active');
    });
    btn.addEventListener('click', () => {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });
  }

  // ===== STICKY NAVBAR =====
  function initStickyNavbar() {
    const navbar = document.querySelector('.eds-navbar');
    if (!navbar) return;
    window.addEventListener('scroll', () => {
      if (window.scrollY > 50) navbar.classList.add('scrolled');
      else navbar.classList.remove('scrolled');
    });
  }

  // ===== DATATABLE INIT =====
  function initDataTables() {
    $$('.eds-table').forEach(table => {
      const id = table.id || 'table-' + Math.random().toString(36).slice(2);
      if (!table.id) table.id = id;
      if (typeof $ !== 'undefined' && $.fn?.DataTable) {
        try {
          $(`#${id}`).DataTable({
            responsive: true,
            autoWidth: false,
            pageLength: 25,
            language: { search: 'Filter records:' }
          });
        } catch (e) {
          // DataTable already initialized
        }
      }
    });
  }

  // ===== CHOICES.JS INIT =====
  function initChoices() {
    if (typeof Choices !== 'undefined') {
      $$('select.eds-form-select').forEach(select => {
        try { new Choices(select, { searchEnabled: true }); } catch (e) {}
      });
    }
  }

  // ===== FLATPICKR INIT =====
  function initDatepickers() {
    if (typeof flatpickr !== 'undefined') {
      $$('input[type="date"].eds-form-input').forEach(input => {
        try {
          flatpickr(input, {
            dateFormat: 'Y-m-d',
            allowInput: true,
          });
        } catch (e) {}
      });
    }
  }

  // ===== CURRENT DATE DISPLAY =====
  function updateDateDisplay() {
    $$('.eds-header-date, .current-date').forEach(el => {
      if (!el.dataset.edsUpdated) {
        const now = new Date();
        const options = { year: 'numeric', month: 'long', day: 'numeric' };
        el.textContent = now.toLocaleDateString('en-US', options);
        el.dataset.edsUpdated = 'true';
      }
    });
  }

  // ===== SEARCH EMPLOYEES (filter list) =====
  function initEmployeeSearch() {
    $$('input[id$="Search"], input[id*="employeeSearch"]').forEach(input => {
      input.addEventListener('keyup', function () {
        const filter = this.value.toLowerCase();
        const list = this.closest('.modal-body, div')?.querySelector('#employeeList, [id*="employeeList"]');
        if (list) {
          $$('.employee-item', list).forEach(item => {
            const label = item.querySelector('label');
            if (label) {
              item.style.display = label.textContent.toLowerCase().includes(filter) ? '' : 'none';
            }
          });
        }
      });
    });
  }

  // ===== INIT ALL =====
  function init() {
    initTabs();
    initSmoothScroll();
    initBackToTop();
    initStickyNavbar();
    initEmployeeSearch();
    updateDateDisplay();

    // Init on next tick for dynamic content
    setTimeout(() => {
      initDataTables();
      initChoices();
      initDatepickers();
    }, 100);

    // Re-init on Bootstrap tab shown
    document.addEventListener('shown.bs.tab', () => {
      setTimeout(() => {
        initDataTables();
      }, 50);
    });

    console.log('✅ EDS App initialized');
  }

  // Auto-init on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // ===== PUBLIC API =====
  return {
    showToast,
    openModal,
    closeModal,
    showLoading,
    hideLoading,
    toggleSidebar,
    setState,
    useState,
    $,
    $$,
    createElement,
  };
})();
