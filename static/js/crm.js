/* ==========================================================================
   EDS Company System — CRM portal
   Tabs, sales analytics filters, the product catalogue editor and the
   WhatsApp thread (newest message first, customer left / agent right).
   ========================================================================== */
(function () {
  'use strict';

  var root = document.getElementById('crmInbox');
  if (!root) return;

  var CURRENCY = root.getAttribute('data-currency') || 'USD';

  function escapeHtml(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function money(value) {
    var number = Number(value);
    if (!isFinite(number)) return CURRENCY + ' 0.00';
    return CURRENCY + ' ' + number.toFixed(2);
  }

  function postJson(url, payload) {
    return fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {}),
    }).then(function (response) {
      return response.json().catch(function () {
        return { success: false, message: 'Unexpected server response.' };
      });
    });
  }

  /* ------------------------------------------------------------------ tabs */
  function initTabs() {
    var tabs = Array.prototype.slice.call(root.querySelectorAll('[data-crm-tab]'));
    var views = Array.prototype.slice.call(root.querySelectorAll('[data-crm-view]'));
    if (!tabs.length) return;

    function show(name) {
      tabs.forEach(function (tab) {
        tab.classList.toggle('is-active', tab.getAttribute('data-crm-tab') === name);
        tab.setAttribute('aria-selected', tab.getAttribute('data-crm-tab') === name ? 'true' : 'false');
      });
      views.forEach(function (view) {
        view.classList.toggle('is-active', view.getAttribute('data-crm-view') === name);
      });
      if (name === 'analytics') analytics.load(false);
    }

    tabs.forEach(function (tab) {
      tab.addEventListener('click', function () { show(tab.getAttribute('data-crm-tab')); });
    });
  }

  /* ------------------------------------------------------------- analytics */
  var analytics = (function () {
    var periodEl = document.getElementById('crmPeriod');
    var paymentEl = document.getElementById('crmPayment');
    var fulfilmentEl = document.getElementById('crmFulfilment');
    var statsEl = document.getElementById('crmAnalyticsStats');
    var topEl = document.getElementById('crmTopItems');
    var recentEl = document.getElementById('crmRecentOrders');
    var bodyEl = document.getElementById('crmOrdersBody');
    var loaded = false;
    var inFlight = false;

    function filters() {
      return {
        period: periodEl ? periodEl.value : 'today',
        payment: paymentEl ? paymentEl.value : 'all',
        fulfilment: fulfilmentEl ? fulfilmentEl.value : 'all',
      };
    }

    function query() {
      var state = filters();
      return 'period=' + encodeURIComponent(state.period) +
             '&payment=' + encodeURIComponent(state.payment) +
             '&fulfilment=' + encodeURIComponent(state.fulfilment);
    }

    function renderStats(data) {
      if (!statsEl) return;
      Object.keys(data || {}).forEach(function (key) {
        if (typeof data[key] === 'object') return;
        Array.prototype.forEach.call(statsEl.querySelectorAll('[data-an="' + key + '"]'), function (node) {
          var value = data[key];
          node.textContent = (key === 'value' || key === 'paid_value') ? money(value) : value;
        });
      });
      Array.prototype.forEach.call(document.querySelectorAll('.ps-chip [data-an="orders"]'), function (node) {
        node.textContent = data.orders;
      });
    }

    function renderTop(items) {
      if (!topEl) return;
      if (!items || !items.length) {
        topEl.innerHTML = '<p class="ps-muted crm-empty-note">No orders in this period yet.</p>';
        return;
      }
      var peak = items.reduce(function (max, item) { return Math.max(max, item.units); }, 0) || 1;
      topEl.innerHTML = items.map(function (item) {
        var width = Math.max(6, Math.round((item.units / peak) * 100));
        return '<div class="crm-top-item">' +
          '<div class="crm-top-head"><strong>' + escapeHtml(item.product) + '</strong>' +
          '<span>' + item.units + ' units</span></div>' +
          '<div class="crm-bar"><i style="width:' + width + '%"></i></div>' +
          '</div>';
      }).join('');
    }

    function renderRecent(orders) {
      if (!recentEl) return;
      if (!orders || !orders.length) {
        recentEl.innerHTML = '<p class="ps-muted crm-empty-note">No orders in this period yet.</p>';
        return;
      }
      recentEl.innerHTML = orders.slice(0, 5).map(function (order) {
        var who = order.customer_name || order.company || ('+' + order.wa_number);
        return '<div class="crm-order-row">' +
          '<div class="crm-order-row-main"><strong>' + escapeHtml(who) + '</strong>' +
          '<small>' + escapeHtml(order.reference) + ' • ' + order.items + ' items • ' +
          escapeHtml(order.created_at) + '</small></div>' +
          '<span class="crm-pill ' + (order.payment_status === 'paid' ? 'is-paid' : 'is-unpaid') + '">' +
          (order.payment_status === 'paid' ? 'Paid' : 'Will pay') + '</span>' +
          '</div>';
      }).join('');
    }

    function renderTable(orders) {
      if (!bodyEl) return;
      if (!orders || !orders.length) {
        bodyEl.innerHTML = '<tr><td colspan="7" class="crm-empty-cell">No orders match these filters.</td></tr>';
        return;
      }
      var state = filters();
      bodyEl.innerHTML = orders.map(function (order) {
        var who = order.customer_name || order.company || ('+' + order.wa_number);
        var contact = order.company ? '<span class="crm-sub">' + escapeHtml(order.company) + '</span>' : '';
        var address = order.address ? '<span class="crm-sub">' + escapeHtml(order.address) + '</span>' : '';
        var paid = order.payment_status === 'paid';
        var delivered = order.delivery_method === 'delivery';
        return '<tr data-reference="' + escapeHtml(order.reference) + '">' +
          '<td><span class="crm-ref">' + escapeHtml(order.reference) + '</span>' + address + '</td>' +
          '<td>' + escapeHtml(who) + contact + '</td>' +
          '<td>' + order.items + '</td>' +
          '<td><span class="crm-pill ' + (paid ? 'is-paid' : 'is-unpaid') + '">' +
          (paid ? '<i class="bi bi-check2"></i> Paid' : '<i class="bi bi-clock"></i> Will pay') + '</span></td>' +
          '<td>' + money(order.amount) + '</td>' +
          '<td><div class="crm-row-actions">' +
          '<button class="crm-mini-btn' + (delivered ? ' is-active' : '') + '" type="button" ' +
          'data-fulfil="delivery" title="Delivered to the customer">Home</button>' +
          '<button class="crm-mini-btn' + (!delivered ? ' is-active' : '') + '" type="button" ' +
          'data-fulfil="collection" title="Collected from the shop">Shop</button>' +
          '<button class="crm-mini-btn" type="button" data-mark-done="1" ' +
          'title="Mark this order as fulfilled">' +
          '<i class="bi bi-check2-circle"></i> Done</button>' +
          '</div></td>' +
          '<td>' + escapeHtml(order.created_at) + '</td>' +
          '</tr>';
      }).join('');

      Array.prototype.forEach.call(bodyEl.querySelectorAll('[data-fulfil]'), function (button) {
        button.addEventListener('click', function () {
          var row = button.closest('tr');
          updateFulfilment(row.getAttribute('data-reference'),
                           { delivery_method: button.getAttribute('data-fulfil') },
                           row);
        });
      });
      Array.prototype.forEach.call(bodyEl.querySelectorAll('[data-mark-done]'), function (button) {
        button.addEventListener('click', function () {
          var row = button.closest('tr');
          updateFulfilment(row.getAttribute('data-reference'), { fulfilled: true }, row);
        });
      });
    }

    function updateFulfilment(reference, payload, row) {
      row.style.opacity = '0.55';
      postJson('/api/crm/orders/' + encodeURIComponent(reference) + '/fulfilment?' + query(), payload)
        .then(function (result) {
          row.style.opacity = '';
          if (!result.success) {
            window.alert(result.message || 'Could not update that order.');
            return;
          }
          renderStats(result.analytics || {});
          renderTop((result.analytics || {}).top_items);
          renderRecent(result.orders);
          renderTable(result.orders);
        })
        .catch(function () {
          row.style.opacity = '';
          window.alert('Could not update that order.');
        });
    }

    function load(silent) {
      if (inFlight) return;
      inFlight = true;
      fetch('/api/crm/analytics?' + query())
        .then(function (response) { return response.json(); })
        .then(function (payload) {
          inFlight = false;
          if (!payload.success) return;
          loaded = true;
          renderStats(payload.analytics || {});
          renderTop((payload.analytics || {}).top_items);
          renderRecent(payload.orders);
          renderTable(payload.orders);
        })
        .catch(function () {
          inFlight = false;
          if (!silent) console.warn('Could not load sales analytics');
        });
    }

    [periodEl, paymentEl, fulfilmentEl].forEach(function (el) {
      if (el) el.addEventListener('change', function () { load(false); });
    });

    return { load: load, isLoaded: function () { return loaded; } };
  })();

  /* ------------------------------------------------------------- catalogue */
  function initCatalog() {
    var grid = document.getElementById('crmProductGrid');
    if (!grid) return;

    function note(product, message, kind) {
      var node = product.querySelector('[data-role="note"]');
      if (!node) return;
      node.textContent = message;
      node.classList.remove('is-ok', 'is-error');
      if (kind) node.classList.add(kind);
      window.setTimeout(function () {
        if (node.textContent === message) node.textContent = '';
      }, 4000);
    }

    Array.prototype.forEach.call(grid.querySelectorAll('.crm-product'), function (product) {
      var id = product.getAttribute('data-product-id');
      var saveButton = product.querySelector('[data-role="save"]');
      if (!saveButton) return;

      saveButton.addEventListener('click', function () {
        var name = product.querySelector('[data-role="name"]').value.trim();
        var summary = product.querySelector('[data-role="summary"]').value.trim();
        if (!name) {
          note(product, 'A product needs a name.', 'is-error');
          return;
        }
        saveButton.disabled = true;
        postJson('/api/crm/products/' + encodeURIComponent(id), { name: name, summary: summary })
          .then(function (result) {
            if (!result.success) {
              note(product, result.message || 'Could not save.', 'is-error');
              saveButton.disabled = false;
              return null;
            }
            // Prices save in the same action so one click stores the whole card.
            var inputs = Array.prototype.slice.call(product.querySelectorAll('[data-role="price"]'));
            return inputs.reduce(function (chain, input) {
              return chain.then(function (state) {
                if (state === false) return false;
                return postJson('/api/crm/products/' + encodeURIComponent(id) + '/prices', {
                  size: input.getAttribute('data-size'),
                  price: input.value.trim(),
                }).then(function (priceResult) {
                  if (!priceResult.success) {
                    note(product, priceResult.message || 'Could not save a price.', 'is-error');
                    return false;
                  }
                  return true;
                });
              });
            }, Promise.resolve(true));
          })
          .then(function (ok) {
            saveButton.disabled = false;
            if (ok === null || ok === false) return;
            note(product, 'Saved', 'is-ok');
            var priced = Array.prototype.slice.call(product.querySelectorAll('[data-role="price"]'))
              .every(function (input) { return input.value.trim() !== ''; });
            var chip = product.querySelector('.crm-product-head .ps-chip');
            if (chip) {
              chip.classList.toggle('is-ok', priced);
              chip.classList.toggle('is-warn', !priced);
              chip.textContent = priced ? 'Priced' : 'Enquiry only';
            }
          })
          .catch(function () {
            saveButton.disabled = false;
            note(product, 'Could not save the product.', 'is-error');
          });
      });
    });
  }

  /* --------------------------------------------------------- activity log */
  function initActivityLog() {
    var modal = document.getElementById('crmActivityModal');
    var openButton = document.getElementById('crmOpenActivity');
    if (!modal || !openButton) return;

    var bodyEl = document.getElementById('crmActivityBody');
    var summaryEl = document.getElementById('crmActivitySummary');
    var periodEl = document.getElementById('crmActivityPeriod');
    var customEl = document.getElementById('crmActivityCustom');
    var fromEl = document.getElementById('crmActivityFrom');
    var toEl = document.getElementById('crmActivityTo');
    var categoryEl = document.getElementById('crmActivityCategory');
    var employeeEl = document.getElementById('crmActivityEmployee');

    var CATEGORY_ICONS = {
      auth: 'bi-box-arrow-in-right',
      catalogue: 'bi-tag-fill',
      chat: 'bi-chat-dots-fill',
      order: 'bi-receipt',
      access: 'bi-shield-lock-fill',
      hr: 'bi-people-fill',
    };

    var categoriesLoaded = false;

    function filters() {
      var state = {
        period: periodEl.value,
        category: categoryEl.value,
        employee: employeeEl.value.trim(),
      };
      // A custom range supersedes the named period on the server too.
      if (state.period === 'custom') {
        state.from = fromEl.value;
        state.to = toEl.value;
      }
      return state;
    }

    function query() {
      var state = filters();
      var parts = ['period=' + encodeURIComponent(state.period),
                   'category=' + encodeURIComponent(state.category)];
      if (state.employee) parts.push('employee=' + encodeURIComponent(state.employee));
      if (state.from) parts.push('from=' + encodeURIComponent(state.from));
      if (state.to) parts.push('to=' + encodeURIComponent(state.to));
      return parts.join('&');
    }

    function escape(value) {
      return String(value === null || value === undefined ? '' : value)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function render(entries) {
      if (!entries.length) {
        bodyEl.innerHTML = '<div class="ps-empty"><i class="bi bi-inbox"></i>' +
          '<h4>No activity in this period</h4>' +
          '<p>Sign-ins, price changes, handovers and manual replies will appear here as they happen.</p></div>';
        return;
      }
      bodyEl.innerHTML = '<div class="crm-log-list">' + entries.map(function (entry) {
        var icon = CATEGORY_ICONS[entry.category] || 'bi-activity';
        var meta = [];
        if (entry.employee_role) meta.push(escape(entry.employee_role));
        if (entry.target) meta.push('Target: ' + escape(entry.target));
        if (entry.ip_address) meta.push('From ' + escape(entry.ip_address));
        return '<div class="crm-log-row">' +
          '<div class="crm-log-icon cat-' + escape(entry.category) + '"><i class="bi ' + icon + '"></i></div>' +
          '<div class="crm-log-main">' +
          '<div class="crm-log-top"><strong>' + escape(entry.employee_name) + '</strong>' +
          '<span class="crm-pill">' + escape(entry.category_label) + '</span>' +
          '<span class="crm-log-when">' + escape(entry.created_at) + '</span></div>' +
          '<p class="crm-log-summary">' + escape(entry.summary) + '</p>' +
          (meta.length ? '<div class="crm-log-meta">' + meta.join(' · ') + '</div>' : '') +
          '</div></div>';
      }).join('') + '</div>';
    }

    function load(silent) {
      bodyEl.style.opacity = '0.55';
      fetch('/api/crm/activity?' + query())
        .then(function (response) { return response.json(); })
        .then(function (payload) {
          bodyEl.style.opacity = '';
          if (!payload.success) {
            bodyEl.innerHTML = '<p class="crm-empty-cell">Could not load the activity log.</p>';
            return;
          }
          render(payload.entries || []);
          summaryEl.textContent = (payload.entries || []).length + ' entries · ' +
            (payload.period_label || '');
          if (!categoriesLoaded && payload.categories) {
            payload.categories.forEach(function (item) {
              var option = document.createElement('option');
              option.value = item.key;
              option.textContent = item.label;
              categoryEl.appendChild(option);
            });
            categoriesLoaded = true;
          }
        })
        .catch(function () {
          bodyEl.style.opacity = '';
          if (!silent) bodyEl.innerHTML = '<p class="crm-empty-cell">Could not load the activity log.</p>';
        });
    }

    function open() {
      modal.hidden = false;
      document.body.style.overflow = 'hidden';
      load(false);
    }

    function close() {
      modal.hidden = true;
      document.body.style.overflow = '';
      window.clearTimeout(searchTimer);
    }

    openButton.addEventListener('click', open);
    document.getElementById('crmActivityClose').addEventListener('click', close);
    document.getElementById('crmActivityRefresh').addEventListener('click', function () { load(false); });

    document.getElementById('crmActivityExport').addEventListener('click', function () {
      // The PDF is generated from the same filters the modal is showing.
      window.location.href = '/api/crm/activity.pdf?' + query();
    });

    // Auto-load as soon as the scope changes, which is how the period buttons
    // are expected to feel.
    periodEl.addEventListener('change', function () {
      var custom = periodEl.value === 'custom';
      customEl.hidden = !custom;
      if (custom) {
        if (!fromEl.value) fromEl.value = new Date().toISOString().slice(0, 10);
        if (!toEl.value) toEl.value = fromEl.value;
      } else {
        load(true);
      }
    });

    [fromEl, toEl].forEach(function (el) {
      el.addEventListener('change', function () { load(true); });
    });

    categoryEl.addEventListener('change', function () { load(true); });

    var searchTimer = null;
    employeeEl.addEventListener('input', function () {
      window.clearTimeout(searchTimer);
      searchTimer = window.setTimeout(function () { load(true); }, 350);
    });

    modal.addEventListener('click', function (event) {
      if (event.target === modal) close();
    });

    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape' && !modal.hidden) close();
    });
  }

  initTabs();
  initCatalog();
  initActivityLog();
})();
