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

  /* ---------------------------------------------------------------- toasts */
  // A short-lived message in the corner. Used for catalogue outcomes so a
  // rejected edit is visible without a dialog the agent has to dismiss.
  var TOAST_ICONS = {
    ok: 'bi-check-circle-fill',
    warn: 'bi-exclamation-triangle-fill',
    error: 'bi-x-circle-fill',
    info: 'bi-info-circle-fill',
  };

  function toast(message, kind, detail) {
    var host = document.getElementById('crmToasts');
    if (!host || !message) return;
    var level = TOAST_ICONS[kind] ? kind : 'info';
    var node = document.createElement('div');
    node.className = 'crm-toast is-' + level;
    node.innerHTML = '<i class="bi ' + TOAST_ICONS[level] + '"></i>' +
      '<div class="crm-toast-body"><span class="crm-toast-text">' + escapeHtml(message) + '</span>' +
      (detail ? '<span class="crm-toast-detail">' + escapeHtml(detail) + '</span>' : '') +
      '</div>' +
      '<button class="crm-toast-close" type="button" aria-label="Dismiss"><i class="bi bi-x-lg"></i></button>';

    function dismiss() {
      if (!node.parentNode) return;
      node.classList.add('is-leaving');
      window.setTimeout(function () {
        if (node.parentNode) node.parentNode.removeChild(node);
      }, 200);
    }

    node.querySelector('.crm-toast-close').addEventListener('click', dismiss);
    host.appendChild(node);
    // Errors stay long enough to be read and copied into a fix.
    window.setTimeout(dismiss, level === 'error' ? 9000 : 5000);
  }

  window.crmToast = toast;

  // Shows a spinner on a button while its request is in flight, then restores
  // the original label. The catalogue save is several chained requests, so the
  // button would otherwise sit looking dead and invite a second click.
  function setBusy(button, busy, label) {
    if (!button) return;
    if (busy) {
      if (!button.hasAttribute('data-idle-html')) {
        button.setAttribute('data-idle-html', button.innerHTML);
      }
      button.disabled = true;
      button.classList.add('is-busy');
      button.setAttribute('aria-busy', 'true');
      button.innerHTML = '<span class="crm-spinner is-btn" aria-hidden="true"></span>' +
        escapeHtml(label || button.getAttribute('data-busy-label') || 'Saving…');
      return;
    }
    button.disabled = false;
    button.classList.remove('is-busy');
    button.removeAttribute('aria-busy');
    var idle = button.getAttribute('data-idle-html');
    if (idle !== null) {
      button.innerHTML = idle;
      button.removeAttribute('data-idle-html');
    }
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

    // The inbox manages its own scrolling, so the shell can drop the page-level
    // bottom padding while it is showing.
    var shell = document.querySelector('.ps-shell');

    function show(name, focusTab) {
      tabs.forEach(function (tab) {
        var selected = tab.getAttribute('data-crm-tab') === name;
        tab.classList.toggle('is-active', selected);
        tab.setAttribute('aria-selected', selected ? 'true' : 'false');
        // Roving tabindex: only the selected tab stays in the tab order, so a
        // keyboard user reaches the panel with one Tab instead of four.
        tab.setAttribute('tabindex', selected ? '0' : '-1');
        if (selected && focusTab) tab.focus();
      });
      views.forEach(function (view) {
        view.classList.toggle('is-active', view.getAttribute('data-crm-view') === name);
      });
      if (shell) shell.classList.toggle('ps-shell-tight', name === 'inbox');
      if (name === 'analytics') analytics.load(false);
      if (name === 'inbox') sizeInbox();
    }

    // The inbox takes whatever the viewport has left after the chrome above it.
    // The CSS fallback already expresses this; the measurement only corrects it
    // when a deployment's chrome differs, and it is skipped rather than guessed
    // when layout is not yet resolved.
    function sizeInbox() {
      var view = root.querySelector('[data-crm-view="inbox"]');
      var box = view ? view.querySelector('.ps-inbox') : null;
      if (!box) return;
      box.style.removeProperty('--crm-inbox-height');
      var top = box.getBoundingClientRect().top;
      if (!top || top < 100) return;
      var available = window.innerHeight - top - 12;
      // Only trust the measurement when it would actually give more room than
      // the stylesheet's own figure, so a stale container cannot shrink the panel.
      var fromCss = window.innerHeight - 305;
      if (available <= fromCss) return;
      box.style.setProperty('--crm-inbox-height', Math.round(available) + 'px');
    }

    tabs.forEach(function (tab, index) {
      tab.addEventListener('click', function () { show(tab.getAttribute('data-crm-tab')); });

      // Arrow keys walk the tabs, Home and End jump to the ends. The view is
      // selected as focus moves so the panel always matches the focused tab.
      tab.addEventListener('keydown', function (event) {
        var last = tabs.length - 1;
        var next = null;
        if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
          next = index === last ? 0 : index + 1;
        } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
          next = index === 0 ? last : index - 1;
        } else if (event.key === 'Home') {
          next = 0;
        } else if (event.key === 'End') {
          next = last;
        }
        if (next === null) return;
        event.preventDefault();
        show(tabs[next].getAttribute('data-crm-tab'), true);
      });
    });

    // Apply the initial state. Without this the selected tab carried no
    // aria-selected until the first click, so the active view was never
    // announced, and every tab kept a tabindex of 0.
    var initial = root.querySelector('[data-crm-tab].is-active');
    show(initial ? initial.getAttribute('data-crm-tab') : 'inbox');

    // Measure once the browser has laid the page out; measuring mid-init reads
    // a not-yet-laid-out box and would size the inbox wrongly on first paint.
    function scheduledSize() {
      window.requestAnimationFrame(function () {
        window.requestAnimationFrame(sizeInbox);
      });
    }

    scheduledSize();
    if (document.fonts && document.fonts.ready) {
      // Web fonts change the chrome's height, so re-measure once they land.
      document.fonts.ready.then(scheduledSize).catch(function () {});
    }
    var resizeTimer = null;
    window.addEventListener('resize', function () {
      window.clearTimeout(resizeTimer);
      resizeTimer = window.setTimeout(function () {
        var active = root.querySelector('[data-crm-view].is-active');
        if (!active || active.getAttribute('data-crm-view') === 'inbox') sizeInbox();
      }, 120);
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

    function emptyState(icon, title, hint) {
      return '<div class="crm-empty-state is-compact">' +
        '<span class="crm-empty-icon"><i class="bi ' + icon + '"></i></span>' +
        '<p class="crm-empty-title">' + title + '</p>' +
        '<p class="crm-empty-hint">' + hint + '</p>' +
        '</div>';
    }

    function renderTop(items) {
      if (!topEl) return;
      if (!items || !items.length) {
        topEl.innerHTML = emptyState('bi-bar-chart-line', 'Nothing sold in this period',
          'Widen the period above to see what is moving.');
        return;
      }
      var peak = items.reduce(function (max, item) { return Math.max(max, item.units); }, 0) || 1;
      topEl.innerHTML = items.map(function (item) {
        var width = Math.max(6, Math.round((item.units / peak) * 100));
        return '<div class="crm-top-item">' +
          '<div class="crm-top-head"><strong>' + escapeHtml(item.product) + '</strong>' +
          '<span class="crm-top-units">' + item.units + ' units</span></div>' +
          '<div class="crm-bar"><i style="width:' + width + '%"></i></div>' +
          '</div>';
      }).join('');
    }

    function renderRecent(orders) {
      if (!recentEl) return;
      if (!orders || !orders.length) {
        recentEl.innerHTML = emptyState('bi-inbox', 'No orders in this period',
          'Orders taken by the assistant land here straight away.');
        return;
      }
      recentEl.innerHTML = orders.slice(0, 5).map(function (order) {
        var who = order.customer_name || order.company || ('+' + order.wa_number);
        var paid = order.payment_status === 'paid';
        return '<div class="crm-order-row">' +
          '<div class="crm-order-row-main"><strong>' + escapeHtml(who) + '</strong>' +
          '<small>' + escapeHtml(order.reference) + ' • ' + order.items + ' items • ' +
          escapeHtml(order.created_at) + '</small></div>' +
          '<span class="crm-pill ' + (paid ? 'is-paid' : 'is-unpaid') + '">' +
          (paid ? 'Paid' : 'Will pay') + '</span>' +
          '</div>';
      }).join('');
    }

    function renderTable(orders) {
      if (!bodyEl) return;
      if (!orders || !orders.length) {
        bodyEl.innerHTML = '<tr><td colspan="7" class="crm-empty-cell">' +
          emptyState('bi-receipt-cutoff', 'No orders match these filters',
            'Try All time, or clear the payment and fulfilment filters.') +
          '</td></tr>';
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
          '<td><span class="crm-who">' + escapeHtml(who) + '</span>' + contact + '</td>' +
          '<td class="crm-col-num">' + order.items + '</td>' +
          '<td><span class="crm-pill ' + (paid ? 'is-paid' : 'is-unpaid') + '">' +
          (paid ? '<i class="bi bi-check2"></i> Paid' : '<i class="bi bi-clock"></i> Will pay') + '</span></td>' +
          '<td class="crm-col-num crm-amount">' +
          (order.amount === null || order.amount === undefined
            ? '<span class="crm-amount-unset">Not priced</span>'
            : money(order.amount)) + '</td>' +
          '<td><div class="crm-row-actions">' +
          '<button class="crm-mini-btn' + (delivered ? ' is-active' : '') + '" type="button" ' +
          'data-fulfil="delivery" title="Delivered to the customer">Home</button>' +
          '<button class="crm-mini-btn' + (!delivered ? ' is-active' : '') + '" type="button" ' +
          'data-fulfil="collection" title="Collected from the shop">Shop</button>' +
          '<button class="crm-mini-btn" type="button" data-mark-done="1" ' +
          'title="Mark this order as fulfilled">' +
          '<i class="bi bi-check2-circle"></i> Done</button>' +
          '<button class="crm-mini-btn" type="button" data-open-order="1" ' +
          'title="Open this order to print documents or take payment">' +
          '<i class="bi bi-receipt"></i> Open</button>' +
          '</div></td>' +
          '<td class="crm-col-date">' + escapeHtml(order.created_at) + '</td>' +
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
      Array.prototype.forEach.call(bodyEl.querySelectorAll('[data-open-order]'), function (button) {
        button.addEventListener('click', function () {
          openOrder(button.closest('tr').getAttribute('data-reference'));
        });
      });
    }

    /* ------------------------------------------------------------- one order --
       The sales view of a single order: its lines, its payment state, and the
       two documents. Delivery notes print whether or not the order is paid,
       because goods often travel before the money does; a receipt is issued
       only once the order is settled. */
    var orderModal = document.getElementById('crmOrderModal');
    var orderBody = document.getElementById('crmOrderBody');
    var orderTitle = document.getElementById('crmOrderTitle');
    var orderRef = null;
    var orderPoll = null;

    function closeOrder() {
      if (orderPoll) { window.clearInterval(orderPoll); orderPoll = null; }
      if (orderModal) orderModal.hidden = true;
      orderRef = null;
    }

    function paymentPill(payment) {
      var status = (payment && payment.status) || 'unpaid';
      var cls = status === 'paid' ? 'is-paid' : 'is-unpaid';
      var label = { paid: 'Paid', pending: 'Awaiting PIN', cancelled: 'Cancelled',
                    failed: 'Failed', unpaid: 'Not paid' }[status] || status;
      return '<span class="crm-pill ' + cls + '">' + escapeHtml(label) + '</span>';
    }

    function renderOrder(order, payment) {
      var lines = order.lines || [];
      var paid = order.payment_status === 'paid';
      var rows = lines.length ? lines.map(function (line) {
        var unit = line.unit_price === null || line.unit_price === undefined
          ? '<span class="crm-unpriced">not recorded</span>'
          : escapeHtml(money(line.unit_price));
        var total = line.line_total === null || line.line_total === undefined
          ? '—' : escapeHtml(money(line.line_total));
        return '<tr>' +
          '<td><strong>' + escapeHtml(line.product) + '</strong>' +
          (line.pack_size ? '<span class="crm-sub">' + escapeHtml(line.pack_size) + '</span>' : '') +
          '</td>' +
          '<td class="crm-od-qty">' + line.quantity + '</td>' +
          '<td class="crm-od-num">' + unit + '</td>' +
          '<td class="crm-od-num">' + total + '</td>' +
          '</tr>';
      }).join('') : '<tr><td colspan="4" class="crm-empty-cell">No line items recorded.</td></tr>';

      var who = order.customer_name || ('+' + order.wa_number);
      orderBody.innerHTML =
        '<div class="crm-od-head">' +
          '<div><h5>' + escapeHtml(who) + '</h5>' +
          (order.company ? '<span class="crm-sub">' + escapeHtml(order.company) + '</span>' : '') +
          (order.phone ? '<span class="crm-sub">' + escapeHtml(order.phone) + '</span>' : '') +
          '</div>' +
          '<div class="crm-od-meta">' + paymentPill(payment) + '</div>' +
        '</div>' +
        '<dl class="crm-od-fields">' +
          '<div><dt>Placed</dt><dd>' + escapeHtml(order.created_at) + '</dd></div>' +
          '<div><dt>Items</dt><dd>' + order.item_count + '</dd></div>' +
          '<div><dt>Fulfilment</dt><dd>' +
            (order.delivery_method === 'delivery' ? 'Delivery' : 'Collection') + '</dd></div>' +
          '<div><dt>Delivery address</dt><dd>' +
            (order.address ? escapeHtml(order.address) : '—') + '</dd></div>' +
        '</dl>' +
        '<div class="crm-table-wrap"><table class="crm-table crm-od-table">' +
          '<thead><tr><th>Item</th><th class="crm-od-qty">Qty</th>' +
          '<th class="crm-od-num">Unit price</th><th class="crm-od-num">Line total</th></tr></thead>' +
          '<tbody>' + rows + '</tbody>' +
        '</table></div>' +
        '<div class="crm-od-total"><span>Total' + (paid ? ' paid' : ' due') + '</span>' +
          '<strong>' + escapeHtml(money(order.total)) + '</strong></div>' +
        (order.priced ? '' :
          '<p class="crm-od-warn"><i class="bi bi-info-circle"></i> This order was taken before ' +
          'line prices were recorded, so unit prices show as not recorded. The total is the amount ' +
          'actually charged.</p>') +
        (payment && payment.awaiting ?
          '<p class="crm-od-note"><i class="bi bi-hourglass-split"></i> Waiting for the customer to ' +
          'enter their EcoCash PIN. Checking automatically…</p>' : '');

      orderTitle.textContent = 'Order ' + order.reference;
    }

    function refreshOrderActions(order, payment) {
      var paid = order.payment_status === 'paid';
      var awaiting = payment && payment.awaiting;
      var requestBtn = document.getElementById('crmOrderRequestPay');
      var receiptBtn = document.getElementById('crmOrderReceipt');
      var sendBtn = document.getElementById('crmOrderSendReceipt');
      var noteEl = document.getElementById('crmOrderNote');

      if (requestBtn) {
        // Nothing to request once it is settled, and no point while the customer
        // already has a prompt on their handset.
        requestBtn.disabled = paid || awaiting;
        requestBtn.innerHTML = awaiting
          ? '<i class="bi bi-hourglass-split"></i> Waiting for PIN'
          : '<i class="bi bi-phone"></i> Request EcoCash payment';
      }
      // A receipt is proof of payment, so both the printed and the sent copy are
      // held back until the money is actually in.
      if (receiptBtn) receiptBtn.disabled = !paid;
      if (sendBtn) sendBtn.disabled = !paid;
      if (noteEl) {
        if (paid) {
          noteEl.textContent = 'Paid ' + (order.paid_at || '') +
            '. The receipt can be printed or emailed to the customer.';
          noteEl.className = 'crm-od-status is-ok';
        } else if (awaiting) {
          noteEl.textContent = 'An EcoCash prompt was sent to the customer.';
          noteEl.className = 'crm-od-status';
        } else {
          noteEl.textContent = 'Not paid. Send the EcoCash prompt when the customer is ready to pay.';
          noteEl.className = 'crm-od-status';
        }
      }
    }

    function loadOrder(reference, quiet) {
      return fetch('/api/crm/orders/' + encodeURIComponent(reference))
        .then(function (response) { return response.json(); })
        .then(function (result) {
          if (!result.success) {
            if (!quiet) window.alert(result.message || 'Could not load that order.');
            return null;
          }
          renderOrder(result.order, result.payment);
          refreshOrderActions(result.order, result.payment);
          return result;
        })
        .catch(function () {
          if (!quiet) window.alert('Could not load that order.');
          return null;
        });
    }

    function openOrder(reference) {
      if (!orderModal || !orderBody) return;
      orderRef = reference;
      orderBody.innerHTML = '<div class="crm-od-loading"><span class="crm-spinner"></span>' +
        '<p>Loading order…</p></div>';
      orderModal.hidden = false;
      loadOrder(reference, false).then(function (result) {
        // A pending EcoCash payment resolves on the customer's handset, so the
        // portal watches for it rather than making the sales person refresh.
        if (result && result.payment && result.payment.awaiting && !orderPoll) {
          orderPoll = window.setInterval(function () {
            if (!orderRef || document.hidden) return;
            fetch('/api/crm/orders/' + encodeURIComponent(orderRef) + '/payment-status')
              .then(function (response) { return response.json(); })
              .then(function (payload) {
                if (!payload.success) return;
                if (payload.payment.paid) {
                  window.clearInterval(orderPoll); orderPoll = null;
                  window.crmToast && window.crmToast('Payment received for ' + orderRef + '.', 'success');
                }
                loadOrder(orderRef, true);
              })
              .catch(function () {});
          }, 6000);
        }
      });
    }

    if (orderModal) {
      orderModal.addEventListener('click', function (event) {
        var action = event.target.closest('[data-order-action]');
        if (action) {
          var kind = action.getAttribute('data-order-action');
          if (kind === 'close') { closeOrder(); return; }
          if (kind === 'delivery-note') {
            window.location.href = '/api/crm/orders/' + encodeURIComponent(orderRef) + '/delivery-note.pdf';
            return;
          }
          if (kind === 'receipt') {
            window.location.href = '/api/crm/orders/' + encodeURIComponent(orderRef) + '/receipt.pdf';
            return;
          }
          if (kind === 'send-receipt') {
            action.disabled = true;
            postJson('/api/crm/orders/' + encodeURIComponent(orderRef) + '/email-receipt', {})
              .then(function (result) {
                action.disabled = false;
                window.crmToast && window.crmToast(
                  result.message || (result.success ? 'Receipt sent.' : 'Could not send the receipt.'),
                  result.success ? 'success' : 'error');
                if (result.success) refresh(true);
              })
              .catch(function () {
                action.disabled = false;
                window.crmToast && window.crmToast('Could not send the receipt.', 'error');
              });
            return;
          }
          if (kind === 'request-payment') {
            action.disabled = true;
            postJson('/api/crm/orders/' + encodeURIComponent(orderRef) + '/request-payment', {})
              .then(function (result) {
                action.disabled = false;
                if (!result.success) {
                  window.crmToast && window.crmToast(result.message || 'Could not request payment.', 'error');
                  return;
                }
                window.crmToast && window.crmToast(
                  result.message || 'An EcoCash prompt was sent to the customer.', 'success');
                openOrder(orderRef);
              })
              .catch(function () {
                action.disabled = false;
                window.crmToast && window.crmToast('Could not request payment.', 'error');
              });
            return;
          }
        }
        if (event.target === orderModal) closeOrder();
      });
      document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && orderModal && !orderModal.hidden) closeOrder();
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

    var limitsEl = document.getElementById('crmCatalogLimits');
    var countEl = document.getElementById('crmCatalogCount');
    var fullEl = document.getElementById('crmCatalogFull');
    var fullCountEl = document.getElementById('crmCatalogFullCount');
    var form = document.getElementById('crmAddProductForm');
    var openButton = document.getElementById('crmAddProductOpen');
    var cancelButton = document.getElementById('crmAddProductCancel');
    var nameInput = document.getElementById('crmAddName');
    var nameCount = document.getElementById('crmAddNameCount');
    var summaryInput = document.getElementById('crmAddSummary');
    var sizesInput = document.getElementById('crmAddSizes');
    var addNote = document.getElementById('crmAddNote');
    // Seed from what the server rendered, so the counters agree with the labels
    // before any API response arrives.
    var renderedLimits = document.getElementById('crmLimits');
    var limits = { max_products: 10, name_limit: 24, summary_limit: 300, pack_limit: 10 };
    if (renderedLimits) {
      try {
        limits = Object.assign(limits, JSON.parse(renderedLimits.textContent));
      } catch (e) {
        // Keep the defaults if the payload is unreadable.
      }
    }

    function note(product, message, kind) {
      var node = product.querySelector('[data-role="note"]');
      if (!node) return;
      node.textContent = message;
      node.classList.remove('is-ok', 'is-error');
      if (kind) node.classList.add(kind);
      window.setTimeout(function () {
        // The unsaved-changes note is a standing state, not a flash, so it stays
        // until the card is saved or reverted.
        if (node.textContent === 'Unsaved changes' && product.classList.contains('is-dirty')) return;
        if (node.textContent === message) node.textContent = '';
      }, 4000);
    }

    // How many cards hold edits that have not reached the server yet.
    var dirtyCards = 0;
    // Set while the page is navigating on purpose, so the guard below does not
    // block the app's own reloads.
    var navigatingOnPurpose = false;

    function markDirty(product, dirty) {
      if (product.classList.contains('is-dirty') === dirty) return;
      product.classList.toggle('is-dirty', dirty);
      dirtyCards += dirty ? 1 : -1;
      if (dirtyCards < 0) dirtyCards = 0;
      if (dirty) {
        note(product, 'Unsaved changes', 'is-error');
      } else {
        var node = product.querySelector('[data-role="note"]');
        if (node && node.textContent === 'Unsaved changes') node.textContent = '';
      }
    }

    // The catalogue count and the add button both depend on WhatsApp's limit, so
    // they are refreshed from whatever the server last reported.
    function applyLimits(incoming) {
      if (incoming) limits = incoming;
      var count = grid.querySelectorAll('.crm-product').length;
      if (countEl) countEl.textContent = count + ' of ' + limits.max_products + ' products';
      if (limitsEl) {
        limitsEl.textContent = 'WhatsApp allows ' + limits.max_products + ' products in one browse ' +
          'list, and each product or pack name must be ' + limits.name_limit + ' characters or fewer.';
      }
      var full = count >= limits.max_products;
      if (fullEl) {
        fullEl.hidden = !full;
        if (full && fullCountEl) fullCountEl.textContent = String(limits.max_products);
      }
      if (openButton) {
        openButton.disabled = full;
        openButton.title = full
          ? 'The catalogue is at WhatsApp\'s limit for one browse list.'
          : 'Add a new product to the catalogue';
      }
    }

    function parseSizes(value) {
      return String(value || '').split(/[,\n]/)
        .map(function (part) { return part.trim(); })
        .filter(function (part) { return part !== ''; });
    }

    Array.prototype.forEach.call(grid.querySelectorAll('.crm-product'), function (product) {
      var id = product.getAttribute('data-product-id');
      var saveButton = product.querySelector('[data-role="save"]');
      if (!saveButton) return;

      // Any edit inside this card is unsaved until its save button is used. The
      // listener sits on the card so repainted price inputs are covered too.
      ['input', 'change'].forEach(function (eventName) {
        product.addEventListener(eventName, function (event) {
          var target = event.target;
          if (!target || !target.tagName) return;
          if (target.tagName !== 'INPUT' && target.tagName !== 'TEXTAREA' &&
              target.tagName !== 'SELECT') return;
          markDirty(product, true);
        });
      });

      // The price boxes are rendered from the saved unit sizes, so a size added
      // in the editor has to be repainted here. Without this its price box only
      // appeared on the next page load. Prices already typed are kept.
      function paintPrices(sizes) {
        var priceGrid = product.querySelector('.crm-price-grid');
        if (!priceGrid || !sizes || !sizes.length) return;
        var typed = {};
        Array.prototype.forEach.call(
          product.querySelectorAll('[data-role="price"]'),
          function (input) { typed[input.getAttribute('data-size')] = input.value; });
        priceGrid.innerHTML = sizes.map(function (size) {
          var value = Object.prototype.hasOwnProperty.call(typed, size) ? typed[size] : '';
          return '<label class="crm-price">' +
            '<span class="crm-price-size">' + escapeHtml(size) + '</span>' +
            '<span class="crm-price-box">' +
            '<span class="crm-price-currency">' + escapeHtml(CURRENCY) + '</span>' +
            '<input class="ps-input" type="number" step="0.01" min="0" placeholder="\u2014"' +
            ' data-role="price" data-size="' + escapeHtml(size) + '"' +
            ' value="' + escapeHtml(value) + '">' +
            '</span></label>';
        }).join('');
      }

      saveButton.addEventListener('click', function () {
        var nameField = product.querySelector('[data-role="name"]');
        var name = nameField.value.trim();
        var description = product.querySelector('[data-role="summary"]').value.trim();
        if (!name) {
          note(product, 'A product needs a name.', 'is-error');
          toast('A product needs a name.', 'error');
          return;
        }
        // Refuse locally before the round trip, so the agent sees the limit
        // problem next to the field they are editing and as a toast.
        if (name.length > limits.name_limit) {
          var over = name.length - limits.name_limit;
          note(product, 'A product name must be ' + limits.name_limit + ' characters or fewer.',
               'is-error');
          toast('Product name is ' + over + ' character' + (over === 1 ? '' : 's') + ' too long.',
                'error',
                'WhatsApp allows ' + limits.name_limit + ' characters in a list row, and this name ' +
                'has ' + name.length + '. Shorten it before saving.');
          nameField.focus();
          return;
        }
        if (description.length > limits.summary_limit) {
          var overDesc = description.length - limits.summary_limit;
          note(product, 'The description must be ' + limits.summary_limit + ' characters or fewer.',
               'is-error');
          toast('Description is ' + overDesc + ' character' + (overDesc === 1 ? '' : 's') + ' too long.',
                'error',
                'WhatsApp fits the description, the product name and the unit sizes into one ' +
                'message, so the description is limited to ' + limits.summary_limit + ' characters.');
          return;
        }
        // Unit sizes are edited on every product, so the checks run for the
        // built-in range as well as products added here.
        var sizesField = product.querySelector('[data-role="sizes"]');
        if (sizesField) {
          var wanted = parseSizes(sizesField.value);
          if (!wanted.length) {
            note(product, 'Give the product at least one unit size.', 'is-error');
            toast('A product needs at least one unit size.', 'error',
                  'Customers choose a unit size before the assistant can take the order.');
            return;
          }
          if (wanted.length > limits.pack_limit) {
            note(product, 'At most ' + limits.pack_limit + ' unit sizes.', 'is-error');
            toast('Too many unit sizes: ' + wanted.length + '.', 'error',
                  'A unit size list is one WhatsApp list, which holds ' + limits.pack_limit +
                  ' rows. Remove ' + (wanted.length - limits.pack_limit) + ' to save.');
            return;
          }
          var tooLong = wanted.filter(function (size) { return size.length > limits.name_limit; });
          if (tooLong.length) {
            note(product, 'A unit size must be ' + limits.name_limit + ' characters or fewer.',
                 'is-error');
            toast('Unit size name is too long.', 'error',
                  '“' + tooLong[0] + '” has ' + tooLong[0].length + ' characters; WhatsApp allows ' +
                  limits.name_limit + '.');
            return;
          }
        }
        setBusy(saveButton, true, 'Saving…');
        postJson('/api/crm/products/' + encodeURIComponent(id), { name: name, summary: description })
          .then(function (result) {
            if (!result.success) {
              note(product, result.message || 'Could not save.', 'is-error');
              toast(result.message || 'Could not save the product.', 'error');
              setBusy(saveButton, false);
              return null;
            }
            // Pack sizes are replaced first: removing a size would otherwise
            // leave a price pointing at a pack that no longer exists.
            var sizesField = product.querySelector('[data-role="sizes"]');
            var chain = Promise.resolve(true);
            if (sizesField) {
              chain = postJson('/api/crm/products/' + encodeURIComponent(id) + '/sizes',
                               { sizes: parseSizes(sizesField.value) })
                .then(function (sizeResult) {
                  if (!sizeResult.success) {
                    note(product, sizeResult.message || 'Could not save the pack sizes.', 'is-error');
                    toast(sizeResult.message || 'Could not save the pack sizes.', 'error');
                    return false;
                  }
                  return true;
                });
            }
            return chain.then(function (ok) {
              if (!ok) return false;
              // Prices save in the same action so one click stores the whole card.
              var inputs = Array.prototype.slice.call(product.querySelectorAll('[data-role="price"]'));
              return inputs.reduce(function (priceChain, input) {
                return priceChain.then(function (state) {
                  if (state === false) return false;
                  return postJson('/api/crm/products/' + encodeURIComponent(id) + '/prices', {
                    size: input.getAttribute('data-size'),
                    price: input.value.trim(),
                  }).then(function (priceResult) {
                    if (!priceResult.success) {
                      note(product, priceResult.message || 'Could not save a price.', 'is-error');
                      toast(priceResult.message || 'Could not save a price.', 'error');
                      return false;
                    }
                    return true;
                  });
                });
              }, Promise.resolve(true));
            });
          })
          .then(function (ok) {
            setBusy(saveButton, false);
            if (ok === null || ok === false) return;
            // The saved sizes are now the truth, so a newly added one gets its
            // price box straight away instead of on the next page load.
            paintPrices(wanted);
            note(product, 'Saved', 'is-ok');
            markDirty(product, false);
            toast('Saved “' + name + '”.', 'ok');
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
            setBusy(saveButton, false);
            note(product, 'Could not save the product.', 'is-error');
            toast('Could not save the product. Check your connection and try again.', 'error');
          });
      });

      var deleteButton = product.querySelector('[data-role="delete"]');
      if (deleteButton) {
        deleteButton.addEventListener('click', function () {
          var productName = product.querySelector('[data-role="name"]').value.trim();
          if (!window.confirm('Remove “' + productName + '” from the catalogue?\n\n' +
                              'The assistant will stop offering it. You can restore it afterwards.')) {
            return;
          }
          setBusy(deleteButton, true, 'Removing…');
          postJson('/api/crm/products/' + encodeURIComponent(id) + '/delete', {})
            .then(function (result) {
              if (!result.success) {
                setBusy(deleteButton, false);
                note(product, result.message || 'Could not remove it.', 'is-error');
                toast(result.message || 'Could not remove the product.', 'error');
                return;
              }
              toast('Removed “' + productName + '”.', 'ok',
                    'It is listed under Removed products if you need it back.');
              product.parentNode.removeChild(product);
              renderRemoved(result.removed, true);
              if (result.limits) applyLimits(result.limits);
              applyLimits(null);
              // A freed slot may make the add button usable again.
              if (grid.querySelectorAll('.crm-product').length < limits.max_products) {
                toast('There is now room to add another product.', 'info');
              }
            })
            .catch(function () {
              setBusy(deleteButton, false);
              note(product, 'Could not remove the product.', 'is-error');
              toast('Could not remove the product.', 'error');
            });
        });
      }
    });

    // ------------------------------------------------------- removed products --
    // Kept in the UI so a removal is reversible without a deploy, and so nobody
    // has to wonder whether a product was removed or never existed.
    var removedWrap = document.getElementById('crmRemovedWrap');
    var removedList = document.getElementById('crmRemovedList');
    var removedCount = document.getElementById('crmRemovedCount');

    function renderRemoved(items, open) {
      if (!removedWrap || !removedList) return;
      var entries = items || [];
      if (!entries.length) {
        removedWrap.hidden = true;
        removedList.innerHTML = '';
        if (removedCount) removedCount.textContent = '0';
        return;
      }
      removedWrap.hidden = false;
      if (open) removedWrap.open = true;
      if (removedCount) removedCount.textContent = String(entries.length);
      removedList.innerHTML = entries.map(function (item) {
        return '<li data-product-id="' + escapeHtml(item.id) + '">' +
          '<span class="crm-removed-name">' + escapeHtml(item.name) + '</span>' +
          '<span class="crm-removed-meta">' + escapeHtml(item.removed_by || 'Unknown') +
          (item.removed_at ? ' · ' + escapeHtml(item.removed_at) : '') + '</span>' +
          '<button class="ps-btn ps-btn-ghost ps-btn-sm" type="button" data-role="restore">' +
          '<i class="bi bi-arrow-counterclockwise"></i> Restore</button></li>';
      }).join('');
      Array.prototype.forEach.call(
        removedList.querySelectorAll('[data-role="restore"]'),
        function (button) {
          button.addEventListener('click', function () {
            var row = button.closest('li');
            var restoreId = row.getAttribute('data-product-id');
            setBusy(button, true, 'Restoring…');
            postJson('/api/crm/products/' + encodeURIComponent(restoreId) + '/restore', {})
              .then(function (result) {
                if (!result.success) {
                  setBusy(button, false);
                  toast(result.message || 'Could not restore it.', 'error');
                  return;
                }
                toast('Restored to the catalogue.', 'ok',
                      'Reloading so the product appears with its saved details.');
                // This reload is intentional, so it must not trip the guard.
                navigatingOnPurpose = true;
                window.setTimeout(function () { window.location.reload(); }, 700);
              })
              .catch(function () {
                setBusy(button, false);
                toast('Could not restore the product.', 'error');
              });
          });
        });
    }
    renderRemoved(window.__crmRemoved || [], false);

    // A product name becomes a WhatsApp list row title, so the remaining
    // characters are shown live on every card as well as the add form.
    function paintOneCount(input, node) {
      if (!node) return;
      var left = limits.name_limit - input.value.trim().length;
      node.textContent = left >= 0 ? left + ' characters left' : Math.abs(left) + ' characters too many';
      node.classList.toggle('is-error', left < 0);
    }

    Array.prototype.forEach.call(grid.querySelectorAll('.crm-product'), function (product) {
      var input = product.querySelector('[data-role="name"]');
      var node = product.querySelector('[data-role="namecount"]');
      if (input && node) {
        input.addEventListener('input', function () { paintOneCount(input, node); });
        paintOneCount(input, node);
      }
      // The description shares one WhatsApp message with the name and the pack
      // list, so its allowance is shown the same way.
      var summary = product.querySelector('[data-role="summary"]');
      var summaryCount = product.querySelector('[data-role="sumcount"]');
      if (summary && summaryCount) {
        var paintSummary = function () {
          var left = limits.summary_limit - summary.value.trim().length;
          summaryCount.textContent = left >= 0
            ? left + ' characters left' : Math.abs(left) + ' characters too many';
          summaryCount.classList.toggle('is-error', left < 0);
        };
        summary.addEventListener('input', paintSummary);
        paintSummary();
      }
      // Unit sizes are capped by the WhatsApp list row limit.
      var sizeField = product.querySelector('[data-role="sizes"]');
      var sizeCount = product.querySelector('[data-role="sizecount"]');
      if (sizeField && sizeCount) {
        var paintSizes = function () {
          var count = parseSizes(sizeField.value).length;
          var over = count > limits.pack_limit;
          sizeCount.textContent = over
            ? count + ' sizes, max ' + limits.pack_limit
            : count + ' of ' + limits.pack_limit + ' sizes';
          sizeCount.classList.toggle('is-error', over);
        };
        sizeField.addEventListener('input', paintSizes);
        paintSizes();
      }
    });

    function paintNameCount() {
      if (!nameCount || !nameInput) return;
      var left = limits.name_limit - nameInput.value.trim().length;
      nameCount.textContent = left + ' left';
      nameCount.classList.toggle('is-error', left < 0);
    }

    var addSummaryCount = document.getElementById('crmAddSummaryCount');
    function paintAddSummaryCount() {
      if (!addSummaryCount || !summaryInput) return;
      var left = limits.summary_limit - summaryInput.value.trim().length;
      addSummaryCount.textContent = left >= 0
        ? left + ' left' : Math.abs(left) + ' too many';
      addSummaryCount.classList.toggle('is-error', left < 0);
    }
    if (summaryInput) {
      summaryInput.addEventListener('input', paintAddSummaryCount);
      paintAddSummaryCount();
    }
    if (nameInput) {
      nameInput.addEventListener('input', paintNameCount);
      paintNameCount();
    }

    if (openButton && form) {
      openButton.addEventListener('click', function () {
        form.hidden = false;
        if (addNote) addNote.textContent = '';
        if (nameInput) nameInput.focus();
      });
    }
    if (cancelButton && form) {
      cancelButton.addEventListener('click', function () {
        form.hidden = true;
        if (addNote) addNote.textContent = '';
      });
    }

    if (form) {
      form.addEventListener('submit', function (event) {
        event.preventDefault();
        var name = (nameInput ? nameInput.value : '').trim();
        var description = (summaryInput ? summaryInput.value : '').trim();
        var sizes = parseSizes(sizesInput ? sizesInput.value : '');

        function refuse(message, detail) {
          if (addNote) addNote.textContent = message;
          toast(message, 'error', detail);
        }

        if (!name) {
          refuse('Give the product a name.');
          if (nameInput) nameInput.focus();
          return;
        }
        // Checked here as well as on the server, so the agent is told before the
        // round trip and the reason is stated in full.
        if (name.length > limits.name_limit) {
          refuse('Product name is ' + (name.length - limits.name_limit) + ' character' +
                 (name.length - limits.name_limit === 1 ? '' : 's') + ' too long.',
                 'WhatsApp allows ' + limits.name_limit + ' characters in a list row, and this ' +
                 'name has ' + name.length + '.');
          if (nameInput) nameInput.focus();
          return;
        }
        if (grid.querySelectorAll('.crm-product').length >= limits.max_products) {
          refuse('The catalogue is full.',
                 'WhatsApp allows ' + limits.max_products + ' products in one browse list. Remove ' +
                 'one before adding another.');
          return;
        }
        if (!sizes.length) {
          refuse('Give the product at least one pack size.',
                 'Customers pick a pack size before the assistant can take an order.');
          if (sizesInput) sizesInput.focus();
          return;
        }
        if (sizes.length > limits.pack_limit) {
          refuse('Too many pack sizes: ' + sizes.length + '.',
                 'A product can have ' + limits.pack_limit + ' pack sizes so its list still fits ' +
                 'WhatsApp\'s ten-row limit.');
          return;
        }
        var tooLong = sizes.filter(function (size) { return size.length > limits.name_limit; });
        if (tooLong.length) {
          refuse('Pack size name is too long.', '“' + tooLong[0] + '” has ' + tooLong[0].length +
                 ' characters; WhatsApp allows ' + limits.name_limit + '.');
          return;
        }
        if (description.length > limits.summary_limit) {
          refuse('Description is ' + (description.length - limits.summary_limit) + ' character' +
                 (description.length - limits.summary_limit === 1 ? '' : 's') + ' too long.',
                 'The limit is ' + limits.summary_limit + ' characters.');
          return;
        }

        var submitButton = form.querySelector('button[type="submit"]');
        setBusy(submitButton, true, 'Adding…');
        postJson('/api/crm/products', {
          name: name,
          summary: description,
          sizes: sizes,
        })
          .then(function (result) {
            setBusy(submitButton, false);
            if (!result.success) {
              var message = result.message || 'Could not create the product.';
              if (addNote) addNote.textContent = message;
              toast(message, 'error');
              // The server knows the authoritative limits, so adopt whatever it
              // reports before the agent tries again.
              if (result.limits) applyLimits(result.limits);
              return;
            }
            toast('Added “' + name + '” to the catalogue.', 'ok');
            // A full reload keeps every card, price input and limit consistent
            // with what the server just stored.
            navigatingOnPurpose = true;
            window.location.reload();
          })
          .catch(function () {
            setBusy(submitButton, false);
            if (addNote) addNote.textContent = 'Could not create the product.';
            toast('Could not create the product. Check your connection and try again.', 'error');
          });
      });
    }

    applyLimits(null);

    // Closing the tab or reloading with unsaved catalogue edits loses them
    // silently, so the browser is asked to confirm first.
    window.addEventListener('beforeunload', function (event) {
      if (navigatingOnPurpose || dirtyCards <= 0) return;
      event.preventDefault();
      // Some browsers still need returnValue set to show their own prompt.
      event.returnValue = '';
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
