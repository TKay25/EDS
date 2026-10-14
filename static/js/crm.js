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
      if (name === 'stock' && !stockLoaded) {
        stockLoaded = true;
        // Once, on the first sight of the tab: a later visit keeps whatever the
        // screen already shows rather than throwing away unsaved counts.
        initStock();
      }
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
        var cancelled = !!order.cancelled;
        var pill = cancelled
          ? '<span class="crm-pill is-cancelled">Cancelled</span>'
          : '<span class="crm-pill ' + (paid ? 'is-paid' : 'is-unpaid') + '">' +
            escapeHtml(order.payment_label || (paid ? 'Paid' : 'Will pay')) + '</span>';
        return '<div class="crm-order-row">' +
          '<div class="crm-order-row-main"><strong>' + escapeHtml(who) + '</strong>' +
          '<small>' + escapeHtml(order.reference) + ' • ' + order.items + ' items • ' +
          escapeHtml(order.created_at) + '</small></div>' +
          pill +
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
        var cancelled = !!order.cancelled;
        // A cancelled order has nothing left to fulfil and no money to take, so
        // the fulfilment buttons are replaced rather than left to be pressed.
        var paymentCell = cancelled
          ? '<span class="crm-pill is-cancelled"><i class="bi bi-x-circle"></i> Cancelled</span>'
          : '<span class="crm-pill ' + (paid ? 'is-paid' : 'is-unpaid') + '">' +
            '<i class="bi ' + (paid ? 'bi-check2' : 'bi-clock') + '"></i> ' +
            escapeHtml(order.payment_label || (paid ? 'Paid' : 'Will pay')) +
            '</span>';
        // The stage the sales team last set, so a row says where the order has
        // got to without having to open it. An unmoved order shows nothing
        // rather than a stage nobody set.
        var stageCell = order.stage_label
          ? '<span class="crm-stage is-' + escapeHtml(order.stage || 'set') + '">' +
            '<i class="bi bi-signpost-split"></i> ' + escapeHtml(order.stage_label) + '</span>'
          : '<span class="crm-sub">—</span>';
        // Who placed the order. An agent order still waiting on the customer is
        // called out, because it is not work to act on yet and would otherwise
        // read like a customer order.
        var sourceCell = order.draft
          ? '<span class="crm-origin is-draft"><i class="bi bi-hourglass-split"></i> ' +
            'Agent · awaiting</span>'
          : (order.agent_created
              ? '<span class="crm-origin is-agent"><i class="bi bi-person-badge"></i> ' +
                'Agent</span>'
              : '<span class="crm-origin is-customer"><i class="bi bi-person"></i> ' +
                'Customer</span>');
        var actionsCell = cancelled
          ? '<span class="crm-row-note">Cancelled by the customer' + '</span>'
          : '<div class="crm-row-actions">' +
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
            '</div>';
        return '<tr data-reference="' + escapeHtml(order.reference) + '"' +
          (cancelled ? ' class="is-cancelled"' : '') + '>' +
          '<td><span class="crm-ref">' + escapeHtml(order.reference) + '</span>' + address + '</td>' +
          '<td><span class="crm-who">' + escapeHtml(who) + '</span>' + contact + '</td>' +
          '<td class="crm-col-num">' + order.items + '</td>' +
          '<td>' + paymentCell + '</td>' +
          '<td>' + stageCell + '</td>' +
          '<td>' + sourceCell + '</td>' +
          '<td class="crm-col-num crm-amount">' +
          (order.amount === null || order.amount === undefined
            ? '<span class="crm-amount-unset">Not priced</span>'
            : money(order.amount)) + '</td>' +
          '<td>' + actionsCell + '</td>' +
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

    function paymentPill(order, payment) {
      var status = (payment && payment.status) || order.payment_status || 'unpaid';
      var cls = status === 'paid' ? 'is-paid' : 'is-unpaid';
      // The label names how the money moved, or where it is going to be taken,
      // and comes from the server so the chatbot and the portal cannot disagree.
      var label = order.payment_label ||
        ({ paid: 'Paid', pending: 'Awaiting PIN', cancelled: 'Cancelled', failed: 'Failed',
           unpaid: 'Will pay' }[status] || status);
      return '<span class="crm-pill ' + cls + '">' + escapeHtml(label) + '</span>';
    }

    function renderPaymentHistory(history) {
      // The block is written by renderOrder, so the element is looked up here
      // rather than cached at startup, when it does not exist yet.
      var historyEl = document.getElementById('crmOrderPayLog');
      if (!historyEl) return;
      if (!history || !history.length) {
        historyEl.innerHTML =
          '<p class="crm-paylog-empty">Nothing has been recorded against this order yet.</p>';
        return;
      }
      historyEl.innerHTML = '<ol class="crm-paylog">' + history.map(function (entry) {
        return '<li><span class="crm-paylog-text">' + escapeHtml(entry.text) + '</span>' +
          '<span class="crm-paylog-at">' + escapeHtml(entry.at || '') + '</span></li>';
      }).join('') + '</ol>';
    }

    function renderOrder(order, payment, history) {
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
          '<div class="crm-od-meta">' + paymentPill(order, payment) + '</div>' +
        '</div>' +
        '<dl class="crm-od-fields">' +
          '<div><dt>Placed</dt><dd>' + escapeHtml(order.created_at) + '</dd></div>' +
          '<div><dt>Items</dt><dd>' + order.item_count + '</dd></div>' +
          '<div><dt>Fulfilment</dt><dd>' +
            (order.delivery_method === 'delivery' ? 'Delivery' : 'Collection') + '</dd></div>' +
          '<div><dt>Delivery address</dt><dd>' +
            (order.address ? escapeHtml(order.address) : '—') + '</dd></div>' +
          '<div><dt>Status</dt><dd>' +
            (order.stage_label ? escapeHtml(order.stage_label) : 'Not set') +
            (order.stage_at ? '<span class="crm-sub">updated ' + escapeHtml(order.stage_at) +
                              (order.stage_by ? ' by ' + escapeHtml(order.stage_by) : '') +
                              '</span>' : '') +
          '</dd></div>' +
          '<div><dt>Created by</dt><dd>' +
            (order.agent_created ? 'Agent' : 'Customer') +
            (order.created_by_name ? '<span class="crm-sub">' +
                                     escapeHtml(order.created_by_name) + '</span>' : '') +
            (order.draft ? '<span class="crm-sub">Awaiting the customer\'s confirmation</span>' : '') +
            (order.declined_at ? '<span class="crm-sub">Declined ' +
                                 escapeHtml(order.declined_at) + '</span>' : '') +
          '</dd></div>' +
        '</dl>' +
        '<div class="crm-table-wrap"><table class="crm-table crm-od-table">' +
          '<thead><tr><th>Item</th><th class="crm-od-qty">Qty</th>' +
          '<th class="crm-od-num">Unit price</th><th class="crm-od-num">Line total</th></tr></thead>' +
          '<tbody>' + rows + '</tbody>' +
        '</table></div>' +
        '<div class="crm-od-total"><span>Total' + (paid ? ' paid' : ' due') + '</span>' +
          '<strong>' + escapeHtml(money(order.total)) + '</strong></div>' +
        '<div class="crm-paylog-block"><h6>Payment history</h6>' +
          '<div id="crmOrderPayLog"></div></div>' +
        (order.priced ? '' :
          '<p class="crm-od-warn"><i class="bi bi-info-circle"></i> This order was taken before ' +
          'line prices were recorded, so unit prices show as not recorded. The total is the amount ' +
          'actually charged.</p>') +
        (payment && payment.awaiting ?
          '<p class="crm-od-note"><i class="bi bi-hourglass-split"></i> Waiting for the customer to ' +
          'enter their EcoCash PIN. Checking automatically…</p>' : '');

      renderPaymentHistory(history);
      renderStagePicker(order);
      orderTitle.textContent = 'Order ' + order.reference;
    }

    /* The stage picker offers only what this order's fulfilment allows, so a
       delivery is never offered "ready for collection at the shop". The labels
       come from the server so the two cannot drift. */
    function renderStagePicker(order) {
      var select = document.getElementById('crmOrderStage');
      var notify = document.getElementById('crmOrderStageNotify');
      var hint = document.getElementById('crmOrderStageHint');
      if (!select) return;
      var current = order.stage || '';
      var stages = order.stages || [];
      var markup = ['<option value="">No status set</option>'];
      stages.forEach(function (stage) {
        markup.push('<option value="' + escapeHtml(stage.value) + '"' +
          (stage.value === current ? ' selected' : '') + '>' +
          escapeHtml(stage.label) + '</option>');
      });
      select.innerHTML = markup.join('');
      select.disabled = !!order.cancelled;
      var saveBtn = document.getElementById('crmOrderStageSave');
      if (saveBtn) saveBtn.disabled = !!order.cancelled;
      if (notify) notify.disabled = !!order.cancelled;
      if (hint) {
        hint.textContent = order.cancelled
          ? 'This order is cancelled, so its status cannot be changed.'
          : 'The customer is sent a WhatsApp message describing what this status means for them.';
      }
    }

    function refreshOrderActions(order, payment) {
      var paid = order.payment_status === 'paid';
      var awaiting = payment && payment.awaiting;
      var requestBtn = document.getElementById('crmOrderRequestPay');
      var receiptBtn = document.getElementById('crmOrderReceipt');
      var sendBtn = document.getElementById('crmOrderSendReceipt');
      var settleEl = document.getElementById('crmOrderSettle');
      var noteEl = document.getElementById('crmOrderNote');

      // Nothing to record once the money is already accounted for.
      if (settleEl) settleEl.hidden = paid;
      // The method the order already records is the one to offer first, so
      // confirming it is a single click rather than a choice every time.
      var methodEl = document.getElementById('crmOrderMethod');
      if (methodEl && !paid && payment && payment.method_key) {
        var known = ['shop', 'delivery', 'ecocash', 'other'];
        if (known.indexOf(payment.method_key) >= 0) methodEl.value = payment.method_key;
      }
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
          var how = (payment && payment.method_label) ? ' ' + payment.method_label : '';
          noteEl.textContent = 'Paid' + how + ' ' + (order.paid_at || '') +
            '. The receipt can be printed or emailed to the customer.';
          noteEl.className = 'crm-od-status is-ok';
        } else if (awaiting) {
          noteEl.textContent = 'An EcoCash prompt was sent to the customer.';
          noteEl.className = 'crm-od-status';
        } else {
          noteEl.textContent = 'Not paid. Send the EcoCash prompt, or mark it received ' +
            'here when the money is handed over.';
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
          renderOrder(result.order, result.payment, result.payment_history);
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

    /* ------------------------------------------------------ build an order --
       An order an agent builds here is sent to the customer to accept or decline.
       The catalogue is read from the same endpoint the products tab uses, so a
       product or pack size the assistant would refuse cannot be offered. */
    var buildModal = document.getElementById('crmBuildOrderModal');
    var buildProductEl = document.getElementById('crmBuildProduct');
    var buildSizeEl = document.getElementById('crmBuildSize');
    var buildQtyEl = document.getElementById('crmBuildQty');
    var buildLinesEl = document.getElementById('crmBuildLines');
    var buildFulfilEl = document.getElementById('crmBuildFulfil');
    var buildShopEl = document.getElementById('crmBuildShop');
    var buildAddressEl = document.getElementById('crmBuildAddress');
    var buildNoteEl = document.getElementById('crmBuildNote');
    var buildSendBtn = document.getElementById('crmBuildSend');
    var buildLines = [];
    var buildCatalog = null;
    var buildShops = [];

    function loadBuildCatalog() {
      if (buildCatalog) return Promise.resolve(buildCatalog);
      return fetch('/api/crm/catalog')
        .then(function (response) { return response.json(); })
        .then(function (result) {
          buildCatalog = (result && result.products) || [];
          return buildCatalog;
        });
    }

    function loadBuildShops() {
      return fetch('/api/crm/shops')
        .then(function (response) { return response.json(); })
        .then(function (result) {
          buildShops = (result && result.shops) || [];
          if (!buildShopEl) return buildShops;
          buildShopEl.innerHTML = ['<option value="">No particular shop</option>']
            .concat(buildShops.map(function (shop) {
              return '<option value="' + escapeHtml(shop.id) + '">' +
                escapeHtml(shop.name) + '</option>';
            })).join('');
          return buildShops;
        })
        .catch(function () { return []; });
    }

    function renderBuildSizes() {
      if (!buildSizeEl || !buildProductEl) return;
      var product = buildCatalog[Number(buildProductEl.value)] || { sizes: [] };
      buildSizeEl.innerHTML = (product.sizes || []).map(function (size) {
        return '<option value="' + escapeHtml(size.size) + '">' + escapeHtml(size.size) +
          (size.price === null || size.price === undefined
            ? ' — price on request' : ' — ' + money(size.price)) + '</option>';
      }).join('');
    }

    function renderBuildLines() {
      if (!buildLinesEl) return;
      if (!buildLines.length) {
        buildLinesEl.innerHTML = '<li class="crm-build-empty">No products added yet.</li>';
      } else {
        buildLinesEl.innerHTML = buildLines.map(function (line, index) {
          return '<li><span>' + escapeHtml(line.name) + ' (' + escapeHtml(line.size) + ')' +
            ' × ' + line.qty + '</span>' +
            '<button class="crm-mini-btn" type="button" data-build-remove="' + index + '">' +
            '<i class="bi bi-x-lg"></i></button></li>';
        }).join('');
      }
      Array.prototype.forEach.call(
        buildLinesEl.querySelectorAll('[data-build-remove]'), function (button) {
          button.addEventListener('click', function () {
            buildLines.splice(Number(button.getAttribute('data-build-remove')), 1);
            renderBuildLines();
          });
        });
    }

    function openBuildOrder() {
      if (!buildModal) return;
      buildLines = [];
      renderBuildLines();
      buildModal.hidden = false;
      loadBuildCatalog().then(function () {
        if (!buildProductEl) return;
        buildProductEl.innerHTML = buildCatalog.map(function (product, index) {
          return '<option value="' + index + '">' + escapeHtml(product.name) + '</option>';
        }).join('');
        renderBuildSizes();
      });
      loadBuildShops();
    }

    function addBuildLine() {
      if (!buildProductEl || !buildSizeEl) return;
      var index = Number(buildProductEl.value);
      var product = buildCatalog[index];
      if (!product) return;
      var size = buildSizeEl.value;
      var qty = parseInt(buildQtyEl ? buildQtyEl.value : '1', 10) || 1;
      if (qty < 1) qty = 1;
      // The same product in the same pack size is one line, so adding it twice
      // raises the quantity rather than producing two lines to keep in step.
      var existing = buildLines.filter(function (line) {
        return line.product === index && line.size === size;
      })[0];
      if (existing) {
        existing.qty += qty;
      } else {
        buildLines.push({ product: index, name: product.name, size: size, qty: qty });
      }
      if (buildQtyEl) buildQtyEl.value = '1';
      renderBuildLines();
    }

    function sendBuildOrder() {
      if (!buildLines.length) {
        window.crmToast && window.crmToast('Add at least one product first.', 'error');
        return;
      }
      var delivering = buildFulfilEl && buildFulfilEl.value === 'delivery';
      var payload = {
        lines: buildLines.map(function (line) {
          return { product: line.product, size: line.size, qty: line.qty };
        }),
        delivery_method: delivering ? 'delivery' : 'collection',
        shop: buildShopEl ? buildShopEl.value : '',
        address: delivering && buildAddressEl ? buildAddressEl.value : '',
        note: buildNoteEl ? buildNoteEl.value : ''
      };
      buildSendBtn.disabled = true;
      postJson('/api/crm/chats/' + encodeURIComponent(activeId) + '/order', payload)
        .then(function (result) {
          buildSendBtn.disabled = false;
          window.crmToast && window.crmToast(
            result.message || (result.success ? 'Order sent for confirmation.' : 'Could not create the order.'),
            result.success ? 'success' : 'error');
          if (result.success) {
            if (buildModal) buildModal.hidden = true;
            if (result.orders) { renderRecent(result.orders); renderTable(result.orders); }
            loadMessages();
          }
        })
        .catch(function () {
          buildSendBtn.disabled = false;
          window.crmToast && window.crmToast('Could not create the order.', 'error');
        });
    }

    if (buildModal) {
      buildModal.addEventListener('change', function (event) {
        if (event.target === buildProductEl) renderBuildSizes();
      });
      buildModal.addEventListener('click', function (event) {
        var action = event.target.closest('[data-order-action]');
        if (action) {
          var kind = action.getAttribute('data-order-action');
          if (kind === 'close-build') { buildModal.hidden = true; return; }
          if (kind === 'build-add') { addBuildLine(); return; }
          if (kind === 'build-send') { sendBuildOrder(); return; }
        }
        if (event.target === buildModal) buildModal.hidden = true;
      });
      var buildOrderBtn = document.getElementById('crmBuildOrder');
      if (buildOrderBtn) {
        buildOrderBtn.addEventListener('click', function () { openBuildOrder(); });
      }
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
                if (result.success) load(true);
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
          if (kind === 'set-stage') {
            var stageEl = document.getElementById('crmOrderStage');
            var notifyEl = document.getElementById('crmOrderStageNotify');
            action.disabled = true;
            postJson('/api/crm/orders/' + encodeURIComponent(orderRef) + '/stage?' + query(),
                     { stage: stageEl ? stageEl.value : '',
                       notify: notifyEl ? notifyEl.checked : true })
              .then(function (result) {
                action.disabled = false;
                window.crmToast && window.crmToast(
                  result.message || (result.success ? 'Order status updated.'
                                                    : 'Could not update the status.'),
                  result.success ? 'success' : 'error');
                if (result.success) {
                  renderStats(result.analytics || {});
                  renderTop((result.analytics || {}).top_items);
                  renderRecent(result.orders);
                  renderTable(result.orders);
                  openOrder(orderRef);
                }
              })
              .catch(function () {
                action.disabled = false;
                window.crmToast && window.crmToast('Could not update the status.', 'error');
              });
            return;
          }
          if (kind === 'record-payment') {
            var methodEl = document.getElementById('crmOrderMethod');
            action.disabled = true;
            postJson('/api/crm/orders/' + encodeURIComponent(orderRef) + '/record-payment',
                     { method: methodEl ? methodEl.value : 'shop' })
              .then(function (result) {
                action.disabled = false;
                window.crmToast && window.crmToast(
                  result.message || (result.success ? 'Payment recorded.' : 'Could not record the payment.'),
                  result.success ? 'success' : 'error');
                if (result.success) { loadOrder(orderRef, true); load(true); }
              })
              .catch(function () {
                action.disabled = false;
                window.crmToast && window.crmToast('Could not record the payment.', 'error');
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

  /* ----------------------------------------------------------------- stock --
     Stock lives per shop, and the shop the customer chose is the one the goods
     come off when they pay. The table is built from the catalogue server-side,
     so every line the business sells can be counted, including ones nobody has
     stocked yet. Only changed lines are sent, so saving is one action for a
     whole shop rather than a click per row. */
  var stockBody = document.getElementById('crmStockBody');
  var stockSelect = document.getElementById('crmShopSelect');
  var stockCurrent = document.getElementById('crmShopCurrent');
  var stockSave = document.getElementById('crmStockSave');
  var stockHint = document.getElementById('crmStockHint');
  var stockMoves = document.getElementById('crmStockMovements');
  var shopForm = document.getElementById('crmShopForm');
  var stockShopId = null;
  var stockLoaded = false;

  function stockDirtyCount() {
    if (!stockBody) return 0;
    var changed = 0;
    Array.prototype.forEach.call(stockBody.querySelectorAll('input[data-line]'), function (input) {
      if (input.value.trim() !== (input.getAttribute('data-was') || '')) changed += 1;
    });
    return changed;
  }

  var stockLastPainted = null;

  function paintStockFoot() {
    var changed = stockDirtyCount();
    // Only touch the button when its label really changes: rewriting it on
    // every keystroke replaces the node under the pointer, which makes the
    // button feel unstable to click.
    if (changed === stockLastPainted) return;
    stockLastPainted = changed;
    if (stockSave) {
      stockSave.disabled = !changed;
      stockSave.innerHTML = changed
        ? '<i class="bi bi-check2"></i> Save ' + changed + ' change' + (changed === 1 ? '' : 's')
        : '<i class="bi bi-check2"></i> Save counts';
    }
    if (changed) {
      stockHint.textContent = changed + ' line' + (changed === 1 ? '' : 's') +
        ' not saved yet.';
      stockHint.className = 'crm-stock-hint is-dirty';
    } else {
      stockHint.textContent = 'Leave a line blank for a product this shop does not hold — ' +
        'blank means nobody has counted it, which is not the same as none left.';
      stockHint.className = 'crm-stock-hint';
    }
  }

  var stockShops = [];

  function renderStockShops(shops, current) {
    if (!stockSelect) return;
    stockShops = shops || [];
    if (!stockShops.length) {
      stockSelect.innerHTML = '<option value="">No shops yet</option>';
      stockSelect.disabled = true;
      if (stockCurrent) {
        stockCurrent.textContent = 'Add a shop to start keeping stock.';
      }
      setShopEditEnabled(false);
      return;
    }
    stockSelect.disabled = false;
    stockSelect.innerHTML = stockShops.map(function (shop) {
      return '<option value="' + escapeHtml(shop.id) + '">' + escapeHtml(shop.name) +
        '</option>';
    }).join('');
    if (current) stockSelect.value = current.id;
    setShopEditEnabled(true);
    paintShopCurrent(current);
  }

  function selectedShop() {
    if (!stockSelect || !stockShops.length) return null;
    var wanted = stockSelect.value;
    return stockShops.filter(function (shop) { return shop.id === wanted; })[0] || null;
  }

  function paintShopCurrent(current) {
    if (!stockCurrent) return;
    var shop = current || selectedShop();
    if (!shop) {
      stockCurrent.textContent = '';
      return;
    }
    stockCurrent.textContent = shop.address
      ? shop.name + ' · ' + shop.address + (shop.phone ? ' · ' + shop.phone : '')
      : shop.name;
  }

  function setShopEditEnabled(enabled) {
    var edit = document.getElementById('crmShopEdit');
    if (edit) edit.disabled = !enabled;
  }

  var stockRows = [];
  var receiveBuilt = '';

  /* The receive form offers the same products and pack sizes the table shows, so
     a delivery can only be booked against a line the shop is actually counted on. */
  function renderReceive(rows) {
    var productEl = document.getElementById('crmReceiveProduct');
    var sizeEl = document.getElementById('crmReceiveSize');
    if (!productEl || !sizeEl) return;
    stockRows = rows || [];
    if (!stockRows.length) {
      productEl.innerHTML = '';
      sizeEl.innerHTML = '';
      productEl.disabled = true;
      sizeEl.disabled = true;
      return;
    }
    productEl.disabled = false;
    sizeEl.disabled = false;
    // Rebuilt only when the catalogue itself changes, so a refresh never wipes a
    // half-filled form.
    var signature = stockRows.map(function (row) {
      return row.product_id + '|' + row.pack_size;
    }).join(',');
    if (signature === receiveBuilt) { paintReceiveSizes(); return; }
    receiveBuilt = signature;
    var chosenProduct = productEl.value;
    var products = [];
    stockRows.forEach(function (row) {
      if (!products.filter(function (p) { return p.id === row.product_id; }).length) {
        products.push({ id: row.product_id, name: row.name });
      }
    });
    productEl.innerHTML = products.map(function (product) {
      return '<option value="' + escapeHtml(product.id) + '">' +
        escapeHtml(product.name) + '</option>';
    }).join('');
    if (chosenProduct && products.filter(function (p) {
      return p.id === chosenProduct; }).length) {
      productEl.value = chosenProduct;
    }
    paintReceiveSizes();
  }

  function paintReceiveSizes() {
    var productEl = document.getElementById('crmReceiveProduct');
    var sizeEl = document.getElementById('crmReceiveSize');
    if (!productEl || !sizeEl || !stockRows.length) return;
    var chosenSize = sizeEl.value;
    var sizes = stockRows.filter(function (row) {
      return row.product_id === productEl.value;
    }).map(function (row) { return row.pack_size; });
    sizeEl.innerHTML = sizes.map(function (size) {
      return '<option value="' + escapeHtml(size) + '">' + escapeHtml(size) + '</option>';
    }).join('');
    if (chosenSize && sizes.indexOf(chosenSize) !== -1) sizeEl.value = chosenSize;
  }

  function saveReceive() {
    var productEl = document.getElementById('crmReceiveProduct');
    var sizeEl = document.getElementById('crmReceiveSize');
    var qtyEl = document.getElementById('crmReceiveQty');
    var noteEl = document.getElementById('crmReceiveNote');
    var button = document.getElementById('crmReceiveSave');
    if (!productEl || !sizeEl || !stockShopId) return;
    var quantity = qtyEl ? qtyEl.value.trim() : '';
    if (!quantity || parseInt(quantity, 10) <= 0) {
      window.crmToast && window.crmToast(
        'How many arrived? Enter a number above zero.', 'error');
      return;
    }
    setBusy(button, true, 'Adding…');
    postJson('/api/crm/stock/receive', {
      shop: stockShopId,
      product_id: productEl.value,
      pack_size: sizeEl.value,
      quantity: quantity,
      note: noteEl ? noteEl.value.trim() : '',
    }).then(function (result) {
      setBusy(button, false);
      window.crmToast && window.crmToast(
        result.message || (result.success ? 'Stock added.' : 'Could not add the stock.'),
        result.success ? 'success' : 'error');
      if (!result.success) return;
      if (qtyEl) qtyEl.value = '';
      if (noteEl) noteEl.value = '';
      renderStockRows(result.rows);
      renderStockMovements(result.movements);
    }).catch(function () {
      setBusy(button, false);
      window.crmToast && window.crmToast('Could not add the stock.', 'error');
    });
  }

  function renderStockRows(rows) {
    if (!stockBody) return;
    if (!rows || !rows.length) {
      stockBody.innerHTML = '<tr><td colspan="3" class="crm-empty-cell">' +
        'Nothing in the catalogue to count yet.</td></tr>';
      renderReceive([]);
      return;
    }
    stockBody.innerHTML = rows.map(function (row) {
      var value = row.quantity === null || row.quantity === undefined ? '' : String(row.quantity);
      return '<tr><td><strong>' + escapeHtml(row.name) + '</strong></td>' +
        '<td>' + escapeHtml(row.pack_size) + '</td>' +
        '<td class="crm-od-num">' +
          '<input class="crm-stock-input" type="number" min="0" step="1" inputmode="numeric" ' +
            'value="' + escapeHtml(value) + '" data-was="' + escapeHtml(value) + '" ' +
            'data-line="' + escapeHtml(row.product_id + '|' + row.pack_size) + '" ' +
            'aria-label="Stock of ' + escapeHtml(row.name + ' ' + row.pack_size) + '">' +
        '</td></tr>';
    }).join('');
    renderReceive(rows);
    paintStockFoot();
  }

  function renderStockMovements(movements) {
    if (!stockMoves) return;
    if (!movements || !movements.length) {
      stockMoves.innerHTML = '<p class="crm-stock-empty">Nothing has moved yet.</p>';
      return;
    }
    stockMoves.innerHTML = '<ul class="crm-stock-moves">' + movements.map(function (move) {
      var up = move.delta > 0;
      // A delivery and a count both raise the figure, so the log says which one
      // it was - otherwise a rise reads as somebody having recounted the shelf.
      var what = up ? (move.reason === 'delivery' ? 'Received ' : 'Added ')
                    : 'Took off ';
      return '<li>' +
        '<span class="crm-stock-move-delta ' + (up ? 'is-up' : 'is-down') + '">' +
          (up ? '+' : '') + move.delta + '</span>' +
        '<span class="crm-stock-move-text">' + what + escapeHtml(String(Math.abs(move.delta))) +
          ' × ' + escapeHtml(move.pack_size) +
          (move.reference ? ' for <strong>' + escapeHtml(move.reference) + '</strong>' : '') +
          (move.by ? ' — ' + escapeHtml(move.by) : '') + '</span>' +
        '<span class="crm-stock-move-at">' + escapeHtml(move.at || '') + '</span>' +
        '</li>';
    }).join('') + '</ul>';
  }

  function loadStock(shopId) {
    if (!stockBody) return;
    if (shopId) stockShopId = shopId;
    stockBody.innerHTML = '<tr><td colspan="3" class="crm-empty-cell">' +
      '<span class="crm-spinner"></span> Loading stock…</td></tr>';
    fetch('/api/crm/stock' + (stockShopId ? '?shop=' + encodeURIComponent(stockShopId) : ''))
      .then(function (response) { return response.json(); })
      .then(function (result) {
        if (!result.success) {
          stockBody.innerHTML = '<tr><td colspan="3" class="crm-empty-cell">' +
            'Could not load stock.</td></tr>';
          return;
        }
        stockShopId = result.shop ? result.shop.id : null;
        renderStockShops(result.shops, result.shop);
        renderStockRows(result.rows);
        renderStockMovements(result.movements);
      })
      .catch(function () {
        stockBody.innerHTML = '<tr><td colspan="3" class="crm-empty-cell">' +
          'Could not load stock.</td></tr>';
      });
  }

  function saveStock() {
    if (!stockBody || !stockShopId) return;
    var counts = [];
    Array.prototype.forEach.call(stockBody.querySelectorAll('input[data-line]'), function (input) {
      var was = input.getAttribute('data-was') || '';
      if (input.value.trim() === was) return;
      var parts = (input.getAttribute('data-line') || '').split('|');
      counts.push({ product_id: parts[0], pack_size: parts.slice(1).join('|'),
                    quantity: input.value.trim() === '' ? '' : input.value.trim() });
    });
    if (!counts.length) return;
    // A blank line means "nobody has counted this", so it is sent as nothing
    // rather than as a zero the shop would then be held to.
    var sendable = counts.filter(function (entry) { return entry.quantity !== ''; });
    if (!sendable.length) {
      window.crmToast && window.crmToast(
        'A blank line means the shop does not hold it. Enter a number to count it.', 'error');
      return;
    }
    setBusy(stockSave, true, 'Saving…');
    postJson('/api/crm/stock', { shop: stockShopId, counts: sendable })
      .then(function (result) {
        setBusy(stockSave, false);
        window.crmToast && window.crmToast(
          result.message || (result.success ? 'Stock saved.' : 'Could not save the stock.'),
          result.success ? 'success' : 'error');
        if (result.success) {
          renderStockRows(result.rows);
          renderStockMovements(result.movements);
        } else {
          paintStockFoot();
        }
      })
      .catch(function () {
        setBusy(stockSave, false);
        window.crmToast && window.crmToast('Could not save the stock.', 'error');
        paintStockFoot();
      });
  }

  function saveShop() {
    var idEl = document.getElementById('crmShopId');
    var nameEl = document.getElementById('crmShopName');
    var addressEl = document.getElementById('crmShopAddress');
    var phoneEl = document.getElementById('crmShopPhone');
    var button = document.getElementById('crmShopSave');
    var editing = idEl ? idEl.value : '';
    var name = nameEl ? nameEl.value.trim() : '';
    if (!name) {
      window.crmToast && window.crmToast('Give the shop a name.', 'error');
      return;
    }
    setBusy(button, true, 'Saving…');
    var payload = {
      name: name,
      address: addressEl ? addressEl.value.trim() : '',
      phone: phoneEl ? phoneEl.value.trim() : '',
    };
    // Sending the id it started from turns this into a rename of that shop. The
    // id is never derived from the new name, so orders and stock rows that point
    // at the shop keep pointing at it.
    if (editing) payload.shop_id = editing;
    // The staff are collected before the shop is saved, so a validation failure
    // on them stops the shop being renamed around a list that never saved.
    var staff = collectShopStaff();
    if (staff.length > STAFF_LIMIT) {
      window.crmToast && window.crmToast(
        'A shop can have up to ' + STAFF_LIMIT + ' staff.', 'error');
      return;
    }
    postJson('/api/crm/shops', payload).then(function (result) {
      if (!result.success) {
        setBusy(button, false);
        window.crmToast && window.crmToast(
          result.message || 'Could not save the shop.', 'error');
        return;
      }
      var shopId = result.shop ? result.shop.id : '';
      // The staff are saved against the shop that was just written, so a brand
      // new shop takes its people in the same action as its name.
      saveShopStaff(shopId).then(function (staffOk) {
        setBusy(button, false);
        window.crmToast && window.crmToast(
          staffOk && staff.length
            ? (result.message || 'Shop saved.') + ' Staff notified of new orders.'
            : (result.message || (result.success ? 'Shop saved.' : 'Could not save the shop.')),
          'success');
        closeShopForm();
        stockShopId = result.shop ? result.shop.id : null;
        renderStockShops(result.shops, result.shop);
        renderStockRows(result.rows);
        renderStockMovements(result.movements);
      });
    }).catch(function () {
      setBusy(button, false);
      window.crmToast && window.crmToast('Could not save the shop.', 'error');
    });
  }

  function openShopForm(shop) {
    if (!shopForm) return;
    var idEl = document.getElementById('crmShopId');
    var nameEl = document.getElementById('crmShopName');
    var addressEl = document.getElementById('crmShopAddress');
    var phoneEl = document.getElementById('crmShopPhone');
    if (idEl) idEl.value = shop ? shop.id : '';
    if (nameEl) nameEl.value = shop ? shop.name : '';
    if (addressEl) addressEl.value = shop ? (shop.address || '') : '';
    if (phoneEl) phoneEl.value = shop ? (shop.phone || '') : '';
    paintShopNameCount();
    // An existing shop loads its saved staff; a new one starts with a blank row.
    loadShopStaff(shop ? shop.id : null);
    shopForm.hidden = false;
    if (nameEl) nameEl.focus();
  }

  function closeShopForm() {
    if (!shopForm) return;
    var idEl = document.getElementById('crmShopId');
    if (idEl) idEl.value = '';
    shopForm.hidden = true;
    paintShopNameCount();
  }

  // The cap is enforced on the field, so this only ever informs: it exists so the
  // limit is visible while typing rather than discovered when the save is refused.
  function paintShopNameCount() {
    var nameEl = document.getElementById('crmShopName');
    var label = document.getElementById('crmShopFormLabel');
    var idEl = document.getElementById('crmShopId');
    if (!nameEl || !label) return;
    var limit = parseInt(nameEl.getAttribute('maxlength'), 10) || 24;
    label.textContent = (idEl && idEl.value ? 'Rename or edit shop' : 'New shop') +
      ' (' + nameEl.value.length + ' / ' + limit + ' characters)';
  }

  /* --------------------------------------------------------- shop staff --
     Up to SHOP_STAFF_LIMIT people per shop, each with a name and a WhatsApp
     number. They are saved with the shop, so the list on screen is exactly who
     gets messaged when an order lands there. */
  var STAFF_LIMIT = 3;
  var shopStaffRows = document.getElementById('crmShopStaffRows');
  var shopStaffCount = document.getElementById('crmShopStaffCount');
  var shopStaffAdd = document.getElementById('crmShopStaffAdd');
  // The numbers already saved for the shop being edited, so a re-open shows the
  // real list rather than whatever was typed and abandoned.
  var shopStaffSaved = [];

  function staffRowMarkup(name, number, index) {
    return '<div class="crm-shop-staff-row" data-staff-row="' + index + '">' +
      '<input class="ps-input" type="text" data-staff-name="1" value="' +
        escapeHtml(name || '') + '" placeholder="Name" autocomplete="off">' +
      '<input class="ps-input" type="text" data-staff-number="1" value="' +
        escapeHtml(number || '') + '" placeholder="07… WhatsApp number" autocomplete="off">' +
      '<button class="crm-mini-btn" type="button" data-staff-remove="' + index + '" ' +
        'title="Remove this person"><i class="bi bi-x-lg"></i></button>' +
      '</div>';
  }

  function paintShopStaff() {
    if (!shopStaffRows) return;
    var rows = shopStaffRows.querySelectorAll('[data-staff-row]');
    Array.prototype.forEach.call(rows, function (row) {
      var button = row.querySelector('[data-staff-remove]');
      if (button) button.disabled = rows.length <= 1;
    });
    if (shopStaffCount) {
      shopStaffCount.textContent = rows.length + ' of ' + STAFF_LIMIT;
    }
    if (shopStaffAdd) shopStaffAdd.disabled = rows.length >= STAFF_LIMIT;
  }

  function renderShopStaff(list) {
    if (!shopStaffRows) return;
    shopStaffSaved = (list || []).filter(function (person) { return person.active !== false; });
    var shown = shopStaffSaved.length ? shopStaffSaved : [{ name: '', wa_number: '' }];
    shopStaffRows.innerHTML = shown.map(function (person, index) {
      return staffRowMarkup(person.name, person.wa_number, index);
    }).join('');
    paintShopStaff();
  }

  function addShopStaffRow() {
    if (!shopStaffRows) return;
    var rows = shopStaffRows.querySelectorAll('[data-staff-row]');
    if (rows.length >= STAFF_LIMIT) return;
    shopStaffRows.insertAdjacentHTML('beforeend',
      staffRowMarkup('', '', rows.length));
    paintShopStaff();
  }

  function collectShopStaff() {
    if (!shopStaffRows) return [];
    var out = [];
    Array.prototype.forEach.call(shopStaffRows.querySelectorAll('[data-staff-row]'),
      function (row) {
        var name = row.querySelector('[data-staff-name]');
        var number = row.querySelector('[data-staff-number]');
        var nameValue = name ? name.value.trim() : '';
        var numberValue = number ? number.value.trim() : '';
        if (!nameValue && !numberValue) return;
        out.push({ name: nameValue, wa_number: numberValue });
      });
    return out;
  }

  function loadShopStaff(shopId) {
    if (!shopStaffRows) return Promise.resolve();
    if (!shopId) { renderShopStaff([]); return Promise.resolve(); }
    return fetch('/api/crm/shops/' + encodeURIComponent(shopId) + '/staff')
      .then(function (response) { return response.json(); })
      .then(function (result) {
        renderShopStaff(result && result.success ? result.staff : []);
      })
      .catch(function () { renderShopStaff([]); });
  }

  function saveShopStaff(shopId) {
    if (!shopId) return Promise.resolve(true);
    return postJson('/api/crm/shops/' + encodeURIComponent(shopId) + '/staff',
                    { staff: collectShopStaff() })
      .then(function (result) {
        if (!result.success) {
          window.crmToast && window.crmToast(result.message || 'Could not save the staff.', 'error');
          return false;
        }
        renderShopStaff(result.staff || []);
        window.crmToast && window.crmToast(result.message || 'Staff saved.', 'success');
        return true;
      })
      .catch(function () {
        window.crmToast && window.crmToast('Could not save the staff.', 'error');
        return false;
      });
  }

  if (shopStaffRows) {
    shopStaffRows.addEventListener('click', function (event) {
      var button = event.target.closest('[data-staff-remove]');
      if (!button || button.disabled) return;
      var row = button.closest('[data-staff-row]');
      if (row) { row.remove(); paintShopStaff(); }
    });
  }
  if (shopStaffAdd) {
    shopStaffAdd.addEventListener('click', function () {
      if (shopStaffAdd.disabled) return;
      addShopStaffRow();
    });
  }

  if (stockSelect) {
    stockSelect.addEventListener('change', function () {
      if (stockDirtyCount() && !window.confirm(
          'You have unsaved counts. Change shop and lose them?')) {
        // Put the dropdown back where it was rather than losing the counts.
        if (stockShopId) stockSelect.value = stockShopId;
        return;
      }
      loadStock(stockSelect.value);
    });
  }
  if (stockBody) {
    stockBody.addEventListener('input', paintStockFoot);
    stockBody.addEventListener('change', paintStockFoot);
  }
  if (stockSave) stockSave.addEventListener('click', saveStock);
  var shopNew = document.getElementById('crmShopNew');
  if (shopNew) {
    shopNew.addEventListener('click', function () {
      if (shopForm && !shopForm.hidden) { closeShopForm(); return; }
      openShopForm(null);
    });
  }
  var shopEdit = document.getElementById('crmShopEdit');
  if (shopEdit) {
    shopEdit.addEventListener('click', function () {
      if (shopForm && !shopForm.hidden) { closeShopForm(); return; }
      openShopForm(selectedShop());
    });
  }
  var shopCancel = document.getElementById('crmShopCancel');
  if (shopCancel) shopCancel.addEventListener('click', closeShopForm);
  var shopNameEl = document.getElementById('crmShopName');
  if (shopNameEl) shopNameEl.addEventListener('input', paintShopNameCount);
  var shopSave = document.getElementById('crmShopSave');
  if (shopSave) shopSave.addEventListener('click', saveShop);
  var receiveSave = document.getElementById('crmReceiveSave');
  if (receiveSave) receiveSave.addEventListener('click', saveReceive);
  var receiveProduct = document.getElementById('crmReceiveProduct');
  if (receiveProduct) receiveProduct.addEventListener('change', paintReceiveSizes);

  function initStock() {
    // Loaded on first sight of the tab rather than on page load, so the inbox
    // does not wait on a stock read it may never be asked for.
    loadStock(null);
  }


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

    // Structural changes (a product added or restored) only show up after a
    // reload, and that reload also throws away edits on every other card. It is
    // only worth doing silently when there is nothing to lose.
    function reloadCatalogue(reason) {
      if (dirtyCards > 0) {
        var ok = window.confirm(
          dirtyCards + (dirtyCards === 1 ? ' product has' : ' products have') +
          ' unsaved changes.\n\nReloading to ' + reason + ' will discard them.\n\n' +
          'Reload and lose those changes?');
        if (!ok) {
          toast('Reload skipped — your unsaved changes were kept.', 'warn',
                'Refresh the page when you are ready for ' + reason + '.');
          return;
        }
      }
      // This reload is intentional, so it must not trip the guard below.
      navigatingOnPurpose = true;
      window.location.reload();
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
                window.setTimeout(function () {
                  reloadCatalogue('show the restored product');
                }, 700);
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
            reloadCatalogue('show the new product');
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
