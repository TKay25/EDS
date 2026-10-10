/* ==========================================================================
   EDS Company System — portal interactions
   (CRM inbox + portal user menu)
   ========================================================================== */
(function () {
  'use strict';

  function escapeHtml(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  /* -------------------------------------------------------------------- crm */
  function initCrm() {
    var root = document.getElementById('crmInbox');
    if (!root) return;

    var listEl = document.getElementById('crmChatList');
    var threadEl = document.getElementById('crmThread');
    var emptyEl = document.getElementById('crmEmpty');
    var nameEl = document.getElementById('crmChatName');
    var numberEl = document.getElementById('crmChatNumber');
    var statusEl = document.getElementById('crmChatStatus');
    var reasonEl = document.getElementById('crmChatReason');
    var actionsEl = document.getElementById('crmChatActions');
    var composerEl = document.getElementById('crmComposer');
    var composerNoteEl = document.getElementById('crmComposerNote');
    var replyInput = document.getElementById('crmReplyInput');
    var searchEl = document.getElementById('crmChatSearch');
    var searchClearEl = document.getElementById('crmChatSearchClear');
    var searchHintEl = document.getElementById('crmSearchHint');
    var filterSelect = document.getElementById('crmFilterSelect');

    var activeId = null;
    var activeStatus = 'bot';
    var timer = null;
    var searchTerm = '';
    var searchTimer = null;

    function statusChip(status, unattended) {
      var label = { bot: 'Assistant', escalated: 'Needs agent', human: 'Agent live', closed: 'Closed' }[status] || status;
      var cls = unattended && status === 'bot' ? 'is-escalated' : 'is-' + status;
      return '<span class="ps-status ' + cls + '">' + escapeHtml(label) + '</span>';
    }

    function renderList(chats) {
      if (!chats.length) {
        var searching = searchTerm !== '';
        listEl.innerHTML = '<div class="ps-empty"><i class="bi bi-' +
          (searching ? 'search' : 'chat-square-text') + '"></i>' +
          '<h4>' + (searching ? 'No matching conversations' : 'No conversations yet') + '</h4>' +
          '<p>' + (searching
            ? 'Nothing matched “' + escapeHtml(searchTerm) + '”. Try a name, a phone number or a product.'
            : 'Every message on the company WhatsApp channel appears here, from customers and staff alike.') +
          '</p></div>';
        return;
      }
      listEl.innerHTML = chats.map(function (chat) {
        var initials = escapeHtml((chat.display_name || 'C').trim().slice(0, 2).toUpperCase());
        var preview = escapeHtml(chat.last_message || 'No messages yet');
        // Staff (leave management) and shopping conversations share one channel,
        // so each row is colour-keyed and tagged the same way the server renders
        // it; otherwise the first refresh would drop the distinction.
        var kind = chat.kind || (chat.is_staff ? 'inhouse' : 'shopping');
        var classes = 'ps-chat-item is-' + escapeHtml(kind) +
          (chat.status === 'human' ? ' is-human' : '') +
          (String(chat.id) === String(activeId) ? ' is-active' : '');
        var tags = '';
        if (chat.is_staff) tags += '<span class="crm-kind-tag">In-house</span>';
        if (chat.status === 'human') tags += '<span class="crm-kind-tag crm-kind-human">Agent live</span>';
        if (chat.unattended && chat.status !== 'human') {
          tags += '<span class="crm-kind-tag crm-kind-needs">Needs human</span>';
        }
        if (chat.unread_count) tags += '<span class="ps-badge">' + chat.unread_count + '</span>';
        var badges = (chat.badges || []).map(function (badge) {
          return '<span class="crm-chat-badge is-' + escapeHtml(badge.tone) + '">' +
            escapeHtml(badge.label) + '</span>';
        }).join('');
        return '' +
          '<div class="' + classes + '" data-chat-id="' + chat.id +
          '" data-kind="' + escapeHtml(kind) + '">' +
          '<div class="ps-chat-avatar">' + initials + '</div>' +
          '<div class="ps-chat-meta">' +
          '<div class="ps-chat-name">' + escapeHtml(chat.display_name || ('+' + chat.wa_number)) +
          tags + '</div>' +
          '<div class="ps-chat-preview">' + preview + '</div>' +
          (badges ? '<div class="crm-chat-badges">' + badges + '</div>' : '') +
          '<div class="ps-chat-time">' + escapeHtml(chat.last_message_at || '') + '</div>' +
          '</div></div>';
      }).join('');

      Array.prototype.forEach.call(listEl.querySelectorAll('.ps-chat-item'), function (item) {
        item.addEventListener('click', function () {
          openChat(item.getAttribute('data-chat-id'));
        });
      });
    }

    function updateSearchChrome(count) {
      if (!searchClearEl) return;
      searchClearEl.hidden = searchTerm === '';
      if (searchHintEl) {
        if (searchTerm === '') {
          searchHintEl.hidden = true;
          searchHintEl.textContent = '';
        } else {
          searchHintEl.hidden = false;
          searchHintEl.textContent = count + (count === 1 ? ' conversation matches' : ' conversations match') +
            ' “' + searchTerm + '”';
        }
      }
    }

    // Delivery problems must stay visible after the send, so they are shown
    // under the composer rather than in a dialog the agent dismisses blindly.
    function setComposerNotice(text, isError) {
      if (!composerNoteEl) return;
      if (!text) {
        composerNoteEl.classList.remove('is-error');
        composerNoteEl.textContent = '';
        renderActions();
        return;
      }
      composerNoteEl.classList.toggle('is-error', Boolean(isError));
      composerNoteEl.textContent = text;
    }

    function bubbleClass(message) {
      if (message.sender === 'system') return 'ps-bubble ps-bubble-system';
      if (message.direction === 'inbound') return 'ps-bubble ps-bubble-in';
      if (message.sender === 'agent') return 'ps-bubble ps-bubble-agent';
      return 'ps-bubble ps-bubble-out';
    }

    function senderLabel(message) {
      if (message.direction === 'inbound') return 'Customer';
      if (message.sender === 'agent') {
        return message.agent ? message.agent : 'Agent';
      }
      if (message.sender === 'system') return 'System';
      if (message.sender === 'staff') return 'Staff assistant';
      return 'Assistant';
    }

    // Every outbound bubble says who wrote it, so an agent can tell at a glance
    // which replies came from the assistant and which from a colleague.
    function senderTag(message) {
      if (message.sender === 'agent') return 'Human';
      if (message.sender === 'system') return 'System';
      if (message.direction === 'outbound') return 'Bot';
      return '';
    }

    function renderActions() {
      var buttons = [];
      if (activeStatus === 'human') {
        buttons.push('<button class="ps-btn ps-btn-ghost ps-btn-sm" data-mode="bot"><i class="bi bi-robot"></i> Return to bot</button>');
      } else {
        buttons.push('<button class="ps-btn ps-btn-ok ps-btn-sm" data-mode="human"><i class="bi bi-person-check"></i> Switch to human</button>');
      }
      if (activeStatus !== 'closed') {
        buttons.push('<button class="ps-btn ps-btn-ghost ps-btn-sm" data-mode="closed"><i class="bi bi-check2-circle"></i> Mark resolved</button>');
      }
      actionsEl.innerHTML = buttons.join('');
      Array.prototype.forEach.call(actionsEl.querySelectorAll('[data-mode]'), function (button) {
        button.addEventListener('click', function () {
          setMode(button.getAttribute('data-mode'));
        });
      });
      // While an agent owns the chat the assistant stays silent, so say so
      // plainly instead of letting a reply look like it reached a bot.
      // A live delivery error outranks this hint and must survive the refresh
      // that follows a send.
      if (composerNoteEl && !composerNoteEl.classList.contains('is-error')) {
        composerNoteEl.textContent = activeStatus === 'human'
          ? 'You are replying as an agent. The bot is paused for this chat.'
          : (activeStatus === 'closed'
              ? 'This conversation is closed. Sending a reply reopens it for the customer.'
              : 'The bot answers automatically. Switch to human to reply yourself.');
      }
    }

    function openChat(chatId) {
      activeId = chatId;
      Array.prototype.forEach.call(listEl.querySelectorAll('.ps-chat-item'), function (item) {
        item.classList.toggle('is-active', item.getAttribute('data-chat-id') === String(chatId));
      });
      loadMessages();
    }

    function loadMessages(silent) {
      if (!activeId) return;
      fetch('/api/crm/chats/' + activeId + '/messages')
        .then(function (response) { return response.json(); })
        .then(function (payload) {
          if (!payload.success) return;
          emptyEl.style.display = 'none';
          // Must stay a flex column: the bubble sides are driven by align-self,
          // which 'block' silently disables and flattens every message left.
          threadEl.style.display = 'flex';
          if (composerEl) composerEl.style.display = 'flex';
          nameEl.textContent = payload.display_name;
          numberEl.textContent = '+' + payload.wa_number;
          if (payload.status) {
            activeStatus = payload.status;
            statusEl.innerHTML = statusChip(payload.status, payload.status === 'escalated');
            renderActions();
          }
          // The API returns newest first so an agent always sees the latest
          // exchange without scrolling. The thread then reverses that so the
          // newest message sits at the bottom, which is how every messaging
          // app reads, and the view is scrolled there on load.
          var ordered = payload.messages.slice().reverse();
          threadEl.innerHTML = ordered.length
            ? ordered.map(function (message) {
                var failed = message.failed && message.direction === 'outbound'
                  ? '<small class="crm-bubble-failed"><i class="bi bi-exclamation-triangle"></i>' +
                    ' Not delivered — WhatsApp\'s 24-hour window closed. The customer must message first.</small>'
                  : '';
                var tag = senderTag(message);
                var tagHtml = tag
                  ? '<span class="crm-bubble-tag is-' + tag.toLowerCase() + '">' + escapeHtml(tag) + '</span>'
                  : '';
                return '<div class="' + bubbleClass(message) + '">' +
                  '<small class="crm-bubble-sender">' + tagHtml +
                  escapeHtml(senderLabel(message)) + '</small>' +
                  escapeHtml(message.body) +
                  '<small>' + escapeHtml(message.at) + '</small>' + failed + '</div>';
              }).join('')
            : '<div class="ps-empty"><p>No messages yet.</p></div>';
          threadEl.scrollTop = threadEl.scrollHeight;
        })
        .catch(function () { if (!silent) console.warn('Could not load conversation'); });
    }

    function setMode(mode) {
      if (!activeId) return;
      fetch('/api/crm/chats/' + activeId + '/mode', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: mode })
      })
        .then(function (response) { return response.json(); })
        .then(function () {
          activeStatus = mode;
          statusEl.innerHTML = statusChip(mode, false);
          if (reasonEl) reasonEl.textContent = '';
          renderActions();
          refresh(true);
          loadMessages(true);
        });
    }

    function refresh(silent) {
      var filter = root.getAttribute('data-filter') || 'all';
      var url = '/api/crm/summary?filter=' + encodeURIComponent(filter);
      if (searchTerm) url += '&search=' + encodeURIComponent(searchTerm);
      fetch(url)
        .then(function (response) { return response.json(); })
        .then(function (payload) {
          if (!payload.success) return;
          renderList(payload.chats || []);
          updateSearchChrome((payload.chats || []).length);
          var stats = payload.stats || {};
          Object.keys(stats).forEach(function (key) {
            var node = document.querySelector('[data-stat="' + key + '"]');
            if (node) node.textContent = stats[key];
          });
        })
        .catch(function () { if (!silent) console.warn('Could not refresh inbox'); });
    }

    if (composerEl) {
      composerEl.addEventListener('submit', function (event) {
        event.preventDefault();
        var value = replyInput.value.trim();
        if (!value || !activeId) return;
        replyInput.value = '';
        setComposerNotice('');
        fetch('/api/crm/chats/' + activeId + '/reply', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message: value })
        })
          .then(function (response) { return response.json(); })
          .then(function (payload) {
            if (payload && payload.delivered === false) {
              setComposerNotice(payload.message || 'WhatsApp did not accept that message.', true);
            }
            loadMessages(true);
            refresh(true);
          })
          .catch(function () {
            setComposerNotice('Could not send the reply. Please try again.', true);
          });
      });
    }

    Array.prototype.forEach.call(document.querySelectorAll('[data-crm-preset]'), function (button) {
      button.addEventListener('click', function () {
        var value = button.getAttribute('data-crm-preset');
        if (filterSelect) filterSelect.value = value;
        root.setAttribute('data-filter', value);
        refresh(true);
      });
    });

    if (filterSelect) {
      filterSelect.addEventListener('change', function () {        root.setAttribute('data-filter', filterSelect.value);
        refresh(true);
      });
    }

    if (searchEl) {
      // Debounced so typing does not fire a request per keystroke.
      searchEl.addEventListener('input', function () {
        searchTerm = searchEl.value.trim();
        window.clearTimeout(searchTimer);
        searchTimer = window.setTimeout(function () { refresh(true); }, 300);
      });

      searchEl.addEventListener('keydown', function (event) {
        if (event.key === 'Escape') {
          searchEl.value = '';
          searchTerm = '';
          refresh(true);
        }
      });
    }

    if (searchClearEl) {
      searchClearEl.addEventListener('click', function () {
        searchEl.value = '';
        searchTerm = '';
        searchEl.focus();
        refresh(true);
      });
    }

    refresh(false);
    timer = window.setInterval(function () { refresh(true); if (activeId) loadMessages(true); }, 20000);
    window.addEventListener('beforeunload', function () {
      window.clearInterval(timer);
      window.clearTimeout(searchTimer);
    });
  }

  /* ----------------------------------------------------------- user menu */
  function initUserMenu() {
    var menu = document.getElementById('psUserMenu');
    if (!menu) return;
    var trigger = menu.querySelector('.ps-usermenu-trigger');
    if (!trigger) return;

    function close() {
      menu.classList.remove('is-open');
      trigger.setAttribute('aria-expanded', 'false');
    }

    trigger.addEventListener('click', function (event) {
      event.stopPropagation();
      var open = menu.classList.toggle('is-open');
      trigger.setAttribute('aria-expanded', open ? 'true' : 'false');
    });

    document.addEventListener('click', function (event) {
      if (!menu.contains(event.target)) close();
    });

    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') close();
    });
  }

  /* -------------------------------------------------- instant navigation
     The portals are separate pages, so make the handover feel seamless:
     prefetch the destinations so they are warm in the browser cache, and fade
     the shell out on click so the swap does not flash white. */
  function initNavigation() {
    var links = document.querySelectorAll('a.ps-nav-link, a.ps-card, a.ps-usermenu-item');

    Array.prototype.forEach.call(links, function (link) {
      var href = link.getAttribute('href');
      if (!href || href.charAt(0) === '#' || link.target === '_blank') return;

      // Warm the cache on hover/focus so the next page is ready before the click.
      var prefetched = false;
      function prefetch() {
        if (prefetched) return;
        prefetched = true;
        var tag = document.createElement('link');
        tag.rel = 'prefetch';
        tag.href = href;
        document.head.appendChild(tag);
      }
      link.addEventListener('mouseenter', prefetch);
      link.addEventListener('focus', prefetch);

      link.addEventListener('click', function (event) {
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.button !== 0) return;
        event.preventDefault();
        document.body.classList.add('ps-leaving');
        var next = href;
        // Navigate on the next frame: the fade starts immediately, but the
        // request is not held back, so the switch stays as fast as a normal link.
        window.requestAnimationFrame(function () {
          window.requestAnimationFrame(function () { window.location.href = next; });
        });
      });
    });
  }

  // ------------------------------------------------------------- notifications --
  // Staff opt in per device. The button is only revealed when the deployment has
  // VAPID keys and the browser supports push, so nobody sees a control that
  // cannot work.
  function initNotifications() {
    var button = document.getElementById('crmNotifyToggle');
    var label = document.getElementById('crmNotifyLabel');
    var note = document.getElementById('crmNotifyNote');
    if (!button || !window.edsPush) return;

    function say(text) {
      if (!note) {
        if (text) window.alert(text);
        return;
      }
      note.textContent = text || '';
      note.hidden = !text;
    }

    function paint(subscribed) {
      button.hidden = false;
      button.setAttribute('aria-pressed', subscribed ? 'true' : 'false');
      button.classList.toggle('is-on', subscribed);
      label.textContent = subscribed ? 'Notifications on' : 'Notify me';
      var icon = button.querySelector('i');
      if (icon) icon.className = subscribed ? 'bi bi-bell-fill' : 'bi bi-bell';
    }

    window.edsPush.config().then(function (config) {
      if (!config || !config.enabled || !window.edsPush.supported()) return;
      window.edsPush.status().then(paint);
    });

    button.addEventListener('click', function () {
      button.disabled = true;
      say('');
      var wasSubscribed = button.getAttribute('aria-pressed') === 'true';
      var action = wasSubscribed ? window.edsPush.unsubscribe() : window.edsPush.subscribe();
      action.then(function (result) {
        button.disabled = false;
        paint(Boolean(result && result.subscribed));
        say(result && result.message ? result.message : '');
      }).catch(function () {
        button.disabled = false;
        say('Notifications could not be changed on this device.');
      });
    });
  }

  function boot() {
    initUserMenu();
    initCrm();
    initNotifications();
    initNavigation();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
