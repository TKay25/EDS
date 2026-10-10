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
        return '' +
          '<div class="ps-chat-item' + (String(chat.id) === String(activeId) ? ' is-active' : '') +
          '" data-chat-id="' + chat.id + '">' +
          '<div class="ps-chat-avatar">' + initials + '</div>' +
          '<div class="ps-chat-meta">' +
          '<div class="ps-chat-name">' + escapeHtml(chat.display_name || ('+' + chat.wa_number)) +
          (chat.is_staff ? ' <span class="ps-tag">Staff</span>' : '') +
          (chat.unread_count ? ' <span class="ps-badge">' + chat.unread_count + '</span>' : '') + '</div>' +
          '<div class="ps-chat-preview">' + preview + '</div>' +
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

    function bubbleClass(message) {
      if (message.sender === 'system') return 'ps-bubble ps-bubble-system';
      if (message.direction === 'inbound') return 'ps-bubble ps-bubble-in';
      if (message.sender === 'agent') return 'ps-bubble ps-bubble-agent';
      return 'ps-bubble ps-bubble-out';
    }

    function senderLabel(message) {
      if (message.direction === 'inbound') return 'Customer';
      if (message.sender === 'agent') return 'Agent';
      if (message.sender === 'system') return 'System';
      return 'Assistant';
    }

    function renderActions() {
      var buttons = [];
      if (activeStatus === 'human') {
        buttons.push('<button class="ps-btn ps-btn-ghost ps-btn-sm" data-mode="bot"><i class="bi bi-robot"></i> Return to assistant</button>');
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
      if (composerNoteEl) {
        composerNoteEl.textContent = activeStatus === 'human'
          ? 'You are replying as an agent. The auto-assistant is paused for this chat.'
          : (activeStatus === 'closed'
              ? 'This conversation is closed. Sending a reply reopens it for the customer.'
              : 'The assistant answers automatically. Switch to human to reply yourself.');
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
          threadEl.style.display = 'block';
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
                return '<div class="' + bubbleClass(message) + '">' +
                  '<small class="crm-bubble-sender">' + escapeHtml(senderLabel(message)) + '</small>' +
                  escapeHtml(message.body) +
                  '<small>' + escapeHtml(message.at) + '</small></div>';
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
        fetch('/api/crm/chats/' + activeId + '/reply', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message: value })
        })
          .then(function (response) { return response.json(); })
          .then(function (payload) {
            if (payload.message) window.alert(payload.message);
            loadMessages(true);
            refresh(true);
          })
          .catch(function () { window.alert('Could not send the reply. Please try again.'); });
      });
    }

    Array.prototype.forEach.call(document.querySelectorAll('[data-crm-filter]'), function (button) {
      button.addEventListener('click', function () {
        Array.prototype.forEach.call(document.querySelectorAll('[data-crm-filter]'), function (other) {
          other.classList.toggle('is-active', other === button);
        });
        root.setAttribute('data-filter', button.getAttribute('data-crm-filter'));
        refresh(true);
      });
    });

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

  function boot() {
    initUserMenu();
    initCrm();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
