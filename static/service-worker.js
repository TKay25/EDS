/* ===== EDS LMS Service Worker ===== */
const CACHE_NAME = 'eds-lms-cache-v2';

// A staff device that opts in receives a push the moment a customer asks for a
// person, even when the CRM tab is closed or the app is not in the foreground.
self.addEventListener('push', event => {
  let payload = {};
  try {
    payload = event.data ? event.data.json() : {};
  } catch (error) {
    payload = { title: 'Brilliant Chemicals', body: event.data ? event.data.text() : '' };
  }
  const title = payload.title || 'Brilliant Chemicals CRM';
  const options = {
    body: payload.body || 'A customer needs attention in the CRM inbox.',
    icon: '/static/images/eds blue.png',
    badge: '/static/images/eds blue.png',
    tag: payload.tag || 'bc-crm',
    renotify: true,
    requireInteraction: true,
    data: { url: payload.url || '/portal/crm' }
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

// Tapping the notification focuses an open CRM tab when there is one, and opens
// a new one otherwise, so the agent lands on the conversation they were alerted about.
self.addEventListener('notificationclick', event => {
  event.notification.close();
  const target = (event.notification.data && event.notification.data.url) || '/portal/crm';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(windowClients => {
      for (const client of windowClients) {
        if (client.url.indexOf(target) !== -1 && 'focus' in client) {
          return client.focus();
        }
      }
      if (clients.openWindow) return clients.openWindow(target);
      return undefined;
    })
  );
});

// Files to cache on install
const PRECACHE_URLS = [
  '/',
  '/dashboard',
  '/static/css/eds-app.css',
  '/static/manifest.json'
];

// Install event - precache core assets
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      return cache.addAll(PRECACHE_URLS);
    })
  );
  self.skipWaiting();
});

// Activate event - clean old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.filter(name => name !== CACHE_NAME)
          .map(name => caches.delete(name))
      );
    })
  );
  self.clients.claim();
});

// Fetch event - network first, fallback to cache
self.addEventListener('fetch', event => {
  // Only handle GET requests
  if (event.request.method !== 'GET') return;

  event.respondWith(
    fetch(event.request)
      .then(response => {
        // Cache successful responses
        if (response.status === 200) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => {
            cache.put(event.request, clone);
          });
        }
        return response;
      })
      .catch(() => {
        // Offline - serve from cache
        return caches.match(event.request);
      })
  );
});
