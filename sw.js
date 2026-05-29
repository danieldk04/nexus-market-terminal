/* NEXUS PRO — Service Worker */
const CACHE = 'nexus-pro-v1';

/* App-shell: bestanden die gecached worden bij installatie */
const SHELL = [
  './pro.html',
  './icons/icon-192.png',
  './icons/icon-512.png',
];

/* Installatie: cache de app-shell */
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting())
  );
});

/* Activatie: verwijder oude caches */
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

/* Fetch strategie:
   - JSON data (memory.json / data.json): network-first zodat data altijd actueel is,
     fallback op cache als offline
   - Externe resources (CDN, fonts): network-first, fallback cache
   - App-shell (pro.html, icons): cache-first voor snelle laadtijd
*/
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  /* Externe CDN / fonts — network-first, cache als fallback */
  if (!url.hostname.includes('danieldk04.github.io') && url.hostname !== location.hostname) {
    e.respondWith(
      fetch(e.request)
        .then(res => {
          if (res.ok) {
            const clone = res.clone();
            caches.open(CACHE).then(c => c.put(e.request, clone));
          }
          return res;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  /* Data-bestanden (JSON): network-first — altijd verse data, cache als offline */
  if (url.pathname.endsWith('.json')) {
    e.respondWith(
      fetch(e.request)
        .then(res => {
          if (res.ok) {
            const clone = res.clone();
            caches.open(CACHE).then(c => c.put(e.request, clone));
          }
          return res;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  /* App-shell: cache-first voor instant laadtijd */
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      return fetch(e.request).then(res => {
        if (res.ok) {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }
        return res;
      });
    })
  );
});
