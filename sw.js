/* NEXUS PRO — Service Worker */
const CACHE = 'nexus-pro-v2';

/* App-shell: gecached bij installatie voor offline fallback */
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

/* Herbruikbare network-first helper: haal van netwerk, sla op in cache,
   val terug op cache als offline. */
function networkFirst(request) {
  return fetch(request)
    .then(res => {
      if (res.ok) {
        const clone = res.clone();
        caches.open(CACHE).then(c => c.put(request, clone));
      }
      return res;
    })
    .catch(() => caches.match(request));
}

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  /* pro.html: network-first zodat dashboard-updates altijd doorkomen.
     Cache dient als offline fallback. */
  if (url.pathname.endsWith('pro.html') || url.pathname.endsWith('/nexus-market-terminal/')) {
    e.respondWith(networkFirst(e.request));
    return;
  }

  /* JSON data (memory.json / data.json): network-first — altijd verse koersen */
  if (url.pathname.endsWith('.json')) {
    e.respondWith(networkFirst(e.request));
    return;
  }

  /* Externe CDN / fonts: network-first, fallback cache */
  if (!url.hostname.includes('danieldk04.github.io') && url.hostname !== location.hostname) {
    e.respondWith(networkFirst(e.request));
    return;
  }

  /* Statische assets (icons, sw.js): cache-first — veranderen zelden */
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
