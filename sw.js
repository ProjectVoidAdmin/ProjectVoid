// Minimal service worker - present mainly to satisfy PWA installability checks
// and give the app a basic offline fallback. Deliberately does not cache
// aggressively so game updates always reach players on next load.
const CACHE_NAME = "project-void-shell-v1";

self.addEventListener("install", (event) => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
  // Network-first: always try the network so players get fresh content,
  // only falling back to a cached copy if they're offline.
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        const copy = response.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
