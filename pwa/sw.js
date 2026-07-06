// Minimal service worker - just needs to exist and control the page for the
// browser to consider this installable as a standalone PWA. It intentionally
// does no caching: the whole point is to always load the live game from
// GitHub Pages, not a stale offline copy.
self.addEventListener('install', () => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', () => {
  // Deliberately not intercepting - let everything go straight to the network.
});
