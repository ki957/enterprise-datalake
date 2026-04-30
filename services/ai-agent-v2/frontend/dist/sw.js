// DataLake AI — Service Worker (PWA shell caching)
// Strategy: cache-first for static assets, network-only for /api/*

const CACHE  = 'datalake-ai-v1'
const STATIC = [
  '/',
  '/manifest.json',
  '/favicon.svg',
]

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(STATIC)).then(() => self.skipWaiting())
  )
})

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  )
})

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url)

  // Never cache API calls — always go to network
  if (url.pathname.startsWith('/api/')) {
    e.respondWith(fetch(e.request))
    return
  }

  // For navigations (SPA routes) return the cached index.html shell
  if (e.request.mode === 'navigate') {
    e.respondWith(
      caches.match('/').then(r => r || fetch(e.request))
    )
    return
  }

  // Static assets: cache-first
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached
      return fetch(e.request).then(response => {
        if (!response || response.status !== 200 || response.type === 'opaque') {
          return response
        }
        const clone = response.clone()
        caches.open(CACHE).then(c => c.put(e.request, clone))
        return response
      }).catch(() => caches.match('/'))
    })
  )
})
