/* Service worker: la app abre aunque no haya internet (los datos se guardan en el celular
   y se sincronizan cuando vuelve la señal). Solo guarda en caché los archivos de la app. */
const CACHE = "ptar-movil-v1";
const ARCHIVOS = ["./", "index.html", "manifest.webmanifest", "icon-192.png", "icon-512.png"];

self.addEventListener("install", e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ARCHIVOS)).then(() => self.skipWaiting()));
});

self.addEventListener("activate", e => {
  e.waitUntil(
    caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", e => {
  const req = e.request;
  const url = new URL(req.url);
  if (req.method !== "GET" || url.origin !== location.origin || url.pathname.includes("/api/")) return;
  // Primero la red (para recibir actualizaciones); si falla, lo guardado.
  e.respondWith(
    fetch(req).then(resp => {
      const copia = resp.clone();
      caches.open(CACHE).then(c => c.put(req, copia));
      return resp;
    }).catch(() => caches.match(req).then(r => r || caches.match("index.html")))
  );
});
