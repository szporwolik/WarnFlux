// WarnFlux dashboard poller — no external dependencies.
// Refreshes the dashboard sections every 5 seconds via fetch.
// On session expiry (401) the browser is sent back to the login page.
  // Client-side UI strings, keyed like internal/i18n. The server stamps
  // <html lang> on every page, so table = I18N[lang] works everywhere.
  var I18N = {
    en: {
      "map.layer.hazards": "Hazards",
      "map.layer.stations": "Stations",
      "map.layer.weather": "Weather",
      "map.layer.radar": "Radar",
      "map.layer.airquality": "Air quality",
      "map.layer.aircraft": "Aircraft",
      "map.aircraft": "Aircraft",
      "map.aircraft.tip": "Aircraft: %s",
      "map.airquality": "Air quality",
      "map.airquality.tip": "Air quality: %s",
      "map.alt": "Alt",
      "map.speed": "Speed",
      "map.climb": "Climb",
      "map.category": "Category",
      "map.seen": "Seen",
      "map.sent": "Sent",
      "map.heard": "Heard",
      "map.hum": "hum",
      "map.wind": "wind",
      "map.gusts": "gusts",
      "map.pressure": "hPa",
      "map.updated": "updated",
      "home.weather.none": "No weather reports yet — APRS weather stations and forecast providers publish them over MQTT.",
      "home.weather.forecast": "Forecast — next days",
      "home.stations.none": "No stations heard yet — ham stations beacon through APRS.",
      "home.aircraft.none": "No aircraft in range right now.",
      "map.km": "km",
      "map.center": "Center the view",
      "map.our_station": "Our station",
      "map.you_are_here": "You are here",
      "map.show_location": "Show my location",
      "map.center_location": "Center on my location",
      "map.show_on_map": "Show %s on the map",
      "map.toggle_layer": "Toggle %s layer",
      "map.from": "From:",
      "map.to": "To:",
      "map.via.radio": "Via: radio (APRS)",
      "map.via.internet": "Via: internet (APRS-IS)",
      "warnings.source": "Source:",
      "notif.empty": "No notifications processed yet",
      "traffic.subscribing": "Subscribing to %s… (%s s)",
      "users.edit_title": "Edit user",
      "users.add_title": "Add user"
    },
    pl: {
      "map.layer.hazards": "Zagrożenia",
      "map.layer.stations": "Stacje",
      "map.layer.weather": "Pogoda",
      "map.layer.radar": "Radar",
      "map.layer.airquality": "Jakość powietrza",
      "map.layer.aircraft": "Samoloty",
      "map.aircraft": "Samolot",
      "map.aircraft.tip": "Samolot: %s",
      "map.airquality": "Jakość powietrza",
      "map.airquality.tip": "Jakość powietrza: %s",
      "map.alt": "Wys.",
      "map.speed": "Prędkość",
      "map.climb": "Wznoszenie",
      "map.category": "Kategoria",
      "map.seen": "Widziany",
      "map.sent": "Wysłano",
      "map.heard": "Słyszano",
      "map.hum": "wilg.",
      "map.wind": "wiatr",
      "map.gusts": "porywy",
      "map.pressure": "hPa",
      "map.updated": "aktualizacja",
      "home.weather.none": "Brak jeszcze raportów pogodowych — publikują je stacje pogodowe APRS i dostawcy prognoz przez MQTT.",
      "home.weather.forecast": "Prognoza — kolejne dni",
      "home.stations.none": "Nie słychać jeszcze żadnych stacji — krótkofalowcy nadają przez APRS.",
      "home.aircraft.none": "W tej chwili brak samolotów w zasięgu.",
      "map.km": "km",
      "map.center": "Wyśrodkuj widok",
      "map.our_station": "Nasza stacja",
      "map.you_are_here": "Jesteś tutaj",
      "map.show_location": "Pokaż moją lokalizację",
      "map.center_location": "Wyśrodkuj na mojej lokalizacji",
      "map.show_on_map": "Pokaż %s na mapie",
      "map.toggle_layer": "Przełącz warstwę %s",
      "map.from": "Od:",
      "map.to": "Do:",
      "map.via.radio": "Przez: radio (APRS)",
      "map.via.internet": "Przez: internet (APRS-IS)",
      "warnings.source": "Źródło:",
      "notif.empty": "Nie przetworzono jeszcze powiadomień",
      "traffic.subscribing": "Subskrybowanie %s… (%s s)",
      "users.edit_title": "Edytuj użytkownika",
      "users.add_title": "Dodaj użytkownika"
    }
  };

  // tr resolves a client-side string in the page language; trf formats it.
  function tr(key) {
    var lang = document.documentElement.getAttribute("lang") || "en";
    var table = I18N[lang] || I18N.en;
    if (table[key] !== undefined) { return table[key]; }
    if (I18N.en[key] !== undefined) { return I18N.en[key]; }
    return key;
  }
  function trf(key) {
    var s = tr(key);
    for (var i = 1; i < arguments.length; i++) {
      s = s.replace(/%[sd]/, String(arguments[i] === undefined ? "" : arguments[i]));
    }
    return s;
  }


(function () {
  "use strict";

  var SECTIONS = [
    { path: "/partials/status", id: "status-section" },
    { path: "/partials/mqtt", id: "mqtt-section" },
    { path: "/partials/plugins", id: "plugins-section" },
    { path: "/partials/actions", id: "actions-section" },
    { path: "/partials/health", id: "health-section" },
    // Public home page: the active-hazard list fragment.
    { path: "/partials/home", id: "home-alerts" }
  ];

  var POLL_MS = 5000;

  function refresh(section) {
    if (!document.getElementById(section.id)) {
      return; // section not on this page (e.g. /partials/home off-site)
    }
    fetch(section.path, {
      headers: { "Accept": "text/html" },
      credentials: "same-origin",
      cache: "no-store"
    })
      .then(function (res) {
        if (res.status === 401) {
          window.location.href = "/login";
          return null;
        }
        if (!res.ok) {
          return null;
        }
        return res.text();
      })
      .then(function (html) {
        if (html === null) {
          return;
        }
        var node = document.getElementById(section.id);
        if (node) {
          // Preserve open/closed state of <details> elements (the home
          // page's collapsed minor section) across the refresh.
          var openStates = [];
          node.querySelectorAll("details").forEach(function (d) {
            openStates.push(d.open);
          });
          node.outerHTML = html;
          var next = document.getElementById(section.id);
          if (next) {
            var details = next.querySelectorAll("details");
            details.forEach(function (d, i) {
              if (openStates[i]) { d.open = true; }
            });
          }
        }
      })
      .catch(function () {
        // Network hiccup: keep the last rendered content and try again.
      });
  }

  setInterval(function () {
    SECTIONS.forEach(refresh);
  }, POLL_MS);
})();

// Logs page: incremental tail with level coloring. Fetches only the lines
// after the last known cursor and auto-scrolls when the viewer is already
// at the bottom (so reading old lines is never interrupted).
(function () {
  "use strict";

  var LOGS_POLL_MS = 2000;
  var MAX_LOG_NODES = 1000;

  function initLogs() {
    var viewer = document.getElementById("log-viewer");
    if (!viewer) {
      return;
    }
    var after = parseInt(viewer.getAttribute("data-after"), 10) || 0;

    function atBottom() {
      return viewer.scrollHeight - viewer.scrollTop - viewer.clientHeight < 40;
    }

    function poll() {
      fetch("/partials/logs?after=" + after, {
        headers: { "Accept": "application/json" },
        credentials: "same-origin",
        cache: "no-store"
      })
        .then(function (res) {
          if (res.status === 401) {
            window.location.href = "/login";
            return null;
          }
          if (!res.ok) {
            return null;
          }
          return res.json();
        })
        .then(function (data) {
          if (!data || !data.lines || data.lines.length === 0) {
            return;
          }
          var stick = atBottom();
          var frag = document.createDocumentFragment();
          data.lines.forEach(function (line) {
            var div = document.createElement("div");
            div.className = "log-line log-" + (line.level || "plain");
            div.textContent = line.text;
            frag.appendChild(div);
            after = line.seq;
          });
          viewer.appendChild(frag);
          viewer.setAttribute("data-after", after);
          while (viewer.children.length > MAX_LOG_NODES) {
            viewer.removeChild(viewer.firstChild);
          }
          if (stick) {
            viewer.scrollTop = viewer.scrollHeight;
          }
        })
        .catch(function () {
          // Network hiccup: keep the last rendered lines and try again.
        });
    }

    poll();
    setInterval(poll, LOGS_POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initLogs);
  } else {
    initLogs();
  }
})();

// MQTT traffic page: same incremental tail as the logs viewer, one row per
// inbound frame. Rows are colored by topic kind (events/active/info/status).
(function () {
  "use strict";

  var TRAFFIC_POLL_MS = 2000;
  var MAX_TRAFFIC_NODES = 200;

  function pad2(n) {
    return n < 10 ? "0" + n : "" + n;
  }

  function lineFor(entry) {
    var t = new Date(entry.at);
    var when = isNaN(t.getTime())
      ? entry.at
      : pad2(t.getHours()) + ":" + pad2(t.getMinutes()) + ":" + pad2(t.getSeconds());
    var cls = "tr-" + entry.kind;
    var text = when + "  [" + entry.kind + "] " + entry.receiver + "  " + entry.topic +
      "  qos=" + entry.qos + (entry.retained ? "  retained" : "") +
      "  " + entry.size + "B";
    return { cls: cls, text: text };
  }

  function initTraffic() {
    var viewer = document.getElementById("traffic-viewer");
    if (!viewer) {
      return;
    }
    var after = parseInt(viewer.getAttribute("data-after"), 10) || 0;

    function atBottom() {
      return viewer.scrollHeight - viewer.scrollTop - viewer.clientHeight < 40;
    }

    function poll() {
      fetch("/partials/traffic?after=" + after, {
        headers: { "Accept": "application/json" },
        credentials: "same-origin",
        cache: "no-store"
      })
        .then(function (res) {
          if (res.status === 401) {
            window.location.href = "/login";
            return null;
          }
          if (!res.ok) {
            return null;
          }
          return res.json();
        })
        .then(function (data) {
          if (!data || !data.entries || data.entries.length === 0) {
            return;
          }
          var stick = atBottom();
          var frag = document.createDocumentFragment();
          data.entries.forEach(function (entry) {
            var line = lineFor(entry);
            var div = document.createElement("div");
            div.className = "log-line " + line.cls;
            div.textContent = line.text;
            frag.appendChild(div);
            after = entry.seq;
          });
          viewer.appendChild(frag);
          viewer.setAttribute("data-after", after);
          while (viewer.children.length > MAX_TRAFFIC_NODES) {
            viewer.removeChild(viewer.firstChild);
          }
          if (stick) {
            viewer.scrollTop = viewer.scrollHeight;
          }
        })
        .catch(function () {
          // Network hiccup: keep the last rendered rows and try again.
        });
    }

    poll();
    setInterval(poll, TRAFFIC_POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initTraffic);
  } else {
    initTraffic();
  }
})();

// Notifications page: full snapshot poll. The list is re-rendered only
// when the data changed, so scrolling and text selection survive polls.
(function () {
  "use strict";

  var NOTIF_POLL_MS = 2500;

  function stepLine(step) {
    var li = document.createElement("li");
    li.className = "nt-step nt-" + step.kind;
    var at = document.createElement("span");
    at.className = "nt-at";
    at.textContent = hhmmss(step.at);
    var tx = document.createElement("span");
    tx.className = "nt-text";
    tx.textContent = step.text;
    li.appendChild(at);
    li.appendChild(tx);
    return li;
  }

  function hhmmss(rfc3339) {
    var t = new Date(rfc3339);
    if (isNaN(t.getTime())) {
      return rfc3339;
    }
    function pad2(n) { return n < 10 ? "0" + n : "" + n; }
    return pad2(t.getHours()) + ":" + pad2(t.getMinutes()) + ":" + pad2(t.getSeconds());
  }

  function itemFor(trail) {
    var art = document.createElement("article");
    art.className = "notif";
    art.id = "notif-" + trail.key;
    art.dataset.key = trail.key;

    var head = document.createElement("div");
    head.className = "notif-head";

    var sev = document.createElement("span");
    sev.className = "sev sev-" + (trail.severity || "unknown").toLowerCase();
    sev.textContent = trail.severity || "unknown";
    head.appendChild(sev);

    var src = document.createElement("strong");
    src.textContent = (trail.source || "?") + " alert";
    head.appendChild(src);

    if (trail.headline) {
      var title = document.createElement("span");
      title.className = "nt-title";
      title.textContent = trail.headline;
      head.appendChild(title);
    }

    var when = document.createElement("span");
    when.className = "muted nt-time";
    when.textContent = fullTime(trail.received_at);
    head.appendChild(when);

    var badge = document.createElement("span");
    badge.className = "badge nt-outcome nt-outcome-" + (trail.outcome || "skipped");
    badge.textContent = trail.outcome || "skipped";
    head.appendChild(badge);

    if (trail.outcome === "failed") {
      art.classList.add("notif-failed");
    }
    art.appendChild(head);

    var steps = document.createElement("ol");
    steps.className = "nt-steps";
    (trail.steps || []).forEach(function (s) {
      steps.appendChild(stepLine(s));
    });
    art.appendChild(steps);
    return art;
  }

  function fullTime(rfc3339) {
    var t = new Date(rfc3339);
    if (isNaN(t.getTime())) {
      return rfc3339;
    }
    function pad2(n) { return n < 10 ? "0" + n : "" + n; }
    return t.getFullYear() + "-" + pad2(t.getMonth() + 1) + "-" + pad2(t.getDate()) +
      " " + pad2(t.getHours()) + ":" + pad2(t.getMinutes()) + ":" + pad2(t.getSeconds());
  }

  function focusKey() {
    var m = /[?&]key=([^&]+)/.exec(window.location.search);
    return m ? decodeURIComponent(m[1]) : "";
  }

  function initNotifications() {
    var list = document.getElementById("notif-list");
    if (!list) {
      return;
    }
    var lastJSON = "";

    function render(trails) {
      var frag = document.createDocumentFragment();
      var fk = focusKey();
      (trails || []).forEach(function (t) {
        var item = itemFor(t);
        if (fk && t.key === fk) {
          item.classList.add("notif-focus");
        }
        frag.appendChild(item);
      });
      list.replaceChildren(frag);
      if (!trails || trails.length === 0) {
        var p = document.createElement("p");
        p.className = "empty";
        p.textContent = tr("notif.empty");
        list.appendChild(p);
      }
    }

    function poll() {
      fetch("/partials/notifications", {
        headers: { "Accept": "application/json" },
        credentials: "same-origin",
        cache: "no-store"
      })
        .then(function (res) {
          if (res.status === 401) {
            window.location.href = "/login";
            return null;
          }
          if (!res.ok) {
            return null;
          }
          return res.json();
        })
        .then(function (data) {
          if (!data || !data.trails) {
            return;
          }
          var cur = JSON.stringify(data.trails);
          if (cur === lastJSON) {
            return;
          }
          lastJSON = cur;
          render(data.trails);
        })
        .catch(function () {
          // Network hiccup: keep the last rendered list and try again.
        });
    }

    poll();
    setInterval(poll, NOTIF_POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initNotifications);
  } else {
    initNotifications();
  }
})();

// Public home page: tab switching between the alert list and the
// placeholder section. The alert fragment itself is polled above.
(function () {
  "use strict";

  function initHomeTabs() {
    var tabs = document.querySelectorAll(".home-tab");
    if (tabs.length === 0) {
      return;
    }

    // Archive tab: the fragment (list + pagination) is fetched lazily and
    // replaced in place by the pagination links.
    var archiveBox = document.getElementById("archive-box");
    var archiveLoaded = false;

    function loadingNote(text) {
      var p = document.createElement("p");
      p.className = "muted";
      p.textContent = text;
      return p;
    }

    function loadArchive(url) {
      if (!archiveBox) {
        return;
      }
      archiveBox.textContent = "";
      archiveBox.appendChild(loadingNote("Loading archive…"));
      fetch(url || "/archive", { headers: { "Accept": "text/html" }, cache: "no-store" })
        .then(function (resp) { return resp.ok ? resp.text() : null; })
        .then(function (html) {
          if (!archiveBox || !html) {
            return;
          }
          archiveBox.innerHTML = html;
          archiveLoaded = true;
        })
        .catch(function () {
          if (archiveBox) {
            archiveBox.textContent = "";
            archiveBox.appendChild(loadingNote("Archive failed to load."));
          }
        });
    }

    tabs.forEach(function (tab) {
      tab.addEventListener("click", function () {
        tabs.forEach(function (t) {
          var active = t === tab;
          t.classList.toggle("active", active);
          t.setAttribute("aria-selected", active ? "true" : "false");
          var panel = document.getElementById("panel-" + t.dataset.tab);
          if (panel) {
            panel.hidden = !active;
          }
        });
        if (tab.dataset.tab === "tab-archive" && !archiveLoaded) {
          loadArchive();
        }
      });
    });

    if (archiveBox) {
      archiveBox.addEventListener("click", function (ev) {
        var link = ev.target.closest ? ev.target.closest("a[href^='/archive?']") : null;
        if (!link) {
          return;
        }
        ev.preventDefault();
        loadArchive(link.getAttribute("href"));
        window.scrollTo({ top: 0, behavior: "smooth" });
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initHomeTabs);
  } else {
    initHomeTabs();
  }
})();

// Public home page: the combined neighbourhood map (Map tab) — Leaflet
// centered on our locator with the collection-radius circle, a RainViewer
// radar overlay, APRS stations held in the retained MQTT state (polled
// every 30 seconds) and the weather layer from /api/weather (polled every
// 5 minutes). Stations, weather, hazards and radar are separate layers
// with independent toggles; every marker carries its detail popup.
// Leaflet and the radar tiles load from CDNs only when the APRS hub is
// enabled (the map element only exists then).
(function () {
  "use strict";

  var el = document.getElementById("aprs-map");
  if (!el) {
    return;
  }

  var STATION_POLL_MS = 30 * 1000;
  var RADAR_REFRESH_MS = 10 * 60 * 1000;

  var lat = parseFloat(el.getAttribute("data-lat"));
  var lon = parseFloat(el.getAttribute("data-lon"));
  var ownLatAttr = el.getAttribute("data-own-lat");
  var ownLonAttr = el.getAttribute("data-own-lon");
  // Where OUR station actually sits (learned from our own position
  // beacon, e.g. the Direwolf PBEACON); falls back to the hub center.
  var ownLat = ownLatAttr ? parseFloat(ownLatAttr) : lat;
  var ownLon = ownLonAttr ? parseFloat(ownLonAttr) : lon;
  var radiusKm = parseFloat(el.getAttribute("data-radius") || "0");
  var ownCall = el.getAttribute("data-callsign") || "";

  var map = null;
  var stationLayer = null;
  var hazardLayer = null;
  var weatherLayer = null;
  var aircraftLayer = null;
  var aqLayer = null;
  var rangeCircle = null;
  var radarLayer = null;
  var baseLayer = null;
  var stationBounds = null;
  var fittedOnce = false;
  // Latest fetches kept for the combined view fit.
  var lastStations = [];
  var lastHazards = [];
  // Markers indexed by callsign (uppercased), so the report cards can
  // focus the map on a station and open its popup.
  var stationMarkers = {};
  var weatherMarkers = {};

  // Theme-aware base map, the same free provider the CQOps dashboard
  // uses: OpenFreeMap vector styles via MapLibre GL — no API keys, no
  // usage limits. Fiord for the dark theme, bright for the light one.
  // When WebGL or the GL glue is unavailable (headless browsers, offline
  // fallback), keyless RASTER tiles take over: OpenStreetMap for the
  // light theme, Esri World Dark Gray for the dark theme.
  function tilesForTheme() {
    var theme = document.documentElement.getAttribute("data-theme");
    // One tile server (OpenFreeMap); OpenMapTiles/OpenStreetMap are data
    // attributions, not additional tile sources.
    var glLabel = '<a href="https://openfreemap.org" target="_blank" rel="noopener noreferrer">OpenFreeMap</a> (data: <a href="https://www.openmaptiles.org/" target="_blank" rel="noopener noreferrer">OpenMapTiles</a> · <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a>)';
    if (theme === "light") {
      return {
        style: "https://tiles.openfreemap.org/styles/bright",
        raster: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        glLabel: glLabel,
        rasterLabel: '<a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a>',
        marker: { color: "#0d47a1", fillColor: "#1976d2" }
      };
    }
    return {
      style: "https://tiles.openfreemap.org/styles/fiord",
      raster: "https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",
      glLabel: glLabel,
      rasterLabel: '<a href="https://www.esri.com/" target="_blank" rel="noopener noreferrer">Esri</a> World Dark Gray',
      marker: { color: "#1565c0", fillColor: "#64b5f6" }
    };
  }

  // webglAvailable probes WebGL synchronously: MapLibre GL throws
  // asynchronously when the context cannot be created, so the availability
  // check must happen BEFORE the layer is constructed.
  function webglAvailable() {
    try {
      var c = document.createElement("canvas");
      return !!(window.WebGLRenderingContext && (c.getContext("webgl2") || c.getContext("webgl")));
    } catch (e) {
      return false;
    }
  }

  // buildBaseLayer renders the base map on its own pane (below radar and
  // markers). It returns the layer plus the attribution mode that was
  // actually used: "gl" (OpenFreeMap vectors) or "raster".
  function buildBaseLayer() {
    var t = tilesForTheme();
    var mode = "raster";
    var layer = null;
    if (typeof L.maplibreGL === "function" && webglAvailable()) {
      try {
        var gl = L.maplibreGL({ style: t.style, attributionControl: false, pane: "aprsBase" });
        var m = gl.getMaplibreMap && gl.getMaplibreMap();
        if (m && m.on) {
          m.on("styleimagemissing", function (e) {
            m.addImage(e.id, { width: 1, height: 1, data: new Uint8ClampedArray([0, 0, 0, 0]) });
          });
        }
        gl.addTo(map);
        layer = gl;
        mode = "gl";
      } catch (e) {
        layer = null; // use the raster fallback below
        if (el) {
          el.querySelectorAll("canvas").forEach(function (c) { c.remove(); });
        }
      }
    }
    if (!layer) {
      layer = L.tileLayer(t.raster, { maxZoom: 18, pane: "aprsBase" }).addTo(map);
      mode = "raster";
    }
    return { layer: layer, mode: mode };
  }

  // syncBaseLayer swaps the base map and the attribution line to match
  // the current theme.
  function syncBaseLayer() {
    if (!map) {
      return;
    }
    var t = tilesForTheme();
    if (baseLayer) {
      map.removeLayer(baseLayer);
    }
    var built = buildBaseLayer();
    baseLayer = built.layer;
    var attrib = document.getElementById("aprs-tiles-attrib");
    if (attrib) {
      attrib.innerHTML = built.mode === "gl" ? t.glLabel : t.rasterLabel;
    }
    if (stationLayer) {
      stationLayer.eachLayer(function (m) {
        if (m._aprsMarker && m.setStyle) {
          m.setStyle({ color: t.marker.color, fillColor: t.marker.fillColor });
        }
      });
    }
    if (rangeCircle) {
      rangeCircle.setStyle({ color: aprsOverlayColors().range });
    }
    // Track and vector colors are read at render time; a refresh picks
    // up the new theme immediately.
    refreshStations();
  }

  function esc(s) {
    var d = document.createElement("div");
    d.textContent = s == null ? "" : String(s);
    return d.innerHTML;
  }

  // Re-center control: a small target button under the zoom buttons that
  // returns the view to the initial center and zoom.
  function addCenterControl(map, recenter) {
    var c = L.control({ position: "topleft" });
    c.onAdd = function () {
      var btn = L.DomUtil.create("button", "wf-center-btn");
      btn.type = "button";
      btn.title = tr("map.center");
      btn.setAttribute("aria-label", tr("map.center"));
      btn.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><circle cx="12" cy="12" r="7" fill="none" stroke="currentColor" stroke-width="2"/><path d="M12 2v4M12 18v4M2 12h4M18 12h4" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>';
      L.DomEvent.disableClickPropagation(btn);
      L.DomEvent.on(btn, "click", recenter);
      return btn;
    };
    c.addTo(map);
  }

  // fmtTime renders an RFC 3339 timestamp in local browser time
  // (minutes). Unparseable values fall back to the raw text.
  function fmtTime(v) {
    if (!v) {
      return "";
    }
    var t = new Date(v);
    if (isNaN(t.getTime())) {
      var raw = String(v).replace("T", " ");
      return raw.length > 16 ? raw.slice(0, 16) : raw;
    }
    function pad2(n) { return n < 10 ? "0" + n : "" + n; }
    return t.getFullYear() + "-" + pad2(t.getMonth() + 1) + "-" + pad2(t.getDate()) +
      " " + pad2(t.getHours()) + ":" + pad2(t.getMinutes());
  }

  function loadScript(src, ok, fail) {
    var s = document.createElement("script");
    s.src = src;
    s.onload = ok;
    s.onerror = fail;
    document.body.appendChild(s);
  }

  function mapLoadError() {
    el.innerHTML = "<p class=\"muted\">Map library failed to load (offline?).</p>";
  }

  // Leaflet → MapLibre GL → the MapLibre Leaflet glue. Each step falls
  // back gracefully: without the glue the raster OpenStreetMap layer is
  // used instead.
  function loadLibraries(cb) {
    if (window.L) {
      cb();
      return;
    }
    var css = document.createElement("link");
    css.rel = "stylesheet";
    css.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
    document.head.appendChild(css);
    loadScript("https://unpkg.com/leaflet@1.9.4/dist/leaflet.js", cb, mapLoadError);
  }

  function loadMapLibre(cb) {
    var css = document.createElement("link");
    css.rel = "stylesheet";
    css.href = "https://cdn.jsdelivr.net/npm/maplibre-gl@4.7.1/dist/maplibre-gl.css";
    document.head.appendChild(css);
    if (window.maplibregl) {
      cb();
      return;
    }
    loadScript("https://cdn.jsdelivr.net/npm/maplibre-gl@4.7.1/dist/maplibre-gl.js", cb, function () { cb(); });
  }

  function loadMapLibreGlue(cb) {
    if (window.L && typeof L.maplibreGL === "function") {
      cb();
      return;
    }
    loadScript("https://cdn.jsdelivr.net/npm/@maplibre/maplibre-gl-leaflet@0.0.22/leaflet-maplibre-gl.js", cb, function () { cb(); });
  }

  // Fit the view so everything displayed on the map is visible: our
  // locator plus every station. With no station data yet the view falls
  // back to the locator at the original zoom.
  function fitToStations() {
    if (!map) {
      return;
    }
    if (stationBounds && stationBounds.isValid()) {
      map.fitBounds(stationBounds, { padding: [30, 30], maxZoom: 13 });
    } else {
      map.setView([ownLat, ownLon], 11);
    }
    fittedOnce = true;
  }

  function initMap() {
    if (map || !window.L) {
      return;
    }
    map = L.map(el, { attributionControl: false }).setView([ownLat, ownLon], 11);
    map.createPane("aprsBase");
    map.getPane("aprsBase").style.zIndex = 200;
    map.createPane("aprsRadar");
    map.getPane("aprsRadar").style.zIndex = 350;
    map.getPane("aprsRadar").style.pointerEvents = "none";
    baseLayer = null;
    syncBaseLayer();
    stationLayer = L.layerGroup().addTo(map);
    hazardLayer = L.layerGroup().addTo(map);
    weatherLayer = L.layerGroup().addTo(map);
    aircraftLayer = L.layerGroup().addTo(map);
    aqLayer = L.layerGroup().addTo(map);

    // Center button: fit the view around our locator and all stations.
    addCenterControl(map, function () {
      fitToStations();
    });

    // Our station marker + collection-radius circle at the position
    // learned from our own beacon (data-own-lat/lon).
    L.circleMarker([ownLat, ownLon], {
      radius: 7, color: "#fff", weight: 2,
      fillColor: "#007a3d", fillOpacity: 1
    }).addTo(map).bindTooltip(ownCall || tr("map.our_station"), { direction: "top" });
    if (radiusKm > 0) {
      rangeCircle = L.circle([ownLat, ownLon], {
        radius: radiusKm * 1000,
        color: aprsOverlayColors().range, weight: 1.5, opacity: 0.55, dashArray: "8 6",
        fill: false, interactive: false
      }).addTo(map);
    }

    enableRadar();
    addLayersControl(map);
    addLocateControl(map);
    refreshStations();
    refreshHazards();
    refreshWeather();
    refreshAircraft();
    refreshAirQuality();
    window.setInterval(refreshStations, STATION_POLL_MS);
    window.setInterval(refreshWeather, WEATHER_POLL_MS);
    window.setInterval(refreshAircraft, AIRCRAFT_POLL_MS);
    window.setInterval(refreshAirQuality, AQ_POLL_MS);
    window.setInterval(refreshHazards, STATION_POLL_MS);
    window.setInterval(refreshRadar, RADAR_REFRESH_MS);

    // Follow theme switches (the theme toggle rewrites data-theme on
    // <html>): swap tiles + attribution + marker colors in place.
    if (window.MutationObserver) {
      new MutationObserver(syncBaseLayer).observe(document.documentElement, {
        attributes: true, attributeFilter: ["data-theme"]
      });
    }

    // The tab is hidden until activated: fix the size once visible.
    var tab = document.querySelector('.home-tab[data-tab="tab-radio"]');
    if (tab) {
      tab.addEventListener("click", function () {
        window.setTimeout(function () {
          if (!map) {
            return;
          }
          map.invalidateSize();
          if (!fittedOnce) {
            fitToStations();
          }
        }, 60);
      });
    }
    window.addEventListener("resize", function () {
      if (map) {
        map.invalidateSize();
      }
    });
  }

  function enableRadar() {
    try {
      fetch("https://api.rainviewer.com/public/weather-maps.json")
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (meta) {
          if (!map || !meta || !meta.radar || !meta.radar.past || !meta.radar.past.length) {
            return;
          }
          var frame = meta.radar.past[meta.radar.past.length - 1];
          if (!frame || !frame.path) {
            return;
          }
          setRadarUrl("https://tilecache.rainviewer.com" + frame.path + "/256/{z}/{x}/{y}/2/1_1.png");
        })
        .catch(function () { /* radar unavailable — map still works */ });
    } catch (e) { /* ignore */ }
  }

  function setRadarUrl(url) {
    if (!map) {
      return;
    }
    var layer = L.tileLayer(url, {
      pane: "aprsRadar", opacity: 0.55, maxNativeZoom: 7, maxZoom: 12
    });
    if (radarLayer) {
      map.removeLayer(radarLayer);
    }
    radarLayer = layer;
    if (radarOn) {
      radarLayer.addTo(map);
    }
  }

  function refreshRadar() {
    // Re-fetch the frame index; a newer frame swaps the tile URL in place.
    enableRadar();
  }

  // APRS symbol decoding: stations carry a two-character symbol code
  // (symbol_table + symbol). The bundled aprs.fi sprite (Heikki
  // Hannikainen OH7LZB, attribution under the map) holds the primary
  // table (/) and the alternate table (\) as 48px cells in a 16-column
  // grid, row-major by character code 33..126. Cells are scaled to
  // 28px on screen via CSS, so the @2x sprite stays crisp on retina.
  var APRS_SYM_SIZE = 28;
  var APRS_SPRITES = {
    "/": "/static/aprs-symbols/aprs-symbols-24-0@2x.png",
    "\\": "/static/aprs-symbols/aprs-symbols-24-1@2x.png"
  };

  function aprsSymbolMarker(s) {
    var table = s.symbol_table === "\\" ? "\\" : "/";
    var code = s.symbol ? s.symbol.charCodeAt(0) : 0;
    if (code < 33 || code > 126) {
      return null; // unknown symbol — the theme-colored circle fallback
    }
    var idx = code - 33;
    var col = idx % 16;
    var row = Math.floor(idx / 16);
    // The callsign label sits under the icon (cqops-style) so stations
    // are identifiable without hovering or clicking.
    var html = '<span class="aprs-sym-wrap">' +
      '<span class="aprs-sym-img" style="background-image:url(\'' + APRS_SPRITES[table] +
      "\');background-position:-" + (col * APRS_SYM_SIZE) + "px -" + (row * APRS_SYM_SIZE) + 'px"></span>' +
      '<span class="wf-station-label">' + esc(s.callsign) + '</span></span>';
    return L.divIcon({
      className: "aprs-sym",
      iconSize: [96, 44],
      iconAnchor: [48, 14],
      html: html
    });
  }

  // Fallback marker for stations without a known symbol: a theme-colored
  // circle with the same callsign label underneath.
  function aprsFallbackIcon(s) {
    var t = tilesForTheme();
    var svg = '<svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">' +
      '<circle cx="7" cy="7" r="5.5" fill="' + t.marker.fillColor + '" stroke="' + t.marker.color + '" stroke-width="2"/></svg>';
    var html = '<span class="aprs-sym-wrap">' + svg +
      '<span class="wf-station-label">' + esc(s.callsign) + '</span></span>';
    return L.divIcon({
      className: "aprs-fallback",
      iconSize: [96, 40],
      iconAnchor: [48, 6],
      html: html
    });
  }

  // Destination point along a course (degrees, 0 = north) at a given
  // distance in km, using a spherical-earth approximation.
  function destPoint(lat, lon, courseDeg, distKm) {
    var R = 6371;
    var brg = courseDeg * Math.PI / 180;
    var d = distKm / R;
    var la1 = lat * Math.PI / 180;
    var lo1 = lon * Math.PI / 180;
    var la2 = Math.asin(Math.sin(la1) * Math.cos(d) + Math.cos(la1) * Math.sin(d) * Math.cos(brg));
    var lo2 = lo1 + Math.atan2(Math.sin(brg) * Math.sin(d) * Math.cos(la1),
      Math.cos(d) - Math.sin(la1) * Math.sin(la2));
    return [la2 * 180 / Math.PI, lo2 * 180 / Math.PI];
  }

  // Overlay colors per theme: the movement tail, its dots and the
  // heading vector stay distinguishable from each other and from the
  // station markers on both tile styles. The range circle is neutral.
  function aprsOverlayColors() {
    if (document.documentElement.getAttribute("data-theme") !== "light") {
      return {
        track: "#4fc3f7",      // tail line + dots (light blue)
        trackBorder: "#01579b",
        heading: "#ff7043",    // vector + arrowhead (orange)
        range: "#9ca3af"       // collection-radius circle (neutral gray)
      };
    }
    return {
      track: "#0277bd",
      trackBorder: "#ffffff",
      heading: "#d32f2f",
      range: "#6b7280"
    };
  }

  function refreshStations() {
    fetch("/api/aprs/stations")
      .then(function (r) { return r.ok ? r.json() : []; })
      .then(function (stations) {
        if (!stationLayer) {
          return;
        }
        lastStations = stations || [];
        stationLayer.clearLayers();
        stationMarkers = {};
        (lastStations).forEach(function (s) {
          if (!s || !s.position || s.self) {
            return; // our own locator has its dedicated marker
          }
          var popup = "<strong>" + esc(s.callsign) + "</strong>";
          if (s.comment) {
            popup += "<br>" + esc(s.comment);
          }
          // When the frame was transmitted (packet timestamp) or, when
          // the packet carried none, when we last heard the station.
          if (s.last_packet_at) {
            popup += "<br>" + tr("map.sent") + ": " + esc(fmtTime(s.last_packet_at));
          }
          popup += "<br>" + tr("map.heard") + ": " + esc(fmtTime(s.last_heard_at));
          if (s.distance_km) {
            popup += "<br>" + Number(s.distance_km).toFixed(1) + " km";
          }
          // How the frame reached us: over the radio via an i-gate, or
          // injected directly from the internet.
          if (s.origin === "rf") {
            popup += "<br>Via: radio (APRS)";
          } else if (s.origin === "internet") {
            popup += "<br>Via: internet (APRS-IS)";
          }
          popup += stationWeatherBlock(s.callsign);
          var icon = aprsSymbolMarker(s);
          var marker;
          if (icon) {
            marker = L.marker([s.position.latitude, s.position.longitude], { icon: icon, riseOnHover: true });
          } else {
            marker = L.marker([s.position.latitude, s.position.longitude], { icon: aprsFallbackIcon(s), riseOnHover: true });
          }
          // Hover shows the essentials; the click popup keeps the full
          // detail view.
          var hover = "<strong>" + esc(s.callsign) + "</strong>";
          if (s.speed_kmh > 0 || s.course_deg) {
            hover += "<br>" + tr("map.speed") + ": " + Number(s.speed_kmh).toFixed(0) + " km/h";
            if (s.course_deg) {
              hover += " @ " + s.course_deg + "\u00b0";
            }
          }
          if (s.altitude_m != null) {
            hover += "<br>" + tr("map.alt") + ": " + Number(s.altitude_m).toFixed(0) + " m";
          }
          if (s.comment) {
            hover += "<br>" + esc(s.comment);
          }
          if (s.status) {
            hover += '<br><span class="muted">' + esc(s.status) + "</span>";
          }
          hover += "<br>" + tr("map.heard") + ": " + esc(fmtTime(s.last_heard_at));
          if (s.distance_km) {
            hover += "<br>" + Number(s.distance_km).toFixed(1) + " km";
          }
          marker.bindTooltip(hover, { sticky: true, direction: "top" });
          marker.bindPopup(popup);

          // Movement tail: up to three earlier positions (oldest first)
          // plus the current one. The points are marked with dots and
          // connected by one line — aprs.fi's track idea.
          var overlay = aprsOverlayColors();
          var trail = [];
          (s.track || []).forEach(function (tp) {
            if (tp && tp.latitude && tp.longitude) {
              trail.push([tp.latitude, tp.longitude]);
            }
          });
          if (trail.length > 0) {
            trail.push([s.position.latitude, s.position.longitude]);
            trail.forEach(function (pt) {
              stationLayer.addLayer(L.circleMarker(pt, {
                radius: 3, color: overlay.trackBorder, weight: 1,
                fillColor: overlay.track, fillOpacity: 1, interactive: false
              }));
            });
            if (trail.length > 1) {
              stationLayer.addLayer(L.polyline(trail, {
                color: overlay.track, weight: 2.5, opacity: 0.85,
                lineCap: "round", lineJoin: "round", interactive: false
              }));
            }
          }

          // Heading vector: one minute of travel in the reported course
          // (speed_kmh / 60), clamped so slow movers stay readable and
          // fast movers stay on-screen.
          if (s.course_deg && s.speed_kmh > 0) {
            var vecKm = Math.min(Math.max(s.speed_kmh / 60, 0.25), 2);
            var head = destPoint(s.position.latitude, s.position.longitude, s.course_deg, vecKm);
            stationLayer.addLayer(L.polyline([[s.position.latitude, s.position.longitude], head], {
              color: overlay.heading, weight: 3, opacity: 0.9, interactive: false
            }));
            stationLayer.addLayer(L.marker(head, {
              interactive: false,
              icon: L.divIcon({
                className: "wf-track-arrow-wrap",
                iconSize: [10, 10],
                iconAnchor: [5, 5],
                html: '<span class="wf-track-arrow" style="transform:rotate(' + s.course_deg +
                  'deg);border-bottom-color:' + overlay.heading + '"></span>'
              })
            }));
          }

          stationLayer.addLayer(marker);
          stationMarkers[String(s.callsign || "").toUpperCase()] = marker;
        });

        computeBounds();
        renderStations();
      })
      .catch(function () { /* transient — next poll retries */ });
  }

  // Hazard icons for active events that carry coordinates (road
  // difficulties, ...): a severity-colored warning triangle with a
  // detail popup, included in the view fit.
  var HAZARD_COLORS = {
    extreme: "#7b1fa2",
    severe: "#d32f2f",
    moderate: "#f57c00",
    minor: "#fbc02d",
    unknown: "#78909c"
  };

  // aprsWarningIcon renders the APRS emergency symbol (alternate
  // table "!") used for events composed through the web UI.
  function aprsWarningIcon() {
    var html = '<span class="aprs-sym-wrap">' +
      '<span class="aprs-sym-img" style="background-image:url(\'' + APRS_SPRITES["\\"] + '\');background-position:-0px -0px"></span>' +
      '</span>';
    return L.divIcon({
      className: "aprs-sym",
      iconSize: [96, 44],
      iconAnchor: [48, 14],
      html: html
    });
  }

  function hazardIcon(sev) {
    var c = HAZARD_COLORS[sev] || HAZARD_COLORS.unknown;
    var svg = '<svg width="22" height="20" viewBox="0 0 22 20" aria-hidden="true">' +
      '<path d="M11 1 L21 19 L1 19 Z" fill="' + c + '" stroke="#fff" stroke-width="1.5" stroke-linejoin="round"/>' +
      '<text x="11" y="15.5" text-anchor="middle" font-size="12" font-weight="bold" fill="#fff">!</text></svg>';
    return L.divIcon({
      className: "wf-hazard-icon",
      iconSize: [22, 20],
      iconAnchor: [11, 18],
      html: svg
    });
  }

  // fmtLocalDate renders an RFC 3339 timestamp in local browser time
  // (minute precision, no seconds) for hazard popups.
  function fmtLocalDate(v) {
    var t = new Date(v);
    if (isNaN(t.getTime())) {
      return v;
    }
    function pad2(n) { return n < 10 ? "0" + n : "" + n; }
    return t.getFullYear() + "-" + pad2(t.getMonth() + 1) + "-" + pad2(t.getDate()) +
      " " + pad2(t.getHours()) + ":" + pad2(t.getMinutes());
  }

  function refreshHazards() {
    fetch("/api/events")
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (data) {
        if (!hazardLayer) {
          return;
        }
        hazardLayer.clearLayers();
        lastHazards = (data && data.events) || [];
        lastHazards.forEach(function (e) {
          if (!e.latitude || !e.longitude) {
            return;
          }
          var popup = '<span class="sev sev-' + (e.severity || "unknown") + '">' +
            esc(e.severity || "unknown") + "</span> <strong>" + esc(e.headline || e.event) + "</strong>";
          if (e.description) {
            popup += "<br>" + esc(e.description).replace(/\n/g, "<br>");
          }
          if (e.effective_at || e.expires_at) {
            var when = [];
            if (e.effective_at) { when.push(tr("map.from") + " " + fmtLocalDate(e.effective_at)); }
            if (e.expires_at) { when.push(tr("map.to") + " " + fmtLocalDate(e.expires_at)); }
            popup += "<br><span class=\"muted\">" + when.join(" · ") + "</span>";
          }
          popup += "<br><span class=\"muted\">" + tr("warnings.source") + " " + esc(e.source) + "</span>";
          // Composed events carry the APRS emergency symbol; every
          // other source keeps the severity-colored triangle.
          var icon = e.source === "compose" ? aprsWarningIcon() : hazardIcon(e.severity);
          var m = L.marker([e.latitude, e.longitude], { icon: icon, riseOnHover: true });
          m.bindTooltip(esc(e.headline || e.event), { sticky: true, direction: "top" });
          m.bindPopup(popup);
          hazardLayer.addLayer(m);
        });
        computeBounds();
      })
      .catch(function () { /* transient — next poll retries */ });
  }

  // ---- merged weather layer (one map for stations, hazards and weather) ----
  var COND_ICONS = {
    clear: "wi-day-sunny",
    mainly_clear: "wi-day-sunny-overcast",
    partly_cloudy: "wi-day-cloudy",
    overcast: "wi-cloudy",
    fog: "wi-fog",
    drizzle: "wi-sprinkle",
    freezing_drizzle: "wi-sleet",
    rain: "wi-rain",
    freezing_rain: "wi-rain-mix",
    snow: "wi-snow",
    snow_grains: "wi-snow",
    showers: "wi-showers",
    snow_showers: "wi-sleet",
    thunderstorm: "wi-thunderstorm",
    thunderstorm_hail: "wi-hail",
    unknown: "wi-na"
  };

  var lastWeather = [];
  var lastForecasts = [];
  var WEATHER_POLL_MS = 5 * 60 * 1000;

  function condIcon(c) { return COND_ICONS[c] || "wi-na"; }

  function fmtNum(v, digits) {
    return v == null ? "" : Number(v).toFixed(digits == null ? 1 : digits);
  }

  // mk builds one element for the report cards (named mk because `el`
  // is already the map container element in this scope).
  function mk(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) { e.className = cls; }
    if (text != null) { e.textContent = text; }
    return e;
  }

  // forecastKey indexes multi-day forecasts by provider + location name.
  function forecastKey() {
    var fk = {};
    lastForecasts.forEach(function (f) {
      fk[f.provider + "\x00" + f.name] = f;
    });
    return fk;
  }

  // weatherByCall indexes APRS weather reports by callsign so station
  // markers can show the weather inline instead of a duplicate pin.
  function weatherByCall() {
    var by = {};
    lastWeather.forEach(function (r) {
      if (r && r.via === "aprs" && r.name) {
        by[String(r.name).toUpperCase()] = r;
      }
    });
    return by;
  }

  function stationWeatherBlock(call) {
    var r = weatherByCall()[String(call || "").toUpperCase()];
    if (!r) {
      return "";
    }
    var html = '<div class="hw-popup-block"><i class="wi ' + condIcon(r.condition) + '" aria-hidden="true"></i> ';
    if (r.temperature_c != null) { html += fmtNum(r.temperature_c) + "°C"; }
    if (r.humidity_pct != null) { html += " · " + tr("map.hum") + " " + fmtNum(r.humidity_pct, 0) + "%"; }
    if (r.wind_speed_kmh != null) {
      html += " · " + tr("map.wind") + " " + fmtNum(r.wind_speed_kmh) + " km/h";
      if (r.wind_direction_deg != null) { html += " @ " + fmtNum(r.wind_direction_deg, 0) + "°"; }
    }
    if (r.pressure_hpa != null) { html += " · " + fmtNum(r.pressure_hpa, 0) + " " + tr("map.pressure"); }
    return html + "</div>";
  }

  function weatherIcon(r) {
    var temp = r.temperature_c != null ? Math.round(r.temperature_c) + "°" : "";
    return L.divIcon({
      className: "hw-pin-wrap",
      iconSize: [52, 22],
      iconAnchor: [26, 11],
      html: '<span class="hw-pin"><i class="wi ' + condIcon(r.condition) +
        '" aria-hidden="true"></i>' + (temp ? '<span class="hw-pin-t">' + temp + '</span>' : '') + '</span>'
    });
  }

  // weatherPopup renders the full detail popup for one report: condition,
  // temperature, humidity, wind, pressure, radiation, the next three
  // forecast days and the report time.
  function weatherPopup(r) {
    var html = '<div class="hw-popup">';
    html += '<div class="hw-popup-head"><i class="wi ' + condIcon(r.condition) + '" aria-hidden="true"></i>';
    html += '<div class="hw-popup-id"><strong>' + esc(r.name) + '</strong><span class="muted">' + esc(r.provider) + '</span></div>';
    html += r.temperature_c != null ? '<span class="hw-popup-temp">' + fmtNum(r.temperature_c) + '°C</span>' : '';
    html += '</div>';
    var meta = [];
    if (r.humidity_pct != null) { meta.push(tr("map.hum") + " " + fmtNum(r.humidity_pct, 0) + "%"); }
    if (r.wind_speed_kmh != null) {
      var w = tr("map.wind") + " " + fmtNum(r.wind_speed_kmh) + " km/h";
      if (r.wind_direction_deg != null) { w += " @ " + fmtNum(r.wind_direction_deg, 0) + "°"; }
      meta.push(w);
    }
    if (r.wind_gusts_kmh != null) { meta.push(tr("map.gusts") + " " + fmtNum(r.wind_gusts_kmh) + " km/h"); }
    if (r.pressure_hpa != null) { meta.push(fmtNum(r.pressure_hpa, 0) + " hPa"); }
    if (r.radiation_usv_h != null) { meta.push(fmtNum(r.radiation_usv_h, 2) + " µSv/h"); }
    if (r.radiation_cpm != null) { meta.push(fmtNum(r.radiation_cpm, 0) + " cpm"); }
    if (meta.length) { html += '<div class="hw-popup-meta">' + meta.join(" · ") + '</div>'; }
    var f = forecastKey()[r.provider + "\x00" + r.name];
    if (f && f.daily && f.daily.length) {
      html += '<div class="hw-fcast">';
      f.daily.slice(0, 3).forEach(function (d) {
        html += '<span class="hw-fday" title="' + esc(d.date || "") + '">' +
          '<i class="wi hw-fday-icon ' + condIcon(d.condition) + '" aria-hidden="true"></i>' +
          '<span class="hw-fday-t">' + (d.temperature_max_c != null ? Math.round(d.temperature_max_c) + "°" : "—") + '</span></span>';
      });
      html += '</div>';
    }
    if (r.generated_at) {
      html += '<div class="hw-popup-time muted">' + tr("map.updated") + " " + esc(fmtTime(r.generated_at)) + '</div>';
    }
    return html + '</div>';
  }

  // renderWeatherLayer puts one pin per weather report on the shared map.
  // APRS stations normally carry their weather inside their own station
  // marker (stationWeatherBlock), so an APRS pin is drawn only for
  // weather stations the station layer does not render (symbol '_'
  // stations are excluded from /api/aprs/stations) — otherwise they would
  // sit in the reports list with no pin on the map at all.
  function renderWeatherLayer() {
    if (!weatherLayer) {
      return;
    }
    weatherLayer.clearLayers();
    weatherMarkers = {};
    var byCall = {};
    (lastStations || []).forEach(function (s) {
      if (s && s.callsign) {
        byCall[String(s.callsign).toUpperCase()] = true;
      }
    });
    lastWeather.forEach(function (r) {
      if (!r || !r.latitude || !r.longitude || (r.latitude === 0 && r.longitude === 0)) {
        return;
      }
      if (r.via === "aprs" && byCall[String(r.name || "").toUpperCase()]) {
        return; // the station marker shows this weather inline
      }
      var marker = L.marker([r.latitude, r.longitude], { icon: weatherIcon(r), riseOnHover: true });
      marker.bindTooltip(
        r.name + (r.temperature_c != null ? " · " + fmtNum(r.temperature_c) + "°C" : ""),
        { direction: "top" }
      );
      marker.bindPopup(weatherPopup(r));
      weatherLayer.addLayer(marker);
      weatherMarkers[String(r.name || "").toUpperCase()] = marker;
    });
  }

  // focusStation centers the main map on one report's station and opens
  // the same popup a click on the map would: the station marker when the
  // station has one, otherwise its weather pin.
  function focusStation(r) {
    if (!map) {
      return;
    }
    var m = stationMarkers[String(r.name || "").toUpperCase()] ||
      weatherMarkers[String(r.name || "").toUpperCase()];
    if (!m) {
      return;
    }
    focusMarker(m);
  }

  // focusMarker centers the map on any pin and opens its popup.
  function focusMarker(m) {
    if (!map || !m) {
      return;
    }
    map.flyTo(m.getLatLng(), Math.max(map.getZoom(), 13), { duration: 0.7 });
    m.openPopup();
  }

  // renderReports builds the report cards below the map. Every report
  // with a position is clickable: it focuses the map on its pin and
  // opens the popup.
  function renderReports() {
    var container = document.getElementById("hw-reports");
    var countEl = document.getElementById("hw-report-count");
    container.textContent = "";
    if (countEl) { countEl.textContent = lastWeather.length ? "(" + lastWeather.length + ")" : ""; }
    if (!lastWeather.length) {
      container.appendChild(mk("p", "muted", tr("home.weather.none")));
      return;
    }
    var fk = forecastKey();
    lastWeather.forEach(function (r, i) {
      var item = mk("button", "hw-report");
      item.type = "button";
      item.id = "hw-report-" + i;
      item.dataset.via = r.via;
      item.appendChild(mk("span", "wi hw-icon " + condIcon(r.condition)));

      var body = mk("span", "hw-body");
      var head = mk("span", "hw-head");
      head.appendChild(mk("strong", null, r.name));
      head.appendChild(mk("span", "hw-provider", r.provider));
      body.appendChild(head);

      var meta = [];
      if (r.temperature_c != null) { meta.push(fmtNum(r.temperature_c) + "°C"); }
      if (r.humidity_pct != null) { meta.push(tr("map.hum") + " " + fmtNum(r.humidity_pct, 0) + "%"); }
      if (r.wind_speed_kmh != null) {
        var w = tr("map.wind") + " " + fmtNum(r.wind_speed_kmh) + " km/h";
        if (r.wind_direction_deg != null) { w += " @ " + fmtNum(r.wind_direction_deg, 0) + "°"; }
        meta.push(w);
      }
      if (r.pressure_hpa != null) { meta.push(fmtNum(r.pressure_hpa, 0) + " hPa"); }
      if (r.radiation_usv_h != null) { meta.push(fmtNum(r.radiation_usv_h, 2) + " µSv/h"); }
      if (r.radiation_cpm != null) { meta.push(fmtNum(r.radiation_cpm, 0) + " cpm"); }
      if (meta.length) { body.appendChild(mk("span", "hw-meta", meta.join(" · "))); }

      var f = fk[r.provider + "\x00" + r.name];
      if (f && f.daily && f.daily.length) {
        var strip = mk("span", "hw-fcast");
        strip.title = tr("home.weather.forecast");
        f.daily.slice(0, 3).forEach(function (d) {
          var chip = mk("span", "hw-fday");
          chip.title = d.date || "";
          chip.appendChild(mk("span", "wi hw-fday-icon " + condIcon(d.condition)));
          chip.appendChild(mk("span", "hw-fday-t",
            d.temperature_max_c != null ? Math.round(d.temperature_max_c) + "°" : "—"));
          strip.appendChild(chip);
        });
        body.appendChild(strip);
      }

      item.appendChild(body);
      if (r.via === "aprs") {
        item.classList.add("hw-aprs");
      }
      // Every report whose pin sits on the map is clickable: it centers
      // the map on the pin and opens its popup. A positionless report has
      // nothing to center on and stays disabled.
      if (r.latitude && r.longitude && !(r.latitude === 0 && r.longitude === 0)) {
        item.title = trf("map.show_on_map", r.name);
        item.addEventListener("click", function () { focusStation(r); });
      } else {
        item.disabled = true;
      }
      container.appendChild(item);
    });
  }

  // renderStations builds the radio-station cards below the map:
  // callsign, comment, speed/altitude and when the station was last
  // heard. Cards focus the map on the station pin, like the weather cards.
  function renderStations() {
    var container = document.getElementById("hw-stations");
    var countEl = document.getElementById("hw-station-count");
    if (!container) {
      return;
    }
    container.textContent = "";
    var visible = (lastStations || []).filter(function (st) {
      return st && st.position && !st.self;
    });
    if (countEl) {
      countEl.textContent = visible.length ? "(" + visible.length + ")" : "";
    }
    if (!visible.length) {
      container.appendChild(mk("p", "muted", tr("home.stations.none")));
      return;
    }
    visible.forEach(function (st) {
      var item = mk("button", "hw-report");
      item.type = "button";
      item.appendChild(mk("span", "hw-icon hw-sta", String(st.callsign || "?").slice(0, 4)));
      var body = mk("span", "hw-body");
      var head = mk("span", "hw-head");
      head.appendChild(mk("strong", null, st.callsign));
      if (st.comment) {
        head.appendChild(mk("span", "hw-provider", st.comment));
      }
      body.appendChild(head);
      var meta = [];
      if (st.speed_kmh > 0) {
        meta.push(tr("map.speed") + " " + fmtNum(st.speed_kmh, 0) + " km/h");
      }
      if (st.altitude_m != null) {
        meta.push(tr("map.alt") + " " + fmtNum(st.altitude_m, 0) + " m");
      }
      meta.push(tr("map.heard") + " " + fmtTime(st.last_heard_at));
      body.appendChild(mk("span", "hw-meta", meta.join(" · ")));
      item.appendChild(body);
      var call = st.callsign;
      item.addEventListener("click", function () {
        focusMarker(stationMarkers[String(call || "").toUpperCase()]);
      });
      container.appendChild(item);
    });
  }

  // renderAircraft builds the aircraft cards below the map: callsign,
  // speed, altitude, climb and when the plane was last seen.
  function renderAircraft() {
    var container = document.getElementById("hw-aircraft");
    var countEl = document.getElementById("hw-aircraft-count");
    if (!container) {
      return;
    }
    container.textContent = "";
    var visible = (lastAircraft || []).filter(function (a) {
      return a && a.latitude && a.longitude;
    });
    if (countEl) {
      countEl.textContent = visible.length ? "(" + visible.length + ")" : "";
    }
    if (!visible.length) {
      container.appendChild(mk("p", "muted", tr("home.aircraft.none")));
      return;
    }
    visible.forEach(function (a) {
      var item = mk("button", "hw-report");
      item.type = "button";
      item.appendChild(mk("span", "hw-icon hw-plane", "✈"));
      var body = mk("span", "hw-body");
      var head = mk("span", "hw-head");
      head.appendChild(mk("strong", null, a.callsign || a.icao24 || "?"));
      if (a.category) {
        head.appendChild(mk("span", "hw-provider", a.category));
      }
      body.appendChild(head);
      var meta = [];
      if (a.speed_kmh != null) {
        meta.push(tr("map.speed") + " " + fmtNum(a.speed_kmh, 0) + " km/h");
      }
      if (a.altitude_m != null) {
        meta.push(tr("map.alt") + " " + fmtNum(a.altitude_m, 0) + " m");
      }
      if (a.vertical_rate_m_s != null) {
        meta.push(tr("map.climb") + " " + fmtNum(a.vertical_rate_m_s, 1) + " m/s");
      }
      if (a.seen_at) {
        meta.push(tr("map.seen") + " " + fmtTime(new Date(a.seen_at * 1000).toISOString()));
      }
      body.appendChild(mk("span", "hw-meta", meta.join(" · ")));
      item.appendChild(body);
      item.addEventListener("click", function () {
        focusMarker(aircraftMarkers[keyAircraft(a)]);
      });
      container.appendChild(item);
    });
  }

  function refreshWeather() {
    fetch("/api/weather")
      .then(function (resp) { return resp.ok ? resp.json() : null; })
      .then(function (data) {
        if (!data) {
          return;
        }
        lastWeather = data.reports || [];
        lastForecasts = data.forecasts || [];
        renderReports();
        renderWeatherLayer();
        // Station popups embed weather blocks — rebuild them so new
        // observations show up without waiting for the station poll.
        refreshStations();
      })
      .catch(function () { /* transient — next poll retries */ });
  }

  // ---- aircraft layer (ADS-B): minute vector + 3-5 minute trail ----
  var aircraftLayer = null;
  var lastAircraft = [];
  var aircraftMarkers = {};
  var AIRCRAFT_POLL_MS = 15 * 1000;

  // planeIcon renders one aircraft as a track-rotated plane glyph;
  // grounded targets are gray, airborne ones amber.
  function planeIcon(a) {
    var rot = a.track_deg != null ? a.track_deg : 0;
    var color = a.on_ground ? "#9e9e9e" : "#ffb300";
    var svg = '<svg width="24" height="24" viewBox="0 0 24 24" aria-hidden="true">' +
      '<g transform="rotate(' + rot + ' 12 12)">' +
      '<path d="M12 2 L21 21 L12 17 L3 21 Z" fill="' + color + '" stroke="#000" stroke-width="0.8" stroke-linejoin="round"/>' +
      '</g></svg>';
    return L.divIcon({ className: "wf-aircraft", iconSize: [24, 24], iconAnchor: [12, 12], html: svg });
  }

  function aircraftPopup(a) {
    var html = '<span class="muted">' + tr("map.aircraft") + '</span><br><strong>' + esc(a.callsign || a.icao24) + "</strong>";
    html += '<br><span class="muted">' + esc(String(a.icao24 || "").toUpperCase()) + "</span>";
    if (a.altitude_m != null) { html += "<br>" + tr("map.alt") + ": " + fmtNum(a.altitude_m, 0) + " m"; }
    if (a.speed_kmh != null) {
      html += "<br>" + tr("map.speed") + ": " + fmtNum(a.speed_kmh, 0) + " km/h";
      if (a.track_deg != null) { html += " @ " + fmtNum(a.track_deg, 0) + "\u00b0"; }
    }
    if (a.vertical_rate_m_s != null) { html += "<br>" + tr("map.climb") + ": " + fmtNum(a.vertical_rate_m_s, 1) + " m/s"; }
    if (a.category) { html += "<br>" + tr("map.category") + ": " + esc(a.category); }
    if (a.seen_at) {
      html += "<br>" + tr("map.seen") + ": " + esc(fmtTime(new Date(a.seen_at * 1000).toISOString()));
    }
    return html;
  }

  function refreshAircraft() {
    fetch("/api/aircraft")
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (data) {
        if (!aircraftLayer) {
          return;
        }
        aircraftLayer.clearLayers();
        lastAircraft = (data && data.aircraft) || [];
        var overlay = aprsOverlayColors();
        lastAircraft.forEach(function (a) {
          if (!a.latitude || !a.longitude) {
            return;
          }
          var marker = L.marker([a.latitude, a.longitude], { icon: planeIcon(a), riseOnHover: true });
          marker.bindTooltip(trf("map.aircraft.tip", a.callsign || a.icao24), { direction: "top" });
          marker.bindPopup(aircraftPopup(a));
          aircraftLayer.addLayer(marker);
          aircraftMarkers[keyAircraft(a)] = marker;

          // The 3-5 minute trail: recent recorded positions as one line.
          var trail = [];
          (a.trail || []).forEach(function (tp) {
            if (tp && tp.latitude && tp.longitude) {
              trail.push([tp.latitude, tp.longitude]);
            }
          });
          if (trail.length > 1) {
            aircraftLayer.addLayer(L.polyline(trail, {
              color: overlay.track, weight: 2, opacity: 0.8, interactive: false
            }));
          }

          // Minute vector: one minute of travel along the reported track,
          // clamped so slow movers stay readable and fast movers stay
          // on-screen (the same idea as the APRS heading vector).
          if (a.track_deg != null && a.speed_kmh > 0) {
            var vecKm = Math.min(Math.max(a.speed_kmh / 60, 0.5), 5);
            var head = destPoint(a.latitude, a.longitude, a.track_deg, vecKm);
            aircraftLayer.addLayer(L.polyline([[a.latitude, a.longitude], head], {
              color: overlay.heading, weight: 3, opacity: 0.9, interactive: false
            }));
            aircraftLayer.addLayer(L.marker(head, {
              interactive: false,
              icon: L.divIcon({
                className: "wf-track-arrow-wrap",
                iconSize: [10, 10],
                iconAnchor: [5, 5],
                html: '<span class="wf-track-arrow" style="transform:rotate(' + a.track_deg +
                  'deg);border-bottom-color:' + overlay.heading + '"></span>'
              })
            }));
          }
        });
        computeBounds();
        renderAircraft();
      })
      .catch(function () { /* transient — next poll retries */ });
  }

  // keyAircraft derives the per-aircraft marker key from its identifiers.
  function keyAircraft(a) {
    return String(a.callsign || a.icao24 || "").toUpperCase();
  }

  // ---- air-quality station layer (GIOŚ official index) ----
  var AQ_POLL_MS = 5 * 60 * 1000;

  // GIOŚ air-quality index levels (0 = very good ... 5 = very bad).
  var AQ_COLORS = ["#2e7d32", "#7cb342", "#fbc02d", "#f57c00", "#d32f2f", "#8e1b1b"];

  function aqColor(level) {
    return level != null && level >= 0 && level < AQ_COLORS.length ? AQ_COLORS[level] : "#78909c";
  }

  function aqIcon(station) {
    var color = aqColor(station.index_level_id);
    return L.divIcon({
      className: "aq-pin-wrap",
      iconSize: [18, 18],
      iconAnchor: [9, 9],
      html: '<span class="aq-pin" style="background:' + color + '"></span>'
    });
  }

  function aqPopup(station) {
    var html = '<span class="muted">' + tr("map.airquality") + '</span><br><strong>' + esc(station.station_name) + "</strong>";
    if (station.index_level_name) {
      html += '<br><span class="aq-level" style="color:' + aqColor(station.index_level_id) + '">' +
        esc(station.index_level_name) + "</span>";
    }
    (station.pollutants || []).forEach(function (p) {
      if (p.level_name) {
        html += "<br>" + esc(p.code) + ": " + esc(p.level_name);
      }
    });
    if (station.generated_at) {
      html += '<br><span class="muted">' + tr("map.updated") + " " + esc(fmtTime(station.generated_at)) + "</span>";
    }
    return html;
  }

  function refreshAirQuality() {
    fetch("/api/airquality")
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (data) {
        if (!aqLayer) {
          return;
        }
        aqLayer.clearLayers();
        ((data && data.stations) || []).forEach(function (station) {
          if (!station.latitude || !station.longitude) {
            return;
          }
          var marker = L.marker([station.latitude, station.longitude], {
            icon: aqIcon(station), riseOnHover: true
          });
          marker.bindTooltip(trf("map.airquality.tip", station.station_name), { direction: "top" });
          marker.bindPopup(aqPopup(station));
          aqLayer.addLayer(marker);
        });
      })
      .catch(function () { /* transient — next poll retries */ });
  }

  // ---- user location (like the Google Maps blue dot) ----
  var userMarker = null;
  var userAccCircle = null;

  // addLocateControl adds the bottom-right "show my location" button.
  // The browser permission prompt only appears after the user clicks it
  // (never on page load). On success a blue dot + accuracy circle appear
  // and the view centers on the user; the button then recenters.
  function addLocateControl(map) {
    var c = L.control({ position: "bottomright" });
    c.onAdd = function () {
      var btn = L.DomUtil.create("button", "wf-locate-btn");
      btn.type = "button";
      btn.title = tr("map.show_location");
      btn.setAttribute("aria-label", tr("map.show_location"));
      btn.innerHTML = '<svg viewBox="0 0 24 24" width="17" height="17" aria-hidden="true"><circle cx="12" cy="12" r="3.2" fill="none" stroke="currentColor" stroke-width="2"/><path d="M12 2v3.5M12 18.5V22M2 12h3.5M18.5 12H22" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>';
      L.DomEvent.disableClickPropagation(btn);
      L.DomEvent.disableScrollPropagation(btn);

      function denied() {
        btn.classList.add("wf-locate-denied");
        btn.title = "Location unavailable (permission denied or no signal)";
        window.setTimeout(function () { btn.classList.remove("wf-locate-denied"); }, 2200);
      }

      L.DomEvent.on(btn, "click", function () {
        if (userMarker) {
          map.setView(userMarker.getLatLng(), Math.max(map.getZoom(), 13));
          return;
        }
        if (!navigator.geolocation) {
          denied();
          return;
        }
        navigator.geolocation.getCurrentPosition(function (pos) {
          var ll = [pos.coords.latitude, pos.coords.longitude];
          if (!userMarker) {
            userMarker = L.circleMarker(ll, {
              radius: 7, color: "#fff", weight: 2,
              fillColor: "#1a73e8", fillOpacity: 1
            }).addTo(map);
            userMarker.bindTooltip(tr("map.you_are_here"), { direction: "top" });
          } else {
            userMarker.setLatLng(ll);
          }
          if (pos.coords.accuracy > 0) {
            if (userAccCircle) { map.removeLayer(userAccCircle); }
            userAccCircle = L.circle(ll, {
              radius: pos.coords.accuracy,
              color: "#1a73e8", weight: 1, opacity: 0.4,
              fillColor: "#1a73e8", fillOpacity: 0.07, interactive: false
            }).addTo(map);
          }
          map.setView(ll, Math.max(map.getZoom(), 13));
          btn.classList.add("wf-locate-active");
          btn.title = tr("map.center_location");
        }, function () {
          denied();
        }, { enableHighAccuracy: false, timeout: 12000, maximumAge: 30000 });
      });
      return btn;
    };
    c.addTo(map);
  }

  // Layer toggles: the overlay families can be switched independently so
  // the combined map stays readable. The order mirrors what this system
  // is about: hazards (the alerts) first, then the radio neighbourhood,
  // weather and radar, with aircraft traffic last.
  var radarOn = true;
  var LAYER_DEFS = [
    ["hazards", tr("map.layer.hazards"), function () { return hazardLayer; }],
    ["stations", tr("map.layer.stations"), function () { return stationLayer; }],
    ["weather", tr("map.layer.weather"), function () { return weatherLayer; }],
    ["radar", tr("map.layer.radar"), function () {
      radarOn = !radarOn;
      return radarLayer;
    }],
    ["airquality", tr("map.layer.airquality"), function () { return aqLayer; }],
    ["aircraft", tr("map.layer.aircraft"), function () { return aircraftLayer; }]
  ];

  function addLayersControl(map) {
    var c = L.control({ position: "topright" });
    c.onAdd = function () {
      var box = L.DomUtil.create("div", "wf-layers");
      LAYER_DEFS.forEach(function (def) {
        var btn = L.DomUtil.create("button", "wf-layer-btn active");
        btn.type = "button";
        btn.textContent = def[1];
        btn.title = trf("map.toggle_layer", def[1].toLowerCase());
        btn.setAttribute("aria-pressed", "true");
        L.DomEvent.disableClickPropagation(btn);
        L.DomEvent.disableScrollPropagation(btn);
        L.DomEvent.on(btn, "click", function () {
          var layer = def[2]();
          var on = btn.classList.toggle("active");
          btn.setAttribute("aria-pressed", String(on));
          if (!layer) { return; }
          if (on) { map.addLayer(layer); } else { map.removeLayer(layer); }
        });
        box.appendChild(btn);
      });
      return box;
    };
    c.addTo(map);
  }

  // Fit the view around our locator, every station and every geo-located
  // hazard; runs after either layer refresh so the union stays current.
  function computeBounds() {
    if (!map) {
      return;
    }
    var b = L.latLngBounds([[ownLat, ownLon]]);
    var has = false;
    lastStations.forEach(function (s) {
      if (s && s.position && !s.self) {
        b.extend([s.position.latitude, s.position.longitude]);
        has = true;
      }
    });
    lastHazards.forEach(function (e) {
      if (e && e.latitude && e.longitude) {
        b.extend([e.latitude, e.longitude]);
        has = true;
      }
    });
    lastWeather.forEach(function (r) {
      if (r && r.latitude && r.longitude && r.via !== "aprs") {
        b.extend([r.latitude, r.longitude]);
        has = true;
      }
    });
    lastAircraft.forEach(function (a) {
      if (a && a.latitude && a.longitude) {
        b.extend([a.latitude, a.longitude]);
        has = true;
      }
    });
    stationBounds = has ? b : null;
    if (!fittedOnce && has) {
      map.invalidateSize();
      if (map.getSize().x > 0) {
        fitToStations();
      }
    }
  }

  loadLibraries(function () {
    loadMapLibre(function () {
      loadMapLibreGlue(initMap);
    });
  });
})();

// Theme switch (dark by default, light on request): one icon button per
// page toggles data-theme on <html> and persists the choice.
(function () {
  "use strict";

  var THEME_KEY = "warnflux-theme";

  function apply(theme) {
    document.documentElement.setAttribute("data-theme", theme);
  }

  function initTheme() {
    var saved = null;
    try {
      saved = localStorage.getItem(THEME_KEY);
    } catch (e) { /* storage unavailable */ }
    apply(saved === "light" ? "light" : "dark");

    document.querySelectorAll(".theme-toggle").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var next = document.documentElement.getAttribute("data-theme") === "light" ? "dark" : "light";
        apply(next);
        try {
          localStorage.setItem(THEME_KEY, next);
        } catch (e) { /* storage unavailable */ }
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initTheme);
  } else {
    initTheme();
  }
})();

// One-time about popup: the config-driven system intro appears as a
// modal on the first visit (per browser) and stays dismissed afterwards
// via localStorage.
(function () {
  "use strict";

  function initAboutPopup() {
    var dialog = document.getElementById("about-dialog");
    if (!dialog) {
      return;
    }
    var seen = false;
    try { seen = localStorage.getItem("warnflux-about-seen") === "1"; } catch (e) { /* storage unavailable */ }
    var close = function () {
      try { localStorage.setItem("warnflux-about-seen", "1"); } catch (e) { /* storage unavailable */ }
      dialog.close();
    };
    var ok = dialog.querySelector(".about-ok");
    if (ok) { ok.addEventListener("click", close); }
    var x = dialog.querySelector(".about-close");
    if (x) { x.addEventListener("click", close); }
    dialog.addEventListener("click", function (e) { if (e.target === dialog) { close(); } });
    dialog.addEventListener("cancel", function (e) { e.preventDefault(); close(); });
    if (!seen) { dialog.showModal(); }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAboutPopup);
  } else {
    initAboutPopup();
  }
})();

// Compose page: optional map picker for the event location.
// A click drops the APRS warning marker (draggable); the lat/lon inputs
// stay in sync and the "Clear location" button removes it.
(function () {
  "use strict";

  var mapEl = document.getElementById("compose-map");
  if (!mapEl) {
    return;
  }
  var latInput = document.getElementById("compose-lat");
  var lonInput = document.getElementById("compose-lon");
  var clearBtn = document.getElementById("compose-loc-clear");

  var centerLat = parseFloat(mapEl.getAttribute("data-lat"));
  var centerLon = parseFloat(mapEl.getAttribute("data-lon"));
  var center = (centerLat && centerLon) ? [centerLat, centerLon] : [50.0, 20.0];

  var map = null;
  var marker = null;

  // The picker marker is the APRS emergency symbol (alternate table "!"),
  // the same icon the public map uses for composed events.
  function warningIcon() {
    return L.divIcon({
      className: "aprs-sym",
      iconSize: [96, 44],
      iconAnchor: [48, 14],
      html: '<span class="aprs-sym-wrap">' +
        '<span class="aprs-sym-img" style="background-image:url(\'/static/aprs-symbols/aprs-symbols-24-1@2x.png\');background-position:-0px -0px"></span>' +
        '</span>'
    });
  }

  function syncInputs() {
    var p = marker.getLatLng();
    latInput.value = p.lat.toFixed(5);
    lonInput.value = p.lng.toFixed(5);
  }

  function dropMarker(latlng) {
    if (!marker) {
      marker = L.marker(latlng, { icon: warningIcon(), draggable: true }).addTo(map);
      marker.on("dragend", syncInputs);
    } else {
      marker.setLatLng(latlng);
    }
    syncInputs();
  }

  function tileURL() {
    var dark = document.documentElement.getAttribute("data-theme") !== "light";
    return dark
      ? "https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}"
      : "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
  }

  function loadLeaflet(cb) {
    if (window.L) {
      cb();
      return;
    }
    var css = document.createElement("link");
    css.rel = "stylesheet";
    css.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
    document.head.appendChild(css);
    var s = document.createElement("script");
    s.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
    s.onload = function () { cb(); };
    s.onerror = function () { /* offline: the lat/lon inputs still work */ };
    document.body.appendChild(s);
  }

  if (clearBtn) {
    clearBtn.addEventListener("click", function () {
      if (marker && map) {
        map.removeLayer(marker);
        marker = null;
      }
      latInput.value = "";
      lonInput.value = "";
    });
  }

  loadLeaflet(function () {
    if (!window.L) {
      return;
    }
    map = L.map(mapEl, { attributionControl: false }).setView(center, 11);
    L.tileLayer(tileURL(), { maxZoom: 18 }).addTo(map);
    map.on("click", function (e) { dropMarker(e.latlng); });

    // Edit flow: an existing location prefills the marker.
    var lat = parseFloat(latInput.value);
    var lon = parseFloat(lonInput.value);
    if (!isNaN(lat) && !isNaN(lon)) {
      dropMarker([lat, lon]);
      map.setView([lat, lon], 13);
    }
  });
})();

// Compose page: fill the form with debug values for quick testing.
(function () {
  "use strict";

  function pad(n) {
    return String(n).padStart(2, "0");
  }

  function localDT(d) {
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
      "T" + pad(d.getHours()) + ":" + pad(d.getMinutes());
  }

  function initDebugFill() {
    var btn = document.getElementById("compose-debug-fill");
    if (!btn) {
      return;
    }
    btn.addEventListener("click", function () {
      var form = btn.closest("form");
      if (!form) {
        return;
      }
      var now = new Date();
      var later = new Date(now.getTime() + 6 * 3600 * 1000);
      var vals = {
        event: "Storm",
        headline: "Debug: strong wind warning",
        severity: "severe",
        urgency: "immediate",
        certainty: "likely",
        status: "active",
        areas: "",
        description: "Debug fill: strong wind gusts expected this evening.",
        instruction: "Secure loose objects and avoid forest areas.",
        effective_at: localDT(now),
        expires_at: localDT(later)
      };
      Object.keys(vals).forEach(function (name) {
        var el = form.querySelector('[name="' + name + '"]');
        if (el) {
          el.value = vals[name];
        }
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initDebugFill);
  } else {
    initDebugFill();
  }
})();

// Application drawer: collapses to an icon rail on desktop (persisted),
// overlays the content on narrow screens.
(function () {
  "use strict";

  var toggle = document.getElementById("drawer-toggle");
  if (!toggle) {
    return;
  }
  var body = document.body;

  if (localStorage.getItem("wf-drawer") === "collapsed") {
    body.classList.add("drawer-collapsed");
  }

  toggle.addEventListener("click", function () {
    if (window.innerWidth <= 860) {
      body.classList.toggle("drawer-open");
      return;
    }
    body.classList.toggle("drawer-collapsed");
    localStorage.setItem(
      "wf-drawer",
      body.classList.contains("drawer-collapsed") ? "collapsed" : "open"
    );
  });
})();

// User menu: the avatar in the top-right opens a dropdown with account
// and sign-out actions. Closes on outside click and Escape.
(function () {
  "use strict";

  var menu = document.querySelector(".user-menu");
  var btn = document.getElementById("user-menu-btn");
  var panel = document.getElementById("user-menu-panel");
  if (!menu || !btn || !panel) {
    return;
  }

  function setOpen(open) {
    panel.hidden = !open;
    btn.setAttribute("aria-expanded", String(open));
    menu.setAttribute("data-open", String(open));
  }

  btn.addEventListener("click", function (e) {
    e.stopPropagation();
    setOpen(panel.hidden);
  });

  document.addEventListener("click", function (e) {
    if (!panel.hidden && !menu.contains(e.target)) {
      setOpen(false);
    }
  });

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape" && !panel.hidden) {
      setOpen(false);
      btn.focus();
    }
  });
})();

// Admin pages with a tab strip (e.g. /traffic): switch panels.
(function () {
  "use strict";

  function initPageTabs() {
    var tabs = document.querySelectorAll(".page-tab[data-tab]");
    if (tabs.length === 0) {
      return;
    }
    tabs.forEach(function (tab) {
      tab.addEventListener("click", function () {
        tabs.forEach(function (t) {
          var active = t === tab;
          t.classList.toggle("active", active);
          t.setAttribute("aria-selected", active ? "true" : "false");
        });
        document.querySelectorAll(".page-panel[data-panel]").forEach(function (p) {
          p.hidden = p.getAttribute("data-panel") !== tab.getAttribute("data-tab");
        });
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initPageTabs);
  } else {
    initPageTabs();
  }
})();

// MQTT browser (second tab of /traffic): temporary subscription on one
// receiver, collected messages rendered as a table.
(function () {
  "use strict";

  var PAYLOAD_PREVIEW = 300;

  function initMQTTBrowse() {
    var form = document.getElementById("browse-form");
    if (!form) {
      return;
    }
    var statusEl = document.getElementById("browse-status");
    var results = document.getElementById("browse-results");
    var rows = document.getElementById("browse-rows");

    function cell(text, cls) {
      var td = document.createElement("td");
      td.textContent = text;
      if (cls) {
        td.className = cls;
      }
      return td;
    }

    function payloadCell(payload) {
      var td = document.createElement("td");
      td.className = "browse-payload";
      var pre = document.createElement("pre");
      pre.textContent = payload.length > PAYLOAD_PREVIEW
        ? payload.slice(0, PAYLOAD_PREVIEW) + "…"
        : payload;
      if (payload) {
        pre.title = payload;
      }
      td.appendChild(pre);
      return td;
    }

    function render(data) {
      if (!data) {
        statusEl.textContent = "Browse failed (empty response).";
        return;
      }
      if (data.error) {
        statusEl.textContent = "Error: " + data.error;
        return;
      }
      var entries = data.entries || [];
      rows.textContent = "";
      entries.forEach(function (e) {
        var tr = document.createElement("tr");
        tr.appendChild(cell(e.topic, "mono"));
        tr.appendChild(cell(String(e.qos)));
        tr.appendChild(cell(e.retained ? "yes" : "no", e.retained ? "browse-ret" : ""));
        tr.appendChild(cell(e.size + " B"));
        tr.appendChild(payloadCell(e.payload || ""));
        rows.appendChild(tr);
      });
      results.hidden = false;
      statusEl.textContent = entries.length + " message(s) · " +
        new Date().toLocaleTimeString();
    }

    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      var receiver = document.getElementById("browse-receiver").value;
      var topic = document.getElementById("browse-topic").value.trim();
      var windowSecs = document.getElementById("browse-window").value;
      if (!topic) {
        return;
      }
      statusEl.hidden = false;
      statusEl.textContent = trf("traffic.subscribing", topic, windowSecs);
      var url = "/api/mqtt/browse?topic=" + encodeURIComponent(topic) +
        "&window=" + encodeURIComponent(windowSecs) +
        "&receiver=" + encodeURIComponent(receiver);
      fetch(url, {
        headers: { "Accept": "application/json" },
        credentials: "same-origin",
        cache: "no-store"
      })
        .then(function (res) {
          return res.json().catch(function () { return null; });
        })
        .then(render)
        .catch(function () {
          statusEl.textContent = "Browse failed.";
        });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initMQTTBrowse);
  } else {
    initMQTTBrowse();
  }
})();

// Users page: edit a user in a modal dialog instead of the page-top form.
(function () {
  "use strict";

  function initUserEditDialog() {
    var dialog = document.getElementById("user-edit-dialog");
    if (!dialog) {
      return;
    }
    // Server-rendered opens (?edit=<id>, failed submits) use the bare
    // open attribute; upgrade them to a real modal with a backdrop.
    if (dialog.hasAttribute("open")) {
      dialog.removeAttribute("open");
      if (typeof dialog.showModal === "function") {
        dialog.showModal();
      }
    }
    var field = function (id) { return document.getElementById(id); };

    function openDialog(mode) {
      var err = dialog.querySelector(".login-error");
      if (err) {
        err.remove();
      }
      var title = document.getElementById("user-edit-title");
      if (title) {
        title.textContent = tr(mode === "edit" ? "users.edit_title" : "users.add_title");
      }
      var password = field("user-edit-password");
      if (password) {
        password.placeholder = mode === "edit"
          ? "new password (optional)"
          : "password (required for member/emcom)";
      }
      if (typeof dialog.showModal === "function") {
        dialog.showModal();
      } else {
        dialog.setAttribute("open", "");
      }
    }

    // Edit prefills server-side (?edit=<id> renders the dialog open with
    // the user's data and checked preference boxes); the Add button just
    // opens the empty dialog.
    var add = document.getElementById("user-add-btn");
    if (add) {
      add.addEventListener("click", function () {
        field("user-edit-id").value = "0";
        field("user-edit-username").value = "";
        field("user-edit-phone").value = "";
        field("user-edit-email").value = "";
        field("user-edit-discord").value = "";
        field("user-edit-role").value = "";
        field("user-edit-aprs").value = "";
        field("user-edit-password").value = "";
        // A fresh add starts with no memberships and every channel on
        // (opt-out is the exceptional state). The checkbox rows are
        // rendered unchecked, so flip the channels back on explicitly.
        dialog.querySelectorAll("input[name='channels']").forEach(function (c) {
          c.checked = true;
        });
        openDialog("add");
      });
    }

    var cancel = document.getElementById("user-edit-cancel");
    if (cancel) {
      cancel.addEventListener("click", function () { dialog.close(); });
    }
    // Click on the backdrop closes the dialog.
    dialog.addEventListener("click", function (ev) {
      if (ev.target === dialog) {
        dialog.close();
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initUserEditDialog);
  } else {
    initUserEditDialog();
  }
})();

// Audit log page: incremental feed of user actions, newest at the bottom.
(function () {
  "use strict";

  var AUDIT_POLL_MS = 2000;
  var MAX_AUDIT_NODES = 500;

  function pad2(n) {
    return n < 10 ? "0" + n : "" + n;
  }

  function lineFor(entry) {
    var t = new Date(entry.at);
    var when = isNaN(t.getTime())
      ? entry.at
      : pad2(t.getHours()) + ":" + pad2(t.getMinutes()) + ":" + pad2(t.getSeconds());
    var cls = "au-" + (entry.action || "").replace(/[^a-z0-9-]/g, "-");
    var text = when + "  [" + entry.user + "] " + entry.action +
      (entry.detail ? "  " + entry.detail : "");
    return { cls: cls, text: text };
  }

  function initAudit() {
    var viewer = document.getElementById("audit-viewer");
    if (!viewer) {
      return;
    }
    var after = parseInt(viewer.getAttribute("data-after"), 10) || 0;

    function poll() {
      fetch("/partials/audit?after=" + after, {
        headers: { "Accept": "application/json" },
        credentials: "same-origin",
        cache: "no-store"
      })
        .then(function (res) {
          if (res.status === 401) {
            window.location.href = "/login";
            return null;
          }
          if (!res.ok) {
            return null;
          }
          return res.json();
        })
        .then(function (data) {
          if (!data || !data.entries || data.entries.length === 0) {
            return;
          }
          var frag = document.createDocumentFragment();
          data.entries.forEach(function (entry) {
            var line = lineFor(entry);
            var div = document.createElement("div");
            div.className = "log-line " + line.cls;
            div.textContent = line.text;
            frag.appendChild(div);
            after = entry.seq;
          });
          viewer.appendChild(frag);
          while (viewer.childNodes.length > MAX_AUDIT_NODES) {
            viewer.removeChild(viewer.firstChild);
          }
          viewer.scrollTop = viewer.scrollHeight;
        })
        .catch(function () {});
    }

    poll();
    setInterval(poll, AUDIT_POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAudit);
  } else {
    initAudit();
  }
})();
