// WarnFlux dashboard poller — no external dependencies.
// Refreshes the dashboard sections every 5 seconds via fetch.
// On session expiry (401) the browser is sent back to the login page.
(function () {
  "use strict";

  var SECTIONS = [
    { path: "/partials/status", id: "status-section" },
    { path: "/partials/mqtt", id: "mqtt-section" },
    { path: "/partials/weather", id: "weather-section" },
    { path: "/partials/warnings", id: "warnings-section" },
    { path: "/partials/plugins", id: "plugins-section" },
    { path: "/partials/actions", id: "actions-section" },
    { path: "/partials/health", id: "health-section" },
    // Public home page: the active-hazard list fragment.
    { path: "/partials/home", id: "home-alerts" }
  ];

  var POLL_MS = 5000;

  function sectionPath(section) {
    // Preserve the user's warnings page across the periodic refreshes.
    if (section.id === "warnings-section") {
      var el = document.getElementById(section.id);
      var page = el ? el.getAttribute("data-wpage") : "";
      if (page && page !== "1") {
        return section.path + "?page=" + page;
      }
    }
    return section.path;
  }

  function refresh(section) {
    if (!document.getElementById(section.id)) {
      return; // section not on this page (e.g. /partials/home off-site)
    }
    fetch(sectionPath(section), {
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
          node.outerHTML = html;
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
        p.textContent = "No notifications processed yet";
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
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initHomeTabs);
  } else {
    initHomeTabs();
  }
})();

// Public home page: Weather tab — current reports from every internet
// provider and every APRS weather station in range, plus the multi-day
// forecasts held in the retained MQTT info topics. Clicking an APRS report
// opens a mini-map popup with the station location.
(function () {
  "use strict";

  var panel = document.getElementById("panel-tab-weather");
  if (!panel) {
    return;
  }

  var POLL_MS = 5 * 60 * 1000;
  var loaded = false;
  var miniMap = null;
  var miniMarker = null;
  var hwMap = null;
  var hwMarkerLayer = null;
  var hwTileLayer = null;

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

  function condIcon(c) { return COND_ICONS[c] || "wi-na"; }

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) { e.className = cls; }
    if (text != null) { e.textContent = text; }
    return e;
  }

  function fmtNum(v, digits) {
    return v == null ? "" : Number(v).toFixed(digits == null ? 1 : digits);
  }

  function renderReports(reports) {
    var container = document.getElementById("hw-reports");
    var countEl = document.getElementById("hw-report-count");
    container.textContent = "";
    if (countEl) { countEl.textContent = reports.length ? "(" + reports.length + ")" : ""; }
    if (!reports.length) {
      container.appendChild(el("p", "muted", "No weather reports yet — APRS weather stations and forecast providers publish them over MQTT."));
      return;
    }
    reports.forEach(function (r, i) {
      var item = el("button", "hw-report");
      item.type = "button";
      item.id = "hw-report-" + i;
      item.dataset.via = r.via;
      item.appendChild(el("span", "wi hw-icon " + condIcon(r.condition)));

      var body = el("span", "hw-body");
      var head = el("span", "hw-head");
      head.appendChild(el("strong", null, r.name));
      head.appendChild(el("span", "hw-provider", r.provider));
      body.appendChild(head);

      var meta = [];
      if (r.temperature_c != null) { meta.push(fmtNum(r.temperature_c) + "°C"); }
      if (r.humidity_pct != null) { meta.push("hum " + fmtNum(r.humidity_pct, 0) + "%"); }
      if (r.wind_speed_kmh != null) {
        var w = "wind " + fmtNum(r.wind_speed_kmh) + " km/h";
        if (r.wind_direction_deg != null) { w += " @ " + fmtNum(r.wind_direction_deg, 0) + "°"; }
        meta.push(w);
      }
      if (r.pressure_hpa != null) { meta.push(fmtNum(r.pressure_hpa, 0) + " hPa"); }
      if (r.radiation_usv_h != null) { meta.push(fmtNum(r.radiation_usv_h, 2) + " µSv/h"); }
      if (r.radiation_cpm != null) { meta.push(fmtNum(r.radiation_cpm, 0) + " cpm"); }
      if (meta.length) { body.appendChild(el("span", "hw-meta", meta.join(" · "))); }

      item.appendChild(body);
      if (r.via === "aprs") {
        item.classList.add("hw-aprs");
        item.title = "Show " + r.name + " on the map";
        item.addEventListener("click", function () { openMiniMap(r); });
      } else {
        item.disabled = true;
      }
      container.appendChild(item);
    });
  }

  function renderForecasts(forecasts) {
    var container = document.getElementById("hw-forecasts");
    container.textContent = "";
    if (!forecasts.length) {
      container.appendChild(el("p", "muted", "No forecast data in MQTT yet"));
      return;
    }
    forecasts.forEach(function (f) {
      var box = el("section", "hw-forecast");
      var head = el("h3", "hw-forecast-head");
      head.appendChild(el("strong", null, f.name));
      head.appendChild(el("span", "hw-provider", f.provider));
      box.appendChild(head);

      var row = el("div", "hw-days");
      (f.daily || []).forEach(function (d) {
        var day = el("div", "hw-day");
        day.appendChild(el("span", "hw-day-date", d.date ? d.date.slice(5) : ""));
        day.appendChild(el("span", "wi hw-day-icon " + condIcon(d.condition)));
        var temps = (d.temperature_min_c != null ? fmtNum(d.temperature_min_c, 0) + "°" : "—") +
          " / " + (d.temperature_max_c != null ? fmtNum(d.temperature_max_c, 0) + "°" : "—");
        day.appendChild(el("span", "hw-day-temps", temps));
        day.appendChild(el("span", "hw-day-rain",
          d.precipitation_sum_mm != null && d.precipitation_sum_mm > 0 ? fmtNum(d.precipitation_sum_mm) + " mm" : ""));
        row.appendChild(day);
      });
      box.appendChild(row);
      container.appendChild(box);
    });
  }

  function refresh() {
    fetch("/api/weather")
      .then(function (resp) { return resp.ok ? resp.json() : null; })
      .then(function (data) {
        if (data) {
          renderReports(data.reports || []);
          renderForecasts(data.forecasts || []);
          renderWeatherMap(data.reports || []);
        }
      })
      .catch(function () { /* transient — next poll retries */ });
  }

  // Mini-map popup for APRS stations: Leaflet loads on demand from the
  // same CDN the neighbourhood map uses.
  function ensureLeaflet(cb) {
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
    s.onerror = function () { /* offline: no map in the popup */ };
    document.body.appendChild(s);
  }

  function openMiniMap(report) {
    var dialog = document.getElementById("wmap-dialog");
    document.getElementById("wmap-title").textContent = report.name + " — APRS weather station";
    var meta = Number(report.latitude).toFixed(4) + ", " + Number(report.longitude).toFixed(4);
    if (report.generated_at) {
      meta += " · report " + report.generated_at.replace("T", " ").slice(0, 16) + "Z";
    }
    document.getElementById("wmap-meta").textContent = meta;
    dialog.showModal();
    ensureLeaflet(function () {
      if (!window.L) { return; }
      if (!miniMap) {
        miniMap = L.map("wmap-map", { attributionControl: false }).setView([report.latitude, report.longitude], 13);
        var dark = document.documentElement.getAttribute("data-theme") !== "light";
        L.tileLayer(dark
          ? "https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}"
          : "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 18 }).addTo(miniMap);
      } else {
        miniMap.setView([report.latitude, report.longitude], 13);
      }
      if (miniMarker) { miniMap.removeLayer(miniMarker); }
      miniMarker = L.marker([report.latitude, report.longitude]).addTo(miniMap);
      miniMarker.bindPopup("<strong>" + report.name + "</strong>").openPopup();
      window.setTimeout(function () { if (miniMap) { miniMap.invalidateSize(); } }, 80);
    });
  }

  var dialog = document.getElementById("wmap-dialog");
  if (dialog) {
    dialog.querySelector(".wmap-close").addEventListener("click", function () { dialog.close(); });
    dialog.addEventListener("click", function (e) { if (e.target === dialog) { dialog.close(); } });
  }

  // Weather overview map: every report gets a temperature pin at its own
  // location (internet providers at their configured coordinates, APRS
  // stations at their positions). Clicking a pin scrolls the page to the
  // matching report card in the list below.
  function hwTileURL() {
    var dark = document.documentElement.getAttribute("data-theme") !== "light";
    return dark
      ? "https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}"
      : "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
  }

  function hwTileLabel() {
    var dark = document.documentElement.getAttribute("data-theme") !== "light";
    var labelEl = document.getElementById("hw-tiles-attrib");
    if (labelEl) {
      labelEl.innerHTML = dark
        ? '<a href="https://www.esri.com/" target="_blank" rel="noopener noreferrer">Esri</a> World Dark Gray'
        : '<a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a>';
    }
  }

  function syncWeatherTiles() {
    if (!hwMap || !hwTileLayer) {
      return;
    }
    hwMap.removeLayer(hwTileLayer);
    hwTileLayer = L.tileLayer(hwTileURL(), { maxZoom: 18 }).addTo(hwMap);
    hwTileLabel();
  }

  function focusReport(i) {
    var item = document.getElementById("hw-report-" + i);
    if (!item) {
      return;
    }
    item.scrollIntoView({ behavior: "smooth", block: "center" });
    item.classList.add("hw-flash");
    window.setTimeout(function () { item.classList.remove("hw-flash"); }, 1600);
  }

  function renderWeatherMap(reports) {
    var wrap = document.querySelector(".hw-map-wrap");
    var points = (reports || []).filter(function (r) {
      return r && r.latitude && r.longitude && (r.latitude !== 0 || r.longitude !== 0);
    });
    if (!points.length) {
      if (wrap) { wrap.hidden = true; }
      return;
    }
    if (wrap) { wrap.hidden = false; }

    ensureLeaflet(function () {
      if (!window.L) { return; }
      if (!hwMap) {
        hwMap = L.map("hw-map", { attributionControl: false }).setView([points[0].latitude, points[0].longitude], 11);
        hwTileLayer = L.tileLayer(hwTileURL(), { maxZoom: 18 }).addTo(hwMap);
        hwMarkerLayer = L.layerGroup().addTo(hwMap);
        hwTileLabel();
        if (window.MutationObserver) {
          new MutationObserver(syncWeatherTiles).observe(document.documentElement, {
            attributes: true, attributeFilter: ["data-theme"]
          });
        }
      }
      hwMarkerLayer.clearLayers();
      var bounds = [];
      points.forEach(function (r, i) {
        var label = r.temperature_c != null ? Math.round(r.temperature_c) + "°" : "·";
        var cls = "hw-pin" + (r.via === "aprs" ? " hw-pin-aprs" : " hw-pin-inet");
        var icon = L.divIcon({
          className: "hw-pin-wrap",
          iconSize: [40, 22],
          iconAnchor: [20, 11],
          html: '<span class="' + cls + '">' + label + "</span>"
        });
        var marker = L.marker([r.latitude, r.longitude], { icon: icon });
        var tip = r.name + (r.temperature_c != null ? " · " + fmtNum(r.temperature_c) + "°C" : "");
        marker.bindTooltip(tip, { direction: "top" });
        marker.on("click", function () { focusReport(i); });
        hwMarkerLayer.addLayer(marker);
        bounds.push([r.latitude, r.longitude]);
      });
      if (bounds.length > 1) {
        hwMap.fitBounds(bounds, { padding: [28, 28], maxZoom: 13 });
      } else {
        hwMap.setView(bounds[0], 12);
      }
      window.setTimeout(function () { if (hwMap) { hwMap.invalidateSize(); } }, 80);
    });
  }

  var tab = document.querySelector('.home-tab[data-tab="tab-weather"]');
  if (tab) {
    tab.addEventListener("click", function () {
      if (!loaded) {
        loaded = true;
        refresh();
        window.setInterval(refresh, POLL_MS);
      }
    });
  }
})();

// Public home page: APRS neighbourhood map (tab 2) — Leaflet map centered
// on our locator with the collection-radius circle, a RainViewer radar
// overlay and the stations held in the retained MQTT state, polled every
// 30 seconds. Leaflet and the radar tiles load from CDNs only when the
// APRS hub is enabled (the map element only exists then).
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
  var radiusKm = parseFloat(el.getAttribute("data-radius") || "0");
  var ownCall = el.getAttribute("data-callsign") || "";

  var map = null;
  var stationLayer = null;
  var radarLayer = null;
  var baseLayer = null;

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
  }

  function esc(s) {
    var d = document.createElement("div");
    d.textContent = s == null ? "" : String(s);
    return d.innerHTML;
  }

  // fmtTime renders an RFC 3339 timestamp compactly (UTC, minutes).
  function fmtTime(v) {
    if (!v) {
      return "";
    }
    var s = String(v).replace("T", " ");
    return s.length > 16 ? s.slice(0, 16) + "Z" : s;
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

  function initMap() {
    if (map || !window.L) {
      return;
    }
    map = L.map(el, { attributionControl: false }).setView([lat, lon], 11);
    map.createPane("aprsBase");
    map.getPane("aprsBase").style.zIndex = 200;
    map.createPane("aprsRadar");
    map.getPane("aprsRadar").style.zIndex = 350;
    map.getPane("aprsRadar").style.pointerEvents = "none";
    baseLayer = null;
    syncBaseLayer();
    stationLayer = L.layerGroup().addTo(map);

    // Our station marker + collection-radius circle.
    L.circleMarker([lat, lon], {
      radius: 7, color: "#fff", weight: 2,
      fillColor: "#007a3d", fillOpacity: 1
    }).addTo(map).bindTooltip(ownCall || "Our station", { direction: "top" });
    if (radiusKm > 0) {
      L.circle([lat, lon], {
        radius: radiusKm * 1000,
        color: "#007a3d", weight: 2, opacity: 0.7, dashArray: "10 6",
        fillColor: "#007a3d", fillOpacity: 0.06, interactive: false
      }).addTo(map);
    }

    enableRadar();
    refreshStations();
    window.setInterval(refreshStations, STATION_POLL_MS);
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
        window.setTimeout(function () { if (map) { map.invalidateSize(); } }, 60);
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
    radarLayer = layer.addTo(map);
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
    var html = '<span class="aprs-sym-img" style="background-image:url(\'' + APRS_SPRITES[table] +
      "\');background-position:-" + (col * APRS_SYM_SIZE) + "px -" + (row * APRS_SYM_SIZE) + 'px"></span>';
    return L.divIcon({
      className: "aprs-sym",
      iconSize: [APRS_SYM_SIZE, APRS_SYM_SIZE],
      iconAnchor: [APRS_SYM_SIZE / 2, APRS_SYM_SIZE / 2],
      html: html
    });
  }

  function refreshStations() {
    fetch("/api/aprs/stations")
      .then(function (r) { return r.ok ? r.json() : []; })
      .then(function (stations) {
        if (!stationLayer) {
          return;
        }
        stationLayer.clearLayers();
        (stations || []).forEach(function (s) {
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
            popup += "<br>Sent: " + esc(fmtTime(s.last_packet_at));
          }
          popup += "<br>Heard: " + esc(fmtTime(s.last_heard_at));
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
          var icon = aprsSymbolMarker(s);
          var marker;
          if (icon) {
            marker = L.marker([s.position.latitude, s.position.longitude], { icon: icon, riseOnHover: true });
          } else {
            marker = L.circleMarker([s.position.latitude, s.position.longitude], {
              radius: 7, color: tilesForTheme().marker.color, weight: 2,
              fillColor: tilesForTheme().marker.fillColor, fillOpacity: 0.9
            });
            marker._aprsMarker = true;
          }
          marker.bindTooltip(esc(s.callsign), { direction: "top" });
          marker.bindPopup(popup);
          stationLayer.addLayer(marker);
        });
      })
      .catch(function () { /* transient — next poll retries */ });
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

// Public home page: collapse the about intro to three lines with a
// More/Less toggle (button hidden when the text already fits).
(function () {
  "use strict";

  function initAbout() {
    var box = document.getElementById("home-about");
    if (!box) {
      return;
    }
    var text = box.querySelector(".home-about-text");
    var btn = box.querySelector(".home-about-toggle");
    if (!text || !btn) {
      return;
    }
    if (text.scrollHeight <= text.clientHeight + 2) {
      btn.hidden = true; // short enough — nothing to expand
      return;
    }
    btn.addEventListener("click", function () {
      var expanded = box.classList.toggle("expanded");
      btn.textContent = expanded ? "Less" : "More";
      btn.setAttribute("aria-expanded", expanded ? "true" : "false");
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAbout);
  } else {
    initAbout();
  }
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
        areas: "niepolomice, wieliczka",
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
      statusEl.textContent = "Subscribing to " + topic + "… (" + windowSecs + " s)";
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
    var field = function (id) { return document.getElementById(id); };

    document.querySelectorAll(".user-edit-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        field("user-edit-id").value = btn.dataset.id || "0";
        field("user-edit-username").value = btn.dataset.username || "";
        field("user-edit-phone").value = btn.dataset.phone || "";
        field("user-edit-email").value = btn.dataset.email || "";
        field("user-edit-discord").value = btn.dataset.discord || "";
        field("user-edit-role").value = btn.dataset.role || "";
        field("user-edit-aprs").value = btn.dataset.aprs || "";
        field("user-edit-password").value = "";
        var err = dialog.querySelector(".login-error");
        if (err) {
          err.remove();
        }
        if (typeof dialog.showModal === "function") {
          dialog.showModal();
        } else {
          dialog.setAttribute("open", "");
        }
      });
    });

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
