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

  function esc(s) {
    var d = document.createElement("div");
    d.textContent = s == null ? "" : String(s);
    return d.innerHTML;
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
    var script = document.createElement("script");
    script.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
    script.onload = cb;
    script.onerror = function () {
      el.innerHTML = "<p class=\"muted\">Map library failed to load (offline?).</p>";
    };
    document.body.appendChild(script);
  }

  function initMap() {
    if (map || !window.L) {
      return;
    }
    map = L.map(el, { attributionControl: false }).setView([lat, lon], 11);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 18
    }).addTo(map);
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

    // The tab is hidden until activated: fix the size once visible.
    var tab = document.querySelector('.home-tab[data-tab="tab-aprs"]');
    if (tab) {
      tab.addEventListener("click", function () {
        window.setTimeout(function () { if (map) { map.invalidateSize(); } }, 60);
      });
    }
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
      opacity: 0.55, maxNativeZoom: 7, maxZoom: 12
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

  function refreshStations() {
    fetch("/api/aprs/stations")
      .then(function (r) { return r.ok ? r.json() : []; })
      .then(function (stations) {
        if (!stationLayer) {
          return;
        }
        stationLayer.clearLayers();
        (stations || []).forEach(function (s) {
          if (!s || !s.position) {
            return;
          }
          var popup = "<strong>" + esc(s.callsign) + "</strong>";
          if (s.comment) {
            popup += "<br>" + esc(s.comment);
          }
          if (s.distance_km) {
            popup += "<br>" + Number(s.distance_km).toFixed(1) + " km";
          }
          if (s.last_heard_at) {
            popup += "<br>" + esc(String(s.last_heard_at).replace("T", " ").slice(0, 16)) + "Z";
          }
          var marker = L.circleMarker([s.position.latitude, s.position.longitude], {
            radius: 7, color: "#0d47a1", weight: 2,
            fillColor: "#42a5f5", fillOpacity: 0.9
          });
          marker.bindTooltip(esc(s.callsign), { direction: "top" });
          marker.bindPopup(popup);
          stationLayer.addLayer(marker);
        });
      })
      .catch(function () { /* transient — next poll retries */ });
  }

  loadLeaflet(initMap);
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
