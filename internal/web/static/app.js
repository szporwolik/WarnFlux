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
    { path: "/partials/actions", id: "actions-section" }
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
