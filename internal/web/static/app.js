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
