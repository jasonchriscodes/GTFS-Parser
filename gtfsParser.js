// === Helpers (unchanged) ===
function parseGtfsTime(t) {
  if (!t || typeof t !== "string" || t.indexOf(":") === -1) return null; // ← guard
  var parts = t.split(":").map(Number);
  var h = (parts[0] || 0) % 24,
    m = parts[1] || 0,
    s = parts[2] || 0;
  return new Date(Date.UTC(1970, 0, 1, h, m, s));
}

function formatGtfsTime(t) {
  if (!t || typeof t !== "string" || t.indexOf(":") === -1) return ""; // ← guard
  var p = t.split(":"),
    h = parseInt(p[0], 10) || 0,
    m = p[1] || "00";
  return ("0" + h).slice(-2) + ":" + m;
}

function getAbbreviation(name) {
  if (!name || typeof name !== "string") return ""; // ← guard
  return name
    .split("/")
    .map(function (c) {
      c = c.trim();
      if (c === c.toUpperCase() && c.indexOf(" ") < 0) return c;
      if (c.toLowerCase().indexOf("stop ") === 0) c = c.substring(5);
      return c
        .split(/[\s-]+/)
        .map(function (w) {
          return w.charAt(0).toUpperCase();
        })
        .join("");
    })
    .join("/");
}

function nearestIndex(pt, coords) {
  var best = 0,
    bestD = Infinity;
  coords.forEach(function (c, i) {
    var dx = c[1] - pt[1],
      dy = c[0] - pt[0],
      d = dx * dx + dy * dy;
    if (d < bestD) {
      bestD = d;
      best = i;
    }
  });
  return best;
}

// Read and clean the CSVs from the zip once
async function parseZip(file) {
  async function toArrayBuffer(f) {
    if (f && typeof f.arrayBuffer === "function") return await f.arrayBuffer();
    return await new Promise(function (resolve, reject) {
      var fr = new FileReader();
      fr.onload = (e) => resolve(e.target.result);
      fr.onerror = reject;
      fr.readAsArrayBuffer(f);
    });
  }
  var data = await toArrayBuffer(file);
  var zip = await JSZip.loadAsync(data);

  function readCsv(fn) {
    var zf = zip.file(fn);
    if (!zf) throw "Missing file in GTFS zip: " + fn;
    return zf.async("string").then(function (s) {
      var rows = Papa.parse(s, { header: true }).data;
      var cleaned = rows.filter(function (r) {
        if (!r) return false;
        for (var k in r)
          if (r[k] != null && String(r[k]).trim() !== "") return true;
        return false;
      });
      if (cleaned.length === 0) {
        throw "The file '" + fn + "' is present but contains no records.";
      }
      return cleaned;
    });
  }
  function readCsvOptional(fn) {
    var zf = zip.file(fn);
    if (!zf) return Promise.resolve([]);
    return zf.async("string").then(function (s) {
      var rows = Papa.parse(s, { header: true }).data;
      return rows.filter(function (r) {
        if (!r) return false;
        for (var k in r)
          if (r[k] != null && String(r[k]).trim() !== "") return true;
        return false;
      });
    });
  }

  var res = await Promise.all([
    readCsv("trips.txt"),
    readCsv("stop_times.txt"),
    readCsv("stops.txt"),
    readCsv("shapes.txt"),
    readCsvOptional("routes.txt"),
  ]);
  return {
    trips: res[0],
    stop_times: res[1],
    stops: res[2],
    shapes: res[3],
    routes: res[4],
  };
}

// GTFS spec: if timepoint is omitted, treat it as 1 (exact time)
// https://gtfs.org/schedule/reference/#stop_timestxt
function toTimePoint(v) {
  if (v === undefined || v === null || v === "") return 1; // default exact
  const s = String(v).trim().toLowerCase();
  if (s === "1" || s === "true" || s === "t" || s === "yes" || s === "y")
    return 1;
  if (s === "0" || s === "false" || s === "f" || s === "no" || s === "n")
    return 0;
  // Fallback: coerce numeric truthiness
  return Number.isFinite(+s) ? (+s ? 1 : 0) : 1;
}

// Build outputs for a specific route_id + direction_id
function buildFromGtfs(gtfs, provider, routeId, dirId) {
  var trips = gtfs.trips,
    stop_times = gtfs.stop_times,
    stops = gtfs.stops,
    shapes = gtfs.shapes;

  // Filter by user input
  var filtered = trips.filter(function (r) {
    return r.route_id === routeId && +r.direction_id === +dirId;
  });

  // Keep only trips that have at least one stop_time row
  var filteredWithTimes = filtered.filter(function (tr) {
    return stop_times.some(function (st) {
      return st.trip_id === tr.trip_id;
    });
  });
  if (!filteredWithTimes.length) {
    throw (
      "No stop_times found for route_id=" + routeId + " direction_id=" + dirId
    );
  }

  // Choose a shape_id (use the most frequent among filtered trips)
  var shapeCounts = {};
  filteredWithTimes.forEach(function (tr) {
    shapeCounts[tr.shape_id] = (shapeCounts[tr.shape_id] || 0) + 1;
  });
  var shapeId = Object.keys(shapeCounts).sort(function (a, b) {
    return shapeCounts[b] - shapeCounts[a];
  })[0];

  // Shape points
  var shapePts = shapes
    .filter(function (s) {
      return s.shape_id === shapeId;
    })
    .sort(function (a, b) {
      return +a.shape_pt_sequence - +b.shape_pt_sequence;
    })
    .map(function (r) {
      return [+r.shape_pt_lat, +r.shape_pt_lon];
    });
  if (!shapePts.length) throw "No shapes found for shape_id=" + shapeId;

  // Merge and order stops (across the filtered trips)
  var routeStops = stop_times
    .filter(function (st) {
      return filteredWithTimes.some(function (tr) {
        return tr.trip_id === st.trip_id;
      });
    })
    .map(function (st) {
      var info = stops.find(function (s) {
        return s.stop_id === st.stop_id;
      });
      if (!info) return null;
      return Object.assign({}, st, info);
    })
    .filter(Boolean);

  routeStops.forEach(function (rs) {
    rs.pos = nearestIndex([+rs.stop_lat, +rs.stop_lon], shapePts);
  });
  routeStops.sort(function (a, b) {
    return a.pos - b.pos;
  });

  // Build busRouteData
  var next_points = [];
  for (var i = 0; i < routeStops.length - 1; i++) {
    var prev = routeStops[i],
      nxt = routeStops[i + 1];
    var pt1 = parseGtfsTime(prev.departure_time);
    var pt2 = parseGtfsTime(nxt.departure_time);
    var durMin = pt1 && pt2 ? (pt2 - pt1) / 60000 : 0;
    if (durMin < 0) durMin = 0;
    next_points.push({
      latitude: +nxt.stop_lat,
      longitude: +nxt.stop_lon,
      address: nxt.stop_name || "",
      duration: durMin.toFixed(1) + " minutes",
      route_coordinates: shapePts
        .slice(prev.pos, nxt.pos + 1)
        .map(function (c) {
          return [c[1], c[0]];
        }),
    });
  }

  var busRouteData = routeStops.length
    ? [
        {
          starting_point: {
            latitude: +routeStops[0].stop_lat,
            longitude: +routeStops[0].stop_lon,
            address: routeStops[0].stop_name || "",
          },
          next_points: next_points,
        },
      ]
    : [];

  // Build scheduleData (join stop_times ↔ stops)
  var scheduleData = filteredWithTimes
    .map(function (tr, i) {
      var times = stop_times
        .filter(function (st) {
          return st.trip_id === tr.trip_id;
        })
        .sort(function (a, b) {
          return +a.stop_sequence - +b.stop_sequence;
        });
      if (!times.length) return null;

      var busStops = times.map(function (st, j) {
        var meta =
          stops.find(function (s) {
            return s.stop_id === st.stop_id;
          }) || {};
        var addr = meta.stop_name || "";
        return {
          name: "Stop " + (j + 1),
          time: formatGtfsTime(st.departure_time),
          latitude: meta.stop_lat != null ? +meta.stop_lat : null,
          longitude: meta.stop_lon != null ? +meta.stop_lon : null,
          address: addr,
          abbreviation: addr ? getAbbreviation(addr) : "",
          time_point: toTimePoint(st.timepoint ?? st.time_point),
        };
      });

      var first = times[0],
        last = times[times.length - 1];
      return {
        routeNo: "Route " + (i + 1),
        startTime: formatGtfsTime(first && first.departure_time),
        endTime: formatGtfsTime(last && last.arrival_time),
        runName: tr.route_id,
        busStops: busStops,
      };
    })
    .filter(Boolean);

  return {
    busRouteData: busRouteData,
    scheduleData: scheduleData,
  };
}

// DEBUG: read first few lines from stops.txt and print as plain text
async function previewStops(file) {
  // Convert File/Blob → ArrayBuffer so JSZip is happy everywhere
  async function toArrayBuffer(f) {
    if (f && typeof f.arrayBuffer === "function") return await f.arrayBuffer();
    return await new Promise(function (resolve, reject) {
      var fr = new FileReader();
      fr.onload = function (e) {
        resolve(e.target.result);
      };
      fr.onerror = function (err) {
        reject(err);
      };
      fr.readAsArrayBuffer(f);
    });
  }

  var data = await toArrayBuffer(file);
  var zip = await JSZip.loadAsync(data);

  var zf = zip.file("stops.txt");
  if (!zf) throw "Missing stops.txt in the GTFS zip";

  var text = await zf.async("string");
  // split on CRLF, LF, CR, and unicode line separators just in case
  var lines = text.split(/\r\n|\n|\r|\u2028|\u2029/);
  // keep non-empty lines
  var nonEmpty = lines.filter(function (l) {
    return l && l.trim() !== "";
  });
  var first10 = nonEmpty.slice(0, 10);

  return {
    file: "stops.txt",
    totalLines: lines.length,
    shown: first10.length,
    snippet: first10.join("\n"), // plain text
  };
}

// === Widget lifecycle & event binding ===
self.onInit = function () {
  var ctrl = this,
    ctx = this.ctx;

  // initial state
  ctx.selectedProvider = "Auckland Transport";
  ctx.isDragOver = false;
  ctx.tps = {};
  ctx.routeFilters = new Set();

  // file input
  var root =
    ctx && ctx.$container && ctx.$container[0] ? ctx.$container[0] : document;
  var dropZoneEl = root.querySelector(".file-drop-zone");
  var providerSelect = document.getElementById("providerSelect");
  var fileInput = document.getElementById("gtfsFileInput");
  var routesById = {};
  var routeIdList = [];
  var generateBtn = document.getElementById("generateBtn");
  var downloadBusBtn = document.getElementById("downloadBusRouteBtn");
  var downloadSchBtn = document.getElementById("downloadScheduleBtn");
  var overlay = document.getElementById("loadingOverlay");
  const routeFilterSelect = document.getElementById("routeFilterSelect");
  const routeFilterChips = document.getElementById("routeFilterChips");
  const schoolInput = document.getElementById("schoolFileInput");
  const gtfsDz = root.querySelector(".file-drop-zone.gtfs");
  const schoolDz = root.querySelector(".file-drop-zone.school");
  const gtfsStatus = document.getElementById("gtfsStatus");
  const schoolStatus = document.getElementById("schoolStatus");

  // ---------- Custom stops (user-defined) ----------
  ctx.customStops = ctx.customStops || []; // [{id,name,lat,lon}]
  ctx.customStopById = ctx.customStopById || new Map();
  ctx.customStopTokens = ctx.customStopTokens || new Map(); // col.id -> token
  ctx.customStopTokenSeq = ctx.customStopTokenSeq || 0;

  // Routing cache: "lon,lat|lon,lat" -> {coords, duration_s, distance_m}
  ctx.routeApiCache = ctx.routeApiCache || new Map();

  const customStopNameEl = document.getElementById("customStopName");
  const customStopLatEl = document.getElementById("customStopLat");
  const customStopLonEl = document.getElementById("customStopLon");
  const addCustomStopBtn = document.getElementById("addCustomStopBtn");
  const customStopChips = document.getElementById("customStopChips");
  const customStopStatus = document.getElementById("customStopStatus");

  function showCustomStopStatus(state, text) {
    if (!customStopStatus) return;
    customStopStatus.hidden = false;
    customStopStatus.className = "zip-status " + (state || "info");
    customStopStatus.textContent = text || "";
    // auto-hide success/info after a bit
    if (state === "ok" || state === "info") {
      setTimeout(() => {
        if (customStopStatus) customStopStatus.hidden = true;
      }, 1800);
    }
  }

  function newCustomStopId() {
    return "cs_" + Date.now() + "_" + Math.random().toString(16).slice(2);
  }

  function isValidLatLon(lat, lon) {
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
    return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
  }

  function upsertCustomStop(cs) {
    ctx.customStopById.set(cs.id, cs);
    const i = (ctx.customStops || []).findIndex((x) => x.id === cs.id);
    if (i >= 0) ctx.customStops[i] = cs;
    else ctx.customStops.push(cs);
  }

  function removeCustomStop(id) {
    ctx.customStops = (ctx.customStops || []).filter((x) => x.id !== id);
    ctx.customStopById.delete(id);
  }

  function renderCustomStopChips() {
    if (!customStopChips) return;
    customStopChips.innerHTML = "";

    (ctx.customStops || []).forEach((cs) => {
      const chip = document.createElement("div");
      chip.className = "chip";

      const t = document.createElement("span");
      t.className = "chip-text";
      t.textContent = `${cs.name} (${cs.lat.toFixed(5)},${cs.lon.toFixed(5)})`;

      const x = document.createElement("button");
      x.type = "button";
      x.className = "chip-x";
      x.textContent = "×";
      x.title = "Remove custom stop";
      x.onclick = () => {
        removeCustomStop(cs.id);
        renderCustomStopChips();
        renderColumns(); // refresh trip/break/rep dropdowns
      };

      chip.appendChild(t);
      chip.appendChild(x);
      customStopChips.appendChild(chip);
    });
  }

  // initial paint (if any)
  renderCustomStopChips();

  addCustomStopBtn?.addEventListener("click", () => {
    const name = (customStopNameEl?.value || "").trim();
    const lat = parseFloat((customStopLatEl?.value || "").trim());
    const lon = parseFloat((customStopLonEl?.value || "").trim());

    if (!name) return showCustomStopStatus("err", "Enter a stop name.");
    if (!isValidLatLon(lat, lon))
      return showCustomStopStatus("err", "Enter valid lat/lon.");

    const cs = { id: newCustomStopId(), name, lat, lon };
    upsertCustomStop(cs);

    if (customStopNameEl) customStopNameEl.value = "";
    if (customStopLatEl) customStopLatEl.value = "";
    if (customStopLonEl) customStopLonEl.value = "";

    showCustomStopStatus("ok", "Custom stop added.");
    renderCustomStopChips();
    renderColumns();
  });

  function nextFrame() {
    return new Promise(requestAnimationFrame);
  }
  function nextTick() {
    return new Promise((r) => setTimeout(r, 0));
  }
  async function showOverlay(text) {
    setLoading(true, text);
    await nextFrame(); // flush style
    await nextTick(); // give the browser a turn to paint
  }

  function hasJsonText(id) {
    const t = (document.getElementById(id)?.textContent || "").trim();
    // crude but effective: JSON we output starts with { or [
    return t.startsWith("{") || t.startsWith("[");
  }

  function setLoading(on, text) {
    if (overlay) {
      overlay.classList.toggle("show", !!on);
      overlay.setAttribute("aria-hidden", on ? "false" : "true");
      var t = overlay.querySelector(".loading-text");
      if (t && text) t.textContent = text;
    }

    fileInput.disabled = !!on;
    schoolInput.disabled = !!on;
    if (providerSelect) providerSelect.disabled = !!on;

    if (on) {
      generateBtn.disabled = true;
      downloadBusBtn.disabled = true;
      downloadSchBtn.disabled = true;
    } else {
      validateUI(); // ← recompute proper enabled/disabled
      downloadBusBtn.disabled = !hasJsonText("busRouteJson");
      downloadSchBtn.disabled = !hasJsonText("scheduleJson");
    }

    document
      .querySelector(".gtfs-widget")
      ?.setAttribute("aria-busy", on ? "true" : "false");
  }

  // let the browser paint the overlay before heavy work
  function afterPaint() {
    return new Promise(requestAnimationFrame);
  }

  fileInput.addEventListener("change", function (evt) {
    ctrl.onFileSelect(evt.target.files);
  });

  schoolInput.addEventListener("change", (evt) => {
    ctrl.onSchoolSelect(evt.target.files);
  });
  // enable/disable download buttons based on content
  function setDownloadEnabled(enabled) {
    downloadBusBtn.disabled = !enabled;
    downloadSchBtn.disabled = !enabled;
  }

  // Robust download with spinner + fallbacks
  async function downloadJSONFromPre(preId, filename, btn) {
    const pre = document.getElementById(preId);
    const text = (pre?.textContent || "").trim();
    const old = btn.textContent;

    if (!text) {
      btn.textContent = "Nothing to download";
      setTimeout(() => (btn.textContent = old), 1500);
      return;
    }

    try {
      // Show the overlay *before* any heavy work so it actually paints.
      await showOverlay("Preparing download…");
      // Give the browser a turn to render:
      await new Promise(requestAnimationFrame);

      // Create the Blob/URL (can take time for big JSON).
      const blob = new Blob([text], { type: "application/json;charset=utf-8" });
      const url = URL.createObjectURL(blob);

      // Trigger download via a temporary <a>.
      const a = document.createElement("a");
      a.href = url;
      a.download = filename; // e.g., "scheduleData.json"
      document.body.appendChild(a);
      a.click();
      a.remove();

      // Revoke a bit later—revoking immediately can cancel the download in some browsers.
      setTimeout(() => URL.revokeObjectURL(url), 4000);

      // Hide spinner and show feedback on the button.
      setLoading(false);
      btn.textContent = "Downloaded!";
      setTimeout(() => (btn.textContent = old), 1500);
    } catch (e) {
      console.error("Download failed:", e);
      // Hide spinner if we showed it
      setLoading(false);
      btn.textContent = "Failed";
      setTimeout(() => (btn.textContent = old), 1800);
    }
  }

  downloadBusBtn.addEventListener("click", function () {
    downloadJSONFromPre("busRouteJson", "busRouteData.json", downloadBusBtn);
  });
  downloadSchBtn.addEventListener("click", function () {
    downloadJSONFromPre("scheduleJson", "scheduleData.json", downloadSchBtn);
  });

  // ---------- Trip Planner helpers ----------
  let tripColSeq = 0; // unique IDs for columns

  function parseHeadsign(headsign) {
    if (!headsign) return null;
    const re = /^(.*?)\s+to\s+(.*?)(?:\s+via\s+.*)?$/i; // "A to B via C"
    const m = headsign.trim().match(re);
    if (!m) return null;
    const dep = m[1].trim();
    const dest = m[2].trim();
    const viaMatch = headsign.match(/\bvia\s+(.*)$/i);
    const via = viaMatch ? viaMatch[1].trim() : "";
    return { dep, dest, via };
  }

  function collectDepDest(gtfs, routeIdFilter) {
    const deps = new Set();
    const dests = new Set();
    const viaMap = new Map(); // key "dep||dest" -> via (last seen)
    (gtfs.trips || []).forEach((tr) => {
      if (routeIdFilter && tr.route_id !== routeIdFilter) return;
      const p = parseHeadsign(tr.trip_headsign);
      if (!p) return;
      deps.add(p.dep);
      dests.add(p.dest);
      if (p.via) viaMap.set(p.dep + "||" + p.dest, p.via);
    });
    const sort = (a, b) =>
      a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" });
    return { deps: [...deps].sort(sort), dests: [...dests].sort(sort), viaMap };
  }

  function findCandidateTrip(gtfs, dep, dest, routeIdFilter) {
    if (!dep || !dest) return null;
    const candidates = (gtfs.trips || []).filter((tr) => {
      if (routeIdFilter && tr.route_id !== routeIdFilter) return false;
      const p = parseHeadsign(tr.trip_headsign);
      return p && p.dep === dep && p.dest === dest;
    });
    if (!candidates.length) return null;
    const st = gtfs.stop_times || [];
    const timesForTrip = (id) => {
      const rows = st
        .filter((r) => r.trip_id === id)
        .sort((a, b) => +a.stop_sequence - +b.stop_sequence);
      if (!rows.length) return null;
      const first = rows[0],
        last = rows[rows.length - 1];
      return {
        depTime: first.departure_time || first.arrival_time || "",
        arrTime: last.arrival_time || last.departure_time || "",
      };
    };
    let best = null;
    candidates.forEach((tr) => {
      const t = timesForTrip(tr.trip_id);
      if (!t) return;
      if (!best || (t.depTime || "") < (best.times.depTime || "")) {
        best = { trip: tr, times: t };
      }
    });
    return best;
  }

  function hhmm(t) {
    if (!t) return "";
    const [h = "00", m = "00"] = String(t).split(":");
    return `${h.padStart(2, "0")}:${m.padStart(2, "0")}`;
  }

  function durationHHMM(start, end) {
    const toMin = (t) => {
      const [h = "0", m = "0", s = "0"] = String(t).split(":").map(Number);
      return h * 60 + m + (s >= 30 ? 1 : 0);
    };
    const a = toMin(start),
      b = toMin(end);
    if (!isFinite(a) || !isFinite(b)) return "";
    const d = Math.max(0, b - a);
    const H = Math.floor(d / 60),
      M = d % 60;
    return (H > 0 ? `${H}h ` : "") + `${M}m`;
  }

  var zipStatus = document.getElementById("zipStatus");

  function explainZipError(e, file) {
    const raw = e && e.message ? e.message : String(e || "");
    if (/Missing file in GTFS zip:/i.test(raw)) {
      const missing = raw.replace(/^.*:\s*/, "");
      return `The ZIP is missing a required GTFS file: <code>${missing}</code>.<br>
            Make sure it includes <code>trips.txt</code>, <code>stop_times.txt</code>, <code>stops.txt</code>, and <code>shapes.txt</code>.`;
    }
    if (/present but contains no records|no records/i.test(raw)) {
      return raw; // already human readable from step 1
    }
    if (
      /End of data|central directory|invalid|corrupt|compression|CRC/i.test(raw)
    ) {
      return `The selected file ${
        file ? `<code>${file.name}</code>` : "you chose"
      } isn’t a valid ZIP archive or is corrupted.`;
    }
    if (/FileReader|Security|NotAllowed/i.test(raw)) {
      return `Your browser blocked reading the file. Try a smaller file or a different browser.`;
    }
    return `Couldn’t read the ZIP.
          <details style="margin-top:6px">
            <summary>Technical details</summary>
            <pre style="white-space:pre-wrap;margin:6px 0 0">${raw}</pre>
          </details>`;
  }

  function setZipStatus(state, text) {
    if (!zipStatus) return;
    zipStatus.hidden = false;
    zipStatus.className = "zip-status " + state; // state: ok | err | info
    zipStatus.textContent = text || "";
  }

  function setStatusHTML(el, state, html) {
    if (!el) return;
    el.hidden = false; // show the badge
    el.className = "zip-status " + state; // "ok" | "err" | "info"
    el.innerHTML = html; // allow <code>…</code>
  }

  // ---------- Planner state & DOM ----------
  ctx.planner = {
    deps: [],
    dests: [],
    viaMap: new Map(),
    cols: [], // {id, dep, dest, via, depTime, arrTime, duration}
  };

  const plannerEl = document.getElementById("planner");
  const tripColsEl = document.getElementById("tripColumns");
  const addTripBtn = document.getElementById("addTripBtn");
  const rosterStartEl = document.getElementById("rosterStart");
  const rosterEndEl = document.getElementById("rosterEnd");

  // Turn vertical mouse wheel into horizontal pan for convenience
  tripColsEl.addEventListener(
    "wheel",
    (e) => {
      if (Math.abs(e.deltaY) > Math.abs(e.deltaX)) {
        tripColsEl.scrollLeft += e.deltaY;
        e.preventDefault();
      }
    },
    { passive: false },
  );

  // start with roster UI disabled until a ZIP is loaded
  rosterStartEl.disabled = true;
  rosterEndEl.disabled = true;

  function isRosterSet() {
    return !!(ctx.roster.start && ctx.roster.end);
  }
  function selectedTripCount() {
    return ctx.planner.cols.filter((c) => !!c.tripId).length;
  }

  function canRemoveTrip() {
    // Only allow removal when a roster window is chosen AND we have >1 columns
    return !!ctx.rosterReady && ctx.planner.cols.length > 1;
  }

  function readyTrip(c) {
    return c.kind === "trip" && !!c.tripId;
  }
  function readySchool(c) {
    return (
      c.kind === "school" &&
      !!c.schoolRouteId &&
      !!c.schoolStart &&
      !!c.schoolEnd
    );
  }
  /** At least one item that can produce schedule/busRoute outputs */
  function hasAnySchedulable() {
    return ctx.planner.cols.some((c) => readyTrip(c) || readySchool(c));
  }

  function validateUI() {
    // Roster inputs enabled only after zip is loaded
    rosterStartEl.disabled = !hasAnyData();
    rosterEndEl.disabled = !hasAnyData();

    // Trips UI gated by roster (selects themselves also check this; see renderColumns)
    const hasTripsData = hasAnyData();
    addTripBtn.disabled = !hasTripsData || !ctx.rosterReady;

    // Generate needs: any data (GTFS or School), roster chosen, and ≥1 schedulable (trip OR school)
    generateBtn.disabled = !(
      hasTripsData &&
      ctx.rosterReady &&
      hasAnySchedulable()
    );
  }

  // Always start with 1 empty column so the UI isn't blank
  if (!ctx.planner.cols.length) addTripColumn();
  renderColumns();

  // Build selects (disabled until ZIP is loaded)
  function makeSelect(opts, value) {
    const sel = document.createElement("select");
    const def = document.createElement("option");
    def.value = "";
    def.textContent = "— Select —";
    sel.appendChild(def);
    opts.forEach((v) => {
      const o = document.createElement("option");
      o.value = v;
      o.textContent = v;
      sel.appendChild(o);
    });
    sel.value = value || "";
    sel.disabled = !ctx.gtfs || !opts.length; // ← keep disabled before ZIP
    return sel;
  }

  function recomputeColumn(col) {
    const cand =
      ctx.gtfs && col.routeId && col.dep && col.dest
        ? findCandidateTripForRoute(
            ctx.gtfs,
            col.routeId,
            col.dep,
            col.dest,
            ctx.roster,
          )
        : null;

    // get 'via' from the route’s headsign map
    let via = "";
    if (ctx.gtfs && col.routeId && col.dep && col.dest) {
      const { viaMap } = collectDepDestForRoute(ctx.gtfs, col.routeId);
      via = viaMap.get(col.dep + "||" + col.dest) || "";
    }
    col.via = via;

    if (!cand) {
      col.depTime = "";
      col.arrTime = "";
      col.duration = "";
      return;
    }
    col.tripId = cand.trip.trip_id;
    col.depTime = cand.times.depTime || "";
    col.arrTime = cand.times.arrTime || "";
    col.duration = durationHHMM(col.depTime, col.arrTime);
  }

  function metaHtml(c) {
    // custom trip meta (route API)
    if (c.customLeg && c.customLeg.kind === "customTrip") {
      const start = c.customLeg.start?.name || "Start";
      const end = c.customLeg.end?.name || "Custom stop";
      const pending = c.customLeg.pending
        ? `<div>Routing: calculating…</div>`
        : "";
      const err = c.customLeg.error
        ? `<div style="color:#a4000f">Routing failed: ${c.customLeg.error}</div>`
        : "";
      const dist = c.customLeg.distance_m
        ? `<div>Distance: ${km(c.customLeg.distance_m).toFixed(2)} km</div>`
        : "";
      const dur = c.customLeg.duration_s
        ? `<div>ETA: ${Math.round(mins(c.customLeg.duration_s))} min</div>`
        : "";
      const depEff = effDepHHMM(c);
      const arrEff = effArrHHMM(c);
      const dep = depEff
        ? `<div>Dep: ${depEff}${c.depOverride ? " (edited)" : ""}</div>`
        : "";
      const arr = arrEff
        ? `<div>Arr: ${arrEff}${c.arrOverride ? " (edited)" : ""}</div>`
        : "";
      return (
        `<div>Custom leg: ${start} →${end}</div>` +
        dep +
        arr +
        dist +
        dur +
        pending +
        err
      );
    }

    if (c.kind === "break") {
      const st = c.breakStart ? `<div>Start: ${hhmm(c.breakStart)}</div>` : "";
      const en = c.breakEnd ? `<div>End: ${hhmm(c.breakEnd)}</div>` : "";
      const du = `<div>Duration: ${Math.max(
        0,
        Number(c.breakMin) || 0,
      )} min</div>`;
      return (
        st + en + du || `<div class="small-text">Set a break duration</div>`
      );
    }
    if (c.kind === "reposition") {
      const stop = STOPS_BY_ID.get(c.repStopId);
      const to = stop ? stop.stop_name : "";
      const st = c.repStart ? `<div>Start: ${hhmm(c.repStart)}</div>` : "";
      const en = c.repEnd ? `<div>End: ${hhmm(c.repEnd)}</div>` : "";
      const toLine = `<div>To: ${to || "—"}</div>`;
      const du = `<div>Duration: ${Math.max(
        0,
        Number(c.repMin) || 0,
      )} min</div>`;
      return (
        toLine + st + en + du || `<div class="small-text">Choose a stop</div>`
      );
    }
    const via = c.via ? `<div>Via: ${c.via}</div>` : "";
    const depEff = effDepHHMM(c);
    const arrEff = effArrHHMM(c);
    const dep = depEff
      ? `<div>Dep: ${depEff}${c.depOverride ? " (edited)" : ""}</div>`
      : "";
    const arr = arrEff
      ? `<div>Arr: ${arrEff}${c.arrOverride ? " (edited)" : ""}</div>`
      : "";
    const dur =
      depEff && arrEff
        ? `<div>Duration: ${durationHHMM(depEff, arrEff)}</div>`
        : "";
    return (
      via + dep + arr + dur ||
      `<div class="small-text">Pick Route → Departure → Destination</div>`
    );
  }

  function renderColumns() {
    tripColsEl.innerHTML = "";
    ctx.planner.cols.forEach((col) => {
      const wrap = document.createElement("div");
      wrap.className = "trip-col";

      // header
      const head = document.createElement("div");
      head.className = "col-head";
      head.innerHTML = `<span>Trip ${col.id}</span>`;
      const act = document.createElement("div");
      act.className = "col-actions";
      const removeBtn = document.createElement("button");
      removeBtn.textContent = "Remove";

      // TYPE selector
      const typeLabel = document.createElement("label");
      typeLabel.textContent = "Type";
      const typeSel = document.createElement("select");
      ["trip", "break", "signIn", "signOff", "reposition", "school"].forEach(
        (v) => {
          const o = document.createElement("option");
          o.value = v;
          o.textContent =
            v === "reposition"
              ? "Reposition (REP)"
              : v === "signIn"
                ? "Sign On"
                : v === "signOff"
                  ? "Sign Off"
                  : v === "school"
                    ? "School run"
                    : v[0].toUpperCase() + v.slice(1);
          typeSel.appendChild(o);
        },
      );

      typeSel.value = col.kind || "trip";
      typeSel.disabled = !ctx.rosterReady || !ctx.gtfs;
      const hasData =
        (ctx.gtfs && (ctx.gtfs.trips || []).length) ||
        (ctx.school && ctx.school.routes && ctx.school.routes.length);
      typeSel.disabled = !ctx.rosterReady || !hasData;
      typeSel.onchange = () => {
        col.kind = typeSel.value;

        if (
          col.kind === "break" ||
          col.kind === "signIn" ||
          col.kind === "signOff"
        ) {
          col.routeId =
            col.dep =
            col.dest =
            col.via =
            col.depTime =
            col.arrTime =
            col.duration =
            col.tripId =
              "";
          col.depOverride = "";
          col.arrOverride = "";
          if (!col.breakMin && col.breakMin !== 0) col.breakMin = 15;
          col.repStopId = "";
          col.repStart = col.repEnd = "";
          col.schoolRouteId = "";
          col.schoolStart = col.schoolEnd = "";
        } else if (col.kind === "reposition") {
          col.routeId =
            col.dep =
            col.dest =
            col.via =
            col.depTime =
            col.arrTime =
            col.duration =
            col.tripId =
              "";
          col.depOverride = "";
          col.arrOverride = "";
          col.breakStart = col.breakEnd = "";
          col.repMin = col.repMin ?? 15;
          col.repStopId = "";
          col.repStart = col.repEnd = "";
          col.schoolRouteId = "";
          col.schoolStart = col.schoolEnd = "";
        } else if (col.kind === "school") {
          // clear trip/break/rep fields
          col.routeId =
            col.dep =
            col.dest =
            col.via =
            col.depTime =
            col.arrTime =
            col.duration =
            col.tripId =
              "";
          col.depOverride = "";
          col.arrOverride = "";
          col.breakStart = col.breakEnd = "";
          col.repStopId = "";
          col.repStart = col.repEnd = "";
          // ensure school defaults
          col.schoolRouteId =
            col.schoolRouteId || ctx.school.routes[0]?.id || "";
          col.schoolStart = "";
          col.schoolEnd = "";
        } else {
          // trip
          col.breakStart = col.breakEnd = "";
          col.repStopId = "";
          col.repStart = col.repEnd = "";
          col.schoolRouteId = "";
          col.schoolStart = col.schoolEnd = "";
        }

        const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
        recomputeSequentialFrom(Math.max(0, idx));
      };

      // remove button state/handler
      removeBtn.disabled = !canRemoveTrip();
      removeBtn.title = !ctx.rosterReady
        ? "Pick a roster window to enable removing trips"
        : ctx.planner.cols.length <= 1
          ? "At least one trip column is required"
          : "Remove this trip";
      removeBtn.onclick = () => {
        if (!canRemoveTrip()) return;
        delete ctx.tps[col.id]; // clean timing-point state for this column
        ctx.planner.cols = ctx.planner.cols.filter((c) => c.id !== col.id);
        recomputeSequentialFrom(0);
      };

      act.appendChild(removeBtn);
      head.appendChild(act);
      wrap.appendChild(head);

      // ---------- two-pane body ----------
      const twoPane = document.createElement("div");
      twoPane.className = "two-pane";
      const left = document.createElement("div");
      left.className = "left-pane";
      const right = document.createElement("div");
      right.className = "right-pane";

      // ROUTE/DEP/DEST (Trip) UI
      if ((col.kind || "trip") === "trip") {
        // ROUTE selector
        const routeLabel = document.createElement("label");
        routeLabel.textContent = "Route";
        const routeSel = document.createElement("select");
        const rDef = document.createElement("option");
        rDef.value = "";
        rDef.textContent = "— Select route —";
        routeSel.appendChild(rDef);

        // Use filtered list if any filters are chosen; otherwise all routes
        const ids = currentRouteIds();

        // Keep the currently selected route visible even if not in the filter
        const ensureCurrent =
          col.routeId && !ids.includes(col.routeId) ? [col.routeId] : [];
        [...new Set([...ensureCurrent, ...ids])].forEach((id) => {
          const o = document.createElement("option");
          o.value = id;
          o.textContent = formatRouteLabel(id, routesById);
          routeSel.appendChild(o);
        });

        routeSel.value = col.routeId || "";
        routeSel.disabled = !ctx.gtfs || ids.length === 0 || !ctx.rosterReady;
        routeSel.onchange = () => {
          col.routeId = routeSel.value || "";
          col.depOverride = "";
          col.arrOverride = "";
          col.dep =
            col.dest =
            col.depTime =
            col.arrTime =
            col.duration =
            col.via =
            col.tripId =
              "";
          const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
          recomputeSequentialFrom(Math.max(0, idx));
        };
        left.appendChild(routeLabel);
        left.appendChild(routeSel);

        // DEPARTURE selector
        const depLabel = document.createElement("label");
        depLabel.textContent = "Departure";
        const depSel = document.createElement("select");
        const dDef = document.createElement("option");
        dDef.value = "";
        dDef.textContent = "— Select —";
        depSel.appendChild(dDef);

        let depOpts = [];
        if (col.routeId)
          depOpts = collectDepDestForRoute(ctx.gtfs || {}, col.routeId).deps;
        depOpts.forEach((v) => {
          const o = document.createElement("option");
          o.value = v;
          o.textContent = v;
          depSel.appendChild(o);
        });
        depSel.value = col.dep || "";
        depSel.disabled =
          !ctx.rosterReady || !col.routeId || depOpts.length === 0;
        depSel.onchange = () => {
          col.dep = depSel.value || "";
          col.depOverride = "";
          col.arrOverride = "";
          col.dest =
            col.depTime =
            col.arrTime =
            col.duration =
            col.via =
            col.tripId =
              "";
          const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
          recomputeSequentialFrom(Math.max(0, idx));
        };
        left.appendChild(depLabel);
        left.appendChild(depSel);

        // DEST + arrival inline
        const destLabel = document.createElement("label");
        destLabel.textContent = "Destination";
        const row = document.createElement("div");
        row.className = "inline";

        const destSel = document.createElement("select");
        const tDef = document.createElement("option");
        tDef.value = "";
        tDef.textContent = "— Select —";
        destSel.appendChild(tDef);

        let destOpts = [];
        if (col.routeId && col.dep) {
          destOpts = destinationsForRouteAndDep(
            ctx.gtfs || {},
            col.routeId,
            col.dep,
          );
        }
        destOpts.forEach((v) => {
          const o = document.createElement("option");
          o.value = v;
          o.textContent = v;
          destSel.appendChild(o);
        });
        destSel.value = col.dest || "";
        destSel.disabled =
          !ctx.rosterReady || !col.routeId || !col.dep || destOpts.length === 0;

        const arrive = document.createElement("div");
        arrive.className = "small-text";
        const effArr = effArrHHMM(col);
        arrive.textContent = effArr
          ? `arrives ${effArr}${col.arrOverride ? " (edited)" : ""}`
          : "";

        destSel.onchange = async () => {
          col.dest = destSel.value || "";
          col.depOverride = "";
          col.arrOverride = "";
          col.tripId = col.via = col.depTime = col.arrTime = col.duration = "";
          try {
            await showOverlay("Finding arrival time…");
            const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
            recomputeSequentialFrom(Math.max(0, idx));
          } finally {
            setLoading(false);
          }
        };

        row.appendChild(destSel);
        row.appendChild(arrive);
        left.appendChild(destLabel);
        left.appendChild(row);

        // Editable times (default from GTFS, clamped to roster)
        const depTimeLabel = document.createElement("label");
        depTimeLabel.textContent = "Departure time";
        const depCtl = makeRosteredTimeControl(
          effDepHHMM(col),
          ctx.rosterReady ? ctx.roster : null,
          (v) => {
            col.depOverride = v || "";
            // keep Arr >= Dep along the 24h ring
            if (
              col.arrOverride &&
              forwardArcMinutes(col.depOverride, col.arrOverride) < 0
            ) {
              col.arrOverride = col.depOverride;
            }
            const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
            recomputeSequentialFrom(Math.max(0, idx));
          },
          1,
          "— Dep —",
        );

        const arrTimeLabel = document.createElement("label");
        arrTimeLabel.textContent = "Arrival time";
        const arrCtl = makeRosteredTimeControl(
          effArrHHMM(col),
          ctx.rosterReady ? ctx.roster : null,
          (v) => {
            col.arrOverride = v || "";
            if (
              col.depOverride &&
              col.arrOverride &&
              forwardArcMinutes(col.depOverride, col.arrOverride) < 0
            ) {
              col.arrOverride = col.depOverride;
            }
            const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
            recomputeSequentialFrom(Math.max(0, idx));
          },
          1,
          "— Arr —",
        );

        // Optional: disable until a trip is fully targeted
        depCtl.disabled =
          !ctx.rosterReady || !col.routeId || !col.dep || !col.dest;
        arrCtl.disabled =
          !ctx.rosterReady || !col.routeId || !col.dep || !col.dest;

        left.appendChild(depTimeLabel);
        left.appendChild(depCtl);
        left.appendChild(arrTimeLabel);
        left.appendChild(arrCtl);

        // meta (via/dep/arr/duration) – stays in left pane
        const meta = document.createElement("div");
        meta.className = "meta";
        meta.innerHTML = metaHtml(col);
        left.appendChild(meta);

        // -------- Right pane: Timing Points UI (only when trip is selected) --------
        if (col.tripId) {
          // Bind timing-point state for this trip
          if (!ctx.tps[col.id] || ctx.tps[col.id].tripId !== col.tripId) {
            ctx.tps[col.id] = { tripId: col.tripId, custom: new Set() };
          }
          const { stops, autoTP } = getTripStopsAndAutoTP(ctx.gtfs, col.tripId);
          const custom = ctx.tps[col.id].custom;
          const selectedUnion = new Set([...autoTP, ...custom]);

          const tpSec = document.createElement("div");
          tpSec.className = "tp-section";
          const title = document.createElement("div");
          title.className = "pane-title";
          title.textContent = "Timing points";
          tpSec.appendChild(title);

          // Add dropdown row
          const tpRow = document.createElement("div");
          tpRow.className = "tp-row";
          const sel = document.createElement("select");
          const def = document.createElement("option");
          def.value = "";
          def.textContent = "— Add intermediate stop —";
          sel.appendChild(def);

          stops.forEach((s) => {
            if (s.isFirst || s.isLast) return;
            if (selectedUnion.has(s.stop_id)) return;
            const o = document.createElement("option");
            o.value = s.stop_id;
            o.textContent = `${s.j} — ${s.abbr || "—"} — ${s.name || ""}`;
            sel.appendChild(o);
          });

          const addBtn = document.createElement("button");
          addBtn.className = "btn small";
          addBtn.type = "button";
          addBtn.textContent = "Add";
          addBtn.disabled = !sel.options || sel.options.length <= 1;
          addBtn.onclick = () => {
            const v = sel.value;
            if (!v) return;
            if (!custom.has(v) && !autoTP.has(v)) {
              custom.add(v);
              renderColumns();
            }
          };

          tpRow.appendChild(sel);
          tpRow.appendChild(addBtn);
          tpSec.appendChild(tpRow);

          // Selected timing points list (union of GTFS + custom)
          const list = document.createElement("div");
          list.className = "tp-list";
          stops.forEach((s) => {
            if (s.isFirst || s.isLast) return;
            const isAuto = autoTP.has(s.stop_id);
            const isCustom = custom.has(s.stop_id);
            if (!(isAuto || isCustom)) return;

            const item = document.createElement("div");
            item.className = "tp-item";

            const leftBits = document.createElement("div");
            leftBits.className = "tp-left";
            const idx = document.createElement("span");
            idx.className = "tp-idx";
            idx.textContent = s.j;
            const ab = document.createElement("span");
            ab.className = "tp-abbr";
            ab.textContent = s.abbr || "—";
            const nm = document.createElement("span");
            nm.textContent = s.name || "";

            leftBits.appendChild(idx);
            leftBits.appendChild(ab);
            leftBits.appendChild(nm);

            const rightBits = document.createElement("div");
            if (isAuto && !isCustom) {
              const tag = document.createElement("span");
              tag.className = "tp-tag";
              tag.textContent = "GTFS";
              rightBits.appendChild(tag);
            } else {
              const rm = document.createElement("button");
              rm.className = "tp-remove";
              rm.type = "button";
              rm.textContent = "Remove";
              rm.onclick = () => {
                custom.delete(s.stop_id);
                renderColumns();
              };
              rightBits.appendChild(rm);
            }

            item.appendChild(leftBits);
            item.appendChild(rightBits);
            list.appendChild(item);
          });

          if (!list.childElementCount) {
            const hint = document.createElement("div");
            hint.className = "tp-hint";
            hint.textContent = "No intermediate timing points yet.";
            tpSec.appendChild(hint);
          }

          tpSec.appendChild(list);
          const foot = document.createElement("div");
          foot.className = "tp-hint";
          foot.textContent = "First/last stops are always included.";
          tpSec.appendChild(foot);

          right.appendChild(tpSec);
        } else {
          const hint = document.createElement("div");
          hint.className = "tp-hint";
          hint.textContent =
            "Select Route, Departure, Destination to manage timing points.";
          right.appendChild(hint);
        }
      } else if (
        col.kind === "break" ||
        col.kind === "signIn" ||
        col.kind === "signOff"
      ) {
        // BREAK UI in left pane; right pane hidden
        const bDurLabel = document.createElement("label");
        const kindLabel =
          col.kind === "signIn"
            ? "Sign On"
            : col.kind === "signOff"
              ? "Sign off"
              : "Break";
        bDurLabel.textContent = `${kindLabel} duration (minutes)`;
        const bDur = document.createElement("input");
        bDur.type = "number";
        bDur.min = "1";
        bDur.step = "1";
        bDur.value = Math.max(1, Number(col.breakMin || 15));
        bDur.oninput = () => {
          col.breakMin = Math.max(1, parseInt(bDur.value, 10) || 1);
          const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
          recomputeSequentialFrom(Math.max(0, idx));
        };

        const bLocLabel = document.createElement("label");
        bLocLabel.textContent = `${kindLabel} location (optional)`;
        const bLocSel = document.createElement("select");

        // default = use last trip end stop
        const defLoc = document.createElement("option");
        defLoc.value = "";
        defLoc.textContent = "— Use last trip end stop —";
        bLocSel.appendChild(defLoc);

        // custom stops
        if (ctx.customStops && ctx.customStops.length) {
          const og = document.createElement("optgroup");
          og.label = "Custom stops";
          ctx.customStops.forEach((cs) => {
            const o = document.createElement("option");
            o.value = "custom:" + cs.id;
            o.textContent = "★ " + cs.name;
            og.appendChild(o);
          });
          bLocSel.appendChild(og);
        }

        // GTFS stops (filtered like REP)
        const opts = filteredStopsOptions();
        if (opts.length) {
          const og2 = document.createElement("optgroup");
          og2.label = "GTFS stops";
          opts.forEach((s) => {
            const o = document.createElement("option");
            o.value = String(s.stop_id);
            o.textContent = stopLabel(s);
            og2.appendChild(o);
          });
          bLocSel.appendChild(og2);
        }

        bLocSel.value = col.breakStopId || "";
        bLocSel.disabled =
          !ctx.rosterReady || (!opts.length && !(ctx.customStops || []).length);
        bLocSel.onchange = () => {
          col.breakStopId = bLocSel.value || "";
          renderColumns();
        };

        left.appendChild(bLocLabel);
        left.appendChild(bLocSel);

        const bStartLabel = document.createElement("label");
        bStartLabel.textContent = "Start";
        const bStart = document.createElement("input");
        bStart.type = "time";
        bStart.disabled = true;
        bStart.value = col.breakStart ? hhmm(col.breakStart) : "";

        const bEndLabel = document.createElement("label");
        bEndLabel.textContent = "End";
        const bEnd = document.createElement("input");
        bEnd.type = "time";
        bEnd.disabled = true;
        bEnd.value = col.breakEnd ? hhmm(col.breakEnd) : "";

        left.appendChild(bDurLabel);
        left.appendChild(bDur);
        left.appendChild(bStartLabel);
        left.appendChild(bStart);
        left.appendChild(bEndLabel);
        left.appendChild(bEnd);
        right.style.display = "none";
      } else if (col.kind === "reposition") {
        const rDurLabel = document.createElement("label");
        rDurLabel.textContent = "Reposition duration (minutes)";
        const rDur = document.createElement("input");
        rDur.type = "number";
        rDur.min = "1";
        rDur.step = "1";
        rDur.value = Math.max(1, Number(col.repMin || 15));
        rDur.oninput = () => {
          col.repMin = Math.max(1, parseInt(rDur.value, 10) || 1);
          const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
          recomputeSequentialFrom(Math.max(0, idx));
        };

        const rToLabel = document.createElement("label");
        rToLabel.textContent = "Reposition to";

        const rToSel = document.createElement("select");
        const def = document.createElement("option");
        def.value = "";
        def.textContent = "— Select stop —";
        rToSel.appendChild(def);

        // Custom stops
        if (ctx.customStops && ctx.customStops.length) {
          const og = document.createElement("optgroup");
          og.label = "Custom stops";
          ctx.customStops.forEach((cs) => {
            const o = document.createElement("option");
            o.value = "custom:" + cs.id;
            o.textContent = "★ " + cs.name;
            og.appendChild(o);
          });
          rToSel.appendChild(og);
        }

        // GTFS stops
        const opts = filteredStopsOptions();
        opts.forEach((s) => {
          const o = document.createElement("option");
          o.value = String(s.stop_id);
          o.textContent = stopLabel(s);
          rToSel.appendChild(o);
        });

        rToSel.value = col.repStopId || "";
        rToSel.disabled =
          !ctx.rosterReady || (!opts.length && !(ctx.customStops || []).length);

        rToSel.onchange = () => {
          col.repStopId = rToSel.value || "";
          const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
          recomputeSequentialFrom(Math.max(0, idx));
        };

        const rStartLabel = document.createElement("label");
        rStartLabel.textContent = "Start";
        const rStart = document.createElement("input");
        rStart.type = "time";
        rStart.disabled = true;
        rStart.value = col.repStart ? hhmm(col.repStart) : "";

        const rEndLabel = document.createElement("label");
        rEndLabel.textContent = "End";
        const rEnd = document.createElement("input");
        rEnd.type = "time";
        rEnd.disabled = true;
        rEnd.value = col.repEnd ? hhmm(col.repEnd) : "";

        left.appendChild(rDurLabel);
        left.appendChild(rDur);
        left.appendChild(rToLabel);
        left.appendChild(rToSel);
        left.appendChild(rStartLabel);
        left.appendChild(rStart);
        left.appendChild(rEndLabel);
        left.appendChild(rEnd);

        right.style.display = "none";
      } else if (col.kind === "school") {
        // LEFT pane: route + times
        const sRouteLabel = document.createElement("label");
        sRouteLabel.textContent = "School route";
        const sRouteSel = document.createElement("select");
        const def = document.createElement("option");
        def.value = "";
        def.textContent = ctx.school.routes.length
          ? "— Select —"
          : "— Load GeoJSON above —";
        sRouteSel.appendChild(def);

        (ctx.school.routes || []).forEach((r) => {
          const o = document.createElement("option");
          o.value = r.id;
          o.textContent = `${r.number ? r.number + " — " : ""}${r.name}`;
          sRouteSel.appendChild(o);
        });
        sRouteSel.value = col.schoolRouteId || "";
        sRouteSel.disabled = !ctx.school.routes.length || !ctx.rosterReady;
        sRouteSel.onchange = () => {
          col.schoolRouteId = sRouteSel.value || "";
          renderColumns();
        };

        // Normalize start/end to roster & ordering
        if (ctx.rosterReady) {
          col.schoolStart = clampHHMMToRoster(
            col.schoolStart || ctx.roster.start,
            ctx.roster.start,
            ctx.roster.end,
          );
          // Do NOT default End to Start; leave blank so UI shows “— End —”
          if (col.schoolEnd) {
            col.schoolEnd = clampHHMMToRoster(
              col.schoolEnd,
              ctx.roster.start,
              ctx.roster.end,
            );
            if (forwardArcMinutes(col.schoolStart, col.schoolEnd) < 0) {
              col.schoolEnd = col.schoolStart;
            }
          }
        }

        // compute the "not before" threshold from previous column(s)
        const thisIdx = ctx.planner.cols.findIndex((c) => c.id === col.id);
        const mustBeAfter = chainThresholdBeforeIndex(thisIdx);

        // Build roster-limited pickers
        const sStartLabel = document.createElement("label");
        sStartLabel.textContent = "Start";
        const sStart = makeRosteredTimeControl(
          col.schoolStart,
          ctx.rosterReady ? ctx.roster : null,
          (v) => {
            // Enforce strictly after previous finish
            let vv = v || "";
            if (vv && forwardArcMinutes(mustBeAfter, vv) <= 0) {
              vv = mustBeAfter; // auto-bump to threshold if user somehow selects earlier
            }
            col.schoolStart = vv;
            // Keep End ≥ Start (along the ring)
            if (
              col.schoolEnd &&
              forwardArcMinutes(col.schoolStart, col.schoolEnd) < 0
            ) {
              col.schoolEnd = col.schoolStart;
            }
            const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
            recomputeSequentialFrom(Math.max(0, idx));
          },
          1,
          "— Start —",
          mustBeAfter, // ← NEW: remove earlier options from dropdown
        );

        const sEndLabel = document.createElement("label");
        sEndLabel.textContent = "End";
        const sEnd = makeRosteredTimeControl(
          col.schoolEnd,
          ctx.rosterReady ? ctx.roster : null,
          (v) => {
            let vv = v || "";
            // Enforce strictly after previous arrival/threshold
            if (vv && forwardArcMinutes(mustBeAfter, vv) <= 0) {
              vv = mustBeAfter;
            }
            // Keep End ≥ Start
            if (col.schoolStart && forwardArcMinutes(col.schoolStart, vv) < 0) {
              vv = col.schoolStart;
            }
            col.schoolEnd = vv;
            const idx = ctx.planner.cols.findIndex((c) => c.id === col.id);
            recomputeSequentialFrom(Math.max(0, idx));
          },
          1,
          "— End —",
          mustBeAfter, // ← removes any option before previous trip arrival
        );

        left.appendChild(sRouteLabel);
        left.appendChild(sRouteSel);
        left.appendChild(sStartLabel);
        left.appendChild(sStart);
        left.appendChild(sEndLabel);
        left.appendChild(sEnd);

        // RIGHT pane: compact preview
        const meta = document.createElement("div");
        meta.className = "meta";
        const r = ctx.school.byId.get(col.schoolRouteId);
        meta.innerHTML = r
          ? `<div>From: ${r.depName}</div><div>To: ${r.destName}</div><div>Coords: ${r.coords.length} pts</div>`
          : `<div class="small-text">Choose a school route and set times</div>`;
        right.appendChild(meta);

        right.style.display = "block";
      }

      // IMPORTANT: attach left/right INTO twoPane
      twoPane.appendChild(left);
      twoPane.appendChild(right);

      // TYPE row goes above two-pane
      const typeRow = document.createElement("div");
      typeRow.style.display = "grid";
      typeRow.style.gridTemplateColumns = "auto 1fr";
      typeRow.style.gap = "8px";
      typeRow.appendChild(typeLabel);
      typeRow.appendChild(typeSel);

      // Assemble card in order
      wrap.appendChild(head); // already present in your code
      wrap.appendChild(typeRow); // <- append, don't insertBefore
      wrap.appendChild(twoPane); // <- then append the two-pane
      tripColsEl.appendChild(wrap);
    });
  }

  function addTripColumn() {
    ctx.planner.cols.push({
      id: ++tripColSeq,
      kind: "trip",
      routeId: "",
      dep: "",
      dest: "",
      via: "",
      depTime: "",
      arrTime: "",
      duration: "",
      tripId: "",
      depOverride: "",
      arrOverride: "",
      breakMin: 15,
      breakStart: "",
      breakEnd: "",
      repMin: 15,
      repStopId: "",
      repStart: "",
      repEnd: "",
      schoolRouteId: "", // one of ctx.school.routes[].id
      schoolStart: "", // "HH:MM"
      schoolEnd: "", // "HH:MM"
      breakStopId: "", // "" means use last trip end; can be "stop_id" or "custom:<id>"
    });
    renderColumns();
    validateUI();

    // ⬇️ auto-pan to the newly added card
    requestAnimationFrame(() => {
      tripColsEl.lastElementChild?.scrollIntoView({
        behavior: "smooth",
        inline: "start",
        block: "nearest",
      });
    });
  }

  addTripBtn.addEventListener("click", addTripColumn);

  // After ZIP selected and parsed:
  ctrl.onFileSelect = async function (files) {
    try {
      if (!files || !files.length) throw "No file selected.";
      const file = files[0];

      // Early sanity checks
      if (!/\.zip$/i.test(file.name)) {
        setZipStatus(
          "err",
          `This doesn’t look like a ZIP file: <code>${file.name}</code>`,
        );
        throw "Selected file is not a .zip";
      }
      if (file.size === 0) {
        setZipStatus(
          "err",
          `The file <code>${file.name}</code> is empty (0 bytes).`,
        );
        throw "Empty file";
      }

      setLoading(true, "Loading ZIP...");
      setZipStatus("info", "Reading ZIP…");

      // Parse the GTFS archive
      ctx.gtfs = await parseZip(file);
      STOPS_BY_ID = stopsByIdMap(ctx.gtfs.stops || []);
      routesById = indexByRouteId(ctx.gtfs.routes || []);
      routeIdList = Array.from(
        new Set((ctx.gtfs.trips || []).map((t) => t.route_id)),
      ).sort(naturalCompare);

      ctx.tripEndpoints = buildTripEndpointsIndex(ctx.gtfs);

      // reset filter on new ZIP and render UI
      ctx.routeFilters = new Set();
      renderRouteFilter();
      fileInput.value = "";

      // After parsing zip:
      ctx.roster.start = rosterStartEl.value || "";
      ctx.roster.end = rosterEndEl.value || "";
      ctx.rosterReady = isRosterSet();
      rosterStartEl.disabled = !hasAnyData();
      rosterEndEl.disabled = !hasAnyData();

      validateUI();

      // Refresh planner lists & recompute
      recomputeSequentialFrom(0);

      // Recompute button state
      validateUI();

      const promptMsg =
        "ZIP loaded. In Trips: pick Route → Departure → Destination, then click Generate.";
      document.getElementById("busRouteJson").textContent = promptMsg;
      document.getElementById("scheduleJson").textContent = promptMsg;

      // Friendly success summary
      const routes = new Set((ctx.gtfs.trips || []).map((t) => t.route_id))
        .size;
      const trips = (ctx.gtfs.trips || []).length;
      const stops = (ctx.gtfs.stops || []).length;
      const shapes = (ctx.gtfs.shapes || []).length;

      setZipStatus(
        "ok",
        `ZIP loaded: <code>${file.name}</code> • ${routes} route${
          routes === 1 ? "" : "s"
        } • ${trips} trip${trips === 1 ? "" : "s"} • ${stops} stop${
          stops === 1 ? "" : "s"
        } • ${shapes} shape row${shapes === 1 ? "" : "s"}`,
      );
      gtfsDz?.classList.add("loaded");
      setStatusHTML(
        gtfsStatus,
        "ok",
        `ZIP loaded: <code>${file.name}</code> • …`,
      );
    } catch (e) {
      const message = explainZipError(e, files && files[0]);
      setZipStatus("err", message);
      const msg = "Error loading zip: " + (e && e.message ? e.message : e);
      document.getElementById("busRouteJson").textContent = msg;
      document.getElementById("scheduleJson").textContent = msg;
      dropZoneEl?.classList.remove("loaded");
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  function buildTripEndpointsIndex(gtfs) {
    const stopTimes = gtfs?.stop_times || [];
    const stops = gtfs?.stops || [];

    // stop_id -> stop_name
    const stopNameById = new Map();
    stops.forEach((s) => {
      stopNameById.set(String(s.stop_id), s.stop_name || "");
    });

    // trip_id -> { minSeq, startStopId, maxSeq, endStopId }
    const agg = new Map();

    for (const st of stopTimes) {
      const tripId = String(st.trip_id || "");
      const stopId = String(st.stop_id || "");
      const seq = parseInt(st.stop_sequence, 10);

      if (!tripId || !stopId || !Number.isFinite(seq)) continue;

      const cur = agg.get(tripId);
      if (!cur) {
        agg.set(tripId, {
          minSeq: seq,
          startStopId: stopId,
          maxSeq: seq,
          endStopId: stopId,
        });
        continue;
      }

      if (seq < cur.minSeq) {
        cur.minSeq = seq;
        cur.startStopId = stopId;
      }
      if (seq > cur.maxSeq) {
        cur.maxSeq = seq;
        cur.endStopId = stopId;
      }
    }

    // finalize: trip_id -> { startName, endName, startStopId, endStopId }
    const out = new Map();
    for (const [tripId, v] of agg.entries()) {
      out.set(tripId, {
        startStopId: v.startStopId,
        endStopId: v.endStopId,
        startName: stopNameById.get(v.startStopId) || "",
        endName: stopNameById.get(v.endStopId) || "",
      });
    }
    return out;
  }

  // Generate on click
  generateBtn.addEventListener("click", async function () {
    try {
      if (!hasAnyData()) throw "Load a GTFS ZIP and/or a School GeoJSON first.";
      await showOverlay("Building outputs…");
      setDownloadEnabled(false);

      // Build outputs in COLUMN ORDER so breaks land between trips
      const cols = ctx.planner.cols;
      const outBusRouteData = [];
      const outSchedule = [];
      let lastTripEndStop = null; // remember last trip's end stop for breaks/REP

      for (const col of cols) {
        if (col.kind === "trip") {
          if (col.tripId) {
            // normal GTFS trip
            const meta = (routesById || {})[col.routeId];
            const extraTP = Array.from(ctx.tps[col.id]?.custom || []);
            const res = buildFromTrip(
              ctx.gtfs,
              col.tripId,
              meta,
              extraTP,
              col.depOverride || null,
              col.arrOverride || null,
            );

            outBusRouteData.push(...res.busRouteData);

            const item = res.scheduleData[0];
            outSchedule.push(item);

            const bs = item.busStops || [];
            if (bs.length) lastTripEndStop = bs[bs.length - 1];
          } else if (
            col.customLeg &&
            col.customLeg.kind === "customTrip" &&
            !col.customLeg.pending
          ) {
            // custom leg trip
            const depHHMM = effDepHHMM(col);
            const arrHHMM = effArrHHMM(col);
            const res = buildFromCustomLeg(col, depHHMM, arrHHMM);

            outBusRouteData.push(...res.busRouteData);
            outSchedule.push(...res.scheduleData);

            const bs = res.scheduleData[0]?.busStops || [];
            if (bs.length) lastTripEndStop = bs[bs.length - 1];
          }
        } else if (col.kind === "reposition") {
          const startHHMM = hhmm(col.repStart);
          const endHHMM = hhmm(col.repEnd);

          // 1) Resolve destination stop for REP (custom:<id> or GTFS stop_id)
          const toStop = resolveStopAny(col.repStopId);

          // 2) Create scheduleData for REP:
          //    Stop S = previous Stop E (lastTripEndStop)
          //    Stop E = destination (toStop)
          const fromStop = lastTripEndStop;
          const repItem = makeRepositionScheduleItemFromTo(
            fromStop,
            toStop,
            startHHMM,
            endHHMM,
          );
          if (repItem) outSchedule.push(repItem);

          // 3) Build busRouteData for REP using ORS:
          //    FROM = previous Stop E location (lastTripEndStop)
          //    TO   = REP destination
          const fromLat = stopLat(fromStop);
          const fromLon = stopLon(fromStop);
          const toLat = stopLat(toStop);
          const toLon = stopLon(toStop);

          if (
            fromLat != null &&
            fromLon != null &&
            toLat != null &&
            toLon != null
          ) {
            try {
              const r = await fetchRouteORS(
                { lat: fromLat, lon: fromLon },
                { lat: toLat, lon: toLon },
              );

              // Ensure coords explicitly start/end at the chosen points
              let coords = (r.coords || []).slice(); // [ [lon,lat], ... ]
              const s = [Number(fromLon), Number(fromLat)];
              const e = [Number(toLon), Number(toLat)];

              if (!coords.length) coords = [s, e];

              const first = coords[0];
              const last = coords[coords.length - 1];
              if (first[0] !== s[0] || first[1] !== s[1]) coords.unshift(s);
              if (last[0] !== e[0] || last[1] !== e[1]) coords.push(e);

              const durMin = Number(r.duration_s || 0) / 60;

              outBusRouteData.push({
                starting_point: {
                  latitude: Number(fromLat),
                  longitude: Number(fromLon),
                  address: fromStop?.address || "",
                },
                next_points: [
                  {
                    latitude: Number(toLat),
                    longitude: Number(toLon),
                    address: toStop?.address || "",
                    duration: `${durMin.toFixed(1)} minutes`,
                    route_coordinates: coords,
                  },
                ],
              });
            } catch (e) {
              // If ORS fails, still keep scheduleData; just omit route coords
              console.warn("REP routing failed:", e);
              outBusRouteData.push({
                starting_point: {
                  latitude: Number(fromLat),
                  longitude: Number(fromLon),
                  address: fromStop?.address || "",
                },
                next_points: [
                  {
                    latitude: Number(toLat),
                    longitude: Number(toLon),
                    address: toStop?.address || "",
                    duration: "0.0 minutes",
                    route_coordinates: [
                      [Number(fromLon), Number(fromLat)],
                      [Number(toLon), Number(toLat)],
                    ],
                  },
                ],
              });
            }
          }

          // 4) Update current location to REP destination (even if routing skipped)
          if (toStop) {
            lastTripEndStop = {
              name: "Stop E",
              time: endHHMM,
              latitude: toLat,
              longitude: toLon,
              address: toStop.address || "",
              abbreviation: toStop.abbreviation || "",
            };
          } else if (repItem?.busStops?.length) {
            lastTripEndStop = repItem.busStops[repItem.busStops.length - 1];
          }
        } else if (
          col.kind === "break" ||
          col.kind === "signIn" ||
          col.kind === "signOff"
        ) {
          const startHHMM = hhmm(col.breakStart);
          const endHHMM = hhmm(col.breakEnd);

          // override location if user chose one
          let loc = lastTripEndStop;

          if (col.breakStopId) {
            if (String(col.breakStopId).startsWith("custom:")) {
              const id = String(col.breakStopId).slice("custom:".length);
              const cs = ctx.customStopById.get(id);
              if (cs) {
                loc = {
                  latitude: cs.lat,
                  longitude: cs.lon,
                  address: cs.name,
                  abbreviation: getAbbreviation(cs.name),
                };
              }
            } else {
              const s = STOPS_BY_ID.get(col.breakStopId);
              if (s) {
                loc = {
                  latitude: s.stop_lat != null ? +s.stop_lat : null,
                  longitude: s.stop_lon != null ? +s.stop_lon : null,
                  address: s.stop_name || "",
                  abbreviation: s.stop_name ? getAbbreviation(s.stop_name) : "",
                };
              }
            }
          }

          const runName =
            col.kind === "signIn"
              ? "Sign On"
              : col.kind === "signOff"
                ? "Sign Off"
                : "Break";

          const bItem = makeBreakScheduleItem(loc, startHHMM, endHHMM, runName);
          if (bItem) outSchedule.push(bItem);

          // IMPORTANT: update lastTripEndStop so REP after Sign On / Break has a FROM location
          if (bItem?.busStops?.length) {
            lastTripEndStop = bItem.busStops[bItem.busStops.length - 1];
          }
        } else if (col.kind === "school") {
          const r = ctx.school.byId.get(col.schoolRouteId);
          const startHHMM = hhmm(col.schoolStart);
          const endHHMM = hhmm(col.schoolEnd);

          if (r && startHHMM && endHHMM) {
            const res = buildFromSchool(r, startHHMM, endHHMM);
            outBusRouteData.push(...res.busRouteData);
            outSchedule.push(...res.scheduleData);

            const bs = res.scheduleData[0]?.busStops || [];
            if (bs.length) lastTripEndStop = bs[bs.length - 1];
          }
        }
      }

      // Guard: if user only added breaks (or no valid trips), there's nothing to output
      if (!outSchedule.length) {
        throw "Pick at least one Trip (breaks need a preceding trip).";
      }

      // Write busRouteData now
      document.getElementById("busRouteJson").textContent = JSON.stringify(
        outBusRouteData,
        null,
        2,
      );

      // Sort, roster-filter, and renumber runNo
      const toMin = (t) => {
        const [h = "0", m = "0"] = String(t || "")
          .split(":")
          .map(Number);
        return Number.isFinite(h) && Number.isFinite(m)
          ? h * 60 + m
          : Number.POSITIVE_INFINITY;
      };

      let scheduleData = outSchedule.slice();
      scheduleData.sort((a, b) => toMin(a.startTime) - toMin(b.startTime));
      scheduleData = scheduleData.map((item, idx) => ({
        ...item,
        runNo: String(idx + 1),
      }));

      // Filter to roster window
      let filteredSchedule = scheduleData.filter((it) => {
        if (
          String(it.runName || "")
            .toLowerCase()
            .startsWith("school")
        ) {
          return intervalOverlapsRoster(
            it.startTime,
            it.endTime,
            ctx.roster.start,
            ctx.roster.end,
          );
        }
        // original behavior for GTFS trips/Break/REP (by start time)
        return isInRosterWindow(it.startTime, ctx.roster.start, ctx.roster.end);
      });

      // Re-sort and re-number after roster filter
      filteredSchedule.sort((a, b) => toMin(a.startTime) - toMin(b.startTime));
      filteredSchedule = filteredSchedule.map((item, idx) => ({
        ...item,
        runNo: String(idx + 1),
      }));

      document.getElementById("scheduleJson").textContent = JSON.stringify(
        filteredSchedule,
        null,
        2,
      );

      setDownloadEnabled(true);
    } catch (e) {
      const msg = "Error: " + (e && e.message ? e.message : e);
      document.getElementById("busRouteJson").textContent = msg;
      document.getElementById("scheduleJson").textContent = msg;
      setDownloadEnabled(false);
    } finally {
      setLoading(false);
    }
  });

  if (gtfsDz) {
    gtfsDz.addEventListener("click", () => {
      if (overlay.classList.contains("show")) return;
      fileInput.click(); // #gtfsFileInput
    });
    gtfsDz.addEventListener("dragover", (e) => {
      e.preventDefault();
      gtfsDz.classList.add("dragover");
    });
    gtfsDz.addEventListener("dragleave", (e) => {
      e.preventDefault();
      gtfsDz.classList.remove("dragover");
    });
    gtfsDz.addEventListener("drop", (e) => {
      e.preventDefault();
      gtfsDz.classList.remove("dragover");
      const f = e.dataTransfer.files;
      if (f.length) ctrl.onFileSelect(f);
    });
  }

  if (schoolDz) {
    schoolDz.addEventListener("click", () => {
      if (overlay.classList.contains("show")) return;
      schoolInput.click(); // #schoolFileInput
    });
    schoolDz.addEventListener("dragover", (e) => {
      e.preventDefault();
      schoolDz.classList.add("dragover");
    });
    schoolDz.addEventListener("dragleave", (e) => {
      e.preventDefault();
      schoolDz.classList.remove("dragover");
    });
    schoolDz.addEventListener("drop", (e) => {
      e.preventDefault();
      schoolDz.classList.remove("dragover");
      const f = e.dataTransfer.files;
      if (f.length) ctrl.onSchoolSelect(f);
    });
  }

  // Build REP schedule item with Stop S (previous Stop E) + Stop E (destination)
  function makeRepositionScheduleItemFromTo(
    fromStop,
    toStop,
    startHHMM,
    endHHMM,
  ) {
    if (!toStop) return null;

    const busStops = [];

    // Stop S = previous Stop E (if available)
    if (fromStop) {
      busStops.push({
        name: "Stop S",
        time: startHHMM,
        latitude: stopLat(fromStop),
        longitude: stopLon(fromStop),
        address: fromStop.address || "",
        abbreviation:
          fromStop.abbreviation ||
          (fromStop.address ? getAbbreviation(fromStop.address) : ""),
        time_point: 1,
      });
    }

    // Stop E = destination stop
    busStops.push({
      name: "Stop E",
      time: endHHMM,
      latitude: stopLat(toStop),
      longitude: stopLon(toStop),
      address: toStop.address || "",
      abbreviation:
        toStop.abbreviation ||
        (toStop.address ? getAbbreviation(toStop.address) : ""),
      time_point: 1,
    });

    return {
      runNo: "",
      startTime: startHHMM,
      endTime: endHHMM,
      runName: "REP",
      busStops,
    };
  }

  function naturalCompare(a, b) {
    // stable-ish natural sort for strings with numbers
    return a.toString().localeCompare(b.toString(), undefined, {
      numeric: true,
      sensitivity: "base",
    });
  }

  function indexByRouteId(routes) {
    var map = {};
    (routes || []).forEach(function (r) {
      map[r.route_id] = r;
    });
    return map;
  }

  function formatRouteLabel(routeId, routesById) {
    var meta = routesById[routeId];
    var nice = (meta && (meta.route_short_name || meta.route_long_name)) || "";
    return nice ? routeId + " — " + nice : routeId;
  }

  // Build deps/dests/via for a *specific* route_id using trip_headsign
  function collectDepDestForRoute(gtfs, routeId) {
    const deps = new Set();
    const dests = new Set();

    const endpoints = ctx.tripEndpoints || new Map();

    (gtfs.trips || []).forEach((tr) => {
      if (tr.route_id !== routeId) return;

      const ep = endpoints.get(String(tr.trip_id));
      if (!ep) return;

      if (ep.startName) deps.add(ep.startName);
      if (ep.endName) dests.add(ep.endName);
    });

    const sort = (a, b) =>
      a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" });

    return { deps: [...deps].sort(sort), dests: [...dests].sort(sort) };
  }

  // Get destinations allowed for (routeId, dep) based on headsigns
  function destinationsForRouteAndDep(gtfs, routeId, dep) {
    const dests = new Set();
    const endpoints = ctx.tripEndpoints || new Map();

    (gtfs.trips || []).forEach((tr) => {
      if (tr.route_id !== routeId) return;

      const ep = endpoints.get(String(tr.trip_id));
      if (!ep) return;

      if ((ep.startName || "") === dep && ep.endName) dests.add(ep.endName);
    });

    const sort = (a, b) =>
      a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" });

    return [...dests].sort(sort);
  }

  // "HH:MM" -> minutes [0..1439]
  function hhmmToMin(hhmm) {
    const [h = "0", m = "0"] = String(hhmm || "").split(":");
    const H = parseInt(h, 10),
      M = parseInt(m, 10);
    if (!Number.isFinite(H) || !Number.isFinite(M)) return null;
    return (H % 24) * 60 + Math.max(0, Math.min(59, M));
  }

  // Build "HH:MM" list inside roster window (supports overnight) at given step minutes
  function enumerateRosterTimes(startHHMM, endHHMM, stepMin) {
    const s = hhmmToMin(startHHMM),
      e = hhmmToMin(endHHMM);
    const out = [];
    if (s == null || e == null || !Number.isFinite(stepMin) || stepMin <= 0)
      return out;

    function pushRange(a, b) {
      for (let m = a; m <= b; m += stepMin) {
        const H = String(Math.floor(m / 60)).padStart(2, "0");
        const M = String(m % 60).padStart(2, "0");
        out.push(`${H}:${M}`);
      }
    }

    if (s <= e) {
      pushRange(s, e);
    } else {
      pushRange(s, 1439);
      pushRange(0, e);
    }
    return out;
  }

  // Create a roster-limited <select> (fallback to <input type="time"> when no roster)
  // NEW: optional minHHMM ensures options are strictly *after* that threshold.
  function makeRosteredTimeControl(
    currentHHMM,
    roster,
    onChange,
    stepMin = 1,
    placeholder = "— Select —",
    minHHMM,
  ) {
    const rs = hhmmToMin(roster?.start),
      re = hhmmToMin(roster?.end);
    const simpleRoster = rs != null && re != null;

    if (!simpleRoster) {
      // Fallback input
      const inp = document.createElement("input");
      inp.type = "time";
      inp.value = currentHHMM || "";
      inp.oninput = () => onChange(inp.value || "");
      return inp;
    }

    const sel = document.createElement("select");
    const def = document.createElement("option");
    def.value = "";
    def.textContent = placeholder;
    sel.appendChild(def);

    // Build all times in the roster window
    let options = enumerateRosterTimes(roster.start, roster.end, stepMin);

    // If a threshold is provided, keep only options STRICTLY after it
    if (minHHMM && roster) {
      const cutoff = rosterRank(minHHMM, roster);
      options = options.filter((t) => rosterRank(t, roster) > cutoff); // strictly after
    }

    options.forEach((hhmm) => {
      const o = document.createElement("option");
      o.value = hhmm;
      o.textContent = hhmm;
      sel.appendChild(o);
    });

    // Select current value only if it’s still valid; otherwise no selection
    const v = options.includes(currentHHMM) ? currentHHMM : "";
    sel.value = v;

    sel.onchange = () => onChange(sel.value || "");
    // Disable if there are no valid options
    sel.disabled = options.length === 0;
    return sel;
  }

  // GTFS "HH:MM[:SS]" (may exceed 24) → minutes in day [0..1439]
  function gtfsToDayMinutes(t) {
    const [h = "0", m = "0", s = "0"] = String(t || "").split(":");
    const H = parseInt(h, 10),
      M = parseInt(m, 10),
      S = parseInt(s, 10) || 0;
    if (!Number.isFinite(H) || !Number.isFinite(M)) return null;
    return (H % 24) * 60 + M + (S >= 30 ? 1 : 0);
  }

  // Is a GTFS departure time within the roster [start..end] (supports overnight windows)
  function isInRosterWindow(gtfsTime, startHHMM, endHHMM) {
    if (!startHHMM || !endHHMM) return true; // no filter
    const start = hhmmToMin(startHHMM);
    const end = hhmmToMin(endHHMM);
    const dep = gtfsToDayMinutes(gtfsTime);
    if (start == null || end == null || dep == null) return true;
    if (start <= end) return dep >= start && dep <= end; // normal
    return dep >= start || dep <= end; // overnight wrap
  }

  // --- HH:MM helpers for roster clamping / overlap (supports overnight windows) ---
  function toHHMM(min) {
    return minToHHMMSS(min).slice(0, 5);
  }

  function clampHHMMToRoster(hhmm, startHHMM, endHHMM) {
    if (!hhmm || !startHHMM || !endHHMM) return hhmm || "";
    const t = hhmmToMin(hhmm),
      s = hhmmToMin(startHHMM),
      e = hhmmToMin(endHHMM);
    if (t == null || s == null || e == null) return hhmm || "";
    // non-overnight
    if (s <= e) return toHHMM(Math.min(Math.max(t, s), e));
    // overnight window: [s..1439] U [0..e]
    if (t >= s || t <= e) return hhmm; // already inside
    // choose closer boundary on the circle
    const toS = (s - t + 1440) % 1440;
    const toE = (t - e + 1440) % 1440;
    return toS <= toE ? toHHMM(s) : toHHMM(e);
  }

  function normalizeWindowIntervals(startMin, endMin) {
    return startMin <= endMin
      ? [[startMin, endMin]]
      : [
          [startMin, 1439],
          [0, endMin],
        ];
  }
  function normalizeArcIntervals(startMin, endMin) {
    return startMin <= endMin
      ? [[startMin, endMin]]
      : [
          [startMin, 1439],
          [0, endMin],
        ];
  }
  function intervalsOverlap(a, b) {
    return a[0] <= b[1] && b[0] <= a[1];
  }

  /** Returns true if [startHHMM, endHHMM] overlaps the roster window [rStart, rEnd] (overnight safe) */
  function intervalOverlapsRoster(startHHMM, endHHMM, rStart, rEnd) {
    const s = hhmmToMin(startHHMM),
      e = hhmmToMin(endHHMM);
    const rs = hhmmToMin(rStart),
      re = hhmmToMin(rEnd);
    if (s == null || e == null || rs == null || re == null) return true; // no filter
    const arc = normalizeArcIntervals(s, e);
    const win = normalizeWindowIntervals(rs, re);
    for (const A of arc)
      for (const B of win) if (intervalsOverlap(A, B)) return true;
    return false;
  }

  /** forward minutes from a to b on 24h ring (used to keep end >= start along the ring) */
  function forwardArcMinutes(aHHMM, bHHMM) {
    const a = hhmmToMin(aHHMM),
      b = hhmmToMin(bHHMM);
    if (a == null || b == null) return 0;
    return (b - a + 1440) % 1440;
  }

  // Find the earliest trip matching routeId + dep + dest
  function findCandidateTripForRoute(
    gtfs,
    routeId,
    depName,
    destName,
    roster,
    notBefore,
  ) {
    if (!routeId || !depName || !destName) return null;

    const trips = (gtfs.trips || []).filter(
      (t) => String(t.route_id) === String(routeId),
    );
    const st = gtfs.stop_times || [];
    const endpoints = ctx.tripEndpoints || new Map();
    const threshold = notBefore ? padToHHMMSS(notBefore) : null;

    let best = null;

    for (const tr of trips) {
      const ep = endpoints.get(String(tr.trip_id));
      if (!ep) continue;

      // dep/dest are STOP NAMES (from endpoints index)
      if (ep.startName !== depName || ep.endName !== destName) continue;

      const rows = st
        .filter((r) => r.trip_id === tr.trip_id)
        .sort((a, b) => +a.stop_sequence - +b.stop_sequence);

      if (!rows.length) continue;

      const depTime = rows[0].departure_time || rows[0].arrival_time || "";
      const arrTime =
        rows[rows.length - 1].arrival_time ||
        rows[rows.length - 1].departure_time ||
        "";

      // roster filter
      if (roster && !isInRosterWindow(depTime, roster.start, roster.end))
        continue;

      // threshold filter
      if (threshold && padToHHMMSS(depTime) < threshold) continue;

      // choose earliest departure that satisfies threshold/roster
      if (!best || (depTime || "") < (best.times.depTime || "")) {
        best = {
          trip: tr,
          times: {
            depTime,
            arrTime,
          },
        };
      }
    }

    return best;
  }

  // Build outputs for a *single* trip_id
  function buildFromTrip(
    gtfs,
    tripId,
    routeMeta,
    extraStopIds,
    overrideStartHHMM,
    overrideEndHHMM,
  ) {
    const trips = gtfs.trips || [];
    const stop_times = gtfs.stop_times || [];
    const stops = gtfs.stops || [];
    const shapes = gtfs.shapes || [];

    const tr = trips.find((t) => t.trip_id === tripId);
    if (!tr) throw "Trip not found: " + tripId;

    // times of this trip
    const times = stop_times
      .filter((st) => st.trip_id === tr.trip_id)
      .sort((a, b) => +a.stop_sequence - +b.stop_sequence);
    if (!times.length) throw "No stop_times for trip " + tripId;

    // shape points for this trip's shape
    const shapeId = tr.shape_id;
    const shapePts = shapes
      .filter((s) => s.shape_id === shapeId)
      .sort((a, b) => +a.shape_pt_sequence - +b.shape_pt_sequence)
      .map((r) => [+r.shape_pt_lat, +r.shape_pt_lon]);
    if (!shapePts.length) throw "No shapes found for shape_id=" + shapeId;

    // stop rows with geo
    const routeStops = times.map((st) => {
      const meta = stops.find((s) => s.stop_id === st.stop_id) || {};
      return Object.assign({}, st, meta);
    });
    routeStops.forEach((rs) => {
      rs.pos = nearestIndex([+rs.stop_lat, +rs.stop_lon], shapePts);
    });
    routeStops.sort((a, b) => a.pos - b.pos);

    // busRouteData
    const next_points = [];
    for (let i = 0; i < routeStops.length - 1; i++) {
      const prev = routeStops[i],
        nxt = routeStops[i + 1];
      const pt1 = parseGtfsTime(prev.departure_time);
      const pt2 = parseGtfsTime(nxt.departure_time);
      let durMin = pt1 && pt2 ? (pt2 - pt1) / 60000 : 0;
      if (durMin < 0) durMin = 0;
      next_points.push({
        latitude: +nxt.stop_lat,
        longitude: +nxt.stop_lon,
        address: nxt.stop_name || "",
        duration: durMin.toFixed(1) + " minutes",
        route_coordinates: shapePts
          .slice(prev.pos, nxt.pos + 1)
          .map((c) => [c[1], c[0]]),
      });
    }

    const busRouteData = [
      {
        starting_point: {
          latitude: +routeStops[0].stop_lat,
          longitude: +routeStops[0].stop_lon,
          address: routeStops[0].stop_name || "",
        },
        next_points,
      },
    ];

    // scheduleData (just this trip)
    const first = times[0],
      last = times[times.length - 1];

    const startHHMM =
      overrideStartHHMM || formatGtfsTime(first && first.departure_time);
    const endHHMM =
      overrideEndHHMM || formatGtfsTime(last && last.arrival_time);

    const lastIdx = times.length - 1;

    // Build filtered busStops (includes first/last + GTFS timing points + extras)
    const busStops = [];
    for (let j = 0; j < times.length; j++) {
      const st = times[j];
      const meta = stops.find((s) => s.stop_id === st.stop_id) || {};
      const addr = meta.stop_name || "";

      const isFirst = j === 0;
      const isLast = j === lastIdx;
      const tp = toTimePoint(st.timepoint ?? st.time_point);

      const include =
        isFirst ||
        isLast ||
        tp === 1 ||
        (extraStopIds || []).includes(st.stop_id);
      if (!include) continue;

      const name = isFirst ? "Stop S" : isLast ? "Stop E" : "Stop " + j;
      const rawTime = isLast
        ? st.arrival_time || st.departure_time
        : st.departure_time || st.arrival_time;

      busStops.push({
        name,
        time: formatGtfsTime(rawTime),
        latitude: meta.stop_lat != null ? +meta.stop_lat : null,
        longitude: meta.stop_lon != null ? +meta.stop_lon : null,
        address: addr,
        abbreviation: addr ? getAbbreviation(addr) : "",
      });
    }

    // Apply overrides AFTER busStops are built
    if (busStops.length) {
      if (startHHMM) busStops[0].time = startHHMM;
      if (endHHMM) busStops[busStops.length - 1].time = endHHMM;
    }

    const routeNo =
      (routeMeta &&
        (routeMeta.route_short_name || routeMeta.route_long_name)) ||
      tr.route_id;

    return {
      busRouteData,
      scheduleData: [
        {
          runNo: routeNo,
          startTime: startHHMM, // use override-aware times
          endTime: endHHMM,
          runName: tr.route_id,
          busStops,
        },
      ],
    };
  }

  // roster window state
  ctx.roster = {
    start: document.getElementById("rosterStart")?.value || "",
    end: document.getElementById("rosterEnd")?.value || "",
  };

  // Recompute columns using the current roster window (with the same full-screen spinner)
  async function applyRosterWindow() {
    try {
      await showOverlay("Applying roster window…");
      await new Promise(requestAnimationFrame);
      await new Promise((r) => setTimeout(r, 0));
      recomputeSequentialFrom(0);
    } finally {
      setLoading(false);
    }
  }

  rosterStartEl?.addEventListener("change", () => {
    ctx.roster.start = rosterStartEl.value || "";
    ctx.rosterReady = isRosterSet();
    applyRosterWindow();
    validateUI();
  });
  rosterEndEl?.addEventListener("change", () => {
    ctx.roster.end = rosterEndEl.value || "";
    ctx.rosterReady = isRosterSet();
    applyRosterWindow();
    validateUI();
  });

  function recomputeSequentialFrom(startIndex = 0) {
    const cols = ctx.planner.cols;

    function chainEndHHMMSS(item) {
      if (!item) return "";

      if (item.kind === "trip") {
        const t = item.arrOverride ? item.arrOverride + ":00" : item.arrTime; // GTFS or override
        return t ? padToHHMMSS(t) : "";
      }

      if (
        item.kind === "break" ||
        item.kind === "signIn" ||
        item.kind === "signOff"
      ) {
        return item.breakEnd ? padToHHMMSS(item.breakEnd) : "";
      }

      if (item.kind === "reposition") {
        return item.repEnd ? padToHHMMSS(item.repEnd) : "";
      }

      if (item.kind === "school") {
        const t = item.schoolEnd || item.schoolStart; // HH:MM
        return t ? padToHHMMSS(t) : "";
      }

      return "";
    }

    // Start threshold = roster start, or previous item end if starting mid-chain
    let last = padToHHMMSS(ctx.roster.start || "00:00");

    if (startIndex > 0) {
      for (let j = startIndex - 1; j >= 0; j--) {
        const t = chainEndHHMMSS(cols[j]);
        if (t) {
          last = t;
          break;
        }
      }
    }

    for (let i = startIndex; i < cols.length; i++) {
      const c = cols[i];

      if (c.kind === "break" || c.kind === "signIn" || c.kind === "signOff") {
        // compute break start/end from 'last' + duration
        const dur = Math.max(0, Number(c.breakMin) || 0);
        c.breakStart = last;
        c.breakEnd = addMinutesHHMMSS(last, dur);
        // Advance the chain
        last = padToHHMMSS(c.breakEnd || last);

        // clear trip-only fields so UI/meta are accurate
        c.tripId = c.via = c.depTime = c.arrTime = c.duration = "";
        c.repStart = c.repEnd = "";
      } else if (c.kind === "reposition") {
        // Reposition: occupies time like a break, moves the bus location
        const dur = Math.max(0, Number(c.repMin) || 0);
        c.repStart = last;
        c.repEnd = addMinutesHHMMSS(last, dur);
        // Advance the chain to the end of reposition
        last = padToHHMMSS(c.repEnd || last);
        // clear trip/break fields
        c.tripId = c.via = c.depTime = c.arrTime = c.duration = "";
        c.breakStart = c.breakEnd = "";
      } else if (c.kind === "school") {
        // Default start to the current chain "last" if missing
        if (!c.schoolStart) c.schoolStart = hhmm(last);

        // Ensure School start is strictly after the chain so far
        const lastHHMM = hhmm(last);
        if (forwardArcMinutes(lastHHMM, c.schoolStart) <= 0) {
          c.schoolStart = lastHHMM;
        }

        // Clamp start to the roster window
        if (ctx.rosterReady) {
          c.schoolStart = clampHHMMToRoster(
            c.schoolStart,
            ctx.roster.start,
            ctx.roster.end,
          );
        }

        // Do NOT auto-set end; leave blank until user picks.
        if (c.schoolEnd && ctx.rosterReady) {
          c.schoolEnd = clampHHMMToRoster(
            c.schoolEnd,
            ctx.roster.start,
            ctx.roster.end,
          );
          // Ensure end is not "before" start along the 24h ring
          if (forwardArcMinutes(c.schoolStart, c.schoolEnd) < 0) {
            c.schoolEnd = c.schoolStart;
          }
        }

        // Advance the chain to the school end (or start if no end yet)
        last = padToHHMMSS(c.schoolEnd || c.schoolStart);

        // clear trip/break/rep-only fields
        c.tripId = c.via = c.depTime = c.arrTime = c.duration = "";
        c.breakStart = c.breakEnd = "";
        c.repStart = c.repEnd = "";
      } else {
        // TRIP
        if (ctx.gtfs && c.routeId && c.dep && c.dest) {
          // --- Custom destination leg: dep is GTFS endpoint name, dest is custom:... ---
          const customDest = resolveCustomStop(c.dest);
          if (customDest) {
            const startStop = resolveRouteStartStopForDep(c.routeId, c.dep);

            // Default dep time from chain threshold if user hasn't edited it
            const depHHMM = c.depOverride || hhmm(last);
            c.depTime = padToHHMMSS(depHHMM);
            c.tripId = ""; // not a GTFS trip
            c.via = "";

            // If we can’t resolve start coords yet, we can’t route
            if (!startStop) {
              c.arrTime = "";
              c.duration = "";
              c.customLeg = {
                kind: "customTrip",
                start: null,
                end: customDest,
              };
              // last unchanged
            } else {
              // Mark as pending now, then compute async and re-chain when done
              c.customLeg = {
                kind: "customTrip",
                start: startStop,
                end: customDest,
                pending: true,
              };

              const token = ++ctx.customStopTokenSeq;
              ctx.customStopTokens.set(c.id, token);

              (async () => {
                try {
                  // optional overlay (only if user just changed dest)
                  // await showOverlay("Calculating route…");

                  const res = await fetchRouteORS(
                    { lat: startStop.lat, lon: startStop.lon },
                    { lat: customDest.lat, lon: customDest.lon },
                  );

                  // stale request guard
                  if (ctx.customStopTokens.get(c.id) !== token) return;

                  const durMin = Math.max(0, Math.round(mins(res.duration_s)));
                  const endHHMM = hhmm(
                    addMinutesHHMMSS(padToHHMMSS(depHHMM), durMin),
                  );

                  c.arrTime = padToHHMMSS(endHHMM);
                  c.duration = durationHHMM(depHHMM, endHHMM);
                  c.customLeg = {
                    kind: "customTrip",
                    start: startStop,
                    end: customDest,
                    pending: false,
                    duration_s: res.duration_s,
                    distance_m: res.distance_m,
                    coords: res.coords, // [lon,lat] list
                  };

                  // re-run chain from this index so later columns shift correctly
                  const idx = cols.findIndex((x) => x.id === c.id);
                  recomputeSequentialFrom(Math.max(0, idx));
                } catch (e) {
                  if (ctx.customStopTokens.get(c.id) !== token) return;
                  c.arrTime = "";
                  c.duration = "";
                  c.customLeg = {
                    kind: "customTrip",
                    start: startStop,
                    end: customDest,
                    pending: false,
                    error: String(e),
                  };
                  renderColumns();
                  validateUI();
                } finally {
                  // setLoading(false);
                }
              })();

              // While pending, don’t advance chain yet
            }
          } else {
            // --- Normal GTFS trip behavior (unchanged) ---
            const cand = findCandidateTripForRoute(
              ctx.gtfs,
              c.routeId,
              c.dep,
              c.dest,
              ctx.roster,
              last,
            );

            if (cand) {
              c.tripId = cand.trip.trip_id;
              c.via = cand.via || "";
              c.depTime = cand.times.depTime || "";
              c.arrTime = cand.times.arrTime || "";
              const depEff = effDepHHMM(c);
              const arrEff = effArrHHMM(c);
              c.duration = depEff && arrEff ? durationHHMM(depEff, arrEff) : "";
              const arrForChain = c.arrOverride
                ? c.arrOverride + ":00"
                : c.arrTime;
              last = padToHHMMSS(arrForChain || last);
            } else {
              c.tripId = c.via = c.depTime = c.arrTime = c.duration = "";
            }
          }
        } else {
          c.tripId = c.via = c.depTime = c.arrTime = c.duration = "";
        }

        // clear break-only fields on trip
        c.breakStart = c.breakEnd = "";
      }
    }

    renderColumns();
    validateUI();
  }

  // --- time helpers for breaks ---
  function hhmmssToMin(t) {
    const [h = "0", m = "0", s = "0"] = String(t || "00:00:00")
      .split(":")
      .map(Number);
    return (h % 24) * 60 + m + (s >= 30 ? 1 : 0);
  }
  function minToHHMMSS(min) {
    const m = ((min % 1440) + 1440) % 1440;
    const H = Math.floor(m / 60),
      M = m % 60;
    return (
      String(H).padStart(2, "0") + ":" + String(M).padStart(2, "0") + ":00"
    );
  }
  function addMinutesHHMMSS(t, minutes) {
    return minToHHMMSS(hhmmssToMin(t) + (Number(minutes) || 0));
  }
  function padToHHMMSS(t) {
    if (!t) return "00:00:00";
    const [h = "0", m = "0", s = "0"] = String(t).split(":");
    return `${String(h).padStart(2, "0")}:${String(m).padStart(
      2,
      "0",
    )}:${String(s || "0").padStart(2, "0")}`;
  }

  function getTripStopsAndAutoTP(gtfs, tripId) {
    const st = gtfs.stop_times || [];
    const stops = gtfs.stops || [];

    const rows = st
      .filter((r) => r.trip_id === tripId)
      .sort((a, b) => +a.stop_sequence - +b.stop_sequence);

    const outStops = [];
    const autoTP = new Set();
    for (let j = 0; j < rows.length; j++) {
      const r = rows[j];
      const meta = stops.find((s) => s.stop_id === r.stop_id) || {};
      const isFirst = j === 0;
      const isLast = j === rows.length - 1;
      const addr = meta.stop_name || "";
      outStops.push({
        j,
        stop_id: r.stop_id,
        isFirst,
        isLast,
        name: addr,
        abbr: addr ? getAbbreviation(addr) : "",
        lat: meta.stop_lat != null ? +meta.stop_lat : null,
        lon: meta.stop_lon != null ? +meta.stop_lon : null,
        timepoint: toTimePoint(r.timepoint ?? r.time_point),
      });
      if (
        !isFirst &&
        !isLast &&
        toTimePoint(r.timepoint ?? r.time_point) === 1
      ) {
        autoTP.add(r.stop_id);
      }
    }
    const firstId = outStops[0]?.stop_id || null;
    const lastId = outStops[outStops.length - 1]?.stop_id || null;
    return { stops: outStops, autoTP, firstId, lastId };
  }

  function currentRouteIds() {
    // If user picked any filters, limit to those; otherwise show all
    return ctx.routeFilters && ctx.routeFilters.size
      ? Array.from(ctx.routeFilters).sort(naturalCompare)
      : routeIdList;
  }

  // --- Stops helpers for Reposition ---
  function stopsByIdMap(stopsArr) {
    const m = new Map();
    (stopsArr || []).forEach((s) => m.set(s.stop_id, s));
    return m;
  }
  let STOPS_BY_ID = new Map(); // set after ZIP load

  function stopLabel(s) {
    const name = s?.stop_name || "";
    const abbr = name ? getAbbreviation(name) : "";
    return (abbr ? `${abbr} — ` : "") + name;
  }

  // Return an array of stops constrained to current route filters (if any)
  function filteredStopsOptions() {
    const gtfs = ctx.gtfs || {};
    const { trips = [], stop_times = [], stops = [] } = gtfs;
    if (!stops.length) return [];
    if (!ctx.routeFilters || ctx.routeFilters.size === 0)
      return stops
        .slice()
        .sort((a, b) => stopLabel(a).localeCompare(stopLabel(b)));

    // limit to stops that appear on trips whose route_id is in the filter
    const allowedRoutes = new Set(ctx.routeFilters);
    const allowedTripIds = new Set(
      trips.filter((t) => allowedRoutes.has(t.route_id)).map((t) => t.trip_id),
    );
    const allowedStopIds = new Set(
      stop_times
        .filter((st) => allowedTripIds.has(st.trip_id))
        .map((st) => st.stop_id),
    );
    return stops
      .filter((s) => allowedStopIds.has(s.stop_id))
      .sort((a, b) => stopLabel(a).localeCompare(stopLabel(b)));
  }

  // Make a Reposition schedule item to the selected stop
  function makeRepositionScheduleItem(stopIdOrCustom, startHHMM, endHHMM) {
    if (!stopIdOrCustom) return null;

    // Custom stop
    if (String(stopIdOrCustom).startsWith("custom:")) {
      const id = String(stopIdOrCustom).slice("custom:".length);
      const cs = ctx.customStopById.get(id);
      if (!cs) return null;

      const address = cs.name || "";
      return {
        runNo: "",
        startTime: startHHMM,
        endTime: endHHMM,
        runName: "REP",
        busStops: [
          {
            name: "Stop E",
            time: endHHMM,
            latitude: cs.lat,
            longitude: cs.lon,
            address,
            abbreviation: address ? getAbbreviation(address) : "",
          },
        ],
      };
    }

    // GTFS stop_id
    const meta = STOPS_BY_ID.get(String(stopIdOrCustom)) || {};
    const address = meta.stop_name || "";
    return {
      runNo: "",
      startTime: startHHMM,
      endTime: endHHMM,
      runName: "REP",
      busStops: [
        {
          name: "Stop E",
          time: endHHMM,
          latitude: meta.stop_lat != null ? +meta.stop_lat : null,
          longitude: meta.stop_lon != null ? +meta.stop_lon : null,
          address,
          abbreviation: address ? getAbbreviation(address) : "",
        },
      ],
    };
  }

  // --- NEW: Resolve either "custom:<id>" or a GTFS stop_id into a routing-ready stop ---
  function resolveStopAny(stopIdOrCustom) {
    if (!stopIdOrCustom) return null;

    // Custom stop
    if (String(stopIdOrCustom).startsWith("custom:")) {
      const id = String(stopIdOrCustom).slice("custom:".length);
      const cs = ctx.customStopById.get(id);
      if (!cs) return null;
      return {
        latitude: cs.lat,
        longitude: cs.lon,
        address: cs.name || "",
        abbreviation: cs.name ? getAbbreviation(cs.name) : "",
      };
    }

    // GTFS stop_id
    const s = STOPS_BY_ID.get(String(stopIdOrCustom));
    if (!s) return null;
    const address = s.stop_name || "";
    return {
      latitude: s.stop_lat != null ? +s.stop_lat : null,
      longitude: s.stop_lon != null ? +s.stop_lon : null,
      address,
      abbreviation: address ? getAbbreviation(address) : "",
    };
  }

  // --- NEW: normalize "stop" object (some places use lat/lon keys) ---
  function stopLat(stop) {
    return stop?.latitude ?? stop?.lat ?? null;
  }
  function stopLon(stop) {
    return stop?.longitude ?? stop?.lon ?? null;
  }

  function renderRouteFilter() {
    // Build dropdown options (exclude already-chosen)
    if (routeFilterSelect) {
      const chosen = new Set(ctx.routeFilters || []);
      routeFilterSelect.innerHTML = "";
      const def = document.createElement("option");
      def.value = "";
      def.textContent = "— Choose route_id (e.g., 70-205) —";
      routeFilterSelect.appendChild(def);

      const available = (routeIdList || []).filter((id) => !chosen.has(id));
      available.forEach((id) => {
        const o = document.createElement("option");
        o.value = id;
        o.textContent = formatRouteLabel(id, routesById);
        routeFilterSelect.appendChild(o);
      });

      routeFilterSelect.disabled = !ctx.gtfs || available.length === 0;
    }

    // Chips
    if (routeFilterChips) {
      routeFilterChips.innerHTML = "";
      (ctx.routeFilters ? Array.from(ctx.routeFilters) : [])
        .sort(naturalCompare)
        .forEach((id) => {
          const chip = document.createElement("div");
          chip.className = "chip";
          const text = document.createElement("span");
          text.className = "chip-text";
          text.textContent = id;
          const x = document.createElement("button");
          x.type = "button";
          x.className = "chip-x";
          x.textContent = "×";
          x.title = "Remove";
          x.onclick = () => {
            ctx.routeFilters.delete(id);
            renderRouteFilter();
            renderColumns(); // re-render trip columns with new route list
          };
          chip.appendChild(text);
          chip.appendChild(x);
          routeFilterChips.appendChild(chip);
        });
    }
  }

  // Add route on select
  routeFilterSelect?.addEventListener("change", () => {
    const v = routeFilterSelect.value;
    if (!v) return;
    ctx.routeFilters.add(v);
    routeFilterSelect.value = "";
    renderRouteFilter();
    renderColumns();
  });

  // ADD: helper to make a Break schedule item that reuses the last trip's end stop
  function makeBreakScheduleItem(locStop, startHHMM, endHHMM, runName) {
    if (!locStop) return null; // still requires a location
    return {
      runNo: "",
      startTime: startHHMM,
      endTime: endHHMM,
      runName: runName || "Break",
      busStops: [
        {
          name: "Stop E",
          time: endHHMM,
          latitude: locStop.latitude ?? locStop.lat ?? null,
          longitude: locStop.longitude ?? locStop.lon ?? null,
          address: locStop.address || "",
          abbreviation:
            locStop.abbreviation ||
            (locStop.address ? getAbbreviation(locStop.address) : ""),
        },
      ],
    };
  }

  // In onInit, add:
  ctx.school = { routes: [], byId: new Map() };

  // Parser
  async function parseSchoolGeoJSON(file) {
    const txt = await file.text();
    const gj = JSON.parse(txt);

    if (!gj || gj.type !== "FeatureCollection") throw "Invalid GeoJSON";

    const routes = [];
    for (const f of gj.features || []) {
      if (!f || !f.geometry || f.geometry.type !== "MultiLineString") continue;
      const props = f.properties || {};
      const id = String(
        props.ROUTEPATTERN || props.OBJECTID || crypto.randomUUID(),
      );
      const name = String(props.ROUTENAME || props.ROUTENUMBER || id);
      const agency = String(props.AGENCYNAME || props.AGENCY || "");
      const number = String(props.ROUTENUMBER || "");
      // Flatten MultiLineString to one polyline (concat parts)
      const parts = f.geometry.coordinates || [];
      const lonlat = parts.flat(); // [[lon,lat], ...]
      if (!lonlat.length) continue;

      // Convert to [lat,lon] for consistency with your code
      const latlon = lonlat.map(([lon, lat]) => [lat, lon]);

      // Derive endpoints
      const startLL = latlon[0];
      const endLL = latlon[latlon.length - 1];

      // Try to split a “A to B” from ROUTENAME (fallback to whole string)
      let depName = name,
        destName = name;
      const m = name.match(/^(.*?)\s+to\s+(.*)$/i);
      if (m) {
        depName = m[1].trim();
        destName = m[2].trim();
      }

      routes.push({
        id: `school:${id}`,
        agency,
        number,
        name,
        depName,
        destName,
        coords: latlon, // [ [lat,lon], ... ]
        start: { lat: startLL[0], lon: startLL[1], name: depName },
        end: { lat: endLL[0], lon: endLL[1], name: destName },
      });
    }

    return routes;
  }

  // Hook for school file selection
  ctrl.onSchoolSelect = async function (files) {
    try {
      if (!files || !files.length) throw "No file selected.";
      const file = files[0];
      if (!/\.(geojson|json)$/i.test(file.name))
        throw "Select a .geojson or .json";

      setLoading(true, "Loading School Bus GeoJSON…");
      setZipStatus("info", "Reading GeoJSON…");

      const routes = await parseSchoolGeoJSON(file);
      if (!routes.length) throw "No MultiLineString routes found in this file.";

      ctx.school.routes = routes;
      ctx.school.byId = new Map(routes.map((r) => [r.id, r]));

      // Enable roster & planner like normal
      ctx.roster.start = rosterStartEl.value || "";
      ctx.roster.end = rosterEndEl.value || "";
      ctx.rosterReady = isRosterSet();
      rosterStartEl.disabled = !hasAnyData();
      rosterEndEl.disabled = !hasAnyData();

      // Repaint filters & columns
      renderColumns();
      validateUI();

      setZipStatus(
        "ok",
        `School GeoJSON loaded: <code>${file.name}</code> • ${
          routes.length
        } route${routes.length === 1 ? "" : "s"}`,
      );
      schoolDz?.classList.add("loaded");
      setStatusHTML(
        schoolStatus,
        "ok",
        `School GeoJSON loaded: <code>${file.name}</code> • ${routes.length} routes`,
      );

      // Friendly prompt
      const msg =
        "School routes loaded. Add a “School run” card, pick a route, set Start/End times, then Generate.";
      document.getElementById("busRouteJson").textContent = msg;
      document.getElementById("scheduleJson").textContent = msg;
    } catch (e) {
      const message = "Error loading school data: " + (e?.message || e);
      setZipStatus("err", message);
      document.getElementById("busRouteJson").textContent = message;
      document.getElementById("scheduleJson").textContent = message;
      dropZoneEl?.classList.remove("loaded");
    } finally {
      setLoading(false);
      schoolInput.value = "";
    }
  };

  function abbr(s) {
    return s ? getAbbreviation(s) : "";
  }

  function buildFromSchool(r, startHHMM, endHHMM) {
    if (!r) return { busRouteData: [], scheduleData: [] };

    // busRouteData: same shape as your GTFS output
    const next_points = [];
    for (let i = 0; i < r.coords.length - 1; i++) {
      const a = r.coords[i],
        b = r.coords[i + 1];
      next_points.push({
        latitude: b[0],
        longitude: b[1],
        address: "",
        duration: "", // no per-segment time
        route_coordinates: [
          [a[1], a[0]],
          [b[1], b[0]],
        ], // [lon,lat] pairs
      });
    }
    const busRouteData = [
      {
        starting_point: {
          latitude: r.start.lat,
          longitude: r.start.lon,
          address: r.start.name || "",
        },
        next_points,
      },
    ];

    // scheduleData: two stops (start/end)
    const busStops = [
      {
        name: "Stop S",
        time: startHHMM || "",
        latitude: r.start.lat,
        longitude: r.start.lon,
        address: r.depName || "",
        abbreviation: abbr(r.depName),
        time_point: 1,
      },
      {
        name: "Stop E",
        time: endHHMM || "",
        latitude: r.end.lat,
        longitude: r.end.lon,
        address: r.destName || "",
        abbreviation: abbr(r.destName),
        time_point: 1,
      },
    ];

    return {
      busRouteData,
      scheduleData: [
        {
          runNo: "", // renumbered later
          startTime: startHHMM || "",
          endTime: endHHMM || "",
          runName: r.number ? `School ${r.number}` : `School`,
          busStops,
        },
      ],
    };
  }
  function hasAnyData() {
    return (
      (ctx.gtfs && (ctx.gtfs.trips || []).length) ||
      (ctx.school && ctx.school.routes && ctx.school.routes.length)
    );
  }

  function effDepHHMM(col) {
    return col.depOverride || (col.depTime ? hhmm(col.depTime) : "");
  }
  function effArrHHMM(col) {
    return col.arrOverride || (col.arrTime ? hhmm(col.arrTime) : "");
  }

  // Returns the HH:MM threshold that the item at index `idx` must start after.
  // Uses previous Trip arrival, Break end, Reposition end, or School end (start if no end).
  function chainThresholdBeforeIndex(idx) {
    const cols = ctx.planner?.cols || [];
    let lastHHMM = ctx.roster?.start || "00:00";
    for (let j = 0; j < idx; j++) {
      const c = cols[j];
      if (c.kind === "trip") {
        if (c.arrOverride) lastHHMM = c.arrOverride;
        else if (c.arrTime) lastHHMM = hhmm(c.arrTime);
      } else if (c.kind === "break") {
        if (c.breakEnd) lastHHMM = hhmm(c.breakEnd);
      } else if (c.kind === "reposition") {
        if (c.repEnd) lastHHMM = hhmm(c.repEnd);
      } else if (c.kind === "school") {
        // Prefer School end; if missing treat start as a zero-length window
        if (c.schoolEnd) lastHHMM = c.schoolEnd;
        else if (c.schoolStart) lastHHMM = c.schoolStart;
      }
    }
    return lastHHMM;
  }

  function rosterRank(hhmm, roster) {
    const m = hhmmToMin(hhmm);
    const s = hhmmToMin(roster?.start);
    return (m - s + 1440) % 1440; // minutes since roster start
  }

  // ---------- Routing API (OpenRouteService) ----------
  // ⚠️ Put your ORS key here (or read it from widget settings)
  const ORS_API_KEY =
    "5b3ce3597851110001cf624804ab2baa18644cc6b65c5829826b6117";

  function routeCacheKey(aLon, aLat, bLon, bLat) {
    return `${aLon},${aLat}|${bLon},${bLat}`;
  }

  async function fetchRouteORS(start, end) {
    // start/end: {lat, lon}
    const key = routeCacheKey(start.lon, start.lat, end.lon, end.lat);
    if (ctx.routeApiCache.has(key)) return ctx.routeApiCache.get(key);

    const url =
      "https://api.openrouteservice.org/v2/directions/driving-car" +
      `?api_key=${encodeURIComponent(ORS_API_KEY)}` +
      `&start=${start.lon},${start.lat}` +
      `&end=${end.lon},${end.lat}` +
      "&format=geojson";

    const resp = await fetch(url);
    if (!resp.ok) throw new Error("ORS HTTP " + resp.status);
    const data = await resp.json();

    const feat = (data.features && data.features[0]) || null;
    const coords = (feat && feat.geometry && feat.geometry.coordinates) || [];
    const seg = feat?.properties?.segments?.[0] || {};
    const duration_s = Number(seg.duration || 0);
    const distance_m = Number(seg.distance || 0);

    const out = {
      coords: coords.map((c) => [Number(c[0]), Number(c[1])]), // [lon,lat]
      duration_s,
      distance_m,
    };

    ctx.routeApiCache.set(key, out);
    return out;
  }

  function km(m) {
    return Number.isFinite(m) ? m / 1000 : 0;
  }
  function mins(s) {
    return Number.isFinite(s) ? s / 60 : 0;
  }

  function resolveRouteStartStopForDep(routeId, depName) {
    const trips = (ctx.gtfs?.trips || []).filter(
      (t) => String(t.route_id) === String(routeId),
    );
    const endpoints = ctx.tripEndpoints || new Map();
    for (const tr of trips) {
      const ep = endpoints.get(String(tr.trip_id));
      if (!ep) continue;
      if ((ep.startName || "") === (depName || "")) {
        const s = STOPS_BY_ID.get(ep.startStopId);
        if (s && s.stop_lat != null && s.stop_lon != null) {
          return {
            lat: +s.stop_lat,
            lon: +s.stop_lon,
            name: s.stop_name || depName,
          };
        }
      }
    }
    return null;
  }

  function resolveCustomStop(val) {
    if (!val || typeof val !== "string") return null;
    if (!val.startsWith("custom:")) return null;
    const id = val.slice("custom:".length);
    const cs = ctx.customStopById.get(id);
    if (!cs) return null;
    return { lat: cs.lat, lon: cs.lon, name: cs.name };
  }

  function buildFromCustomLeg(col, depHHMM, arrHHMM) {
    const leg = col.customLeg;
    if (!leg || !leg.coords || !leg.start || !leg.end)
      return { busRouteData: [], scheduleData: [] };

    // Ensure coords start/end explicitly
    let coords = (leg.coords || []).slice();
    if (coords.length) {
      const first = coords[0];
      const lastC = coords[coords.length - 1];
      const s = [leg.start.lon, leg.start.lat];
      const e = [leg.end.lon, leg.end.lat];
      if (first[0] !== s[0] || first[1] !== s[1]) coords.unshift(s);
      if (lastC[0] !== e[0] || lastC[1] !== e[1]) coords.push(e);
    } else {
      coords = [
        [leg.start.lon, leg.start.lat],
        [leg.end.lon, leg.end.lat],
      ];
    }

    const busRouteData = [
      {
        starting_point: {
          latitude: leg.start.lat,
          longitude: leg.start.lon,
          address: leg.start.name || "",
        },
        next_points: [
          {
            latitude: leg.end.lat,
            longitude: leg.end.lon,
            address: leg.end.name || "",
            duration: `${Math.round(mins(leg.duration_s || 0))} minutes`,
            route_coordinates: coords, // [lon,lat]
          },
        ],
      },
    ];

    const scheduleData = [
      {
        runNo: "",
        startTime: depHHMM || "",
        endTime: arrHHMM || "",
        runName: "Custom",
        busStops: [
          {
            name: "Stop S",
            time: depHHMM || "",
            latitude: leg.start.lat,
            longitude: leg.start.lon,
            address: leg.start.name || "",
            abbreviation: leg.start.name ? getAbbreviation(leg.start.name) : "",
            time_point: 1,
          },
          {
            name: "Stop E",
            time: arrHHMM || "",
            latitude: leg.end.lat,
            longitude: leg.end.lon,
            address: leg.end.name || "",
            abbreviation: leg.end.name ? getAbbreviation(leg.end.name) : "",
            time_point: 1,
          },
        ],
      },
    ];

    return { busRouteData, scheduleData };
  }
};
