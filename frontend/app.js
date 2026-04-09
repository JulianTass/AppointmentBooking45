const API = {
  appointments: "/api/appointments",
  reschedule: "/api/reschedule",
  cancel: "/api/cancel",
  delete: "/api/delete",
  deleteAll: "/api/delete-all",
  myAppointments: "/api/my-appointments",
  rescheduleExisting: "/api/reschedule-existing",
};

const $ = (id) => document.getElementById(id);

const els = {
  monthLabel: $("monthLabel"),
  monthSub: $("monthSub"),
  calendarCells: $("calendarCells"),
  toastHost: $("toastHost"),
  btnPrev: $("btnPrev"),
  btnNext: $("btnNext"),
  btnRefresh: $("btnRefresh"),
  btnRandomise: $("btnRandomise"),
  btnToggleManual: $("btnToggleManual"),
  btnDeleteAll: $("btnDeleteAll"),
  layoutMain: $("layoutMain"),
  manualPanel: $("manualPanel"),
  manualForm: $("manualForm"),
  bookedList: $("bookedList"),
  bookedCount: $("bookedCount"),
  bookedHeading: $("bookedHeading"),
  bookedFilterDate: $("bookedFilterDate"),
  fClientId: $("fClientId"),
  fClientName: $("fClientName"),
  fClientDob: $("fClientDob"),
  fStartDate: $("fStartDate"),
  fStartTime: $("fStartTime"),
  fDuration: $("fDuration"),

  // Modal
  modalOverlay: $("modalOverlay"),
  detailsModal: $("detailsModal"),
  modalTitle: $("modalTitle"),
  modalSubtitle: $("modalSubtitle"),
  btnCloseModal: $("btnCloseModal"),
  btnCancelBooking: $("btnCancelBooking"),
  btnRescheduleExisting: $("btnRescheduleExisting"),
  btnDeleteBooking: $("btnDeleteBooking"),
  rescheduleBox: $("rescheduleBox"),
  // details
  dBookingRef: $("dBookingRef"),
  dClientId: $("dClientId"),
  dClientName: $("dClientName"),
  dClientDob: $("dClientDob"),
  dStart: $("dStart"),
  dDuration: $("dDuration"),

  // reschedule UI
  rClientDob: $("rClientDob"),
  rSlots: $("rSlots"),
  btnSubmitReschedule: $("btnSubmitReschedule"),
  btnCloseReschedule: $("btnCloseReschedule"),
  rResult: $("rResult"),
};

const state = {
  monthDate: new Date(),
  events: [],
  selectedEvent: null,
  bookedFilterDateKey: "",
  isManualPanelOpen: false,
  autoRefreshTimer: null,
};

const NAMES = [
  "Ava Thompson",
  "Noah Nguyen",
  "Mia Richardson",
  "Oliver Clarke",
  "Sophia Patel",
  "William Lee",
  "Isabella Martin",
  "James Anderson",
  "Emily Brown",
  "Lucas Wilson",
];

function pad2(n) {
  return String(n).padStart(2, "0");
}

function toAusDateTimeDisplay(date) {
  // date is a local Date; we only need dd/MM/YYYY HH:mm for display and API boundaries when using components.
  return `${pad2(date.getDate())}/${pad2(date.getMonth() + 1)}/${date.getFullYear()} ${pad2(
    date.getHours()
  )}:${pad2(date.getMinutes())}`;
}

function fromDatetimeLocalToAus(value) {
  // datetime-local value is "YYYY-MM-DDTHH:mm"
  if (!value) return "";
  const [datePart, timePart] = value.split("T");
  const [yyyy, mm, dd] = datePart.split("-");
  const [hh, min] = timePart.split(":");
  return `${dd}/${mm}/${yyyy} ${hh}:${min}`;
}

function ausToDatetimeLocal(aus) {
  // aus is "DD/MM/YYYY HH:mm"
  if (!aus) return "";
  const [datePart, timePart] = aus.split(" ");
  const [dd, mm, yyyy] = datePart.split("/");
  const [hh, min] = timePart.split(":");
  return `${yyyy}-${mm}-${dd}T${hh}:${min}`;
}

function monthLabel(d) {
  const month = d.toLocaleString("en-AU", { month: "long" });
  return `${month} ${d.getFullYear()}`;
}

function formatTime12h(hh, mm) {
  const hour24 = Number(hh);
  const mins = String(mm).padStart(2, "0");
  const suffix = hour24 >= 12 ? "pm" : "am";
  const hour12 = hour24 % 12 === 0 ? 12 : hour24 % 12;
  if (mins === "00") return `${hour12}${suffix}`;
  return `${hour12}:${mins} ${suffix}`;
}

function formatAusDateTime12h(aus) {
  // Converts "DD/MM/YYYY HH:mm" => "DD/MM/YYYY h:mm am/pm"
  if (!aus || typeof aus !== "string") return aus || "";
  const parts = aus.trim().split(" ");
  if (parts.length < 2) return aus;
  const datePart = parts[0];
  const timePart = parts[1];
  const timeBits = timePart.split(":");
  if (timeBits.length < 2) return aus;
  return `${datePart} ${formatTime12h(timeBits[0], timeBits[1])}`;
}

function formatTimeOnly12h(hhmm) {
  if (!hhmm || typeof hhmm !== "string") return hhmm || "";
  const bits = hhmm.split(":");
  if (bits.length < 2) return hhmm;
  return formatTime12h(bits[0], bits[1]);
}

function getTimePeriod(hhmm) {
  const bits = String(hhmm || "").split(":");
  const hour = Number(bits[0]);
  if (Number.isNaN(hour)) return "afternoon";
  return hour < 12 ? "morning" : "afternoon";
}

function monthBoundaryStrings(d) {
  // Use calendar components as Australia/Sydney local boundaries. Backend interprets these as Australia/Sydney.
  const year = d.getFullYear();
  const month = d.getMonth(); // 0-11
  const first = new Date(year, month, 1, 0, 0, 0, 0);
  const next = new Date(year, month + 1, 1, 0, 0, 0, 0);
  const from = toAusDateTimeDisplay(first);
  const to = toAusDateTimeDisplay(next);
  return { from, to };
}

function dateKeyFromYMD(y, m, day) {
  return `${y}-${pad2(m + 1)}-${pad2(day)}`;
}

function todayDateKeyLocal() {
  const now = new Date();
  return dateKeyFromYMD(now.getFullYear(), now.getMonth(), now.getDate());
}

function dateToDateInputValue(date) {
  // "YYYY-MM-DD" for <input type="date">
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
}

function fromDateInputToAusDateOnly(value) {
  // value is "YYYY-MM-DD" => "DD/MM/YYYY"
  if (!value) return "";
  const [yyyy, mm, dd] = value.split("-");
  if (!yyyy || !mm || !dd) return value;
  return `${dd}/${mm}/${yyyy}`;
}

function ausDateOnlyToDateInputValue(ausDateOnly) {
  // "DD/MM/YYYY" => "YYYY-MM-DD"
  if (!ausDateOnly) return "";
  const [dd, mm, yyyy] = String(ausDateOnly).trim().split("/");
  if (!dd || !mm || !yyyy) return "";
  return `${yyyy}-${mm}-${dd}`;
}

function toDobDateOnlyDisplay(value) {
  // Accepts either "DD/MM/YYYY" or "DD/MM/YYYY HH:mm" and always returns "DD/MM/YYYY".
  if (!value) return "";
  const raw = String(value).trim();
  const datePart = raw.split(" ")[0];
  const bits = datePart.split("/");
  if (bits.length === 3) return `${bits[0]}/${bits[1]}/${bits[2]}`;
  return datePart;
}

function dobForApi(value) {
  // Compatibility: keep DOB as date-only in UI, but send midnight time so
  // older backend builds (expecting datetime) still accept requests.
  const dateOnly = toDobDateOnlyDisplay(value);
  if (!dateOnly) return "";
  return `${dateOnly} 00:00`;
}

function startLocalAusFromDateTimeInputs() {
  const dateAus = fromDateInputToAusDateOnly(els.fStartDate.value);
  const timeHHMM = els.fStartTime.value || "";
  if (!dateAus || !timeHHMM) return "";
  return `${dateAus} ${timeHHMM}`;
}

function showToast(message, kind = "ok") {
  const div = document.createElement("div");
  div.className = `toast ${kind === "error" ? "toast--error" : "toast--ok"}`;
  div.textContent = message;
  els.toastHost.appendChild(div);
  setTimeout(() => {
    if (div.parentNode) div.parentNode.removeChild(div);
  }, 4500);
}

async function apiFetchAppointments(from, to) {
  const url = `${API.appointments}?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`;
  const res = await fetch(url, { method: "GET" });
  const data = await res.json();
  if (!data.ok) throw new Error(data.error || "Failed to fetch appointments");
  const list = Array.isArray(data.appointments) ? data.appointments : [];
  return list.map((ev) => {
    const out = { ...ev };
    // Be tolerant to payload shape differences so events still render.
    if (!out.startDateKey) {
      const d = out.bookedStartDate || out.bookedDate || (typeof out.startLocal === "string" ? out.startLocal.split(" ")[0] : "");
      if (d && d.includes("/")) {
        const [dd, mm, yyyy] = d.split("/");
        out.startDateKey = `${yyyy}-${mm}-${dd}`;
      }
    }
    if (!out.startTime) out.startTime = out.bookedStartTime || out.bookedTime || "";
    return out;
  });
}

function clearCalendar() {
  els.calendarCells.innerHTML = "";
}

function renderMonthGrid() {
  clearCalendar();

  const y = state.monthDate.getFullYear();
  const m = state.monthDate.getMonth();

  const firstDay = new Date(y, m, 1);
  // JS getDay: 0=Sun..6=Sat. Convert to Monday index 0..6.
  const offset = (firstDay.getDay() + 6) % 7;
  const daysInMonth = new Date(y, m + 1, 0).getDate();

  const totalCells = 42; // 6 weeks
  for (let i = 0; i < totalCells; i++) {
    const dayNumber = i - offset + 1;
    const cell = document.createElement("div");
    cell.className = "dayCell";
    if (dayNumber < 1 || dayNumber > daysInMonth) {
      cell.className = "dayCell dayCell--empty";
      cell.innerHTML = `<div class="dayCell__dateRow"><div class="dayCell__date">&nbsp;</div></div>`;
      els.calendarCells.appendChild(cell);
      continue;
    }

    const dateKey = dateKeyFromYMD(y, m, dayNumber);
    cell.dataset.dateKey = dateKey;
    cell.addEventListener("click", () => onDayCellClick(y, m, dayNumber));

    const header = document.createElement("div");
    header.className = "dayCell__dateRow";
    header.innerHTML = `<div class="dayCell__date">${dayNumber}</div><div class="dayCell__badge"></div>`;
    const eventsWrap = document.createElement("div");
    eventsWrap.className = "events";
    cell.appendChild(header);
    cell.appendChild(eventsWrap);

    const events = state.events.filter((e) => e.startDateKey === dateKey);
    if (events.length) {
      header.querySelector(".dayCell__badge").textContent = String(events.length);
      const toShow = events.slice(0, 3);
      toShow.forEach((ev) => {
        const pill = document.createElement("div");
        pill.className = "eventPill";
        const period = getTimePeriod(ev.startTime);
        pill.className += period === "morning" ? " eventPill--morning" : " eventPill--afternoon";
        if (ev.status === "CANCELLED") pill.className += " eventPill--cancelled";
        pill.tabIndex = 0;
        pill.setAttribute("role", "button");
        pill.dataset.bookingRef = ev.bookingReference;
        pill.dataset.clientDob = ev.clientDob;
        pill.dataset.clientId = ev.clientId;
        pill.dataset.clientName = ev.clientName;

        pill.innerHTML = `
          <div class="eventPill__t">
            <span>${formatTimeOnly12h(ev.startTime)}</span>
            <span class="eventPill__status">${(ev.status || "CONFIRMED").toLowerCase()}</span>
          </div>
          <div class="eventPill__b">${ev.clientName} • ${ev.bookingReference} • ${period}</div>
        `;
        pill.addEventListener("click", () => openDetails(ev));
        pill.addEventListener("keydown", (e) => {
          if (e.key === "Enter" || e.key === " ") openDetails(ev);
        });
        eventsWrap.appendChild(pill);
      });
      const remaining = events.length - toShow.length;
      if (remaining > 0) {
        const more = document.createElement("div");
        more.className = "more";
        more.textContent = `+${remaining} more`;
        eventsWrap.appendChild(more);
      }
    }
    els.calendarCells.appendChild(cell);
  }
}

function onDayCellClick(y, m, day) {
  // Keep the selected time (if any) and only replace the date.
  const existingTime = els.fStartTime.value || "09:00";
  const [hhStr, mmStr] = existingTime.split(":");
  const hh = Number(hhStr);
  const mm = Number(mmStr);
  const safeH = Number.isNaN(hh) ? 9 : hh;
  const safeM = Number.isNaN(mm) ? 0 : mm;

  const clickedDate = new Date(y, m, day, 12, 0, 0, 0); // noon avoids DST edge cases
  els.fStartDate.value = dateToDateInputValue(clickedDate);
  els.fStartTime.value = `${pad2(safeH)}:${pad2(safeM)}`;

  const dt = new Date(y, m, day, safeH, safeM, 0, 0);
  setManualPanelVisible(true);
  els.fStartDate.focus();
  showToast(`Selected ${formatAusDateTime12h(toAusDateTimeDisplay(dt))} for booking.`, "ok");
}

function setManualPanelVisible(isVisible) {
  state.isManualPanelOpen = Boolean(isVisible);
  els.manualPanel.classList.toggle("manualPanel--hidden", !state.isManualPanelOpen);
  els.layoutMain.classList.toggle("layout--with-panel", state.isManualPanelOpen);
  els.btnToggleManual.textContent = state.isManualPanelOpen ? "Close booking" : "Add booking";
}

function closeModal() {
  els.modalOverlay.hidden = true;
  els.rescheduleBox.hidden = true;
  els.rResult.textContent = "";
  state.selectedEvent = null;
}

function openModal() {
  els.modalOverlay.hidden = false;
}

function openDetails(event) {
  state.selectedEvent = event;
  els.modalTitle.textContent = `Appointment • ${event.bookingReference}`;
  els.modalSubtitle.textContent = `${formatAusDateTime12h(event.startLocal)} - ${formatAusDateTime12h(event.endLocal)}`;
  els.dBookingRef.textContent = event.bookingReference;
  els.dClientId.textContent = event.clientId;
  els.dClientName.textContent = event.clientName;
  els.dClientDob.textContent = toDobDateOnlyDisplay(event.clientDob);
  els.dStart.textContent = `${formatAusDateTime12h(event.startLocal)} (${formatTimeOnly12h(event.startTime)})`;
  els.dDuration.textContent = `${event.durationMinutes} minutes`;

  els.rClientDob.value = ausDateOnlyToDateInputValue(toDobDateOnlyDisplay(event.clientDob));
  els.rSlots.value = "";
  els.rResult.textContent = "";
  els.rescheduleBox.hidden = true;
  openModal();
}

async function cancelSelected() {
  const ev = state.selectedEvent;
  if (!ev) return;
  try {
    const res = await fetch(API.cancel, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ bookingReference: ev.bookingReference, clientDob: dobForApi(ev.clientDob) }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Cancel failed");
    showToast("Appointment cancelled.", "ok");
    closeModal();
    await loadMonth();
  } catch (e) {
    showToast(String(e.message || e), "error");
  }
}

async function deleteAppointment(ev) {
  if (!ev) return;
  try {
    const res = await fetch(API.delete, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ bookingReference: ev.bookingReference, clientDob: dobForApi(ev.clientDob) }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Delete failed");
    showToast("Appointment deleted.", "ok");
    if (state.selectedEvent && state.selectedEvent.bookingReference === ev.bookingReference) {
      closeModal();
    }
    await loadMonth();
  } catch (e) {
    showToast(String(e.message || e), "error");
  }
}

async function deleteAllAppointments() {
  const ok = window.confirm("Delete ALL bookings? This cannot be undone.");
  if (!ok) return;
  try {
    const res = await fetch(API.deleteAll, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Delete all failed");
    showToast("All bookings deleted.", "ok");
    await refreshNow();
  } catch (e) {
    showToast(String(e.message || e), "error");
  }
}

async function rescheduleSelected() {
  const ev = state.selectedEvent;
  if (!ev) return;
  const dobAus = dobForApi(fromDateInputToAusDateOnly(els.rClientDob.value));
  const durationMinutes = ev.durationMinutes || 30;
  const slotParts = (els.rSlots.value || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  if (!dobAus) return showToast("Please enter DOB.", "error");
  if (!slotParts.length) return showToast("Please enter at least one requested slot.", "error");

  try {
    els.rResult.textContent = "Trying…";
    const res = await fetch(API.rescheduleExisting, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        bookingReference: ev.bookingReference,
        clientDob: dobAus,
        requestedSlots: slotParts,
        durationMinutes,
      }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Reschedule failed");

    const to = data.appointment?.startLocal || "";
    els.rResult.textContent = to
      ? `Success: rescheduled to ${to}`
      : "Success: rescheduled appointment.";

    showToast("Reschedule successful.", "ok");
    await loadMonth();
    closeModal();
  } catch (e) {
    els.rResult.textContent = String(e.message || e);
    showToast(String(e.message || e), "error");
  }
}

function computeRandomRange(now, daysAhead) {
  const startMs = now.getTime();
  const endMs = now.getTime() + daysAhead * 24 * 60 * 60 * 1000;
  return { startMs, endMs };
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomPick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function dateToDatetimeLocalValue(date) {
  // Converts a local Date into "YYYY-MM-DDTHH:mm" without timezone shifting.
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}T${pad2(
    date.getHours()
  )}:${pad2(date.getMinutes())}`;
}

function buildRandomAusDateTime(startDate, endDate) {
  const base = new Date(randomInt(startDate.getTime(), endDate.getTime()));
  // Snap to 30-minute increments
  const mins = base.getMinutes();
  const snapped = Math.round(mins / 30) * 30;
  base.setMinutes(snapped);
  base.setSeconds(0);
  base.setMilliseconds(0);
  return base;
}

function clampToBusinessHours(d) {
  // Keep within 09:00 - 16:00 and snap to 30-min increments.
  const copy = new Date(d);
  copy.setSeconds(0);
  copy.setMilliseconds(0);
  let hour = copy.getHours();
  let minute = copy.getMinutes();
  const step = 30;
  minute = Math.round(minute / step) * step;
  if (minute >= 60) {
    minute = 0;
    hour += 1;
  }
  if (hour < 9) {
    hour = 9;
    minute = 0;
  }
  if (hour > 16 || (hour === 16 && minute > 0)) {
    hour = 16;
    minute = 0;
  }
  copy.setHours(hour, minute, 0, 0);
  return copy;
}

async function randomiseBookings() {
  try {
    els.btnRandomise.disabled = true;
    els.monthSub.textContent = "Randomising…";

    const now = new Date();
    const range = computeRandomRange(now, 30);
    const slotCount = 12;
    const appointments = [];

    for (let i = 0; i < slotCount; i++) {
      const slotDate = clampToBusinessHours(buildRandomAusDateTime(new Date(range.startMs), new Date(range.endMs)));
      const startLocal = toAusDateTimeDisplay(slotDate);

      const dobYear = randomInt(1955, 2004);
      const dobMonth = randomInt(1, 12);
      const dobDay = randomInt(1, 28); // avoid month-length issues
      const dob = new Date(dobYear, dobMonth - 1, dobDay, 12, 0, 0, 0);
      const clientDob = `${pad2(dobDay)}/${pad2(dobMonth)}/${dobYear}`;

      const clientName = randomPick(NAMES);
      const clientId = `C${randomInt(1000, 9999)}`;

      appointments.push({
        clientId,
        clientName,
        clientDob: dobForApi(clientDob),
        startLocal,
        durationMinutes: 30,
      });
    }

    const res = await fetch(API.reschedule, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ appointments }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Randomise failed");

    const bookedCount = data.bookedCount || 0;
    const taken = (data.results || []).filter((r) => !r.available).length;

    showToast(`Randomise complete: ${bookedCount} booked${taken ? `, ${taken} taken` : ""}.`, taken ? "error" : "ok");
    els.monthSub.textContent = "Loading appointments…";
    await loadMonth();
  } catch (e) {
    showToast(String(e.message || e), "error");
    els.monthSub.textContent = "Load failed.";
  } finally {
    els.btnRandomise.disabled = false;
  }
}

async function loadMonth() {
  const { from, to } = monthBoundaryStrings(state.monthDate);
  els.monthSub.textContent = "Loading appointments…";
  try {
    // Always render the calendar grid so the UI doesn't appear blank
    // if the API fails (e.g. frontend opened directly without backend).
    state.events = [];
    renderMonthGrid();

    state.events = await apiFetchAppointments(from, to);
    els.monthSub.textContent = state.events.length
      ? `${state.events.length} appointment(s) this month`
      : "No bookings yet.";
    renderMonthGrid();
    renderBookedList();
  } catch (e) {
    state.events = [];
    els.monthSub.textContent = "Failed to load appointments.";
    showToast(String(e.message || e), "error");
    renderMonthGrid();
    renderBookedList();
  }
}

function syncMonthToBookedFilterDate() {
  const selectedKey = state.bookedFilterDateKey || "";
  if (!selectedKey) return;
  const [y, m] = selectedKey.split("-").map((p) => Number(p));
  if (Number.isNaN(y) || Number.isNaN(m)) return;
  const currentY = state.monthDate.getFullYear();
  const currentM = state.monthDate.getMonth() + 1;
  if (currentY !== y || currentM !== m) {
    state.monthDate = new Date(y, m - 1, 1);
    els.monthLabel.textContent = monthLabel(state.monthDate);
  }
}

async function refreshNow({ showToastMessage = false } = {}) {
  syncMonthToBookedFilterDate();
  await loadMonth();
  if (showToastMessage) {
    showToast("Calendar refreshed.", "ok");
  }
}

function renderBookedList() {
  const selectedKey = state.bookedFilterDateKey || todayDateKeyLocal();
  const list = (state.events || [])
    .filter((e) => e.startDateKey === selectedKey)
    .slice()
    .sort((a, b) => (a.startLocal > b.startLocal ? 1 : -1));

  els.bookedHeading.textContent =
    selectedKey === todayDateKeyLocal() ? "Booked appointments (today)" : `Booked appointments (${selectedKey})`;
  els.bookedCount.textContent = String(list.length);
  els.bookedList.innerHTML = "";
  if (!list.length) {
    const empty = document.createElement("div");
    empty.className = "bookedEmpty";
    empty.textContent =
      selectedKey === todayDateKeyLocal()
        ? "No appointments booked for today."
        : `No appointments booked for ${selectedKey}.`;
    els.bookedList.appendChild(empty);
    return;
  }

  list.forEach((ev) => {
    const card = document.createElement("div");
    card.className = "bookedCard";
    card.innerHTML = `
      <div class="bookedCard__top">
        <div class="bookedCard__ref">${ev.bookingReference}</div>
        <div class="eventPill__status">${(ev.status || "CONFIRMED").toLowerCase()}</div>
      </div>
      <div class="bookedCard__name">${ev.clientName}</div>
      <div class="bookedCard__meta">Client ID: ${ev.clientId}</div>
      <div class="bookedCard__meta">DOB: ${toDobDateOnlyDisplay(ev.clientDob)}</div>
      <div class="bookedCard__meta">Appointment: ${formatAusDateTime12h(ev.startLocal)} (${ev.durationMinutes} mins)</div>
      <div class="bookedCard__actions">
        <button class="btn" data-action="view">View</button>
        <button class="btn btn--danger" data-action="cancel">Cancel</button>
        <button class="btn" data-action="reschedule">Reschedule</button>
        <button class="btn btn--danger" data-action="delete">Delete</button>
      </div>
    `;
    card.querySelector('[data-action="view"]').addEventListener("click", () => openDetails(ev));
    card.querySelector('[data-action="cancel"]').addEventListener("click", async () => {
      state.selectedEvent = ev;
      await cancelSelected();
    });
    card.querySelector('[data-action="reschedule"]').addEventListener("click", () => {
      openDetails(ev);
      els.rescheduleBox.hidden = false;
    });
    card.querySelector('[data-action="delete"]').addEventListener("click", async () => {
      await deleteAppointment(ev);
    });
    els.bookedList.appendChild(card);
  });
}

function defaultFormValues() {
  const now = new Date();
  const plus30 = new Date(now.getTime() + 30 * 60 * 1000);
  // Round to nearest 5 minutes.
  plus30.setMinutes(Math.ceil(plus30.getMinutes() / 5) * 5);
  plus30.setSeconds(0);
  plus30.setMilliseconds(0);

  // DOB default: random-ish recent year to make testing easier.
  const dob = new Date(now.getFullYear() - 35, 0, 1, 0, 0, 0, 0);

  els.fClientId.value = els.fClientId.value || `C${randomInt(1000, 9999)}`;
  els.fClientName.value = els.fClientName.value || randomPick(NAMES);
  els.fDuration.value = els.fDuration.value || 30;

  els.fClientDob.value = els.fClientDob.value || dateToDateInputValue(dob);
  els.fStartDate.value = els.fStartDate.value || dateToDateInputValue(plus30);
  els.fStartTime.value = els.fStartTime.value || `${pad2(plus30.getHours())}:${pad2(plus30.getMinutes())}`;
}

function wireEvents() {
  els.btnPrev.addEventListener("click", () => {
    state.monthDate.setMonth(state.monthDate.getMonth() - 1);
    els.monthLabel.textContent = monthLabel(state.monthDate);
    loadMonth();
  });
  els.btnNext.addEventListener("click", () => {
    state.monthDate.setMonth(state.monthDate.getMonth() + 1);
    els.monthLabel.textContent = monthLabel(state.monthDate);
    loadMonth();
  });
  els.btnRefresh.addEventListener("click", async () => {
    els.btnRefresh.disabled = true;
    const originalText = els.btnRefresh.textContent;
    els.btnRefresh.textContent = "Refreshing…";
    try {
      await refreshNow({ showToastMessage: true });
    } finally {
      els.btnRefresh.textContent = originalText;
      els.btnRefresh.disabled = false;
    }
  });

  els.btnToggleManual.addEventListener("click", () => {
    const nextVisible = !state.isManualPanelOpen;
    setManualPanelVisible(nextVisible);
    if (nextVisible) {
      setTimeout(() => els.fClientId.focus(), 0);
    }
  });
  els.btnDeleteAll.addEventListener("click", deleteAllAppointments);

  els.bookedFilterDate.addEventListener("change", async () => {
    const value = (els.bookedFilterDate.value || "").trim(); // YYYY-MM-DD
    if (!value) return;
    state.bookedFilterDateKey = value;

    // Jump to selected month so the API fetch includes that date's month.
    const [y, m] = value.split("-").map((p) => Number(p));
    if (!Number.isNaN(y) && !Number.isNaN(m)) {
      state.monthDate = new Date(y, m - 1, 1);
      els.monthLabel.textContent = monthLabel(state.monthDate);
      await loadMonth();
      return;
    }
    renderBookedList();
  });

  els.manualForm.addEventListener("submit", async (e) => {
    e.preventDefault();

    const body = {
      clientId: els.fClientId.value.trim(),
      clientName: els.fClientName.value.trim(),
      clientDob: dobForApi(fromDateInputToAusDateOnly(els.fClientDob.value)),
      startLocal: startLocalAusFromDateTimeInputs(),
      durationMinutes: Number(els.fDuration.value || 30),
    };

    try {
      els.monthSub.textContent = "Booking…";
      const res = await fetch(API.appointments, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!data.ok) throw new Error(data.error || "Booking failed");

      showToast("Appointment booked.", "ok");
      els.monthSub.textContent = "Loading appointments…";
      await loadMonth();
      defaultFormValues();
      setManualPanelVisible(false);
    } catch (err) {
      showToast(String(err.message || err), "error");
      els.monthSub.textContent = "Booking failed.";
    }
  });

  els.btnRandomise.addEventListener("click", randomiseBookings);

  els.btnCloseModal.addEventListener("click", closeModal);
  els.modalOverlay.addEventListener("click", (e) => {
    if (e.target === els.modalOverlay) closeModal();
  });
  els.btnCancelBooking.addEventListener("click", cancelSelected);
  els.btnDeleteBooking.addEventListener("click", () => deleteAppointment(state.selectedEvent));

  els.btnRescheduleExisting.addEventListener("click", () => {
    els.rescheduleBox.hidden = false;
    els.rResult.textContent = "";
    // If user hasn't entered anything, help with sensible defaults.
    if (!els.rSlots.value) {
      const d = new Date(state.selectedEvent ? state.selectedEvent.startLocal : new Date());
      // No strict default here because parsing requires Australian format; leave empty.
    }
  });
  els.btnCloseReschedule.addEventListener("click", () => {
    els.rescheduleBox.hidden = true;
  });
  els.btnSubmitReschedule.addEventListener("click", rescheduleSelected);
}

async function init() {
  // Set initial month to current month.
  state.monthDate = new Date();
  state.monthDate.setDate(1);
  state.bookedFilterDateKey = todayDateKeyLocal();
  els.bookedFilterDate.value = state.bookedFilterDateKey;
  els.monthLabel.textContent = monthLabel(state.monthDate);
  setManualPanelVisible(false);
  defaultFormValues();
  wireEvents();
  await refreshNow();

  // Keep UI in sync with external API posts (e.g. Postman) without manual reload.
  state.autoRefreshTimer = window.setInterval(() => {
    refreshNow().catch(() => {
      // Ignore periodic refresh errors; existing UI state remains.
    });
  }, 60000);
}

init();

