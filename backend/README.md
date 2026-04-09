# AppointmentBooking45 Backend

This is a lightweight backend API (Python standard library only) that stores appointments in SQLite and serves the front-end from `../frontend`.

## Run

From this repo root:

```bash
python3 backend/server.py
```

Then open:

- Front-end + API at: `http://localhost:8080/`

Change port:

```bash
PORT=8081 python3 backend/server.py
```

## Run on port 3000 (ngrok-friendly)

Default port is now `3000`, so this works out of the box:

```bash
python3 backend/server.py
```

Open:

- Front-end + API at: `http://localhost:3000/`

Expose all APIs + UI via ngrok:

```bash
ngrok http 3000
```

If your ngrok domain is, for example, `https://myownapis.ngrok.app`, all API endpoints are then available under:

- `https://myownapis.ngrok.app/api/appointments`
- `https://myownapis.ngrok.app/api/book-new`
- `https://myownapis.ngrok.app/api/reschedule-existing`
- ...etc.

## API Endpoints

- `GET /api/appointments?from=DD/MM/YYYY HH:mm&to=DD/MM/YYYY HH:mm`
  - Returns confirmed appointments in that range.
- `POST /api/appointments`
  - Manual single booking.
- `POST /api/reschedule`
  - Multi-slot booking/rescheduling (books any slots that are free; returns per-slot errors for conflicts).
  - Supports:
    - single-client + `requestedSlots: string[]`
    - or `appointments: [{clientId, clientName, clientDob, startLocal, durationMinutes?}, ...]`
- `POST /api/cancel`
  - Cancels by `bookingReference` + `clientDob`.
- `POST /api/my-appointments`
  - Looks up an appointment by `bookingReference` + `clientDob` and returns it if still confirmed.
- `POST /api/reschedule-existing`
  - Moves an existing booking reference to the first available requested slot.

## Time Format

Inputs accept `DD/MM/YYYY HH:mm` (interpreted as `Australia/Sydney`), and ISO 8601 datetimes.

Responses return display-friendly Australian local datetime strings.

