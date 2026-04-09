import json
import mimetypes
import os
import random
import re
import sqlite3
import string
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

try:
    _TZ_SYD = ZoneInfo("Australia/Sydney")
except Exception:
    # Fallback to UTC if zoneinfo isn't available (should be available on modern Python).
    _TZ_SYD = ZoneInfo("UTC")


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STATIC_DIR = os.path.join(ROOT_DIR, "frontend")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(DATA_DIR, "appointments.db")


DATE_AU_RE = re.compile(r"^\s*(\d{2})/(\d{2})/(\d{4})\s+(\d{2}):(\d{2})\s*$")
DATE_AU_DATE_ONLY_RE = re.compile(r"^\s*(\d{2})/(\d{2})/(\d{4})\s*$")
DATE_AU_12H_RE = re.compile(
    r"^\s*(\d{2})/(\d{2})/(\d{4})\s+(\d{1,2})(?::(\d{2}))?\s*([AaPp][Mm])\s*$"
)
LOOSE_KV_RE = re.compile(
    r'(?P<key>clientId|clientID|client_id|clientName|client_name|clientDob|clientDOB|client_dob|bookingReference|date|time|durationMinutes)\s*[:=]\s*"?(?P<value>[^",\}\n]+)"?',
    re.IGNORECASE,
)


def _ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


def _connect() -> sqlite3.Connection:
    _ensure_dirs()
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS appointments (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              booking_reference TEXT NOT NULL UNIQUE,
              client_id TEXT NOT NULL,
              client_name TEXT NOT NULL,
              client_dob_epoch INTEGER NOT NULL,
              start_epoch INTEGER NOT NULL,
              end_epoch INTEGER NOT NULL,
              status TEXT NOT NULL DEFAULT 'CONFIRMED',
              created_epoch INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_appointments_start ON appointments(start_epoch);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_appointments_end ON appointments(end_epoch);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_appointments_status ON appointments(status);"
        )
        conn.commit()

        # One-off migration: normalize any legacy long booking references to BK###.
        _migrate_booking_references(conn)


def _parse_datetime_au(dt_str: str) -> datetime:
    """
    Parses an Australian local datetime string into an aware datetime in Australia/Sydney.
    Accepts either:
      - 'DD/MM/YYYY HH:mm' (no timezone; treated as Australia/Sydney local time)
      - 'DD/MM/YYYY h[:mm]am/pm' (e.g. '05/04/2026 7pm' or '05/04/2026 7:30pm')
      - ISO 8601 strings (with or without timezone). If timezone missing, treated as Australia/Sydney.
    """
    dt_str = (dt_str or "").strip()
    if not dt_str:
        raise ValueError("Missing date/time")

    m = DATE_AU_RE.match(dt_str)
    if m:
        dd, mm, yyyy, hh, min_ = m.groups()
        naive = datetime(
            int(yyyy),
            int(mm),
            int(dd),
            int(hh),
            int(min_),
            0,
            tzinfo=_TZ_SYD,
        )
        return naive

    m12 = DATE_AU_12H_RE.match(dt_str)
    if m12:
        dd, mm, yyyy, hour12_str, min_opt, ampm = m12.groups()
        hour12 = int(hour12_str)
        minute = int(min_opt) if min_opt is not None else 0
        if hour12 < 1 or hour12 > 12:
            raise ValueError("Invalid 12-hour time (hour must be 1-12)")
        if minute < 0 or minute > 59:
            raise ValueError("Invalid minute")
        ampm_l = ampm.lower()
        hour24 = hour12 % 12
        if ampm_l == "pm":
            hour24 += 12
        return datetime(
            int(yyyy),
            int(mm),
            int(dd),
            hour24,
            minute,
            0,
            tzinfo=_TZ_SYD,
        )

    # Attempt ISO 8601.
    try:
        iso = datetime.fromisoformat(dt_str)
    except Exception as e:
        raise ValueError(f"Invalid datetime '{dt_str}': {e}") from e

    if iso.tzinfo is None:
        # Treat as Sydney local time.
        return iso.replace(tzinfo=_TZ_SYD)
    return iso.astimezone(_TZ_SYD)


def _parse_date_au(date_str: str) -> datetime:
    """
    Parses an Australian local date-only string into an aware datetime at local midnight.

    Accepts:
      - 'DD/MM/YYYY' (required for DOB identifier)
      - If a datetime is accidentally sent ('DD/MM/YYYY HH:mm'), the time part is ignored.
      - ISO 8601 dates ('YYYY-MM-DD') optionally with time (time ignored).
    """
    raw = (date_str or "").strip()
    if not raw:
        raise ValueError("Missing date")

    # Backward compatibility: if a time is included, only keep the date portion.
    if " " in raw:
        raw = raw.split(" ")[0]

    m = DATE_AU_DATE_ONLY_RE.match(raw)
    if m:
        dd, mm, yyyy = m.groups()
        return datetime(int(yyyy), int(mm), int(dd), 0, 0, 0, tzinfo=_TZ_SYD)

    # Attempt ISO date parsing (accept date or datetime).
    try:
        iso_dt = datetime.fromisoformat(raw)
    except Exception as e:
        raise ValueError(f"Invalid date '{date_str}': {e}") from e

    if isinstance(iso_dt, datetime):
        if iso_dt.tzinfo is None:
            return iso_dt.replace(tzinfo=_TZ_SYD, hour=0, minute=0, second=0, microsecond=0)
        iso_local = iso_dt.astimezone(_TZ_SYD)
        return iso_local.replace(hour=0, minute=0, second=0, microsecond=0)

    # Fallback (should not happen in practice).
    return datetime.now(tz=_TZ_SYD).replace(hour=0, minute=0, second=0, microsecond=0)


def _dt_to_epoch_seconds(dt: datetime) -> int:
    # Ensure aware datetime.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_TZ_SYD)
    return int(dt.timestamp())


def _epoch_to_local_display(epoch_seconds: int) -> str:
    dt_local = datetime.fromtimestamp(epoch_seconds, tz=_TZ_SYD)
    return dt_local.strftime("%d/%m/%Y %H:%M")


def _epoch_to_local_display_12h(epoch_seconds: int) -> str:
    """e.g. 15/04/2026 9am, 16/04/2026 1:30pm"""
    return f"{_epoch_to_local_date_display(epoch_seconds)} {_epoch_to_local_time_12h(epoch_seconds)}"


def _epoch_to_local_date_display(epoch_seconds: int) -> str:
    dt_local = datetime.fromtimestamp(epoch_seconds, tz=_TZ_SYD)
    return dt_local.strftime("%d/%m/%Y")


def _dob_day_range_epochs(client_dob_midnight: datetime) -> Tuple[int, int]:
    """
    Returns (start_epoch_in_sydney, end_epoch_in_sydney_exclusive) for the local DOB day.
    """
    # client_dob_midnight is expected to be aware at local midnight.
    day = client_dob_midnight.date()
    next_day = day + timedelta(days=1)
    next_midnight = datetime(next_day.year, next_day.month, next_day.day, 0, 0, 0, tzinfo=_TZ_SYD)
    start_epoch = _dt_to_epoch_seconds(client_dob_midnight)
    end_epoch = _dt_to_epoch_seconds(next_midnight)
    return start_epoch, end_epoch


def _epoch_to_local_date_key(epoch_seconds: int) -> str:
    dt_local = datetime.fromtimestamp(epoch_seconds, tz=_TZ_SYD)
    return dt_local.strftime("%Y-%m-%d")


def _epoch_to_local_time_display(epoch_seconds: int) -> str:
    dt_local = datetime.fromtimestamp(epoch_seconds, tz=_TZ_SYD)
    return dt_local.strftime("%H:%M")


def _epoch_to_local_time_12h(epoch_seconds: int) -> str:
    dt_local = datetime.fromtimestamp(epoch_seconds, tz=_TZ_SYD)
    hour = dt_local.hour
    minute = dt_local.minute
    suffix = "pm" if hour >= 12 else "am"
    hour12 = 12 if hour % 12 == 0 else hour % 12
    if minute == 0:
        return f"{hour12}{suffix}"
    return f"{hour12}:{minute:02d}{suffix}"


def _generate_booking_reference(conn: sqlite3.Connection) -> str:
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(50):
        ref = "BK" + "".join(random.choices(alphabet, k=10))
        existing = conn.execute(
            "SELECT 1 FROM appointments WHERE booking_reference = ? LIMIT 1", (ref,)
        ).fetchone()
        if existing:
            continue
        return ref
    # In practice, we should never reach this due to UNIQUE constraint, but just in case.
    return "BK" + "".join(random.choices(alphabet, k=10))


def _temp_booking_reference() -> str:
    # Temporary unique-ish ref used before converting to user-friendly BK{id}.
    return f"TMP{int(datetime.now(tz=_TZ_SYD).timestamp() * 1000)}{random.randint(100, 999)}"


def _generate_short_booking_reference(conn: sqlite3.Connection, row_id: int) -> str:
    """
    Generates a short booking reference in BK### format while preserving uniqueness.
    """
    preferred = row_id % 1000
    for offset in range(1000):
        n = (preferred + offset) % 1000
        candidate = f"BK{n:03d}"
        exists = conn.execute(
            "SELECT 1 FROM appointments WHERE booking_reference = ? LIMIT 1", (candidate,)
        ).fetchone()
        if not exists:
            return candidate
    raise APIError("Unable to generate unique booking reference", HTTPStatus.INTERNAL_SERVER_ERROR)


def _migrate_booking_references(conn: sqlite3.Connection) -> None:
    """
    For any existing rows whose booking_reference is not already BK###,
    assign a new unique short reference using the same generator.
    """
    # Find legacy refs (not BK followed by exactly 3 digits).
    cur = conn.execute(
        """
        SELECT id, booking_reference
        FROM appointments
        WHERE booking_reference NOT GLOB 'BK[0-9][0-9][0-9]'
        """
    )
    rows = cur.fetchall()
    if not rows:
        return

    for row in rows:
        row_id = int(row["id"])
        new_ref = _generate_short_booking_reference(conn, row_id)
        conn.execute(
            "UPDATE appointments SET booking_reference = ? WHERE id = ?",
            (new_ref, row_id),
        )
    conn.commit()


def _overlaps_sql() -> str:
    # Overlap if: existing.start < new.end AND existing.end > new.start
    return "start_epoch < ? AND end_epoch > ? AND status = 'CONFIRMED'"


class APIError(Exception):
    def __init__(self, message: str, status_code: int = 400, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.details = details or {}


def _json_body(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    def _read_chunked_body() -> bytes:
        chunks: List[bytes] = []
        # Minimal chunked-transfer parser (hex-size CRLF data CRLF ... 0 CRLF CRLF)
        while True:
            size_line = handler.rfile.readline().strip()
            if not size_line:
                break
            # Ignore optional chunk extensions after ';'
            size_hex = size_line.split(b";", 1)[0]
            try:
                chunk_size = int(size_hex, 16)
            except Exception:
                break
            if chunk_size == 0:
                # Consume trailing headers terminator if present.
                handler.rfile.readline()
                break
            chunk = handler.rfile.read(chunk_size)
            chunks.append(chunk)
            # Consume trailing CRLF after chunk.
            handler.rfile.read(2)
        return b"".join(chunks)

    raw = b""
    transfer_encoding = (handler.headers.get("Transfer-Encoding", "") or "").lower()
    if "chunked" in transfer_encoding:
        raw = _read_chunked_body()
    else:
        length = int(handler.headers.get("Content-Length", "0") or "0")
        if length > 0:
            raw = handler.rfile.read(length)

    if not raw:
        return {}
    def _decode_nested(value: Any) -> Dict[str, Any]:
        # Direct dict.
        if isinstance(value, dict):
            return value
        # JSON-encoded string.
        if isinstance(value, str):
            nested = json.loads(value)
            if isinstance(nested, dict):
                return nested
        raise APIError("JSON body must be an object", HTTPStatus.BAD_REQUEST)

    def _parse_loose_text_payload(text: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for m in LOOSE_KV_RE.finditer(text):
            key = m.group("key")
            val = m.group("value").strip()
            if key and val:
                out[key] = val
        return out

    try:
        parsed = json.loads(raw.decode("utf-8"))
        obj = _decode_nested(parsed)

        # Common gateway wrappers (Genesys / webhook middleware):
        # { "body": {...} } or { "body": "{\"clientId\":\"...\"}" } etc.
        for wrapper_key in ("body", "payload", "data", "request"):
            if wrapper_key in obj:
                try:
                    inner = _decode_nested(obj[wrapper_key])
                    return inner
                except Exception:
                    # If wrapper key exists but not decodable, keep original obj.
                    break

        # If there is a single key whose value is a dict/stringified dict, unwrap it.
        if len(obj) == 1:
            only_val = next(iter(obj.values()))
            try:
                inner = _decode_nested(only_val)
                return inner
            except Exception:
                pass

        return obj
    except APIError:
        raise
    except Exception:
        # Fallback for non-JSON clients:
        # 1) form-urlencoded
        # 2) loose key-value text payloads
        text = raw.decode("utf-8", errors="ignore")
        form = parse_qs(text)
        if form:
            flat: Dict[str, Any] = {k: (v[0] if v else "") for k, v in form.items()}
            if flat:
                return flat

        loose = _parse_loose_text_payload(text)
        if loose:
            return loose

        raise APIError("Invalid JSON body", HTTPStatus.BAD_REQUEST)


def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _validate_required(obj: Dict[str, Any], keys: List[str]) -> None:
    for k in keys:
        if k not in obj or obj[k] in (None, ""):
            raise APIError(f"Missing required field: {k}", HTTPStatus.BAD_REQUEST)


def _first_present(obj: Dict[str, Any], candidates: List[str]) -> Any:
    for key in candidates:
        if key in obj and obj[key] not in (None, ""):
            return obj[key]
    return None


def _find_in_nested(value: Any, candidates: List[str]) -> Any:
    """
    Recursively searches nested dict/list/stringified-json structures for any
    of the candidate keys. Returns the first non-empty value found.
    """
    if isinstance(value, dict):
        hit = _first_present(value, candidates)
        if hit not in (None, ""):
            return hit
        for v in value.values():
            nested = _find_in_nested(v, candidates)
            if nested not in (None, ""):
                return nested
        return None
    if isinstance(value, list):
        for item in value:
            nested = _find_in_nested(item, candidates)
            if nested not in (None, ""):
                return nested
        return None
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("{") or s.startswith("["):
            try:
                parsed = json.loads(s)
                return _find_in_nested(parsed, candidates)
            except Exception:
                return None
    return None


def _collect_top_level_keys(value: Any) -> List[str]:
    """
    Returns a short list of top-level-like keys for debugging request shape.
    """
    keys: List[str] = []
    if isinstance(value, dict):
        keys.extend([str(k) for k in value.keys()])
        # Also collect one nested dict level if present.
        for v in value.values():
            if isinstance(v, dict):
                keys.extend([f"nested:{k}" for k in v.keys()])
    elif isinstance(value, list):
        keys.append("list")
        if value and isinstance(value[0], dict):
            keys.extend([f"list0:{k}" for k in value[0].keys()])
    elif isinstance(value, str):
        keys.append("string")
    return keys[:30]


def _generate_client_id(conn: sqlite3.Connection) -> str:
    """
    Generates a simple client ID like C1234.
    Not globally unique by constraint, but we try to avoid active collisions.
    """
    for _ in range(100):
        cid = f"C{random.randint(1000, 9999)}"
        exists = conn.execute(
            "SELECT 1 FROM appointments WHERE client_id = ? LIMIT 1", (cid,)
        ).fetchone()
        if not exists:
            return cid
    return f"C{random.randint(1000, 9999)}"


def _normalize_common_fields(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize common alias keys from integrations so all POST APIs can use the
    same canonical field names.
    """
    normalized = dict(body)
    alias_map = {
        "clientId": ["clientID", "client_id", "clientid", "ClientId", "ClientID"],
        "clientName": ["client_name", "clientname", "ClientName"],
        "clientDob": ["clientDOB", "client_dob", "clientdob", "dob", "DOB"],
        "bookingReference": [
            "booking_reference",
            "bookingreference",
            "bkReference",
            "bk_reference",
            "BookingReference",
        ],
        "startLocal": ["start_local", "start", "startDateTime", "start_datetime", "dateTime", "newStartLocal"],
        "requestedSlots": ["requested_slots", "slots", "requestedDateTimes"],
        "durationMinutes": ["duration", "duration_minutes", "durationMins", "DurationMinutes"],
        "date": ["newDate", "bookingDate", "appointmentDate"],
        "time": ["newTime", "bookingTime", "appointmentTime"],
    }
    for canonical, aliases in alias_map.items():
        if canonical in normalized and normalized[canonical] not in (None, ""):
            continue
        for alias in aliases:
            if alias in normalized and normalized[alias] not in (None, ""):
                normalized[canonical] = normalized[alias]
                break
    return normalized


def _appointment_row_to_payload(row: sqlite3.Row) -> Dict[str, Any]:
    start_epoch = int(row["start_epoch"])
    end_epoch = int(row["end_epoch"])
    dob_epoch = int(row["client_dob_epoch"])
    start_date = _epoch_to_local_date_display(start_epoch)
    end_date = _epoch_to_local_date_display(end_epoch)
    start_time_24h = _epoch_to_local_time_display(start_epoch)
    end_time_24h = _epoch_to_local_time_display(end_epoch)
    start_time_12h = _epoch_to_local_time_12h(start_epoch)
    end_time_12h = _epoch_to_local_time_12h(end_epoch)
    return {
        "bookingReference": row["booking_reference"],
        "clientId": row["client_id"],
        "clientName": row["client_name"],
        "clientDob": _epoch_to_local_date_display(dob_epoch),
        "startLocal": _epoch_to_local_display(start_epoch),
        "endLocal": _epoch_to_local_display(end_epoch),
        "startDateKey": _epoch_to_local_date_key(start_epoch),
        "startTime": start_time_24h,
        # Clearer API fields for clients/testing tools.
        "bookedDate": start_date,
        "bookedTime": start_time_24h,
        "bookedTime12h": start_time_12h,
        "bookedEndTime": end_time_24h,
        "bookedEndTime12h": end_time_12h,
        # Explicit split fields for clients that want date/time separated.
        "bookedStartDate": start_date,
        "bookedStartTime": start_time_24h,
        "bookedStartTime12h": start_time_12h,
        "bookedEndDate": end_date,
        "bookedEndTime": end_time_24h,
        "bookedEndTime12h": end_time_12h,
        "durationMinutes": max(0, int((end_epoch - start_epoch) // 60)),
        "status": row["status"],
    }


def _check_slot_conflict(
    conn: sqlite3.Connection,
    start_epoch: int,
    end_epoch: int,
    exclude_booking_ref: Optional[str] = None,
) -> bool:
    sql = f"SELECT id FROM appointments WHERE {_overlaps_sql()}"
    params: List[Any] = [end_epoch, start_epoch]
    if exclude_booking_ref:
        sql += " AND booking_reference != ?"
        params.append(exclude_booking_ref)
    sql += " LIMIT 1"
    cur = conn.execute(sql, tuple(params))
    return cur.fetchone() is not None


class Handler(BaseHTTPRequestHandler):
    server_version = "AppointmentBookingHTTP/0.1"

    def _send_json(self, status_code: int, payload: Dict[str, Any]) -> None:
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        # Keep it simple for local hosting; tighten in production.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def _send_text(self, status_code: int, text: str) -> None:
        self.send_response(status_code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(text.encode("utf-8"))

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api_get(parsed)
        else:
            self._serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            self._send_text(HTTPStatus.NOT_FOUND, "Not found")
            return

        try:
            self._handle_api_post(parsed)
        except APIError as e:
            self._send_json(
                e.status_code,
                {"ok": False, "error": e.message, "details": e.details},
            )
        except Exception as e:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": f"Internal error: {e}"},
            )

    def _serve_static(self, path: str) -> None:
        # Only serve files under STATIC_DIR.
        if path in ("", "/"):
            path = "/index.html"

        # Basic path traversal protection.
        rel = path.lstrip("/")
        # Allow opening `.../frontend/index.html` during local testing by
        # mapping it to the real file under `STATIC_DIR/`.
        if rel.startswith("frontend/"):
            rel = rel[len("frontend/") :]
        rel = rel.replace("..", "")
        fs_path = os.path.abspath(os.path.join(STATIC_DIR, rel))
        if not fs_path.startswith(os.path.abspath(STATIC_DIR)):
            self._send_text(HTTPStatus.FORBIDDEN, "Forbidden")
            return

        if not os.path.exists(fs_path) or not os.path.isfile(fs_path):
            self._send_text(HTTPStatus.NOT_FOUND, "Not found")
            return

        ctype, _ = mimetypes.guess_type(fs_path)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", ctype or "application/octet-stream")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        with open(fs_path, "rb") as f:
            self.wfile.write(f.read())

    def _handle_api_get(self, parsed) -> None:
        try:
            qs = parse_qs(parsed.query)
            path = parsed.path

            if path == "/api/appointments":
                _from = (qs.get("from", [""])[0] or "").strip()
                _to = (qs.get("to", [""])[0] or "").strip()
                _validate_required({"from": _from, "to": _to}, ["from", "to"])

                from_dt = _parse_datetime_au(_from)
                to_dt = _parse_datetime_au(_to)
                from_epoch = _dt_to_epoch_seconds(from_dt)
                to_epoch = _dt_to_epoch_seconds(to_dt)
                if to_epoch <= from_epoch:
                    raise APIError("Invalid date range", HTTPStatus.BAD_REQUEST)

                with _connect() as conn:
                    cur = conn.execute(
                        """
                        SELECT * FROM appointments
                        WHERE status = 'CONFIRMED'
                          AND start_epoch >= ?
                          AND start_epoch < ?
                        ORDER BY start_epoch ASC
                        """,
                        (from_epoch, to_epoch),
                    )
                    rows = cur.fetchall()
                payload = [_appointment_row_to_payload(r) for r in rows]
                self._send_json(HTTPStatus.OK, {"ok": True, "appointments": payload})
                return

            if path == "/api/my-appointment":
                # GET variant of "my appointments" lookup:
                # /api/my-appointment?bookingReference=BK123&clientDob=DD/MM/YYYY
                # or /api/my-appointment?bookingReference=BK123&clientId=C1234
                qs = parse_qs(parsed.query)
                booking_ref = (qs.get("bookingReference", [""])[0] or "").strip()
                client_dob_str = (qs.get("clientDob", [""])[0] or "").strip()
                client_id = (qs.get("clientId", [""])[0] or "").strip()
                # Tolerate malformed query shapes such as:
                # ?bookingReference=BK123&11/04/1991
                # where the clientDob key is missing.
                if not client_dob_str and parsed.query:
                    raw_parts = [p.strip() for p in parsed.query.split("&") if p.strip()]
                    for part in raw_parts:
                        if "=" not in part:
                            candidate = part
                            try:
                                # validate by parsing as DOB date
                                _parse_date_au(candidate)
                                client_dob_str = candidate
                                break
                            except Exception:
                                continue
                _validate_required({"bookingReference": booking_ref, "clientDob": client_dob_str}, ["bookingReference", "clientDob"])

                client_dob_dt = _parse_date_au(client_dob_str)
                dob_start_epoch, dob_end_epoch = _dob_day_range_epochs(client_dob_dt)

                with _connect() as conn:
                    sql = """
                        SELECT * FROM appointments
                        WHERE booking_reference = ?
                    """
                    params: List[Any] = [booking_ref]
                    sql += " AND client_dob_epoch >= ? AND client_dob_epoch < ?"
                    params.extend([dob_start_epoch, dob_end_epoch])
                    if client_id:
                        sql += " AND client_id = ?"
                        params.append(client_id)
                    row = conn.execute(sql, tuple(params)).fetchone()

                if not row:
                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "ok": False,
                            "error": "I have not been able to find your booking.",
                            "reason": "No booking found for the provided booking reference and date of birth.",
                        },
                    )
                    return

                appointment_payload = _appointment_row_to_payload(row)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "bookingReference": appointment_payload["bookingReference"],
                        "clientId": appointment_payload["clientId"],
                        "clientName": appointment_payload["clientName"],
                        "date": appointment_payload["bookedStartDate"],
                        "time": appointment_payload["bookedStartTime12h"],
                        "status": appointment_payload["status"],
                        "appointment": appointment_payload,
                    },
                )
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Not found"})
        except APIError as e:
            self._send_json(e.status_code, {"ok": False, "error": e.message, "details": e.details})
        except Exception as e:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": f"Internal error: {e}"},
            )

    def _handle_api_post(self, parsed) -> None:
        path = parsed.path
        body = _json_body(self)
        # Some clients (including certain action integrations) send POST inputs
        # as query params with an empty body. Merge query values as fallback.
        qs = parse_qs(parsed.query)
        qs_flat: Dict[str, Any] = {k: (v[0] if isinstance(v, list) and v else "") for k, v in qs.items()}
        if not body and qs_flat:
            body = qs_flat
        elif qs_flat:
            for k, v in qs_flat.items():
                body.setdefault(k, v)
        body = _normalize_common_fields(body)

        with _connect() as conn:
            if path == "/api/appointments":
                self._handle_create_single(conn, body)
                return
            if path == "/api/reschedule":
                self._handle_bulk_book(conn, body)
                return
            if path == "/api/cancel":
                self._handle_cancel(conn, body)
                return
            if path == "/api/cancellation":
                # Hard-delete: same as /api/delete (bookingReference + clientDob). Use for integrations that expect "cancellation" to remove the row.
                self._handle_delete(conn, body)
                return
            if path == "/api/delete":
                self._handle_delete(conn, body)
                return
            if path == "/api/delete-all":
                self._handle_delete_all(conn, body)
                return
            if path == "/api/book-new":
                self._handle_book_new(conn, body)
                return
            if path == "/api/my-appointments":
                self._handle_my_appointment(conn, body)
                return
            if path == "/api/my-appointment":
                # POST alias:
                # - lookup by bookingReference + clientDob
                # - OR reschedule when date/time fields are provided
                if (
                    body.get("startLocal")
                    or body.get("newStartLocal")
                    or body.get("dateTime")
                    or body.get("requestedSlots")
                    or (body.get("date") and body.get("time"))
                ):
                    self._handle_reschedule_existing(conn, body)
                else:
                    self._handle_my_appointment(conn, body)
                return
            if path == "/api/reschedule-existing":
                self._handle_reschedule_existing(conn, body)
                return
            if path == "/api/check-availability":
                self._handle_check_availability(conn, body)
                return

        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Not found"})

    def _handle_check_availability(self, conn: sqlite3.Connection, body: Dict[str, Any]) -> None:
        """
        POST /api/check-availability
        Body: { "slots": ["DD/MM/YYYY 9am", "DD/MM/YYYY 1pm", ...], "durationMinutes"?: number }
        Times accept 24h (10:00) or 12h (9am, 1:30pm). Response startLocal uses the same 12h style.
        Also accepts "requestedSlots" (same array) for consistency with other APIs.
        Also accepts optional single/multi fields like:
          - dateTime
          - dateTime1/dateTime2/dateTime3
          - slot1/slot2/slot3
        Missing/null/blank optional values are ignored.
        Returns 200 with per-slot { startLocal, available } and error when unavailable or unparsable.
        """
        raw = body.get("slots") or body.get("requestedSlots")
        slots: List[str] = []
        if isinstance(raw, str):
            # Allow comma-separated values in simple integrations.
            pieces = [p.strip() for p in raw.split(",")]
            slots.extend([p for p in pieces if p])
        elif isinstance(raw, list):
            for item in raw:
                s_item = str(item or "").strip()
                if s_item and s_item.lower() != "null":
                    slots.append(s_item)

        # Fallback for integration payloads that send optional single fields.
        fallback_keys = [
            "dateTime",
            "dateTime1",
            "dateTime2",
            "dateTime3",
            "slot",
            "slot1",
            "slot2",
            "slot3",
            "startLocal",
            "startLocal1",
            "startLocal2",
            "startLocal3",
        ]
        if not slots:
            for key in fallback_keys:
                v = body.get(key)
                s_val = str(v or "").strip()
                if s_val and s_val.lower() != "null":
                    slots.append(s_val)

        if not slots:
            raise APIError(
                (
                    'Provide at least one date/time via "slots"/"requestedSlots" '
                    'or one of: dateTime, dateTime1, dateTime2, dateTime3.'
                ),
                HTTPStatus.BAD_REQUEST,
            )
        duration_minutes = _coerce_int(body.get("durationMinutes", 30), 30)
        if duration_minutes <= 0 or duration_minutes > 24 * 60:
            raise APIError("Invalid durationMinutes", HTTPStatus.BAD_REQUEST)

        results: List[Dict[str, Any]] = []
        for slot_str in slots:
            s = str(slot_str).strip()
            if not s:
                results.append({"startLocal": s, "available": False, "error": "Empty slot"})
                continue
            try:
                start_dt = _parse_datetime_au(s)
                start_epoch = _dt_to_epoch_seconds(start_dt)
                end_epoch = start_epoch + int(duration_minutes * 60)
                taken = _check_slot_conflict(conn, start_epoch, end_epoch)
                if taken:
                    results.append(
                        {
                            "startLocal": _epoch_to_local_display_12h(start_epoch),
                            "available": False,
                            "error": "Not available, booked.",
                        }
                    )
                else:
                    results.append(
                        {
                            "startLocal": _epoch_to_local_display_12h(start_epoch),
                            "available": True,
                        }
                    )
            except Exception as e:
                results.append({"startLocal": s, "available": False, "error": str(e)})

        self._send_json(
            HTTPStatus.OK,
            {"ok": True, "durationMinutes": duration_minutes, "results": results},
        )

    def _handle_create_single(self, conn: sqlite3.Connection, body: Dict[str, Any]) -> None:
        _validate_required(body, ["clientId", "clientName", "clientDob", "startLocal"])
        client_id = str(body["clientId"])
        client_name = str(body["clientName"])
        client_dob_str = str(body["clientDob"])
        start_local_str = str(body["startLocal"])
        duration_minutes = _coerce_int(body.get("durationMinutes", 30), 30)

        if duration_minutes <= 0 or duration_minutes > 24 * 60:
            raise APIError("Invalid durationMinutes", HTTPStatus.BAD_REQUEST)

        # DOB is a date-only identifier.
        client_dob_dt = _parse_date_au(client_dob_str)
        start_dt = _parse_datetime_au(start_local_str)
        start_epoch = _dt_to_epoch_seconds(start_dt)
        end_epoch = start_epoch + int(duration_minutes * 60)
        if _check_slot_conflict(conn, start_epoch, end_epoch):
            raise APIError("Slot is taken sorry.", HTTPStatus.CONFLICT)

        created_epoch = int(datetime.now(tz=_TZ_SYD).timestamp())
        temp_ref = _temp_booking_reference()
        cur = conn.execute(
            """
            INSERT INTO appointments
              (booking_reference, client_id, client_name, client_dob_epoch, start_epoch, end_epoch, status, created_epoch)
            VALUES (?, ?, ?, ?, ?, ?, 'CONFIRMED', ?)
            """,
            (
                temp_ref,
                client_id,
                client_name,
                _dt_to_epoch_seconds(client_dob_dt),
                start_epoch,
                end_epoch,
                created_epoch,
            ),
        )
        row_id = cur.lastrowid
        booking_ref = _generate_short_booking_reference(conn, row_id)
        conn.execute(
            "UPDATE appointments SET booking_reference = ? WHERE id = ?",
            (booking_ref, row_id),
        )
        conn.commit()

        cur = conn.execute("SELECT * FROM appointments WHERE id = ?", (row_id,))
        row = cur.fetchone()
        payload = _appointment_row_to_payload(row)
        self._send_json(HTTPStatus.OK, {"ok": True, "appointment": payload})

    def _handle_bulk_book(self, conn: sqlite3.Connection, body: Dict[str, Any]) -> None:
        """
        Booking/reschedule API.
        Supports two body shapes:
          A) Single client + multiple slots:
             { clientId, clientName, clientDob, requestedSlots: [ 'DD/MM/YYYY HH:mm', ...], durationMinutes? }
          B) Multiple appointments:
             { appointments: [ { clientId, clientName, clientDob, startLocal, durationMinutes? }, ... ] }
        """
        results: List[Dict[str, Any]] = []

        appointments_input: Optional[List[Dict[str, Any]]] = None
        single_client_slots: Optional[List[str]] = None

        if isinstance(body.get("appointments"), list):
            appointments_input = body["appointments"]
        else:
            # Single client + slots
            single_client_slots = body.get("requestedSlots") or []

        # Default durationMinutes for slot shape A.
        default_duration = _coerce_int(body.get("durationMinutes", 30), 30)

        if appointments_input is None:
            _validate_required(body, ["clientId", "clientName", "clientDob", "requestedSlots"])
            if not isinstance(single_client_slots, list) or not single_client_slots:
                raise APIError("requestedSlots must be a non-empty array", HTTPStatus.BAD_REQUEST)
            client_id = str(body["clientId"])
            client_name = str(body["clientName"])
            client_dob = str(body["clientDob"])
            appointments_input = [
                {
                    "clientId": client_id,
                    "clientName": client_name,
                    "clientDob": client_dob,
                    "startLocal": slot,
                    "durationMinutes": default_duration,
                }
                for slot in single_client_slots
            ]

        if not isinstance(appointments_input, list) or not appointments_input:
            raise APIError("appointments must be a non-empty array", HTTPStatus.BAD_REQUEST)

        # Parse + allocate with conflict checks.
        allocated_intervals: List[Tuple[int, int]] = []
        created_refs: List[str] = []

        with conn:
            for item in appointments_input:
                try:
                    _validate_required(item, ["clientId", "clientName", "clientDob", "startLocal"])
                    client_id = str(item["clientId"])
                    client_name = str(item["clientName"])
                    client_dob_str = str(item["clientDob"])
                    start_local_str = str(item["startLocal"])
                    duration_minutes = _coerce_int(item.get("durationMinutes", default_duration), default_duration)
                    if duration_minutes <= 0 or duration_minutes > 24 * 60:
                        raise APIError("Invalid durationMinutes", HTTPStatus.BAD_REQUEST)

                    client_dob_dt = _parse_date_au(client_dob_str)
                    start_dt = _parse_datetime_au(start_local_str)
                    start_epoch = _dt_to_epoch_seconds(start_dt)
                    end_epoch = start_epoch + int(duration_minutes * 60)

                    # Check conflicts against current DB.
                    if _check_slot_conflict(conn, start_epoch, end_epoch):
                        results.append(
                            {
                                "requestedStartLocal": start_local_str,
                                "available": False,
                                "error": "Slot is taken sorry.",
                            }
                        )
                        continue

                    # Check conflicts within this same request.
                    overlaps_with_request = any(
                        (s < end_epoch and e > start_epoch) for (s, e) in allocated_intervals
                    )
                    if overlaps_with_request:
                        results.append(
                            {
                                "requestedStartLocal": start_local_str,
                                "available": False,
                                "error": "Slot is taken sorry.",
                            }
                        )
                        continue

                    temp_ref = _temp_booking_reference()
                    created_epoch = int(datetime.now(tz=_TZ_SYD).timestamp())
                    cur = conn.execute(
                        """
                        INSERT INTO appointments
                          (booking_reference, client_id, client_name, client_dob_epoch, start_epoch, end_epoch, status, created_epoch)
                        VALUES (?, ?, ?, ?, ?, ?, 'CONFIRMED', ?)
                        """,
                        (
                            temp_ref,
                            client_id,
                            client_name,
                            _dt_to_epoch_seconds(client_dob_dt),
                            start_epoch,
                            end_epoch,
                            created_epoch,
                        ),
                    )
                    row_id = cur.lastrowid
                    booking_ref = _generate_short_booking_reference(conn, row_id)
                    conn.execute(
                        "UPDATE appointments SET booking_reference = ? WHERE id = ?",
                        (booking_ref, row_id),
                    )
                    allocated_intervals.append((start_epoch, end_epoch))
                    created_refs.append(booking_ref)

                    row = conn.execute(
                        "SELECT * FROM appointments WHERE id = ?", (row_id,)
                    ).fetchone()
                    results.append(
                        {
                            "requestedStartLocal": start_local_str,
                            "available": True,
                            "appointment": _appointment_row_to_payload(row),
                        }
                    )
                except APIError as e:
                    results.append(
                        {
                            "requestedStartLocal": item.get("startLocal", ""),
                            "available": False,
                            "error": e.message,
                        }
                    )
                except sqlite3.IntegrityError:
                    results.append(
                        {
                            "requestedStartLocal": item.get("startLocal", ""),
                            "available": False,
                            "error": "Slot is taken sorry.",
                        }
                    )

        created_count = len(created_refs)
        self._send_json(
            HTTPStatus.OK,
            {
                "ok": True,
                "bookedCount": created_count,
                "results": results,
            },
        )

    def _handle_cancel(self, conn: sqlite3.Connection, body: Dict[str, Any]) -> None:
        _validate_required(body, ["bookingReference", "clientDob"])
        booking_ref = str(body["bookingReference"])
        client_dob_str = str(body["clientDob"])

        client_dob_dt = _parse_date_au(client_dob_str)
        dob_start_epoch, dob_end_epoch = _dob_day_range_epochs(client_dob_dt)

        row = conn.execute(
            """
            SELECT * FROM appointments
            WHERE booking_reference = ?
              AND client_dob_epoch >= ?
              AND client_dob_epoch < ?
              AND status = 'CONFIRMED'
            """,
            (booking_ref, dob_start_epoch, dob_end_epoch),
        ).fetchone()

        if not row:
            raise APIError("Booking not found for given booking reference and DOB", HTTPStatus.NOT_FOUND)

        conn.execute(
            "UPDATE appointments SET status = 'CANCELLED' WHERE booking_reference = ?",
            (booking_ref,),
        )
        conn.commit()
        self._send_json(HTTPStatus.OK, {"ok": True, "cancelled": {"bookingReference": booking_ref}})

    def _handle_my_appointment(self, conn: sqlite3.Connection, body: Dict[str, Any]) -> None:
        _validate_required(body, ["bookingReference", "clientDob"])
        booking_ref = str(body["bookingReference"])
        client_id = str(body.get("clientId", "") or "").strip()
        client_dob_str = str(body.get("clientDob", "") or "").strip()
        client_dob_dt = _parse_date_au(client_dob_str)
        dob_start_epoch, dob_end_epoch = _dob_day_range_epochs(client_dob_dt)

        sql = """
            SELECT * FROM appointments
            WHERE booking_reference = ?
        """
        params: List[Any] = [booking_ref]
        sql += " AND client_dob_epoch >= ? AND client_dob_epoch < ?"
        params.extend([dob_start_epoch, dob_end_epoch])
        if client_id:
            sql += " AND client_id = ?"
            params.append(client_id)
        row = conn.execute(sql, tuple(params)).fetchone()

        if not row:
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": False,
                    "error": "I have not been able to find your booking.",
                    "reason": "No booking found for the provided booking reference and date of birth.",
                },
            )
            return

        appointment_payload = _appointment_row_to_payload(row)
        self._send_json(
            HTTPStatus.OK,
            {
                "ok": True,
                "bookingReference": appointment_payload["bookingReference"],
                "clientId": appointment_payload["clientId"],
                "clientName": appointment_payload["clientName"],
                "date": appointment_payload["bookedStartDate"],
                "time": appointment_payload["bookedStartTime12h"],
                "status": appointment_payload["status"],
                "appointment": appointment_payload,
            },
        )

    def _handle_delete(self, conn: sqlite3.Connection, body: Dict[str, Any]) -> None:
        _validate_required(body, ["bookingReference", "clientDob"])
        booking_ref = str(body["bookingReference"])
        client_dob_dt = _parse_date_au(str(body["clientDob"]))
        dob_start_epoch, dob_end_epoch = _dob_day_range_epochs(client_dob_dt)

        row = conn.execute(
            """
            SELECT * FROM appointments
            WHERE booking_reference = ?
              AND client_dob_epoch >= ?
              AND client_dob_epoch < ?
            """,
            (booking_ref, dob_start_epoch, dob_end_epoch),
        ).fetchone()

        if not row:
            raise APIError("Booking not found for given booking reference and DOB", HTTPStatus.NOT_FOUND)

        conn.execute(
            """
            DELETE FROM appointments
            WHERE booking_reference = ?
              AND client_dob_epoch >= ?
              AND client_dob_epoch < ?
            """,
            (booking_ref, dob_start_epoch, dob_end_epoch),
        )
        conn.commit()
        self._send_json(HTTPStatus.OK, {"ok": True, "deleted": {"bookingReference": booking_ref}})

    def _handle_delete_all(self, conn: sqlite3.Connection, body: Dict[str, Any]) -> None:
        cur = conn.execute("SELECT COUNT(*) AS c FROM appointments")
        row = cur.fetchone()
        count_before = int(row["c"]) if row else 0
        conn.execute("DELETE FROM appointments")
        conn.commit()
        self._send_json(HTTPStatus.OK, {"ok": True, "deletedCount": count_before})

    def _handle_book_new(self, conn: sqlite3.Connection, body: Dict[str, Any]) -> None:
        """
        Books a brand-new appointment without requiring bookingReference.

        Required:
          - clientName
          - clientDob (date-only identifier, DD/MM/YYYY preferred)
          - one of:
              * startLocal / dateTime / newStartLocal   (single datetime)
              * date + time                             (separate fields)
        Optional:
          - durationMinutes (default 30)

        Behavior:
          - If slot taken -> 409 "Slot is taken sorry."
          - If free -> creates new booking and auto-generates BK###.
        """
        # Accept common key variants from integrations/gateways.
        client_id_raw = _find_in_nested(
            body, ["clientId", "clientID", "client_id", "clientid", "ClientId", "ClientID"]
        )
        client_name_raw = _find_in_nested(
            body, ["clientName", "client_name", "clientname", "ClientName"]
        )
        client_dob_raw = _find_in_nested(
            body, ["clientDob", "clientDOB", "client_dob", "clientdob", "dob", "DOB"]
        )

        debug_details = {
            "receivedKeys": _collect_top_level_keys(body),
            "expectsOneOf": {
                "clientId": ["clientId", "clientID", "client_id", "clientid", "ClientId", "ClientID"],
                "clientName": ["clientName", "client_name", "clientname", "ClientName"],
                "clientDob": ["clientDob", "clientDOB", "client_dob", "clientdob", "dob", "DOB"],
                "start": ["startLocal", "dateTime", "newStartLocal", "date+time"],
            },
        }
        if client_name_raw in (None, ""):
            raise APIError("Missing required field: clientName", HTTPStatus.BAD_REQUEST, debug_details)
        if client_dob_raw in (None, ""):
            raise APIError("Missing required field: clientDob", HTTPStatus.BAD_REQUEST, debug_details)

        # clientId is optional; auto-generate when missing/blank.
        client_id = str(client_id_raw).strip() if client_id_raw not in (None, "") else _generate_client_id(conn)
        client_name = str(client_name_raw).strip()
        client_dob_str = str(client_dob_raw).strip()

        start_local = _find_in_nested(body, ["startLocal", "dateTime", "newStartLocal"])
        date_part = _find_in_nested(body, ["date", "newDate"])
        time_part = _find_in_nested(body, ["time", "newTime"])
        if not start_local and date_part and time_part:
            start_local = f"{str(date_part).strip()} {str(time_part).strip()}"
        if not start_local:
            raise APIError(
                "Provide one of startLocal/dateTime/newStartLocal or date + time",
                HTTPStatus.BAD_REQUEST,
                debug_details,
            )

        duration_minutes = _coerce_int(body.get("durationMinutes", 30), 30)
        if duration_minutes <= 0 or duration_minutes > 24 * 60:
            raise APIError("Invalid durationMinutes", HTTPStatus.BAD_REQUEST)

        client_dob_dt = _parse_date_au(client_dob_str)
        start_dt = _parse_datetime_au(str(start_local))
        start_epoch = _dt_to_epoch_seconds(start_dt)
        end_epoch = start_epoch + int(duration_minutes * 60)

        if _check_slot_conflict(conn, start_epoch, end_epoch):
            raise APIError("Slot is taken sorry.", HTTPStatus.CONFLICT)

        created_epoch = int(datetime.now(tz=_TZ_SYD).timestamp())
        temp_ref = _temp_booking_reference()
        cur = conn.execute(
            """
            INSERT INTO appointments
              (booking_reference, client_id, client_name, client_dob_epoch, start_epoch, end_epoch, status, created_epoch)
            VALUES (?, ?, ?, ?, ?, ?, 'CONFIRMED', ?)
            """,
            (
                temp_ref,
                client_id,
                client_name,
                _dt_to_epoch_seconds(client_dob_dt),
                start_epoch,
                end_epoch,
                created_epoch,
            ),
        )
        row_id = cur.lastrowid
        booking_ref = _generate_short_booking_reference(conn, row_id)
        conn.execute(
            "UPDATE appointments SET booking_reference = ? WHERE id = ?",
            (booking_ref, row_id),
        )
        conn.commit()

        row = conn.execute("SELECT * FROM appointments WHERE id = ?", (row_id,)).fetchone()
        appointment_payload = _appointment_row_to_payload(row)
        self._send_json(
            HTTPStatus.OK,
            {
                "ok": True,
                # Top-level fields for easy mapping in integration platforms.
                "bookingReference": appointment_payload["bookingReference"],
                "clientId": appointment_payload["clientId"],
                "clientName": appointment_payload["clientName"],
                "clientDob": appointment_payload["clientDob"],
                "date": appointment_payload["bookedStartDate"],
                "time": appointment_payload["bookedStartTime12h"],
                "durationMinutes": appointment_payload["durationMinutes"],
                "appointment": appointment_payload,
            },
        )

    def _handle_reschedule_existing(self, conn: sqlite3.Connection, body: Dict[str, Any]) -> None:
        """
        Re-schedules an existing booking.

        Supported body shapes:
          1) Single date/time strict check-and-book (preferred):
             { bookingReference, clientDob, startLocal|newStartLocal|dateTime, durationMinutes? }
             - If slot is taken -> "Slot is taken sorry."
             - If available -> update booking.

          2) Multiple slots fallback:
             { bookingReference, clientDob, requestedSlots:[...], durationMinutes? }
             - Books the first available requested slot.
        """
        _validate_required(body, ["bookingReference", "clientDob"])
        booking_ref = str(body["bookingReference"])
        client_dob_str = str(body["clientDob"])
        client_id = str(body.get("clientId", "") or "").strip()

        requested_slots = body.get("requestedSlots")
        single_slot = (
            body.get("startLocal")
            or body.get("newStartLocal")
            or body.get("dateTime")
        )

        # Support separate date/time payload fields for simple clients.
        date_part = body.get("date") or body.get("newDate")
        time_part = body.get("time") or body.get("newTime")
        if not single_slot and date_part and time_part:
            single_slot = f"{str(date_part).strip()} {str(time_part).strip()}"

        if single_slot:
            requested_slots = [single_slot]
            strict_single = True
        else:
            strict_single = False
            if not isinstance(requested_slots, list) or not requested_slots:
                raise APIError(
                    "Provide either startLocal/newStartLocal/dateTime or requestedSlots[]",
                    HTTPStatus.BAD_REQUEST,
                )

        duration_minutes = _coerce_int(body.get("durationMinutes", 30), 30)

        if duration_minutes <= 0:
            raise APIError("Invalid durationMinutes", HTTPStatus.BAD_REQUEST)

        client_dob_dt = _parse_date_au(client_dob_str)
        dob_start_epoch, dob_end_epoch = _dob_day_range_epochs(client_dob_dt)

        sql = """
            SELECT * FROM appointments
            WHERE booking_reference = ?
              AND client_dob_epoch >= ?
              AND client_dob_epoch < ?
              AND status = 'CONFIRMED'
        """
        params: List[Any] = [booking_ref, dob_start_epoch, dob_end_epoch]
        if client_id:
            sql += " AND client_id = ?"
            params.append(client_id)
        existing = conn.execute(sql, tuple(params)).fetchone()

        if not existing:
            return self._send_json(
                HTTPStatus.OK,
                {
                    "found": False,
                    "status": "NOT_FOUND",
                    "userMessage": "No booking found for the provided reference and date of birth",
                    "suggestedAction": "retry_then_new_booking",
                    "retryConfig": {
                        "maxAttempts": 2,
                        "retryPrompt": "Let me try that again - could you double-check your booking reference and date of birth?",
                        "fieldsToReconfirm": ["bookingReference", "clientDob"],
                    },
                    "onMaxRetriesExceeded": {
                        "action": "offer_new_booking",
                        "prompt": "I still can't find an existing booking with those details. Would you like me to create a new booking for you instead?",
                    },
                },
            )

        # Try slots in order; for single-slot strict mode, fail immediately if taken.
        for slot in requested_slots:
            slot_str = str(slot)
            slot_dt = _parse_datetime_au(slot_str)
            start_epoch = _dt_to_epoch_seconds(slot_dt)
            end_epoch = start_epoch + int(duration_minutes * 60)

            # Prevent false-positive "success" when requested slot equals current slot.
            if (
                int(existing["start_epoch"]) == int(start_epoch)
                and int(existing["end_epoch"]) == int(end_epoch)
            ):
                if strict_single:
                    existing_payload = _appointment_row_to_payload(existing)
                    raise APIError(
                        "This booking is already at that date/time. Please choose a different slot.",
                        HTTPStatus.BAD_REQUEST,
                        {
                            "bookingReference": booking_ref,
                            "currentStart": existing_payload.get("startLocal"),
                            "requestedStart": _epoch_to_local_display(_dt_to_epoch_seconds(slot_dt)),
                        },
                    )
                continue

            if _check_slot_conflict(conn, start_epoch, end_epoch, exclude_booking_ref=booking_ref):
                if strict_single:
                    raise APIError("Slot is taken sorry.", HTTPStatus.CONFLICT)
                continue

            # Update in one shot. Keep booking_reference constant.
            conn.execute(
                """
                UPDATE appointments
                SET start_epoch = ?, end_epoch = ?, created_epoch = ?
                WHERE booking_reference = ?
                """,
                (start_epoch, end_epoch, int(datetime.now(tz=_TZ_SYD).timestamp()), booking_ref),
            )
            conn.commit()

            updated = conn.execute(
                "SELECT * FROM appointments WHERE booking_reference = ?", (booking_ref,)
            ).fetchone()
            updated_payload = _appointment_row_to_payload(updated)
            existing_payload = _appointment_row_to_payload(existing)
            return self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "bookingReference": updated_payload["bookingReference"],
                    "clientId": updated_payload["clientId"],
                    "clientName": updated_payload["clientName"],
                    "status": updated_payload["status"],
                    "fromDate": existing_payload["bookedStartDate"],
                    "fromTime": existing_payload["bookedStartTime12h"],
                    "toDate": updated_payload["bookedStartDate"],
                    "toTime": updated_payload["bookedStartTime12h"],
                    "appointment": updated_payload,
                    "rescheduledFrom": existing_payload,
                },
            )

        if strict_single:
            raise APIError("Slot is taken sorry.", HTTPStatus.CONFLICT)
        raise APIError("All requested slots are taken sorry.", HTTPStatus.CONFLICT)


class ThreadedHTTPServer(ThreadingHTTPServer):
    daemon_threads = True


def main() -> None:
    _init_db()
    # Default to 3000 for ngrok/local API sharing workflows.
    port = int(os.environ.get("PORT", "3000"))
    host = os.environ.get("HOST", "0.0.0.0")
    httpd = ThreadedHTTPServer((host, port), Handler)
    print(f"Serving backend + frontend at http://{host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()

