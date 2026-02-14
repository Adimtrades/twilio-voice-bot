// server.js
// Sell-ready: Multi-tenant, intent detection, customer SMS, dupe detection, history,
// human fallback, calendar backup, inbound SMS Y/N (DB-backed via Supabase optional),
// missed revenue alerts, customer memory notes, revenue analytics + admin endpoint

try { require("dotenv").config(); } catch (e) {}

const express = require("express");
const twilio = require("twilio");
const { google } = require("googleapis");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");

const app = express();
app.set("trust proxy", true);
app.use(express.urlencoded({ extended: false }));

const VoiceResponse = twilio.twiml.VoiceResponse;
const MessagingResponse = twilio.twiml.MessagingResponse;

/* ============================================================================
MULTI-TRADIE CONFIG
============================================================================ */
function parseTradiesJson() {
const raw = process.env.TRADIES_JSON;
if (!raw) return {};
try { return JSON.parse(raw); }
catch {
try { return JSON.parse(raw.replace(/\r?\n/g, "")); }
catch {
console.warn("TRADIES_JSON parse failed. Check JSON formatting.");
return {};
}
}
}

const TRADIES = parseTradiesJson();

function getTradieKey(req) {
return (req.body.To || req.query.tid || "default").trim();
}

function getTradieConfig(req) {
const key = getTradieKey(req);
const t = TRADIES[key] || TRADIES.default || {};

const businessDays =
Array.isArray(t.businessDays) && t.businessDays.length
? t.businessDays
: (process.env.BUSINESS_DAYS || "1,2,3,4,5")
.split(",").map((x) => Number(x.trim())).filter(Boolean);

// optional pricing model per tradie for analytics
const avgJobValue = Number(t.avgJobValue ?? process.env.AVG_JOB_VALUE ?? 250);
const closeRate = Number(t.closeRate ?? process.env.CLOSE_RATE ?? 0.6); // 60% default

return {
key,
ownerSmsTo: t.ownerSmsTo || process.env.OWNER_SMS_TO || "",
smsFrom: t.smsFrom || process.env.TWILIO_SMS_FROM || "",
timezone: t.timezone || process.env.TIMEZONE || "Australia/Sydney",
businessStartHour: Number(t.businessStartHour ?? process.env.BUSINESS_START_HOUR ?? 7),
businessEndHour: Number(t.businessEndHour ?? process.env.BUSINESS_END_HOUR ?? 17),
businessDays,
calendarId: t.calendarId || process.env.GOOGLE_CALENDAR_ID || "",
googleServiceJson: t.googleServiceJson || process.env.GOOGLE_SERVICE_JSON || "",
avgJobValue,
closeRate
};
}

// Global controls
const MISSED_CALL_ALERT_TRIES = Number(process.env.MISSED_CALL_ALERT_TRIES || 2);
const MAX_SILENCE_TRIES = Number(process.env.MAX_SILENCE_TRIES || 10);
const CAL_RETRY_ATTEMPTS = Number(process.env.CAL_RETRY_ATTEMPTS || 3);
const GATHER_START_TIMEOUT = Number(process.env.GATHER_START_TIMEOUT || 25);
const GATHER_SPEECH_TIMEOUT = process.env.GATHER_SPEECH_TIMEOUT || "auto";
const MAX_REJECTS_ADDRESS = Number(process.env.MAX_REJECTS_ADDRESS || 2);
const MAX_REJECTS_TIME = Number(process.env.MAX_REJECTS_TIME || 2);
const DUP_WINDOW_DAYS = Number(process.env.DUP_WINDOW_DAYS || 14);

// Optional: Twilio signature validation
const REQUIRE_TWILIO_SIG = String(process.env.TWILIO_WEBHOOK_AUTH || "false").toLowerCase() === "true";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

// Admin endpoint
const ADMIN_DASH_PASSWORD = process.env.ADMIN_DASH_PASSWORD || ""; // set strong value

/* ============================================================================
Twilio helpers
============================================================================ */
function getTwilioClient() {
const sid = process.env.TWILIO_ACCOUNT_SID;
const token = process.env.TWILIO_AUTH_TOKEN;
if (!sid || !token) return null;
return twilio(sid, token);
}

async function sendSms({ from, to, body }) {
const client = getTwilioClient();
if (!client) return console.warn("SMS skipped: Missing TWILIO creds");
if (!from) return console.warn("SMS skipped: Missing smsFrom/TWILIO_SMS_FROM");
if (!to) return console.warn("SMS skipped: missing 'to'");
await client.messages.create({ from, to, body });
}

async function sendOwnerSms(tradie, body) {
if (!tradie.ownerSmsTo) return;
return sendSms({ from: tradie.smsFrom, to: tradie.ownerSmsTo, body });
}
async function sendCustomerSms(tradie, toCustomer, body) {
return sendSms({ from: tradie.smsFrom, to: toCustomer, body });
}

/* ============================================================================
Webhook signature check
============================================================================ */
function validateTwilioSignature(req) {
if (!REQUIRE_TWILIO_SIG) return true;
if (!TWILIO_AUTH_TOKEN) return false;

const signature = req.headers["x-twilio-signature"];
if (!signature) return false;

const proto = (req.headers["x-forwarded-proto"] || "https").split(",")[0].trim();
const host = (req.headers["x-forwarded-host"] || req.headers["host"] || "").split(",")[0].trim();
const url = `${proto}://${host}${req.originalUrl}`;

try {
return twilio.validateRequest(TWILIO_AUTH_TOKEN, signature, url, req.body);
} catch {
return false;
}
}

/* ============================================================================
Supabase (Adimtrades’s Project)
Tables used:
- customer_prefs (notes per customer per tradie)
- pending_confirmations (Y/N)
- metrics_daily (analytics counters)
============================================================================ */
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || "";
const SUPABASE_PREFS_TABLE = process.env.SUPABASE_TABLE || "customer_prefs";
const SUPABASE_PENDING_TABLE = process.env.SUPABASE_PENDING_TABLE || "pending_confirmations";
const SUPABASE_METRICS_TABLE = process.env.SUPABASE_METRICS_TABLE || "metrics_daily";

function supaHeaders() {
return {
apikey: SUPABASE_SERVICE_KEY,
Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
"Content-Type": "application/json"
};
}

function supaReady() {
return !!(SUPABASE_URL && SUPABASE_SERVICE_KEY);
}

async function upsertRow(table, row, keyColumn = "key") {
if (!supaReady()) return false;
const url = `${SUPABASE_URL}/rest/v1/${table}`;
try {
await fetch(url, {
method: "POST",
headers: { ...supaHeaders(), Prefer: "resolution=merge-duplicates" },
body: JSON.stringify([row])
});
return true;
} catch {
return false;
}
}

async function getOne(table, query) {
if (!supaReady()) return null;
const url = `${SUPABASE_URL}/rest/v1/${table}?${query}&limit=1`;
try {
const r = await fetch(url, { headers: supaHeaders() });
const data = await r.json();
return Array.isArray(data) && data[0] ? data[0] : null;
} catch {
return null;
}
}

async function delWhere(table, query) {
if (!supaReady()) return false;
const url = `${SUPABASE_URL}/rest/v1/${table}?${query}`;
try {
await fetch(url, { method: "DELETE", headers: supaHeaders() });
return true;
} catch {
return false;
}
}

/* ============================================================================
Customer memory notes (per tradie + phone)
============================================================================ */
function makeCustomerKey(tradieKey, phone) {
return `${tradieKey}::${phone || ""}`;
}

async function getCustomerNote(tradieKey, phone) {
if (!phone) return null;

// key column in customer_prefs should be "key"
const key = makeCustomerKey(tradieKey, phone);
const row = await getOne(SUPABASE_PREFS_TABLE, `key=eq.${encodeURIComponent(key)}&select=preferred_note`);
return row?.preferred_note ? String(row.preferred_note) : null;
}

async function setCustomerNote(tradieKey, phone, note) {
if (!phone) return false;
const key = makeCustomerKey(tradieKey, phone);
return upsertRow(SUPABASE_PREFS_TABLE, {
key,
tradie_key: tradieKey,
phone,
preferred_note: note,
updated_at: new Date().toISOString()
});
}

/* ============================================================================
Pending confirmations (DB preferred)
============================================================================ */
function makePendingKey(tradieKey, customerPhone) {
return `${tradieKey}::${customerPhone || ""}`;
}

async function upsertPendingConfirmationDb(key, payload) {
return upsertRow(SUPABASE_PENDING_TABLE, { key, ...payload, updated_at: new Date().toISOString() });
}
async function getPendingConfirmationDb(key) {
return getOne(SUPABASE_PENDING_TABLE, `key=eq.${encodeURIComponent(key)}&select=*`);
}
async function deletePendingConfirmationDb(key) {
return delWhere(SUPABASE_PENDING_TABLE, `key=eq.${encodeURIComponent(key)}`);
}

// fallback in-memory
const pendingConfirmations = new Map();
function setPendingConfirmationMemory(key, payload) { pendingConfirmations.set(key, { ...payload, createdAt: Date.now() }); }
function getPendingConfirmationMemory(key) { return pendingConfirmations.get(key) || null; }
function clearPendingConfirmationMemory(key) { pendingConfirmations.delete(key); }

/* ============================================================================
Analytics (metrics_daily)
We store per tradie per date:
- calls_total
- missed_calls_saved (handoff/missed leads)
- bookings_created
- est_revenue
============================================================================ */
function todayKey(tradieKey, tz) {
const d = DateTime.now().setZone(tz).toFormat("yyyy-LL-dd");
return `${tradieKey}::${d}`;
}

async function incMetric(tradie, fields) {
// MVP: do a read then upsert (OK for low traffic)
if (!supaReady()) return false;

const key = todayKey(tradie.key, tradie.timezone);
const existing = await getOne(SUPABASE_METRICS_TABLE, `key=eq.${encodeURIComponent(key)}&select=*`);

const base = existing || {
key,
tradie_key: tradie.key,
date: DateTime.now().setZone(tradie.timezone).toFormat("yyyy-LL-dd"),
calls_total: 0,
missed_calls_saved: 0,
bookings_created: 0,
est_revenue: 0,
updated_at: new Date().toISOString()
};

const next = {
...base,
calls_total: base.calls_total + (fields.calls_total || 0),
missed_calls_saved: base.missed_calls_saved + (fields.missed_calls_saved || 0),
bookings_created: base.bookings_created + (fields.bookings_created || 0),
est_revenue: Number(base.est_revenue) + Number(fields.est_revenue || 0),
updated_at: new Date().toISOString()
};

return upsertRow(SUPABASE_METRICS_TABLE, next);
}

/* ============================================================================
Intent detection
============================================================================ */
function detectIntent(text) {
const t = (text || "").toLowerCase();

const emergency = [
"burst", "flood", "leak", "gas", "sparking", "no power", "smoke", "fire",
"blocked", "sewage", "overflow", "urgent", "emergency", "asap", "now"
];
const quote = ["quote", "pricing", "how much", "estimate", "cost", "rate"];
const existing = ["i booked", "already booked", "existing", "last time", "repeat", "returning"];
const cancel = ["cancel", "reschedule", "change", "move", "postpone"];

const has = (arr) => arr.some((w) => t.includes(w));

if (has(cancel)) return "CANCEL_RESCHEDULE";
if (has(emergency)) return "EMERGENCY";
if (has(quote)) return "QUOTE";
if (has(existing)) return "EXISTING_CUSTOMER";
return "NEW_BOOKING";
}

function intentLabel(intent) {
switch (intent) {
case "EMERGENCY": return "Emergency";
case "QUOTE": return "Quote request";
case "EXISTING_CUSTOMER": return "Existing customer";
case "CANCEL_RESCHEDULE": return "Cancel / reschedule";
default: return "New booking";
}
}

/* ============================================================================
Session store (calls)
============================================================================ */
const sessions = new Map();

function getSession(callSid, fromNumber = "") {
if (!sessions.has(callSid)) {
sessions.set(callSid, {
step: "intent", // <-- NEW: start with intent
intent: "NEW_BOOKING",
job: "",
address: "",
name: "",
time: "",
bookedStartMs: null,
lastPrompt: "",
tries: 0,
from: fromNumber || "",
rejects: { address: 0, time: 0 },
lastAtAddress: null,
duplicateEvent: null,
confirmMode: "new",
customerNote: null,
startedAt: Date.now()
});
} else {
const s = sessions.get(callSid);
if (fromNumber && !s.from) s.from = fromNumber;
}
return sessions.get(callSid);
}
function resetSession(callSid) { sessions.delete(callSid); }

/* ============================================================================
General helpers
============================================================================ */
function cleanSpeech(text) {
if (!text) return "";
return String(text).trim().replace(/\s+/g, " ");
}

function ask(twiml, prompt, actionUrl) {
const gather = twiml.gather({
input: "speech",
action: actionUrl,
method: "POST",
timeout: GATHER_START_TIMEOUT,
speechTimeout: GATHER_SPEECH_TIMEOUT,
actionOnEmptyResult: true,
language: "en-AU",
profanityFilter: false
});
gather.say(prompt || "Sorry, can you repeat that?", { voice: "Polly.Amy", language: "en-AU" });
twiml.pause({ length: 1 });
}

function shouldReject(step, speech, confidence) {
const s = (speech || "").toLowerCase();
if (!speech || speech.length < 2) return true;
const junk = new Set(["hello", "hi", "yeah", "yep", "okay", "ok"]);
if (junk.has(s)) return step !== "job" && step !== "intent";
const minConf = step === "job" ? 0.30 : 0.10;
if (typeof confidence === "number" && confidence > 0 && confidence < minConf) {
if (speech.split(" ").length <= 2) return true;
}
return false;
}

function normStr(s) {
return String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

/* ============================================================================
Time parsing (timezone-safe)
============================================================================ */
function normalizeTimeText(text, tz) {
if (!text) return "";
let t = String(text).toLowerCase().trim();

t = t.replace(/\b(\d{1,2})\s*:\s*(\d{2})\s*p\.?\s*m\.?\b/g, "$1:$2pm");
t = t.replace(/\b(\d{1,2})\s*:\s*(\d{2})\s*a\.?\s*m\.?\b/g, "$1:$2am");
t = t.replace(/\b(\d{1,2})\s*p\.?\s*m\.?\b/g, "$1pm");
t = t.replace(/\b(\d{1,2})\s*a\.?\s*m\.?\b/g, "$1am");
t = t.replace(/\s+/g, " ");

const now = DateTime.now().setZone(tz);
if (t.includes("tomorrow")) {
const d = now.plus({ days: 1 }).toFormat("cccc d LLL yyyy");
t = t.replace(/\btomorrow\b/g, d);
}
if (t.includes("tonight") || t.includes("today")) {
const d = now.toFormat("cccc d LLL yyyy");
t = t.replace(/\btonight\b/g, d);
t = t.replace(/\btoday\b/g, d);
}
return t;
}

function parseRequestedDateTime(naturalText, tz) {
const ref = DateTime.now().setZone(tz).toJSDate();
const norm = normalizeTimeText(naturalText, tz);

const results = chrono.parse(norm, ref, { forwardDate: true });
if (!results || results.length === 0) return null;

const s = results[0].start;
if (!s) return null;

const year = s.get("year");
const month = s.get("month");
const day = s.get("day");
const hour = s.get("hour");
const minute = s.get("minute") ?? 0;

if (!year || !month || !day || hour == null) return null;

const dt = DateTime.fromObject({ year, month, day, hour, minute, second: 0, millisecond: 0 }, { zone: tz });
return dt.isValid ? dt : null;
}

function extractTimeIfPresent(text, tz) { return parseRequestedDateTime(text, tz); }
function toGoogleDateTime(dt) { return dt.toISO({ includeOffset: true, suppressMilliseconds: true }); }

function looksLikeAsap(text) {
const t = (text || "").toLowerCase();
return (
t.includes("asap") ||
t.includes("anytime") ||
t.includes("whenever") ||
t.includes("dont care") ||
t.includes("don’t care") ||
t.includes("no preference") ||
t.includes("soon as possible") ||
t === "soon"
);
}

function formatForVoice(dt) { return dt.toFormat("ccc d LLL, h:mm a"); }

function isAfterHoursNow(tradie) {
const now = DateTime.now().setZone(tradie.timezone);
const isBizDay = tradie.businessDays.includes(now.weekday);
const isBizHours = now.hour >= tradie.businessStartHour && now.hour < tradie.businessEndHour;
return !(isBizDay && isBizHours);
}
function nextBusinessOpenSlot(tradie) {
const tz = tradie.timezone;
let dt = DateTime.now().setZone(tz);

const isBizDay = tradie.businessDays.includes(dt.weekday);
const isBizHours = dt.hour >= tradie.businessStartHour && dt.hour < tradie.businessEndHour;
if (isBizDay && isBizHours) return dt.plus({ minutes: 10 }).startOf("minute");

dt = dt.plus({ days: 1 }).startOf("day");
while (!tradie.businessDays.includes(dt.weekday)) dt = dt.plus({ days: 1 }).startOf("day");

return dt.set({ hour: tradie.businessStartHour, minute: 0, second: 0, millisecond: 0 });
}

/* ============================================================================
Google Calendar
============================================================================ */
function parseGoogleServiceJson(raw) {
if (!raw) throw new Error("Missing GOOGLE_SERVICE_JSON env/config");
let parsed;
try { parsed = JSON.parse(raw); }
catch { parsed = JSON.parse(raw.replace(/\r?\n/g, "\\n")); }
if (typeof parsed === "string") parsed = JSON.parse(parsed);
return parsed;
}

function getCalendarClient(tradie) {
const credentials = parseGoogleServiceJson(tradie.googleServiceJson);
const auth = new google.auth.GoogleAuth({
credentials,
scopes: ["https://www.googleapis.com/auth/calendar"]
});
return google.calendar({ version: "v3", auth });
}

function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

async function insertCalendarEventWithRetry(calendar, calendarId, requestBody) {
let lastErr = null;
for (let attempt = 1; attempt <= CAL_RETRY_ATTEMPTS; attempt++) {
try {
return await calendar.events.insert({ calendarId, requestBody });
} catch (err) {
lastErr = err;
console.error(`Calendar insert failed (attempt ${attempt}/${CAL_RETRY_ATTEMPTS})`, err?.message || err);
if (attempt < CAL_RETRY_ATTEMPTS) await sleep(attempt === 1 ? 200 : 800);
}
}
throw lastErr;
}

async function getLastBookingAtAddress(calendar, calendarId, tz, address) {
const timeMin = DateTime.now().setZone(tz).minus({ days: 365 }).toISO();
const timeMax = DateTime.now().setZone(tz).toISO();

const resp = await calendar.events.list({
calendarId,
q: address,
timeMin,
timeMax,
singleEvents: true,
orderBy: "startTime",
maxResults: 30
});

const items = resp?.data?.items || [];
if (!items.length) return null;

const addrN = normStr(address);
const candidates = items.filter((ev) => {
const locN = normStr(ev.location);
const sumN = normStr(ev.summary);
const descN = normStr(ev.description);
return locN.includes(addrN) || sumN.includes(addrN) || descN.includes(addrN);
});

const last = (candidates.length ? candidates : items)
.sort((a, b) => {
const aISO = a.start?.dateTime || a.start?.date || "";
const bISO = b.start?.dateTime || b.start?.date || "";
return String(aISO).localeCompare(String(bISO));
})
.pop();

if (!last) return null;

const whenISO = last.start?.dateTime || last.start?.date;
const when = whenISO ? DateTime.fromISO(whenISO, { setZone: true }).setZone(tz) : null;

return {
summary: last.summary || "Previous booking",
whenText: when ? when.toFormat("ccc d LLL yyyy") : "Unknown date"
};
}

async function findDuplicate(calendar, calendarId, tz, name, address, startDt) {
const t0 = startDt.minus({ days: DUP_WINDOW_DAYS });
const t1 = startDt.plus({ days: DUP_WINDOW_DAYS });

const resp = await calendar.events.list({
calendarId,
q: address,
timeMin: t0.toISO(),
timeMax: t1.toISO(),
singleEvents: true,
orderBy: "startTime",
maxResults: 50
});

const items = resp?.data?.items || [];
if (!items.length) return null;

const nameN = normStr(name);
const addrN = normStr(address);

for (const ev of items) {
const locN = normStr(ev.location);
const sumN = normStr(ev.summary);
const descN = normStr(ev.description);

const addrMatch = locN.includes(addrN) || descN.includes(addrN) || sumN.includes(addrN);
const nameMatch = sumN.includes(nameN) || descN.includes(nameN);

if (addrMatch && nameMatch) {
const whenISO = ev.start?.dateTime || ev.start?.date;
const when = whenISO ? DateTime.fromISO(whenISO, { setZone: true }).setZone(tz) : null;
return {
id: ev.id,
summary: ev.summary || "Existing booking",
whenText: when ? when.toFormat("ccc d LLL, h:mm a") : "Unknown time"
};
}
}
return null;
}

async function deleteEventSafe(calendar, calendarId, eventId) {
if (!eventId) return;
try { await calendar.events.delete({ calendarId, eventId }); }
catch (e) { console.warn("Delete failed:", e?.message || e); }
}

/* ============================================================================
Human handoff + missed revenue alerts
============================================================================ */
function flowProgress(session) {
const parts = [];
if (session.intent) parts.push(`Intent=${intentLabel(session.intent)}`);
if (session.name) parts.push(`Name=${session.name}`);
if (session.job) parts.push(`Job=${session.job}`);
if (session.address) parts.push(`Address=${session.address}`);
if (session.time) parts.push(`Time=${session.time}`);
return parts.join(" | ") || "No details captured";
}

async function missedRevenueAlert(tradie, session, reason) {
// Analytics: missed calls saved (lead salvage)
await incMetric(tradie, {
missed_calls_saved: 1,
// Estimated revenue from a saved lead:
est_revenue: tradie.avgJobValue * tradie.closeRate
});

const body =
`MISSED LEAD ALERT 💸\n` +
`Reason: ${reason}\n` +
`TradieKey: ${tradie.key}\n` +
`Caller: ${session.from || "Unknown"}\n` +
`${flowProgress(session)}\n` +
`Action: Call/text back ASAP.`;

await sendOwnerSms(tradie, body).catch(() => {});
}

async function humanHandoff(tradie, session, reason) {
const body =
`HUMAN HANDOFF NEEDED\n` +
`Reason: ${reason}\n` +
`TradieKey: ${tradie.key}\n` +
`Caller: ${session.from || "Unknown"}\n` +
`${flowProgress(session)}`;

await sendOwnerSms(tradie, body).catch(() => {});
}

/* ============================================================================
ADMIN DASH (simple JSON) - password protected
GET /admin/metrics?tid=+614...&pw=...
============================================================================ */
app.get("/admin/metrics", async (req, res) => {
const pw = String(req.query.pw || "");
if (!ADMIN_DASH_PASSWORD || pw !== ADMIN_DASH_PASSWORD) return res.status(403).json({ error: "Forbidden" });

const tid = String(req.query.tid || "default");
const tradie = getTradieConfig({ body: { To: tid }, query: {} });

if (!supaReady()) return res.json({ ok: false, error: "Supabase not configured" });

// last 30 days metrics for that tradie
const end = DateTime.now().setZone(tradie.timezone);
const start = end.minus({ days: 30 }).toFormat("yyyy-LL-dd");
const endStr = end.toFormat("yyyy-LL-dd");

const url = `${SUPABASE_URL}/rest/v1/${SUPABASE_METRICS_TABLE}?tradie_key=eq.${encodeURIComponent(tradie.key)}&date=gte.${start}&date=lte.${endStr}&select=*`;
try {
const r = await fetch(url, { headers: supaHeaders() });
const rows = await r.json();

const totals = (Array.isArray(rows) ? rows : []).reduce(
(acc, x) => {
acc.calls_total += Number(x.calls_total || 0);
acc.missed_calls_saved += Number(x.missed_calls_saved || 0);
acc.bookings_created += Number(x.bookings_created || 0);
acc.est_revenue += Number(x.est_revenue || 0);
return acc;
},
{ calls_total: 0, missed_calls_saved: 0, bookings_created: 0, est_revenue: 0 }
);

return res.json({
ok: true,
tradie_key: tradie.key,
window: { start, end: endStr },
totals,
rows
});
} catch (e) {
return res.json({ ok: false, error: String(e?.message || e) });
}
});

/* ============================================================================
VOICE ROUTE
============================================================================ */
app.post("/voice", async (req, res) => {
if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

const tradie = getTradieConfig(req);
const twiml = new VoiceResponse();

const callSid = req.body.CallSid || req.body.CallSID || "unknown";
const fromNumber = req.body.From || "";
const confidence = Number(req.body.Confidence || 0);
const speechRaw = req.body.SpeechResult || "";
const speech = cleanSpeech(speechRaw);

const session = getSession(callSid, fromNumber);

// Analytics: count calls once (first webhook hit)
if (!session._countedCall) {
session._countedCall = true;
await incMetric(tradie, { calls_total: 1 });
}

console.log(`TID=${tradie.key} CALLSID=${callSid} FROM=${fromNumber} STEP=${session.step} Speech="${speech}" Confidence=${confidence}`);

// No speech
if (!speech) {
session.tries += 1;

if (session.tries === MISSED_CALL_ALERT_TRIES) {
const txt =
`MISSED/QUIET CALL ALERT\n` +
`TradieKey: ${tradie.key}\n` +
`From: ${session.from || "Unknown"}\n` +
`Progress: step=${session.step}\n` +
`Time: ${DateTime.now().setZone(tradie.timezone).toFormat("ccc d LLL yyyy, h:mm a")}`;
sendOwnerSms(tradie, txt).catch(() => {});
}

if (session.tries >= MAX_SILENCE_TRIES) {
// missed revenue alert (hang/silent)
await missedRevenueAlert(tradie, session, "Silent timeout");
twiml.say("No worries. We'll call you back shortly.", { voice: "Polly.Amy", language: "en-AU" });
twiml.hangup();
resetSession(callSid);
return res.type("text/xml").send(twiml.toString());
}

const promptMap = {
intent: "How can we help today? You can say emergency, quote, reschedule, or new booking.",
job: "What job do you need help with?",
address: "What is the address?",
name: "What is your name?",
time: "What time would you like?",
confirm: "Just say yes to confirm, or no to change the time."
};

const prompt = session.lastPrompt || promptMap[session.step] || "Can you repeat that?";
session.lastPrompt = prompt;
ask(twiml, prompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

session.tries = 0;

if (session.step !== "confirm" && shouldReject(session.step, speech, confidence)) {
if (session.step === "address") session.rejects.address += 1;
if (session.step === "time") session.rejects.time += 1;

if (session.rejects.address >= MAX_REJECTS_ADDRESS) {
await missedRevenueAlert(tradie, session, "Address capture failed (auto-callback)");
twiml.say("No worries. We'll call you back shortly to confirm the address.", { voice: "Polly.Amy", language: "en-AU" });
twiml.hangup();
resetSession(callSid);
return res.type("text/xml").send(twiml.toString());
}

if (session.rejects.time >= MAX_REJECTS_TIME) {
await missedRevenueAlert(tradie, session, "Time capture failed (auto-callback)");
twiml.say("No worries. We'll call you back shortly to confirm the time.", { voice: "Polly.Amy", language: "en-AU" });
twiml.hangup();
resetSession(callSid);
return res.type("text/xml").send(twiml.toString());
}

session.lastPrompt = "Sorry, can you repeat that?";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

try {
const tz = tradie.timezone;

// STEP: intent (new)
if (session.step === "intent") {
session.intent = detectIntent(speech);

// Pull customer note early (memory)
if (session.from) {
session.customerNote = await getCustomerNote(tradie.key, session.from);
}

// Route by intent
if (session.intent === "CANCEL_RESCHEDULE") {
// Quick capture minimal info then handoff
session.step = "name";
session.lastPrompt = "No worries. What is your name so we can reschedule you?";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

if (session.intent === "QUOTE") {
session.step = "job";
session.lastPrompt = "Sure. What do you need a quote for?";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

if (session.intent === "EMERGENCY") {
// Emergency: capture address fast + alert owner immediately
session.step = "address";
session.lastPrompt = "Understood. What is the address right now?";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

// existing/new booking goes into normal booking flow
session.step = "job";
session.lastPrompt = "What job do you need help with?";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

// STEP: job
if (session.step === "job") {
session.job = speech;
session.step = "address";
session.lastPrompt = "What is the address?";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

// STEP: address
if (session.step === "address") {
session.address = speech;

// Emergency alert immediately once address captured
if (session.intent === "EMERGENCY") {
await sendOwnerSms(
tradie,
`EMERGENCY LEAD 🚨\nCaller: ${session.from || "Unknown"}\nAddress: ${session.address}\nNote: Call back immediately.`
).catch(() => {});
}

// Calendar history at address
if (tradie.calendarId && tradie.googleServiceJson) {
try {
const calendar = getCalendarClient(tradie);
session.lastAtAddress = await getLastBookingAtAddress(calendar, tradie.calendarId, tz, session.address);
} catch {}
}

session.step = "name";
session.lastPrompt = "What is your name?";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

// STEP: name
if (session.step === "name") {
session.name = speech;

// If cancel/reschedule intent -> immediate handoff after name
if (session.intent === "CANCEL_RESCHEDULE") {
await missedRevenueAlert(tradie, session, "Cancel/reschedule request");
twiml.say("No worries. We'll contact you shortly to reschedule.", { voice: "Polly.Amy", language: "en-AU" });
twiml.hangup();
resetSession(callSid);
return res.type("text/xml").send(twiml.toString());
}

session.step = "time";
session.lastPrompt = "What time would you like?";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

// STEP: time
if (session.step === "time") {
session.time = speech;

let dt = null;
if (!looksLikeAsap(session.time)) dt = parseRequestedDateTime(session.time, tz);
if (!dt && isAfterHoursNow(tradie)) dt = nextBusinessOpenSlot(tradie);
if (!dt) dt = DateTime.now().setZone(tz).plus({ minutes: 10 }).startOf("minute");

session.bookedStartMs = dt.toMillis();
session.duplicateEvent = null;
session.confirmMode = "new";

if (tradie.calendarId && tradie.googleServiceJson) {
try {
const calendar = getCalendarClient(tradie);
const dup = await findDuplicate(calendar, tradie.calendarId, tz, session.name, session.address, dt);
if (dup) session.duplicateEvent = dup;
} catch {}
}

const whenForVoice = formatForVoice(dt);

// Mention customer note (memory) briefly (owner sees it too via SMS)
const noteLine = session.customerNote ? ` I have a note on your account: ${session.customerNote}.` : "";

if (session.duplicateEvent) {
session.step = "confirm";
session.lastPrompt =
`I heard: ${session.job}, at ${session.address}, for ${session.name}, on ${whenForVoice}.${noteLine} ` +
`I also found an existing booking around ${session.duplicateEvent.whenText}. ` +
`Say yes to keep both bookings, say update to replace the old one, or say no to change the time.`;
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

session.step = "confirm";
session.lastPrompt =
`Alright. I heard: ${session.job}, at ${session.address}, for ${session.name}, on ${whenForVoice}.${noteLine} ` +
`Is that correct? Say yes to confirm, or no to change the time.`;
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

// STEP: confirm
if (session.step === "confirm") {
const s = speech.toLowerCase();

const isYes = s.includes("yes") || s.includes("yeah") || s.includes("yep") || s.includes("correct");
const isNo = s.includes("no") || s.includes("nope") || s.includes("wrong") || s.includes("change");
const isUpdate = s.includes("update") || s.includes("replace");

if (isNo) {
const maybe = extractTimeIfPresent(speech, tz);
if (maybe) {
session.time = speech;
session.bookedStartMs = maybe.toMillis();
session.lastPrompt = `Got it. Updated time: ${formatForVoice(maybe)}. Say yes to confirm, or no to change.`;
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

session.step = "time";
session.time = "";
session.bookedStartMs = null;
session.lastPrompt = "No problem. What time would you like instead?";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

if (session.duplicateEvent && isUpdate) {
session.confirmMode = "update";
} else if (!isYes && !isUpdate) {
session.lastPrompt = session.duplicateEvent
? "Sorry — say yes to keep both, update to replace the old booking, or no to change the time."
: "Sorry — just say yes to confirm, or no to change the time.";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
}

const start = DateTime.fromMillis(session.bookedStartMs || Date.now(), { zone: tz });
const end = start.plus({ hours: 1 });

const displayWhen = start.toFormat("ccc d LLL yyyy, h:mm a");
const summaryText = `${session.name} needs ${session.job} at ${session.address}.`;

// Analytics: booking created + est revenue
await incMetric(tradie, {
bookings_created: 1,
est_revenue: tradie.avgJobValue * 1 // booking created (stronger than missed lead)
});

// Customer SMS receipt + pending confirmation (DB preferred)
if (session.from) {
const customerTxt =
`Booking request received ✅\n` +
`Type: ${intentLabel(session.intent)}\n` +
`Job: ${session.job}\n` +
`Address: ${session.address}\n` +
`When: ${displayWhen}\n` +
`We’ll confirm shortly.\n` +
`Reply Y to confirm / N to reschedule.`;

sendCustomerSms(tradie, session.from, customerTxt).catch(() => {});

const pendingKey = makePendingKey(tradie.key, session.from);
const payload = {
key: pendingKey,
tradie_key: tradie.key,
customer_phone: session.from,
name: session.name,
job: session.job,
address: session.address,
when_text: displayWhen,
timezone: tz
};

const wroteDb = await upsertPendingConfirmationDb(pendingKey, payload);
if (!wroteDb) setPendingConfirmationMemory(pendingKey, payload);
}

const historyLine = session.lastAtAddress
? `\nHistory: ${session.lastAtAddress.summary} on ${session.lastAtAddress.whenText}`
: "";

const memoryLine = session.customerNote ? `\nCustomer note: ${session.customerNote}` : "";

// Calendar insert if configured
if (tradie.calendarId && tradie.googleServiceJson) {
const calendar = getCalendarClient(tradie);

try {
if (session.confirmMode === "update" && session.duplicateEvent?.id) {
await deleteEventSafe(calendar, tradie.calendarId, session.duplicateEvent.id);
}

await insertCalendarEventWithRetry(calendar, tradie.calendarId, {
summary: `${session.job} - ${session.name}`,
description:
`${summaryText}\n` +
`Intent: ${intentLabel(session.intent)}\n` +
`Caller: ${session.from || "Unknown"}\n` +
`Spoken time: ${session.time}\n` +
(session.customerNote ? `Customer note: ${session.customerNote}\n` : ""),
location: session.address,
start: { dateTime: toGoogleDateTime(start), timeZone: tz },
end: { dateTime: toGoogleDateTime(end), timeZone: tz }
});
} catch (calErr) {
await sendOwnerSms(
tradie,
`MANUAL FOLLOW-UP NEEDED (Calendar failed)\n` +
`Intent: ${intentLabel(session.intent)}\n` +
`Name: ${session.name}\n` +
`Job: ${session.job}\n` +
`Address: ${session.address}\n` +
`Caller: ${session.from || "Unknown"}\n` +
`Spoken: ${session.time}\n` +
`Intended: ${displayWhen} (${tz})${historyLine}${memoryLine}\n` +
`Reason: ${calErr?.message || calErr}`
);

if (session.from) {
sendCustomerSms(tradie, session.from, `Thanks — we received your request ✅\nWe’ll confirm shortly.`)
.catch(() => {});
}

twiml.say("Thanks. We received your booking request and will confirm shortly.", { voice: "Polly.Amy", language: "en-AU" });
twiml.hangup();
resetSession(callSid);
return res.type("text/xml").send(twiml.toString());
}
}

// Customer memory note auto-update (basic)
// You can replace with a proper question later: "Any parking or gate code notes?"
if (session.from) {
const autoNote = `Last booking: ${session.job}.`;
await setCustomerNote(tradie.key, session.from, autoNote);
}

await sendOwnerSms(
tradie,
`NEW ${intentLabel(session.intent).toUpperCase()} ✅\n` +
`Name: ${session.name}\n` +
`Job: ${session.job}\n` +
`Address: ${session.address}\n` +
`Caller: ${session.from || "Unknown"}\n` +
`Booked: ${displayWhen} (${tz})${historyLine}${memoryLine}` +
(session.duplicateEvent ? `\nDupFound: ${session.duplicateEvent.whenText} (${session.confirmMode})` : "")
).catch(() => {});

twiml.say(`Booked. Thanks ${session.name}. We will see you ${formatForVoice(start)}.`, { voice: "Polly.Amy", language: "en-AU" });

resetSession(callSid);
twiml.hangup();
return res.type("text/xml").send(twiml.toString());
}

// fallback
session.step = "intent";
session.lastPrompt = "How can we help today? You can say emergency, quote, reschedule, or new booking.";
ask(twiml, session.lastPrompt, "/voice");
return res.type("text/xml").send(twiml.toString());
} catch (err) {
console.error("VOICE ERROR:", err);

const s = sessions.get(req.body.CallSid || req.body.CallSID || "unknown");
await sendOwnerSms(
getTradieConfig(req),
`SYSTEM ERROR\nTradieKey: ${getTradieKey(req)}\nFrom: ${s?.from || "Unknown"}\nStep: ${s?.step || "?"}\nError: ${err?.message || err}`
).catch(() => {});

// Missed revenue alert if it died mid-flow
if (s?.from) await missedRevenueAlert(getTradieConfig(req), s, "System error mid-call");

twiml.say("Sorry, there was a system error. Please try again.", { voice: "Polly.Amy", language: "en-AU" });
twiml.hangup();

resetSession(req.body.CallSid || req.body.CallSID || "unknown");
return res.type("text/xml").send(twiml.toString());
}
});

/* ============================================================================
SMS ROUTE (customer replies Y/N)
============================================================================ */
app.post("/sms", async (req, res) => {
if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

const tradie = getTradieConfig(req);
const from = (req.body.From || "").trim();
const body = (req.body.Body || "").trim().toLowerCase();

const twiml = new MessagingResponse();

const pendingKey = makePendingKey(tradie.key, from);

let pending = await getPendingConfirmationDb(pendingKey);
if (!pending) pending = getPendingConfirmationMemory(pendingKey);

if (!pending) {
twiml.message("Thanks — we received your message. If you need help, reply with your address or call us back.");
return res.type("text/xml").send(twiml.toString());
}

const nice =
`Caller: ${from}\n` +
`Name: ${pending.name}\n` +
`Job: ${pending.job}\n` +
`Address: ${pending.address}\n` +
`When: ${pending.when_text} (${pending.timezone || tradie.timezone})`;

if (body === "y" || body === "yes" || body.startsWith("y ")) {
await sendOwnerSms(tradie, `CUSTOMER CONFIRMED ✅\n${nice}`).catch(() => {});
twiml.message("Confirmed ✅ Thanks — see you then.");

await deletePendingConfirmationDb(pendingKey).catch(() => {});
clearPendingConfirmationMemory(pendingKey);
return res.type("text/xml").send(twiml.toString());
}

if (body === "n" || body === "no" || body.startsWith("n ")) {
await sendOwnerSms(tradie, `CUSTOMER RESCHEDULE REQUEST ❗\n${nice}\nAction: Please call/text to reschedule.`).catch(() => {});
twiml.message("No worries — we’ll contact you shortly to reschedule.");

await deletePendingConfirmationDb(pendingKey).catch(() => {});
clearPendingConfirmationMemory(pendingKey);
return res.type("text/xml").send(twiml.toString());
}

twiml.message("Reply Y to confirm or N to reschedule.");
return res.type("text/xml").send(twiml.toString());
});

// Health check
app.get("/", (req, res) => res.send("Voice bot running"));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("Server listening on", PORT));
