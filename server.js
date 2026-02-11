// server.js
// Sell-ready: Multi-tenant, customer SMS, dupe detection, history, human fallback, calendar backup, inbound SMS Y/N

try { require("dotenv").config(); } catch (e) {}

const express = require("express");
const twilio = require("twilio");
const { google } = require("googleapis");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");

const app = express();

// Twilio sends form-encoded for Voice + SMS webhooks
app.use(express.urlencoded({ extended: false }));

const VoiceResponse = twilio.twiml.VoiceResponse;
const MessagingResponse = twilio.twiml.MessagingResponse;

/* ============================================================================
  MULTI-TRADIE CONFIG (multi-tenant) via ENV

  TRADIES_JSON maps the Twilio Voice "To" number (the Twilio number the caller dialed)
  to that tradie's settings.

  Example:
  {
    "+61489272876": {
      "ownerSmsTo": "+61431778238",
      "smsFrom": "+61489272876",
      "timezone": "Australia/Sydney",
      "businessStartHour": 7,
      "businessEndHour": 17,
      "businessDays": [1,2,3,4,5],
      "calendarId": "your_calendar_id@group.calendar.google.com"
    }
  }

  Fallback envs:
    OWNER_SMS_TO, TWILIO_SMS_FROM, TIMEZONE, BUSINESS_START_HOUR, BUSINESS_END_HOUR, BUSINESS_DAYS, GOOGLE_CALENDAR_ID
    GOOGLE_SERVICE_JSON (or per-tradie googleServiceJson)

  SECURITY (recommended for selling):
    TWILIO_WEBHOOK_AUTH=true + TWILIO_AUTH_TOKEN to validate signatures.
============================================================================ */

function parseTradiesJson() {
  const raw = process.env.TRADIES_JSON;
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (e) {
    try {
      return JSON.parse(raw.replace(/\r?\n/g, ""));
    } catch {
      console.warn("TRADIES_JSON parse failed. Check JSON formatting.");
      return {};
    }
  }
}

const TRADIES = parseTradiesJson();

function getTradieKey(req) {
  // Voice: req.body.To is your Twilio number (the number dialed)
  // SMS: req.body.To is also your Twilio number
  return req.body.To || req.query.tid || "default";
}

function getTradieConfig(req) {
  const key = getTradieKey(req);
  const t = TRADIES[key] || TRADIES.default || {};

  const businessDays =
    Array.isArray(t.businessDays) && t.businessDays.length
      ? t.businessDays
      : (process.env.BUSINESS_DAYS || "1,2,3,4,5")
          .split(",")
          .map((x) => Number(x.trim()))
          .filter(Boolean);

  return {
    key,
    ownerSmsTo: t.ownerSmsTo || process.env.OWNER_SMS_TO || "+61431778238",
    smsFrom: t.smsFrom || process.env.TWILIO_SMS_FROM, // must be SMS-capable Twilio number
    timezone: t.timezone || process.env.TIMEZONE || "Australia/Sydney",
    businessStartHour: Number(t.businessStartHour ?? process.env.BUSINESS_START_HOUR ?? 7),
    businessEndHour: Number(t.businessEndHour ?? process.env.BUSINESS_END_HOUR ?? 17),
    businessDays,
    calendarId: t.calendarId || process.env.GOOGLE_CALENDAR_ID || "",
    googleServiceJson: t.googleServiceJson || process.env.GOOGLE_SERVICE_JSON || ""
  };
}

// Global controls
const MISSED_CALL_ALERT_TRIES = Number(process.env.MISSED_CALL_ALERT_TRIES || 2);
const MAX_SILENCE_TRIES = Number(process.env.MAX_SILENCE_TRIES || 10);
const CAL_RETRY_ATTEMPTS = Number(process.env.CAL_RETRY_ATTEMPTS || 3);

// Gather timing (wait longer)
const GATHER_START_TIMEOUT = Number(process.env.GATHER_START_TIMEOUT || 25);
const GATHER_SPEECH_TIMEOUT = process.env.GATHER_SPEECH_TIMEOUT || "auto";

// Human handoff thresholds
const MAX_REJECTS_ADDRESS = Number(process.env.MAX_REJECTS_ADDRESS || 2);
const MAX_REJECTS_TIME = Number(process.env.MAX_REJECTS_TIME || 2);

// Duplicate window days
const DUP_WINDOW_DAYS = Number(process.env.DUP_WINDOW_DAYS || 14);

// Optional: Twilio signature validation (recommended)
const REQUIRE_TWILIO_SIG = String(process.env.TWILIO_WEBHOOK_AUTH || "false").toLowerCase() === "true";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

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
  if (!client) {
    console.warn("SMS skipped: Missing TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN");
    return;
  }
  if (!from) {
    console.warn("SMS skipped: Missing smsFrom/TWILIO_SMS_FROM");
    return;
  }
  if (!to) {
    console.warn("SMS skipped: missing 'to'");
    return;
  }
  await client.messages.create({ from, to, body });
}

async function sendOwnerSms(tradie, body) {
  return sendSms({ from: tradie.smsFrom, to: tradie.ownerSmsTo, body });
}

async function sendCustomerSms(tradie, toCustomer, body) {
  return sendSms({ from: tradie.smsFrom, to: toCustomer, body });
}

/* ============================================================================
  Session store (calls)
============================================================================ */
const sessions = new Map();

function getSession(callSid, fromNumber = "") {
  if (!sessions.has(callSid)) {
    sessions.set(callSid, {
      step: "job",
      job: "",
      address: "",
      name: "",
      time: "",
      bookedStartMs: null,
      lastPrompt: "",
      tries: 0,
      from: fromNumber || "",
      rejects: { address: 0, time: 0 },
      lastAtAddress: null,   // { whenText, summary }
      duplicateEvent: null,  // { id, whenText, summary }
      confirmMode: "new"     // "new" | "update"
    });
  } else {
    const s = sessions.get(callSid);
    if (fromNumber && !s.from) s.from = fromNumber;
  }
  return sessions.get(callSid);
}

function resetSession(callSid) {
  sessions.delete(callSid);
}

/* ============================================================================
  Inbound SMS “Y/N” tracking (sell-ready baseline)
  NOTE: In-memory is OK for MVP. For real SaaS, store this in DB.
============================================================================ */
const pendingConfirmations = new Map(); // key: customerPhone, value: booking summary

function setPendingConfirmation(customerPhone, payload) {
  if (!customerPhone) return;
  pendingConfirmations.set(customerPhone, { ...payload, createdAt: Date.now() });
}

function getPendingConfirmation(customerPhone) {
  if (!customerPhone) return null;
  return pendingConfirmations.get(customerPhone) || null;
}

function clearPendingConfirmation(customerPhone) {
  if (!customerPhone) return;
  pendingConfirmations.delete(customerPhone);
}

/* ============================================================================
  Helpers
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
  if (junk.has(s)) return step !== "job";

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

// Build Luxon DateTime IN tz from chrono components (prevents drift)
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

  const dt = DateTime.fromObject(
    { year, month, day, hour, minute, second: 0, millisecond: 0 },
    { zone: tz }
  );
  return dt.isValid ? dt : null;
}

function extractTimeIfPresent(text, tz) {
  return parseRequestedDateTime(text, tz);
}

// Google wants RFC3339 + timeZone. includeOffset prevents the “4am shift”.
function toGoogleDateTime(dt) {
  return dt.toISO({ includeOffset: true, suppressMilliseconds: true });
}

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

function formatForVoice(dt) {
  return dt.toFormat("ccc d LLL, h:mm a");
}

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
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    parsed = JSON.parse(raw.replace(/\r?\n/g, "\\n"));
  }
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

// Customer history: last event at same address (last 365 days)
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

  const last = (candidates.length ? candidates : items).sort((a, b) => {
    const aISO = a.start?.dateTime || a.start?.date || "";
    const bISO = b.start?.dateTime || b.start?.date || "";
    return String(aISO).localeCompare(String(bISO));
  }).pop();

  if (!last) return null;

  const whenISO = last.start?.dateTime || last.start?.date;
  const when = whenISO ? DateTime.fromISO(whenISO, { setZone: true }).setZone(tz) : null;

  return {
    summary: last.summary || "Previous booking",
    whenText: when ? when.toFormat("ccc d LLL yyyy") : "Unknown date"
  };
}

// Duplicate check: same name+address within ±DUP_WINDOW_DAYS
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
  try {
    await calendar.events.delete({ calendarId, eventId });
  } catch (e) {
    console.warn("Delete failed:", e?.message || e);
  }
}

/* ============================================================================
  OPTIONAL: Preferences DB (Supabase) - safe if not configured.
  NOTE: Node 18+ has global fetch. If not, add dependency node-fetch.
============================================================================ */
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || "";
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || "customer_prefs";

async function upsertCustomerPref(phone, note) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !phone) return;
  const url = `${SUPABASE_URL}/rest/v1/${SUPABASE_TABLE}`;
  const payload = [{ phone, preferred_note: note, updated_at: new Date().toISOString() }];

  await fetch(url, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates"
    },
    body: JSON.stringify(payload)
  }).catch(() => {});
}

/* ============================================================================
  Human handoff
============================================================================ */
async function humanHandoff(tradie, session, reason) {
  const body =
    `HUMAN HANDOFF NEEDED\n` +
    `Reason: ${reason}\n` +
    `TradieKey: ${tradie.key}\n` +
    `Caller: ${session.from || "Unknown"}\n` +
    `Name: ${session.name || "-"}\n` +
    `Job: ${session.job || "-"}\n` +
    `Address: ${session.address || "-"}\n` +
    `Time said: ${session.time || "-"}`;

  await sendOwnerSms(tradie, body).catch(() => {});
}

/* ============================================================================
  Webhook signature check (recommended for selling)
============================================================================ */
function validateTwilioSignature(req) {
  if (!REQUIRE_TWILIO_SIG) return true;
  if (!TWILIO_AUTH_TOKEN) return false;
  const signature = req.headers["x-twilio-signature"];
  if (!signature) return false;

  // Build full URL Twilio requested (Render behind proxy needs host/proto)
  const proto = (req.headers["x-forwarded-proto"] || "https").split(",")[0].trim();
  const host = (req.headers["x-forwarded-host"] || req.headers["host"] || "").split(",")[0].trim();
  const url = `${proto}://${host}${req.originalUrl}`;

  return twilio.validateRequest(TWILIO_AUTH_TOKEN, signature, url, req.body);
}

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

  console.log(
    `TID=${tradie.key} CALLSID=${callSid} FROM=${fromNumber} STEP=${session.step} Speech="${speech}" Confidence=${confidence}`
  );

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
      twiml.say("No worries. We'll call you back shortly.", { voice: "Polly.Amy", language: "en-AU" });
      await humanHandoff(tradie, session, "Caller silent / timeout");
      twiml.hangup();
      resetSession(callSid);
      return res.type("text/xml").send(twiml.toString());
    }

    const promptMap = {
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

  // Got speech
  session.tries = 0;

  // Reject handling + human handoff for address/time
  if (session.step !== "confirm" && shouldReject(session.step, speech, confidence)) {
    if (session.step === "address") session.rejects.address += 1;
    if (session.step === "time") session.rejects.time += 1;

    if (session.rejects.address >= MAX_REJECTS_ADDRESS) {
      twiml.say("No worries. I'll have the team call you back to confirm the address.", {
        voice: "Polly.Amy",
        language: "en-AU"
      });
      await humanHandoff(tradie, session, "Address capture failed");
      twiml.hangup();
      resetSession(callSid);
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.rejects.time >= MAX_REJECTS_TIME) {
      twiml.say("No worries. I'll have the team call you back to confirm the time.", {
        voice: "Polly.Amy",
        language: "en-AU"
      });
      await humanHandoff(tradie, session, "Time capture failed");
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

    // STEP: job
    if (session.step === "job") {
      session.job = speech;
      session.step = "address";
      session.lastPrompt = "What is the address?";
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: address (customer history lookup after captured)
    if (session.step === "address") {
      session.address = speech;

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
      session.step = "time";
      session.lastPrompt = "What time would you like?";
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: time (dupe check + confirm)
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

      if (session.duplicateEvent) {
        session.step = "confirm";
        session.lastPrompt =
          `I heard: ${session.job}, at ${session.address}, for ${session.name}, on ${whenForVoice}. ` +
          `I also found an existing booking around ${session.duplicateEvent.whenText}. ` +
          `Say yes to keep both bookings, say update to replace the old one, or say no to change the time.`;
        ask(twiml, session.lastPrompt, "/voice");
        return res.type("text/xml").send(twiml.toString());
      }

      session.step = "confirm";
      session.lastPrompt =
        `Alright. I heard: ${session.job}, at ${session.address}, for ${session.name}, on ${whenForVoice}. ` +
        `Is that correct? Say yes to confirm, or no to change the time.`;
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: confirm (YES / NO / UPDATE)
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

      // Proceed with booking (calendar optional)
      const start = DateTime.fromMillis(session.bookedStartMs || Date.now(), { zone: tz });
      const end = start.plus({ hours: 1 });

      const displayWhen = start.toFormat("ccc d LLL yyyy, h:mm a");
      const summaryText = `${session.name} needs ${session.job} at ${session.address}.`;

      // Customer SMS receipt (trust booster)
      if (session.from) {
        const customerTxt =
          `Booking request received ✅\n` +
          `Job: ${session.job}\n` +
          `Address: ${session.address}\n` +
          `When: ${displayWhen}\n` +
          `We’ll confirm shortly.\n` +
          `Reply Y to confirm / N to reschedule.`;
        sendCustomerSms(tradie, session.from, customerTxt).catch(() => {});

        // Track pending confirmation (MVP)
        setPendingConfirmation(session.from, {
          tradieKey: tradie.key,
          ownerSmsTo: tradie.ownerSmsTo,
          smsFrom: tradie.smsFrom,
          name: session.name,
          job: session.job,
          address: session.address,
          when: displayWhen,
          timezone: tz
        });
      }

      // Owner SMS (include history)
      const historyLine = session.lastAtAddress
        ? `\nHistory: ${session.lastAtAddress.summary} on ${session.lastAtAddress.whenText}`
        : "";

      // Calendar insert if configured, else SMS-only
      if (tradie.calendarId && tradie.googleServiceJson) {
        const calendar = getCalendarClient(tradie);

        try {
          if (session.confirmMode === "update" && session.duplicateEvent?.id) {
            await deleteEventSafe(calendar, tradie.calendarId, session.duplicateEvent.id);
          }

          await insertCalendarEventWithRetry(calendar, tradie.calendarId, {
            summary: `${session.job} - ${session.name}`,
            description: `${summaryText}\nCaller: ${session.from || "Unknown"}\nSpoken time: ${session.time}`,
            location: session.address,
            start: { dateTime: toGoogleDateTime(start), timeZone: tz },
            end: { dateTime: toGoogleDateTime(end), timeZone: tz }
          });
        } catch (calErr) {
          // Backup channel when calendar fails: manual follow-up SMS
          await sendOwnerSms(
            tradie,
            `MANUAL FOLLOW-UP NEEDED (Calendar failed)\n` +
              `Name: ${session.name}\n` +
              `Job: ${session.job}\n` +
              `Address: ${session.address}\n` +
              `Caller: ${session.from || "Unknown"}\n` +
              `Spoken: ${session.time}\n` +
              `Intended: ${displayWhen} (${tz})${historyLine}\n` +
              `Reason: ${calErr?.message || calErr}`
          );

          // Customer reassurance
          if (session.from) {
            sendCustomerSms(
              tradie,
              session.from,
              `Thanks — we received your request ✅\nWe’ll confirm the time shortly.`
            ).catch(() => {});
          }

          twiml.say("Thanks. We received your booking request and will confirm shortly.", {
            voice: "Polly.Amy",
            language: "en-AU"
          });
          twiml.hangup();
          resetSession(callSid);
          return res.type("text/xml").send(twiml.toString());
        }
      }

      // Optional preferences (MVP placeholder)
      upsertCustomerPref(session.from, `Last job: ${session.job}`).catch(() => {});

      await sendOwnerSms(
        tradie,
        `NEW BOOKING ✅\n` +
          `Name: ${session.name}\n` +
          `Job: ${session.job}\n` +
          `Address: ${session.address}\n` +
          `Caller: ${session.from || "Unknown"}\n` +
          `Spoken: ${session.time}\n` +
          `Booked: ${displayWhen} (${tz})${historyLine}` +
          (session.duplicateEvent ? `\nDupFound: ${session.duplicateEvent.whenText} (${session.confirmMode})` : "")
      ).catch(() => {});

      twiml.say(`Booked. Thanks ${session.name}. We will see you ${formatForVoice(start)}.`, {
        voice: "Polly.Amy",
        language: "en-AU"
      });

      resetSession(callSid);
      twiml.hangup();
      return res.type("text/xml").send(twiml.toString());
    }

    // fallback
    session.step = "job";
    session.lastPrompt = "What job do you need help with?";
    ask(twiml, session.lastPrompt, "/voice");
    return res.type("text/xml").send(twiml.toString());
  } catch (err) {
    console.error("VOICE ERROR:", err);

    const s = sessions.get(req.body.CallSid || req.body.CallSID || "unknown");
    sendOwnerSms(
      getTradieConfig(req),
      `SYSTEM ERROR\nTradieKey: ${getTradieKey(req)}\nFrom: ${s?.from || "Unknown"}\nStep: ${s?.step || "?"}\nError: ${err?.message || err}`
    ).catch(() => {});

    twiml.say("Sorry, there was a system error. Please try again.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.hangup();

    resetSession(req.body.CallSid || req.body.CallSID || "unknown");
    return res.type("text/xml").send(twiml.toString());
  }
});

/* ============================================================================
  SMS ROUTE (customer replies Y/N)
  Twilio Messaging webhook -> POST /sms

  MVP behavior:
    Y: notify owner "customer confirmed"
    N: notify owner "customer wants reschedule"
============================================================================ */
app.post("/sms", async (req, res) => {
  if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

  const tradie = getTradieConfig(req);
  const from = (req.body.From || "").trim();
  const body = (req.body.Body || "").trim().toLowerCase();

  const twiml = new MessagingResponse();

  const pending = getPendingConfirmation(from);

  if (!pending) {
    twiml.message("Thanks — we received your message. If you need help, reply with your address or call us back.");
    return res.type("text/xml").send(twiml.toString());
  }

  if (body === "y" || body.startsWith("y ")) {
    await sendOwnerSms(
      tradie,
      `CUSTOMER CONFIRMED ✅\n` +
        `Caller: ${from}\n` +
        `Name: ${pending.name}\n` +
        `Job: ${pending.job}\n` +
        `Address: ${pending.address}\n` +
        `When: ${pending.when} (${pending.timezone})`
    ).catch(() => {});
    twiml.message("Confirmed ✅ Thanks — see you then.");
    clearPendingConfirmation(from);
    return res.type("text/xml").send(twiml.toString());
  }

  if (body === "n" || body.startsWith("n ")) {
    await sendOwnerSms(
      tradie,
      `CUSTOMER RESCHEDULE REQUEST ❗\n` +
        `Caller: ${from}\n` +
        `Name: ${pending.name}\n` +
        `Job: ${pending.job}\n` +
        `Address: ${pending.address}\n` +
        `Original: ${pending.when} (${pending.timezone})\n` +
        `Action: Please call/text to reschedule.`
    ).catch(() => {});
    twiml.message("No worries — we’ll contact you shortly to reschedule.");
    clearPendingConfirmation(from);
    return res.type("text/xml").send(twiml.toString());
  }

  twiml.message("Reply Y to confirm or N to reschedule.");
  return res.type("text/xml").send(twiml.toString());
});

// Health check
app.get("/", (req, res) => res.send("Voice bot running"));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("Server listening on", PORT));
