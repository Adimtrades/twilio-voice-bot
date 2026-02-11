// server.js

try { require("dotenv").config(); } catch (e) {}

const express = require("express");
const twilio = require("twilio");
const { google } = require("googleapis");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");

const app = express();
app.use(express.urlencoded({ extended: false }));

const VoiceResponse = twilio.twiml.VoiceResponse;

/* ============================================================================
  MULTI-TRADIE CONFIG (multi-tenant) via ENV

  ✅ Recommended: Set TRADIES_JSON to map the Twilio "To" number (your Twilio voice number)
  to that tradie's settings.

  Example TRADIES_JSON:
  {
    "+61280001234": {
      "ownerSmsTo": "+61431778238",
      "smsFrom": "+61280001234",
      "timezone": "Australia/Sydney",
      "businessStartHour": 7,
      "businessEndHour": 17,
      "businessDays": [1,2,3,4,5],
      "calendarId": "your_calendar_id@group.calendar.google.com"
    }
  }

  Fallbacks still supported via env:
    OWNER_SMS_TO, TWILIO_SMS_FROM, TIMEZONE, BUSINESS_START_HOUR, BUSINESS_END_HOUR, BUSINESS_DAYS, GOOGLE_CALENDAR_ID

  Optional: if you want per-tradie google credentials, add "googleServiceJson" inside each tradie config.
============================================================================ */

function parseTradiesJson() {
  const raw = process.env.TRADIES_JSON;
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (e) {
    // tolerate accidental newline formatting
    try { return JSON.parse(raw.replace(/\r?\n/g, "")); } catch { return {}; }
  }
}

const TRADIES = parseTradiesJson();

function getTradieKey(req) {
  // Twilio sends To = your Twilio voice number that was dialed
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
    smsFrom: t.smsFrom || process.env.TWILIO_SMS_FROM, // Twilio SMS-capable number
    timezone: t.timezone || process.env.TIMEZONE || "Australia/Sydney",
    businessStartHour: Number(t.businessStartHour ?? process.env.BUSINESS_START_HOUR ?? 7),
    businessEndHour: Number(t.businessEndHour ?? process.env.BUSINESS_END_HOUR ?? 17),
    businessDays,
    calendarId: t.calendarId || process.env.GOOGLE_CALENDAR_ID,
    // Optional per-tradie creds
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

/* ============================================================================
  Twilio SMS helpers (per-tradie "from")
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
  Session store
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
      // calendar memory
      lastAtAddress: null,     // { whenText, summary }
      duplicateEvent: null,    // { id, whenText, summary }
      confirmMode: "new"       // "new" | "update"
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

  gather.say(prompt || "Sorry, can you repeat that?", {
    voice: "Polly.Amy",
    language: "en-AU"
  });

  // keep TwiML open a touch
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

  // tonight/today/tomorrow -> explicit date
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

// Google wants RFC3339 + timeZone; include offset to prevent 4am shifts
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
  Google Calendar client + reads for memory/dupe
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

function normStr(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

// Customer history: last event at same address (in last 365 days)
async function getLastBookingAtAddress(calendar, calendarId, tz, address) {
  const q = address;
  const timeMin = DateTime.now().setZone(tz).minus({ days: 365 }).toISO();
  const timeMax = DateTime.now().setZone(tz).toISO();

  const resp = await calendar.events.list({
    calendarId,
    q,
    timeMin,
    timeMax,
    singleEvents: true,
    orderBy: "startTime",
    maxResults: 20
  });

  const items = resp?.data?.items || [];
  if (!items.length) return null;

  // Pick the most recent with matching address-ish
  const addrN = normStr(address);
  const candidates = items
    .filter((ev) => normStr(ev.location).includes(addrN) || normStr(ev.summary).includes(addrN) || normStr(ev.description).includes(addrN))
    .sort((a, b) => String(a.start?.dateTime || a.start?.date).localeCompare(String(b.start?.dateTime || b.start?.date)));

  const last = candidates[candidates.length - 1] || items[items.length - 1];
  const whenISO = last.start?.dateTime || last.start?.date;
  const when = whenISO ? DateTime.fromISO(whenISO, { setZone: true }).setZone(tz) : null;

  return {
    summary: last.summary || "Previous booking",
    whenText: when ? when.toFormat("ccc d LLL yyyy") : "Unknown date"
  };
}

// Duplicate check: same name+address within ±14 days of chosen start
async function findDuplicate(calendar, calendarId, tz, name, address, startDt) {
  const t0 = startDt.minus({ days: DUP_WINDOW_DAYS });
  const t1 = startDt.plus({ days: DUP_WINDOW_DAYS });

  const resp = await calendar.events.list({
    calendarId,
    q: address, // cheap filter
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
  Optional Preferences DB (Supabase) - no extra deps (uses fetch)

  Setup (optional):
    SUPABASE_URL=https://xxxxx.supabase.co
    SUPABASE_SERVICE_KEY=xxxx (service role key)
    SUPABASE_TABLE=customer_prefs

  Table columns (suggested):
    phone text primary key
    preferred_note text
    updated_at timestamp

  This is OPTIONAL and safe if not configured.
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

async function getCustomerPref(phone) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !phone) return null;

  const url = `${SUPABASE_URL}/rest/v1/${SUPABASE_TABLE}?phone=eq.${encodeURIComponent(phone)}&select=preferred_note&limit=1`;
  try {
    const r = await fetch(url, {
      headers: {
        apikey: SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`
      }
    });
    const data = await r.json();
    return Array.isArray(data) && data[0]?.preferred_note ? String(data[0].preferred_note) : null;
  } catch {
    return null;
  }
}

/* ============================================================================
  Human handoff helpers
============================================================================ */
async function humanHandoff(tradie, session, reason) {
  const body =
    `HUMAN HANDOFF NEEDED\n` +
    `Reason: ${reason}\n` +
    `Caller: ${session.from || "Unknown"}\n` +
    `Name: ${session.name || "-"}\n` +
    `Job: ${session.job || "-"}\n` +
    `Address: ${session.address || "-"}\n` +
    `Time said: ${session.time || "-"}`;

  await sendOwnerSms(tradie, body).catch(() => {});
}

/* ============================================================================
  Voice route
============================================================================ */
app.post("/voice", async (req, res) => {
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

      // If calendar is configured, pull last booking at this address (memory)
      if (tradie.calendarId && tradie.googleServiceJson) {
        try {
          const calendar = getCalendarClient(tradie);
          session.lastAtAddress = await getLastBookingAtAddress(calendar, tradie.calendarId, tz, session.address);
        } catch (e) {
          // ignore history errors
        }
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

      // Duplicate check before confirm (if calendar configured)
      if (tradie.calendarId && tradie.googleServiceJson) {
        try {
          const calendar = getCalendarClient(tradie);
          const dup = await findDuplicate(calendar, tradie.calendarId, tz, session.name, session.address, dt);
          if (dup) session.duplicateEvent = dup;
        } catch (e) {
          // ignore dupe errors
        }
      }

      const whenForVoice = formatForVoice(dt);

      // Confirm prompt with duplicate info
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

      // If they said "No, Thursday 5pm" capture the new time immediately
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

      // If dupe exists and they say "update"
      if (session.duplicateEvent && isUpdate) {
        session.confirmMode = "update";
      } else if (!isYes && !isUpdate) {
        // not understood
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
          `We'll confirm shortly.\n` +
          `Reply Y to confirm / N to reschedule (coming soon).`;
        sendCustomerSms(tradie, session.from, customerTxt).catch(() => {});
      }

      // Owner SMS (include history)
      const historyLine = session.lastAtAddress
        ? `\nHistory: ${session.lastAtAddress.summary} on ${session.lastAtAddress.whenText}`
        : "";

      // Calendar insert if configured, else SMS-only
      if (tradie.calendarId && tradie.googleServiceJson) {
        const calendar = getCalendarClient(tradie);

        try {
          // If "update", delete the duplicate event first
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
          // Backup channel when calendar fails: send manual follow-up SMS with full details
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

          twiml.say("Thanks. We received your booking request and will confirm shortly.", {
            voice: "Polly.Amy",
            language: "en-AU"
          });
          twiml.hangup();
          resetSession(callSid);
          return res.type("text/xml").send(twiml.toString());
        }
      }

      // Optional: store simple preference note (example: store job as last preference)
      // Replace this with a real preference capture later ("Any gate code or parking note?")
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

    const tradie = getTradieConfig(req);
    const callSid = req.body.CallSid || req.body.CallSID || "unknown";
    const s = sessions.get(callSid);

    sendOwnerSms(
      tradie,
      `SYSTEM ERROR\nFrom: ${s?.from || "Unknown"}\nStep: ${s?.step || "?"}\nError:s: ${err?.message || err}`
    ).catch(() => {});

    twiml.say("Sorry, there was a system error. Please try again.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.hangup();

    resetSession(callSid);
    return res.type("text/xml").send(twiml.toString());
  }
});

// Health check
app.get("/", (req, res) => res.send("Voice bot running"));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("Server listening on", PORT));
