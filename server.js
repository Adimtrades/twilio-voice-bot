// server.js

try {
  require("dotenv").config();
} catch (e) {}

const express = require("express");
const twilio = require("twilio");
const { google } = require("googleapis");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");

const app = express();
app.use(express.urlencoded({ extended: false }));

const VoiceResponse = twilio.twiml.VoiceResponse;

// --------------------
// SETTINGS
// --------------------
const ADMIN_SMS_TO = "+61431778238";
const DEFAULT_TZ = process.env.TIMEZONE || "Australia/Sydney";

const BUSINESS_START_HOUR = Number(process.env.BUSINESS_START_HOUR || 7);
const BUSINESS_END_HOUR = Number(process.env.BUSINESS_END_HOUR || 17);

const BUSINESS_DAYS = (process.env.BUSINESS_DAYS || "1,2,3,4,5")
  .split(",")
  .map((x) => Number(x.trim()))
  .filter(Boolean);

const MISSED_CALL_ALERT_TRIES = Number(process.env.MISSED_CALL_ALERT_TRIES || 2);
const MAX_SILENCE_TRIES = Number(process.env.MAX_SILENCE_TRIES || 6);
const CAL_RETRY_ATTEMPTS = Number(process.env.CAL_RETRY_ATTEMPTS || 3);

// --------------------
// Twilio SMS helpers
// --------------------
function getTwilioClient() {
  const sid = process.env.TWILIO_ACCOUNT_SID;
  const token = process.env.TWILIO_AUTH_TOKEN;
  if (!sid || !token) return null;
  return twilio(sid, token);
}

async function sendSms(to, messageText) {
  const from = process.env.TWILIO_SMS_FROM;
  const client = getTwilioClient();

  if (!client) {
    console.warn("SMS skipped: Missing TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN");
    return;
  }
  if (!from) {
    console.warn("SMS skipped: Missing TWILIO_SMS_FROM");
    return;
  }

  await client.messages.create({ from, to, body: messageText });
}

async function sendOwnerSms(messageText) {
  return sendSms(ADMIN_SMS_TO, messageText);
}

// --------------------
// Session store
// --------------------
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
      from: fromNumber || ""
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

// --------------------
// Helpers
// --------------------
function cleanSpeech(text) {
  if (!text) return "";
  return String(text).trim().replace(/\s+/g, " ");
}

function ask(twiml, prompt, actionUrl) {
  const gather = twiml.gather({
    input: "speech",
    action: actionUrl,
    method: "POST",
    timeout: 15,
    speechTimeout: "auto",
    language: "en-AU",
    profanityFilter: false
  });

  gather.say(prompt || "Sorry, can you repeat that?", {
    voice: "Polly.Amy",
    language: "en-AU"
  });
}

function shouldReject(step, speech, confidence) {
  const s = speech.toLowerCase();
  if (!speech || speech.length < 2) return true;

  const junk = new Set(["hello", "hi", "yeah", "yep", "okay", "ok"]);
  if (junk.has(s)) return step !== "job";

  const minConf = step === "job" ? 0.30 : 0.10;
  if (typeof confidence === "number" && confidence > 0 && confidence < minConf) {
    if (speech.split(" ").length <= 2) return true;
  }
  return false;
}

// --------------------
// ✅ TIMEZONE + GOOGLE FIX
// --------------------
function normalizeTimeText(text, tz) {
  if (!text) return "";
  let t = String(text).toLowerCase().trim();

  // STT cleanup
  t = t.replace(/\b(\d{1,2})\s*:\s*(\d{2})\s*p\.?\s*m\.?\b/g, "$1:$2pm");
  t = t.replace(/\b(\d{1,2})\s*:\s*(\d{2})\s*a\.?\s*m\.?\b/g, "$1:$2am");
  t = t.replace(/\b(\d{1,2})\s*p\.?\s*m\.?\b/g, "$1pm");
  t = t.replace(/\b(\d{1,2})\s*a\.?\s*m\.?\b/g, "$1am");
  t = t.replace(/\s+/g, " ");

  // Tonight/today/tomorrow -> explicit date in the right tz
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

// Build a Luxon DateTime IN the target timezone using chrono's parsed components
function parseRequestedDateTime(naturalText, tz) {
  const ref = DateTime.now().setZone(tz).toJSDate();
  const norm = normalizeTimeText(naturalText, tz);

  const results = chrono.parse(norm, ref, { forwardDate: true });
  if (!results || results.length === 0) return null;

  const r = results[0];
  const s = r.start;
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

// ✅ Send Google Calendar RFC3339 WITH OFFSET (prevents the 4am shift)
function toGoogleDateTime(dt) {
  return dt.toISO({ includeOffset: true, suppressMilliseconds: true });
}

function isAfterHoursNow(tz) {
  const now = DateTime.now().setZone(tz);
  const isBizDay = BUSINESS_DAYS.includes(now.weekday);
  const isBizHours = now.hour >= BUSINESS_START_HOUR && now.hour < BUSINESS_END_HOUR;
  return !(isBizDay && isBizHours);
}

function nextBusinessOpenSlot(tz) {
  let dt = DateTime.now().setZone(tz);

  const isBizDay = BUSINESS_DAYS.includes(dt.weekday);
  const isBizHours = dt.hour >= BUSINESS_START_HOUR && dt.hour < BUSINESS_END_HOUR;
  if (isBizDay && isBizHours) {
    return dt.plus({ minutes: 10 }).startOf("minute");
  }

  dt = dt.plus({ days: 1 }).startOf("day");
  while (!BUSINESS_DAYS.includes(dt.weekday)) dt = dt.plus({ days: 1 }).startOf("day");

  return dt.set({ hour: BUSINESS_START_HOUR, minute: 0, second: 0, millisecond: 0 });
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

// --------------------
// Google Calendar client
// --------------------
function parseGoogleServiceJson() {
  const raw = process.env.GOOGLE_SERVICE_JSON;
  if (!raw) throw new Error("Missing GOOGLE_SERVICE_JSON env variable");

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    parsed = JSON.parse(raw.replace(/\r?\n/g, "\\n"));
  }

  if (typeof parsed === "string") parsed = JSON.parse(parsed);
  return parsed;
}

function getCalendarClient() {
  const credentials = parseGoogleServiceJson();
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
      console.error(
        `Calendar insert failed (attempt ${attempt}/${CAL_RETRY_ATTEMPTS})`,
        err?.message || err
      );
      if (attempt < CAL_RETRY_ATTEMPTS) await sleep(attempt === 1 ? 200 : 800);
    }
  }
  throw lastErr;
}

// --------------------
// Voice route
// --------------------
app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  const callSid = req.body.CallSid || req.body.CallSID || "unknown";
  const fromNumber = req.body.From || "";
  const confidence = Number(req.body.Confidence || 0);
  const speechRaw = req.body.SpeechResult || "";
  const speech = cleanSpeech(speechRaw);

  const session = getSession(callSid, fromNumber);
  console.log(
    `CALLSID=${callSid} FROM=${fromNumber} STEP=${session.step} Speech="${speech}" Confidence=${confidence}`
  );

  if (!speech) {
    session.tries += 1;

    if (session.tries === MISSED_CALL_ALERT_TRIES) {
      const txt =
        `MISSED/QUIET CALL ALERT\n` +
        `From: ${session.from || "Unknown"}\n` +
        `Progress: step=${session.step}\n` +
        `Time: ${DateTime.now().setZone(DEFAULT_TZ).toFormat("ccc d LLL yyyy, h:mm a")}`;
      sendOwnerSms(txt).catch(() => {});
    }

    if (session.tries >= MAX_SILENCE_TRIES) {
      twiml.say("No worries. Call back when you're ready.", {
        voice: "Polly.Amy",
        language: "en-AU"
      });
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

  session.tries = 0;

  if (session.step !== "confirm" && shouldReject(session.step, speech, confidence)) {
    session.lastPrompt = "Sorry, can you repeat that?";
    ask(twiml, session.lastPrompt, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const tz = DEFAULT_TZ;

    if (session.step === "job") {
      session.job = speech;
      session.step = "address";
      session.lastPrompt = "What is the address?";
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "address") {
      session.address = speech;
      session.step = "name";
      session.lastPrompt = "What is your name?";
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "name") {
      session.name = speech;
      session.step = "time";
      session.lastPrompt = "What time would you like?";
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "time") {
      session.time = speech;

      let dt = null;

      if (!looksLikeAsap(session.time)) {
        dt = parseRequestedDateTime(session.time, tz);
      }

      // After-hours fallback
      if (!dt && isAfterHoursNow(tz)) {
        dt = nextBusinessOpenSlot(tz);
      }

      // Last fallback
      if (!dt) {
        dt = DateTime.now().setZone(tz).plus({ minutes: 10 }).startOf("minute");
      }

      session.bookedStartMs = dt.toMillis();

      session.step = "confirm";
      session.lastPrompt =
        `Alright. I heard: ${session.job}, at ${session.address}, for ${session.name}, on ${formatForVoice(dt)}. ` +
        `Is that correct? Say yes to confirm, or no to change the time.`;

      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "confirm") {
      const s = speech.toLowerCase();
      const isYes = s.includes("yes") || s.includes("yeah") || s.includes("yep") || s.includes("correct");
      const isNo = s.includes("no") || s.includes("nope") || s.includes("wrong") || s.includes("change");

      if (!isYes && !isNo) {
        session.lastPrompt = "Sorry — just say yes to confirm, or no to change the time.";
        ask(twiml, session.lastPrompt, "/voice");
        return res.type("text/xml").send(twiml.toString());
      }

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

      // YES -> create calendar event
      const calendarId = process.env.GOOGLE_CALENDAR_ID;
      if (!calendarId) throw new Error("Missing GOOGLE_CALENDAR_ID env variable");

      const calendar = getCalendarClient();

      const start = DateTime.fromMillis(session.bookedStartMs || Date.now(), { zone: tz });
      const end = start.plus({ hours: 1 });

      const summaryText = `${session.name} needs ${session.job} at ${session.address}.`;
      const displayWhen = start.toFormat("ccc d LLL yyyy, h:mm a");

      await insertCalendarEventWithRetry(calendar, calendarId, {
        summary: `${session.job} - ${session.name}`,
        description: `${summaryText}\nCaller: ${session.from || "Unknown"}\nSpoken time: ${session.time}`,
        location: session.address,
        start: {
          dateTime: toGoogleDateTime(start),
          timeZone: tz
        },
        end: {
          dateTime: toGoogleDateTime(end),
          timeZone: tz
        }
      });

      await sendOwnerSms(
        `NEW BOOKING ✅\n` +
          `Name: ${session.name}\n` +
          `Job: ${session.job}\n` +
          `Address: ${session.address}\n` +
          `Caller: ${session.from || "Unknown"}\n` +
          `Spoken: ${session.time}\n` +
          `Booked: ${displayWhen} (${tz})`
      );

      twiml.say(`Booked. Thanks ${session.name}. See you ${formatForVoice(start)}.`, {
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

    sendOwnerSms(
      `SYSTEM ERROR\nFrom: ${sessions.get(callSid)?.from || "Unknown"}\nStep: ${sessions.get(callSid)?.step || "?"}\nError: ${
        err?.message || err
      }`
    ).catch(() => {});

    twiml.say("Sorry, there was a system error. Please try again.", {
      voice: "Polly.Amy",
      language: "en-AU"
    });
    twiml.hangup();

    resetSession(callSid);
    return res.type("text/xml").send(twiml.toString());
  }
});

// Health check
app.get("/", (req, res) => res.send("Voice bot running"));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("Server listening on", PORT));
