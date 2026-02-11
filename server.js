// server.js

// Safe dotenv: works locally, won't crash if missing in prod
try {
  require("dotenv").config();
} catch (e) {
  // ignore in Render if not needed
}

const express = require("express");
const twilio = require("twilio");
const { google } = require("googleapis");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");

const app = express();

// Twilio sends application/x-www-form-urlencoded
app.use(express.urlencoded({ extended: false }));

const VoiceResponse = twilio.twiml.VoiceResponse;

// --------------------
// SETTINGS (edit if you want)
// --------------------
const ADMIN_SMS_TO = "+61431778238"; // owner (0431... => +61431...)
const DEFAULT_TZ = process.env.TIMEZONE || "Australia/Sydney";

// Business hours for after-hours auto booking
const BUSINESS_START_HOUR = Number(process.env.BUSINESS_START_HOUR || 7);  // 7am
const BUSINESS_END_HOUR = Number(process.env.BUSINESS_END_HOUR || 17);     // 5pm
// 1=Mon ... 7=Sun (Luxon)
const BUSINESS_DAYS = (process.env.BUSINESS_DAYS || "1,2,3,4,5")
  .split(",")
  .map((x) => Number(x.trim()))
  .filter(Boolean);

// If caller stays silent this many times, alert owner as "missed call"
const MISSED_CALL_ALERT_TRIES = Number(process.env.MISSED_CALL_ALERT_TRIES || 2);

// Max no-speech tries before we end call
const MAX_SILENCE_TRIES = Number(process.env.MAX_SILENCE_TRIES || 6);

// Calendar insert retry attempts
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
  const from = process.env.TWILIO_SMS_FROM; // Twilio SMS-capable number
  const client = getTwilioClient();

  if (!client) {
    console.warn("SMS skipped: Missing TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN");
    return;
  }
  if (!from) {
    console.warn("SMS skipped: Missing TWILIO_SMS_FROM");
    return;
  }

  await client.messages.create({
    from,
    to,
    body: messageText
  });
}

async function sendOwnerSms(messageText) {
  return sendSms(ADMIN_SMS_TO, messageText);
}

// --------------------
// Session store
// Key by CallSid (better than From)
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
      // computed after time parse:
      bookedStartISO: "",
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
    timeout: 15,            // give them a bit longer to start talking
    speechTimeout: "auto",  // better for natural pauses
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
  // Allow "yes/no" for confirm step
  if (junk.has(s)) return step !== "job";

  const minConf = step === "job" ? 0.30 : 0.10;

  if (typeof confidence === "number" && confidence > 0 && confidence < minConf) {
    if (speech.split(" ").length <= 2) return true;
  }

  return false;
}

// --------------------
// Time parsing + timezone-safe formatting
// --------------------
function parseRequestedDateTime(naturalText, tz) {
  const ref = DateTime.now().setZone(tz).toJSDate();
  const parsed = chrono.parseDate(naturalText, ref, { forwardDate: true });
  if (!parsed) return null;
  return DateTime.fromJSDate(parsed, { zone: tz }).toJSDate();
}

function toGoogleDateTime(dateObj, tz) {
  // IMPORTANT: include offset (no "Z") so Calendar won’t shift it
  return DateTime.fromJSDate(dateObj, { zone: tz }).toISO({
    includeOffset: true,
    suppressMilliseconds: true
  });
}

function isAfterHoursNow(tz) {
  const now = DateTime.now().setZone(tz);
  const hour = now.hour;
  const isBizDay = BUSINESS_DAYS.includes(now.weekday);
  const isBizHours = hour >= BUSINESS_START_HOUR && hour < BUSINESS_END_HOUR;
  return !(isBizDay && isBizHours);
}

function nextBusinessOpenSlot(tz) {
  // Next business day at BUSINESS_START_HOUR:00
  let dt = DateTime.now().setZone(tz);

  // If still within business hours today, use +10 mins
  const isBizDay = BUSINESS_DAYS.includes(dt.weekday);
  const isBizHours = dt.hour >= BUSINESS_START_HOUR && dt.hour < BUSINESS_END_HOUR;
  if (isBizDay && isBizHours) {
    return dt.plus({ minutes: 10 }).startOf("minute").toJSDate();
  }

  // Otherwise move forward day-by-day to next business day
  dt = dt.plus({ days: 1 }).startOf("day");
  while (!BUSINESS_DAYS.includes(dt.weekday)) {
    dt = dt.plus({ days: 1 }).startOf("day");
  }
  dt = dt.set({ hour: BUSINESS_START_HOUR, minute: 0, second: 0, millisecond: 0 });
  return dt.toJSDate();
}

function looksLikeAsap(text) {
  const t = (text || "").toLowerCase();
  return (
    t.includes("asap") ||
    t.includes("anytime") ||
    t.includes("whenever") ||
    t.includes("don’t care") ||
    t.includes("dont care") ||
    t.includes("no preference") ||
    t.includes("soon as possible") ||
    t === "soon"
  );
}

function formatForVoice(dateObj, tz) {
  return DateTime.fromJSDate(dateObj, { zone: tz }).toFormat("ccc d LLL, h:mm a");
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
      console.error(`Calendar insert failed (attempt ${attempt}/${CAL_RETRY_ATTEMPTS})`, err?.message || err);

      // small backoff
      if (attempt < CAL_RETRY_ATTEMPTS) {
        await sleep(attempt === 1 ? 200 : 800);
      }
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

  console.log(`CALLSID=${callSid} FROM=${fromNumber} STEP=${session.step} Speech="${speech}" Confidence=${confidence}`);

  // Handle no-speech hits (start of call / timeouts)
  if (!speech) {
    session.tries += 1;

    // Missed call capture alert (early)
    if (session.tries === MISSED_CALL_ALERT_TRIES) {
      const txt =
        `MISSED/QUIET CALL ALERT\n` +
        `From: ${session.from || "Unknown"}\n` +
        `Progress: step=${session.step}\n` +
        `Time: ${DateTime.now().setZone(DEFAULT_TZ).toFormat("ccc d LLL yyyy, h:mm a")}`;
      sendOwnerSms(txt).catch((e) => console.error("Owner SMS failed:", e?.message || e));
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

  // We got speech → reset tries
  session.tries = 0;

  // Confirm step is special: accept yes/no even if confidence low
  if (session.step !== "confirm" && shouldReject(session.step, speech, confidence)) {
    const repromptMap = {
      job: "Sorry — what job do you need help with?",
      address: "Sorry — what is the address?",
      name: "Sorry — what is your name?",
      time: "Sorry — what time would you like?"
    };

    const reprompt = repromptMap[session.step] || "Sorry, can you repeat that?";
    session.lastPrompt = reprompt;

    ask(twiml, reprompt, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    // Step machine
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

      const tz = DEFAULT_TZ;

      // After-hours auto booking logic:
      // - If they say ASAP/anytime/etc, or parsing fails after hours -> next business open slot
      let parsedStart = null;
      if (!looksLikeAsap(session.time)) {
        parsedStart = parseRequestedDateTime(session.time, tz);
      }

      if (!parsedStart && isAfterHoursNow(tz)) {
        parsedStart = nextBusinessOpenSlot(tz);
      }

      // If still nothing, fallback to now+10 mins
      const start = parsedStart ? new Date(parsedStart) : new Date(Date.now() + 10 * 60 * 1000);

      session.bookedStartISO = toGoogleDateTime(start, tz);

      const whenForVoice = formatForVoice(start, tz);

      // Booking confirmation read-back (YES/NO)
      session.step = "confirm";
      session.lastPrompt =
        `Alright. I heard: ${session.job}, at ${session.address}, for ${session.name}, on ${whenForVoice}. ` +
        `Is that correct? Say yes to confirm, or no to change the time.`;

      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "confirm") {
      const s = speech.toLowerCase();

      const isYes =
        s.includes("yes") || s.includes("yeah") || s.includes("yep") || s.includes("correct") || s.includes("that is");
      const isNo =
        s.includes("no") || s.includes("nope") || s.includes("wrong") || s.includes("change");

      if (!isYes && !isNo) {
        session.lastPrompt = "Sorry — just say yes to confirm, or no to change the time.";
        ask(twiml, session.lastPrompt, "/voice");
        return res.type("text/xml").send(twiml.toString());
      }

      if (isNo) {
        // Simple: only redo time (fast + robust)
        session.step = "time";
        session.time = "";
        session.bookedStartISO = "";
        session.lastPrompt = "No problem. What time would you like instead?";
        ask(twiml, session.lastPrompt, "/voice");
        return res.type("text/xml").send(twiml.toString());
      }

      // YES -> proceed to booking
      const calendarId = process.env.GOOGLE_CALENDAR_ID;
      if (!calendarId) throw new Error("Missing GOOGLE_CALENDAR_ID env variable");

      const calendar = getCalendarClient();
      const tz = DEFAULT_TZ;

      const start = DateTime.fromISO(session.bookedStartISO, { setZone: true }).toJSDate();
      const end = new Date(start.getTime() + 60 * 60 * 1000);

      const summaryText = `${session.name} needs ${session.job} at ${session.address}.`;
      const displayWhen = DateTime.fromJSDate(start, { zone: tz }).toFormat("ccc d LLL yyyy, h:mm a");

      // Create event (with retry)
      try {
        await insertCalendarEventWithRetry(calendar, calendarId, {
          summary: `${session.job} - ${session.name}`,
          description: `${summaryText}\nCaller: ${session.from || "Unknown"}\nSpoken time: ${session.time}`,
          location: session.address,
          start: {
            dateTime: toGoogleDateTime(start, tz),
            timeZone: tz
          },
          end: {
            dateTime: toGoogleDateTime(end, tz),
            timeZone: tz
          }
        });
      } catch (calErr) {
        // Owner alert if calendar failed even after retries
        const failTxt =
          `BOOKING FAILED (Calendar)\n` +
          `Name: ${session.name}\n` +
          `Job: ${session.job}\n` +
          `Address: ${session.address}\n` +
          `Spoken time: ${session.time}\n` +
          `Caller: ${session.from || "Unknown"}\n` +
          `Reason: ${calErr?.message || calErr}`;
        await sendOwnerSms(failTxt);

        twiml.say("Sorry, booking failed on our side. We'll call you back shortly.", {
          voice: "Polly.Amy",
          language: "en-AU"
        });
        twiml.hangup();
        resetSession(callSid);
        return res.type("text/xml").send(twiml.toString());
      }

      // Owner SMS on success
      const smsText =
        `NEW BOOKING ✅\n` +
        `Name: ${session.name}\n` +
        `Job: ${session.job}\n` +
        `Address: ${session.address}\n` +
        `Caller: ${session.from || "Unknown"}\n` +
        `Spoken: ${session.time}\n` +
        `Booked: ${displayWhen} (${tz})`;

      await sendOwnerSms(smsText);

      twiml.say(`Booked. Thanks ${session.name}. See you ${formatForVoice(start, tz)}.`, {
        voice: "Polly.Amy",
        language: "en-AU"
      });

      resetSession(callSid);
      twiml.hangup();
      return res.type("text/xml").send(twiml.toString());
    }

    // Fallback
    session.step = "job";
    session.lastPrompt = "What job do you need help with?";
    ask(twiml, session.lastPrompt, "/voice");
    return res.type("text/xml").send(twiml.toString());
  } catch (err) {
    console.error("VOICE ERROR:", err);

    // Also alert owner
    const errTxt =
      `SYSTEM ERROR\n` +
      `From: ${session.from || "Unknown"}\n` +
      `Step: ${session.step}\n` +
      `Error: ${err?.message || err}`;
    sendOwnerSms(errTxt).catch(() => {});

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
