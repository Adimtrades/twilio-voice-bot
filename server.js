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
// SMS settings
// --------------------
const ADMIN_SMS_TO = "+61431778238"; // your number (AU 0431... => +61431...)

// Build Twilio REST client ONLY if env vars exist (so it won’t crash)
function getTwilioClient() {
  const sid = process.env.TWILIO_ACCOUNT_SID;
  const token = process.env.TWILIO_AUTH_TOKEN;
  if (!sid || !token) return null;
  return twilio(sid, token);
}

async function sendBookingSms(messageText) {
  const from = process.env.TWILIO_SMS_FROM; // must be a Twilio number that can send SMS
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
    to: ADMIN_SMS_TO,
    body: messageText
  });
}

// --------------------
// Session store
// Key by CallSid (better than From)
// --------------------
const sessions = new Map();

function getSession(callSid) {
  if (!sessions.has(callSid)) {
    sessions.set(callSid, {
      step: "job",
      job: "",
      address: "",
      name: "",
      time: "",
      lastPrompt: "",
      tries: 0
    });
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
    timeout: 12,
    speechTimeout: 2,
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

  const junk = new Set(["hello", "hi", "yeah", "yes", "yep", "nope", "okay", "ok"]);
  if (junk.has(s)) {
    return step !== "job" || speech.length < 5;
  }

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
  return DateTime.fromJSDate(dateObj, { zone: tz }).toISO({
    includeOffset: true,
    suppressMilliseconds: true
  });
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

// --------------------
// Voice route
// --------------------
app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  const callSid = req.body.CallSid || req.body.CallSID || "unknown";
  const confidence = Number(req.body.Confidence || 0);
  const speechRaw = req.body.SpeechResult || "";
  const speech = cleanSpeech(speechRaw);

  const session = getSession(callSid);

  console.log(`CALLSID=${callSid} STEP=${session.step} Speech="${speech}" Confidence=${confidence}`);

  // Handle no-speech hits
  if (!speech) {
    session.tries += 1;

    if (session.tries >= 6) {
      twiml.say("No worries. Please call back when ready.", {
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
      time: "What time would you like?"
    };

    const prompt = session.lastPrompt || promptMap[session.step] || "Can you repeat that?";
    session.lastPrompt = prompt;

    ask(twiml, prompt, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  session.tries = 0;

  if (shouldReject(session.step, speech, confidence)) {
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

      const summary = `${session.name} needs ${session.job} at ${session.address} at ${session.time}.`;

      twiml.say(`Booking confirmed. ${summary}`, {
        voice: "Polly.Amy",
        language: "en-AU"
      });

      const calendarId = process.env.GOOGLE_CALENDAR_ID;
      if (!calendarId) throw new Error("Missing GOOGLE_CALENDAR_ID env variable");

      const calendar = getCalendarClient();
      const tz = process.env.TIMEZONE || "Australia/Sydney";

      const parsedStart = parseRequestedDateTime(session.time, tz);

      const start = parsedStart ? new Date(parsedStart) : new Date(Date.now() + 10 * 60 * 1000);
      const end = new Date(start.getTime() + 60 * 60 * 1000);

      // Create calendar event
      await calendar.events.insert({
        calendarId,
        requestBody: {
          summary: `${session.job} - ${session.name}`,
          description: summary,
          location: session.address,
          start: {
            dateTime: toGoogleDateTime(start, tz),
            timeZone: tz
          },
          end: {
            dateTime: toGoogleDateTime(end, tz),
            timeZone: tz
          }
        }
      });

      // Send SMS to you
      const smsText =
        `NEW BOOKING\n` +
        `Name: ${session.name}\n` +
        `Job: ${session.job}\n` +
        `Address: ${session.address}\n` +
        `Time spoken: ${session.time}\n` +
        `Booked: ${DateTime.fromJSDate(start, { zone: tz }).toFormat("ccc d LLL yyyy, h:mm a")} (${tz})`;

      await sendBookingSms(smsText);

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
