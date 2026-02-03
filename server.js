// server.js
const express = require("express");
const { google } = require("googleapis");
const VoiceResponse = require("twilio").twiml.VoiceResponse;

// Node 18+ has fetch built in. If not, upgrade node.
const app = express();

// Twilio sends x-www-form-urlencoded
app.use(express.urlencoded({ extended: false }));

// -------------------------
// Simple in-memory memory (OK for testing, use DB later)
// -------------------------
const memory = new Map(); // key: caller number, value: { history: [] }

// -------------------------
// Env
// -------------------------
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";

const GCAL_SERVICE_ACCOUNT_JSON = process.env.GCAL_SERVICE_ACCOUNT_JSON;
const GCAL_CALENDAR_ID = process.env.GCAL_CALENDAR_ID;
const TZ = process.env.TZ || "Australia/Sydney";

const PORT = process.env.PORT || 3000;

// -------------------------
// Google Calendar Client
// -------------------------
function getCalendarClient() {
  if (!GCAL_SERVICE_ACCOUNT_JSON) {
    throw new Error("Missing GCAL_SERVICE_ACCOUNT_JSON");
  }
  const creds = JSON.parse(GCAL_SERVICE_ACCOUNT_JSON);

  const auth = new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    ["https://www.googleapis.com/auth/calendar"]
  );

  return google.calendar({ version: "v3", auth });
}

// -------------------------
// Calendar booking helper (used by /test-booking now, and later by phone flow)
// -------------------------
async function createBooking({ title, description, location, start, end }) {
  if (!GCAL_CALENDAR_ID) throw new Error("Missing GCAL_CALENDAR_ID");

  const cal = getCalendarClient();

  const event = await cal.events.insert({
    calendarId: GCAL_CALENDAR_ID,
    requestBody: {
      summary: title,
      description,
      location,
      start: { dateTime: start.toISOString(), timeZone: TZ },
      end: { dateTime: end.toISOString(), timeZone: TZ }
    }
  });

  return event.data;
}

// -------------------------
// OpenAI helper (short receptionist replies)
// -------------------------
async function askAI(from, userText) {
  if (!OPENAI_API_KEY) return "System is not configured. Please try again later.";

  const state = memory.get(from) || { history: [] };

  const messages = [
    {
      role: "system",
      content:
        "You are a phone receptionist for a tradie/handyman. Speak naturally. Keep replies under 12 words. Ask ONE question at a time. Goal: collect name, job type, suburb/address, urgency, and preferred time window. If user already gave a detail, do not ask again."
    },
    ...state.history,
    { role: "user", content: userText || "Caller said nothing." }
  ];

  try {
    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: OPENAI_MODEL,
        messages,
        temperature: 0.3,
        max_tokens: 60
      })
    });

    if (!resp.ok) {
      const txt = await resp.text();
      console.log("OpenAI error:", resp.status, txt);
      // fallback response so call still flows
      return "Sorry—what suburb are you in?";
    }

    const data = await resp.json();
    const reply = data?.choices?.[0]?.message?.content?.trim() || "Sorry—can you repeat that?";

    // store short memory
    state.history.push({ role: "user", content: userText });
    state.history.push({ role: "assistant", content: reply });
    // keep last 8 turns max
    state.history = state.history.slice(-16);

    memory.set(from, state);

    return reply;
  } catch (e) {
    console.log("askAI exception:", e);
    return "Sorry—can you repeat that?";
  }
}

// -------------------------
// HEALTH CHECK
// -------------------------
app.get("/", (req, res) => {
  res.send("OK: twilio-voice-bot running");
});

// -------------------------
// TEST BOOKING ENDPOINT (browser test)
// -------------------------
app.get("/test-booking", async (req, res) => {
  try {
    const start = new Date(Date.now() + 60 * 60 * 1000); // 1 hour from now
    const end = new Date(start);
    end.setMinutes(end.getMinutes() + 60);

    const ev = await createBooking({
      title: "Test Booking (AI Bot)",
      description: "Created from /test-booking",
      location: "Newcastle NSW",
      start,
      end
    });

    res.json({ ok: true, eventId: ev.id, link: ev.htmlLink });
  } catch (e) {
    console.log("test-booking error:", e);
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// -------------------------
// TWILIO VOICE WEBHOOK
// -------------------------
app.post("/voice", async (req, res) => {
  try {
    const twiml = new VoiceResponse();

    // caller number (E.164 format)
    const from = req.body.From || "unknown";
    const speech = (req.body.SpeechResult || "").trim();
    const confidence = req.body.Confidence;

    if (speech) {
      console.log(`From: ${from} SpeechResult: ${speech} Confidence: ${confidence}`);
    } else {
      console.log(`From: ${from} (no speech captured)`);
    }

    // If no speech, prompt again (improves success)
    if (!speech) {
      twiml.say("Sorry, I didn't catch that.");
      twiml.gather({
        input: "speech",
        action: "/voice",
        method: "POST",
        timeout: 5,
        speechTimeout: "auto",
        language: "en-AU",
        hints: "leaking sink, blocked drain, plumber, electrician, quote, today, tomorrow, Newcastle, Maitland, Lake Macquarie",
      }).say("What do you need help with?");
      res.type("text/xml").send(twiml.toString());
      return;
    }

    // Ask AI for next question
    const aiReply = await askAI(from, speech);

    // Speak + gather again (loop)
    const gather = twiml.gather({
      input: "speech",
      action: "/voice",
      method: "POST",
      timeout: 6,
      speechTimeout: "auto",
      language: "en-AU",
      hints: "name, address, suburb, urgency, today, tomorrow, morning, afternoon, Newcastle, Maitland, Lake Macquarie",
    });

    gather.say(aiReply);

    // If caller stays silent, Twilio will continue here
    twiml.say("Sorry—call back anytime. Goodbye.");
    twiml.hangup();

    res.type("text/xml").send(twiml.toString());
  } catch (e) {
    console.log("voice webhook error:", e);
    const twiml = new VoiceResponse();
    twiml.say("Sorry, something went wrong. Please call back.");
    twiml.hangup();
    res.type("text/xml").send(twiml.toString());
  }
});

// -------------------------
// START
// -------------------------
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
