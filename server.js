// server.js (FULL REWRITE)

const express = require("express");
const { google } = require("googleapis");
const OpenAI = require("openai");
const twilio = require("twilio");

const VoiceResponse = twilio.twiml.VoiceResponse;

const app = express();

// Twilio sends x-www-form-urlencoded
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// ------------------------
// ENV VARS (Render)
// ------------------------
const PORT = process.env.PORT || 10000;

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";

// Put the full service account JSON into this env var (Render supports multi-line)
const GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
// Your calendar ID (often looks like: something@group.calendar.google.com)
const CALENDAR_ID = process.env.CALENDAR_ID || "primary";

// Optional: business name / timezone
const BUSINESS_NAME = process.env.BUSINESS_NAME || "Trades Reception";
const DEFAULT_TIMEZONE = process.env.DEFAULT_TIMEZONE || "Australia/Sydney";

// ------------------------
// OpenAI client
// ------------------------
const openai = OPENAI_API_KEY
  ? new OpenAI({ apiKey: OPENAI_API_KEY })
  : null;

// ------------------------
// In-memory session store (per caller)
// NOTE: good for testing; use DB later for production
// ------------------------
const sessions = new Map();

function getSessionKey(req) {
  // Twilio sends "From" like +614xxxxxxxx
  return (req.body.From || req.query.from || "unknown").trim();
}

function getOrCreateSession(key) {
  if (!sessions.has(key)) {
    sessions.set(key, {
      history: [],
      booking: {
        name: null,
        job: null,
        suburb: null,
        address: null,
        urgency: null,
        preferredTime: null
      },
      noSpeechCount: 0
    });
  }
  return sessions.get(key);
}

// ------------------------
// Utilities
// ------------------------
function safeText(s) {
  if (!s) return "Sorry, I didn’t catch that.";
  // Twilio can throw “Invalid text” if empty or weird objects
  return String(s).replace(/\s+/g, " ").trim().slice(0, 800);
}

function isBookingComplete(b) {
  // You can relax these requirements if you want faster bookings
  return Boolean(
    b.job &&
    (b.suburb || b.address) &&
    (b.urgency || b.preferredTime)
  );
}

// ------------------------
// Google Calendar client
// ------------------------
function getCalendarClient() {
  if (!GOOGLE_SERVICE_ACCOUNT_JSON) {
    throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON env var.");
  }

  let creds;
  try {
    creds = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
  } catch (e) {
    throw new Error(
      "GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON. Paste the full JSON content exactly."
    );
  }

  const auth = new google.auth.JWT({
    email: creds.client_email,
    key: creds.private_key,
    scopes: ["https://www.googleapis.com/auth/calendar"]
  });

  return google.calendar({ version: "v3", auth });
}

async function createCalendarEvent({ summary, description, startISO, endISO }) {
  const calendar = getCalendarClient();

  const event = await calendar.events.insert({
    calendarId: CALENDAR_ID,
    requestBody: {
      summary,
      description,
      start: { dateTime: startISO, timeZone: DEFAULT_TIMEZONE },
      end: { dateTime: endISO, timeZone: DEFAULT_TIMEZONE }
    }
  });

  return {
    eventId: event.data.id,
    link: event.data.htmlLink
  };
}

// ------------------------
// OpenAI logic: ask 1 question at a time + update booking fields
// ------------------------
async function callOpenAIAndUpdateSession(session, userText) {
  if (!openai) {
    return {
      reply: "System not configured. OpenAI key missing.",
      sessionUpdated: false
    };
  }

  const system = `
You are a phone receptionist for a trades business in Australia.
You MUST be fast, natural, and ask ONLY ONE short question at a time (max ~12 words).
Goal: collect details to book a job:
- job (what needs doing)
- suburb OR address
- urgency OR preferred time window
Optional: name

Return STRICT JSON only, no extra text, in this exact shape:
{
  "reply": "question or short confirmation",
  "updates": {
    "name": null or string,
    "job": null or string,
    "suburb": null or string,
    "address": null or string,
    "urgency": null or string,
    "preferredTime": null or string
  }
}

Rules:
- If caller gave info, fill it.
- If missing something, ask for the MOST important missing field next.
- Keep reply short and clear.
- If you think booking info is complete, reply should confirm booking created.
`;

  const messages = [
    { role: "system", content: system },
    ...session.history.slice(-10), // keep it short
    { role: "user", content: userText || "" }
  ];

  const resp = await openai.chat.completions.create({
    model: OPENAI_MODEL,
    messages,
    temperature: 0.3
  });

  const raw = resp.choices?.[0]?.message?.content || "{}";

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    // fallback if model outputs weird text
    return {
      reply: "Quickly—what suburb are you in?",
      sessionUpdated: false
    };
  }

  const updates = parsed.updates || {};
  session.booking.name = updates.name ?? session.booking.name;
  session.booking.job = updates.job ?? session.booking.job;
  session.booking.suburb = updates.suburb ?? session.booking.suburb;
  session.booking.address = updates.address ?? session.booking.address;
  session.booking.urgency = updates.urgency ?? session.booking.urgency;
  session.booking.preferredTime =
    updates.preferredTime ?? session.booking.preferredTime;

  // store conversation history (short)
  session.history.push({ role: "user", content: userText });
  session.history.push({ role: "assistant", content: parsed.reply || "" });
  session.history = session.history.slice(-20);

  return {
    reply: parsed.reply || "What suburb are you in?",
    sessionUpdated: true
  };
}

// ------------------------
// Twilio Gather builder
// ------------------------
function gatherSpeech(twiml, prompt, actionPath = "/voice") {
  // speechTimeout="auto" helps not cutting off early
  // timeout="6" gives caller a moment to speak
  const gather = twiml.gather({
    input: "speech",
    action: actionPath,
    method: "POST",
    speechTimeout: "auto",
    timeout: 6,
    enhanced: true
  });

  gather.say(safeText(prompt), { voice: "alice" });

  // If no speech captured, Twilio will hit action anyway with no SpeechResult.
  // We also add a short fallback.
  twiml.pause({ length: 1 });
}

// ------------------------
// Routes
// ------------------------
app.get("/health", (req, res) => {
  res.json({ ok: true });
});

/**
 * MAIN VOICE WEBHOOK
 * Set Twilio "A call comes in" to:
 *   https://YOUR-RENDER-URL/voice   (HTTP POST)
 */
app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  const from = getSessionKey(req);
  const session = getOrCreateSession(from);

  const speech = (req.body.SpeechResult || "").trim();
  const confidence = Number(req.body.Confidence || 0);

  // 1) First hit / new call (no speech yet)
  if (!speech) {
    session.noSpeechCount += 1;

    if (session.noSpeechCount === 1) {
      gatherSpeech(
        twiml,
        `Hi, this is ${BUSINESS_NAME}. What do you need help with?`
      );
      return res.type("text/xml").send(twiml.toString());
    }

    // 2) If caller stays silent, don’t spam repeated full questions
    // Keep it calm and short
    if (session.noSpeechCount === 2) {
      gatherSpeech(twiml, "Sorry—say that again, please.");
      return res.type("text/xml").send(twiml.toString());
    }

    // 3) After a few no-speech events, give a simple instruction + option
    gatherSpeech(
      twiml,
      "I can book it. Say the job and your suburb.",
      "/voice"
    );
    return res.type("text/xml").send(twiml.toString());
  }

  // Reset no-speech counter once we have speech
  session.noSpeechCount = 0;

  // Basic log (shows in Render logs)
  console.log(`From: ${from} SpeechResult: ${speech} Confidence: ${confidence}`);

  // 2) Call OpenAI to produce next question + update booking fields
  let aiReply = "What suburb are you in?";
  try {
    const ai = await callOpenAIAndUpdateSession(session, speech);
    aiReply = ai.reply;
  } catch (err) {
    console.error("OpenAI error:", err);
    aiReply = "Sorry—what suburb are you in?";
  }

  // 3) If booking is complete, create calendar event
  if (isBookingComplete(session.booking)) {
    try {
      // super simple time handling:
      // Default: create a placeholder event starting now+10min lasting 30min
      const start = new Date(Date.now() + 10 * 60 * 1000);
      const end = new Date(start.getTime() + 30 * 60 * 1000);

      const b = session.booking;

      const summary = b.job
        ? `New job: ${safeText(b.job).slice(0, 60)}`
        : "New booking";

      const description = [
        `Caller: ${from}`,
        b.name ? `Name: ${b.name}` : null,
        b.job ? `Job: ${b.job}` : null,
        b.suburb ? `Suburb: ${b.suburb}` : null,
        b.address ? `Address: ${b.address}` : null,
        b.urgency ? `Urgency: ${b.urgency}` : null,
        b.preferredTime ? `Preferred time: ${b.preferredTime}` : null
      ]
        .filter(Boolean)
        .join("\n");

      const created = await createCalendarEvent({
        summary,
        description,
        startISO: start.toISOString(),
        endISO: end.toISOString()
      });

      twiml.say(
        safeText("Booked. We’ll confirm the time by text. Thanks!"),
        { voice: "alice" }
      );

      // Optional: say event was created (don’t read the link aloud)
      console.log("Calendar event created:", created);

      twiml.hangup();
      return res.type("text/xml").send(twiml.toString());
    } catch (err) {
      console.error("Calendar error:", err);
      // If calendar fails, continue conversation instead of crashing
    }
  }

  // 4) Continue: ask the next single question
  gatherSpeech(twiml, aiReply, "/voice");
  return res.type("text/xml").send(twiml.toString());
});

/**
 * TEST: creates a calendar event to confirm Google Calendar works
 * Visit:
 *   https://YOUR-RENDER-URL/test-booking
 */
app.get("/test-booking", async (req, res) => {
  try {
    const start = new Date(Date.now() + 10 * 60 * 1000);
    const end = new Date(start.getTime() + 30 * 60 * 1000);

    const created = await createCalendarEvent({
      summary: "Test booking",
      description: "Render test event",
      startISO: start.toISOString(),
      endISO: end.toISOString()
    });

    res.json({ ok: true, ...created });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

// ------------------------
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
