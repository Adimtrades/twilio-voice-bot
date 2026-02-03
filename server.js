// server.js
const express = require("express");
const twilio = require("twilio");
const { google } = require("googleapis");

// Node 18+ has fetch built-in
const app = express();

// Twilio sends application/x-www-form-urlencoded
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// -------------------- ENV --------------------
const {
  OPENAI_API_KEY,
  GCAL_SERVICE_ACCOUNT_JSON,
  GCAL_CALENDAR_ID,
  TZ = "Australia/Sydney",
  BASE_URL // optional: set to your render URL; if empty we infer
} = process.env;

function requireEnv(name) {
  if (!process.env[name] || !process.env[name].trim()) {
    throw new Error(`Missing env var: ${name}`);
  }
}

// -------------------- In-memory sessions --------------------
// Keyed by CallSid so each caller has their own flow
const sessions = new Map();

function getSession(callSid) {
  if (!sessions.has(callSid)) {
    sessions.set(callSid, {
      // collected fields
      name: null,
      jobType: null,
      suburb: null,
      address: null,
      urgency: null, // "URGENT" / "Standard"
      preferredTime: null, // free-text
      durationMins: 60,
      budget: null, // optional
      notes: [],
      // flow
      step: "jobType"
    });
  }
  return sessions.get(callSid);
}

function setNote(sess, text) {
  if (!text) return;
  const clean = String(text).trim();
  if (clean) sess.notes.push(clean);
}

function normalize(text) {
  return (text || "").toString().trim();
}

// -------------------- OpenAI helper --------------------
async function askAI({ system, user }) {
  requireEnv("OPENAI_API_KEY");

  const resp = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      temperature: 0.3,
      max_tokens: 80,
      messages: [
        { role: "system", content: system },
        { role: "user", content: user }
      ]
    })
  });

  const data = await resp.json();
  if (!resp.ok) {
    throw new Error(`OpenAI error: ${resp.status} ${JSON.stringify(data)}`);
  }
  return data.choices?.[0]?.message?.content?.trim() || "";
}

// -------------------- Google Calendar client --------------------
function getCalendarClient() {
  requireEnv("GCAL_SERVICE_ACCOUNT_JSON");
  requireEnv("GCAL_CALENDAR_ID");

  // Clean common hidden chars / BOM and parse JSON
  const cleaned = GCAL_SERVICE_ACCOUNT_JSON.trim().replace(/^\uFEFF/, "");
  const creds = JSON.parse(cleaned);

  const jwt = new google.auth.JWT({
    email: creds.client_email,
    key: creds.private_key,
    scopes: ["https://www.googleapis.com/auth/calendar"]
  });

  return google.calendar({ version: "v3", auth: jwt });
}

function mapsLink(address, suburb) {
  const q = encodeURIComponent([address, suburb].filter(Boolean).join(", "));
  return `https://www.google.com/maps/search/?api=1&query=${q}`;
}

// Pick a simple default time if caller didn’t specify properly.
// (Pro version later = parse natural language properly + check free slots)
function defaultStartEndISO(durationMins = 60) {
  // Next day 9:00am local time (simple, reliable)
  const now = new Date();
  const d = new Date(now.getTime());
  d.setDate(d.getDate() + 1);
  d.setHours(9, 0, 0, 0);

  const startISO = d.toISOString();
  const end = new Date(d.getTime() + durationMins * 60 * 1000);
  const endISO = end.toISOString();

  return { startISO, endISO };
}

async function createCalendarEvent(sess, callerPhone) {
  const cal = getCalendarClient();

  const urgencyTag = (sess.urgency || "Standard").toUpperCase();
  const job = sess.jobType || "Service";
  const suburb = sess.suburb || "Unknown suburb";
  const name = sess.name || "Customer";

  // Title format: JOB – SUBURB – NAME (and urgency prefix if urgent)
  const summary =
    urgencyTag === "URGENT"
      ? `🚨 ${job} – ${suburb} – ${name}`
      : `${job} – ${suburb} – ${name}`;

  const fullAddress = [sess.address, sess.suburb].filter(Boolean).join(", ");
  const gmaps = mapsLink(sess.address, sess.suburb);

  const notesText = sess.notes.length ? `\nNotes:\n- ${sess.notes.join("\n- ")}` : "";

  const budgetLine = sess.budget ? `\nBudget/Quote: ${sess.budget}` : "";
  const prefLine = sess.preferredTime ? `\nPreferred time: ${sess.preferredTime}` : "";
  const durLine = `\nEstimated duration: ${sess.durationMins} mins`;

  // Simple time handling (default). You can later parse preferredTime properly.
  const { startISO, endISO } = defaultStartEndISO(sess.durationMins);

  const description =
`Caller: ${callerPhone || "Unknown"}
Name: ${name}
Job: ${job}
Urgency: ${urgencyTag}
Suburb: ${suburb}
Address: ${sess.address || "Not provided"}${budgetLine}${prefLine}${durLine}

Google Maps: ${gmaps}${notesText}
`;

  const event = {
    summary,
    description,
    start: { dateTime: startISO, timeZone: TZ },
    end: { dateTime: endISO, timeZone: TZ }
  };

  const res = await cal.events.insert({
    calendarId: GCAL_CALENDAR_ID,
    requestBody: event
  });

  return {
    eventId: res.data.id,
    htmlLink: res.data.htmlLink
  };
}

// -------------------- Voice UX helpers --------------------
function twimlSayGather({ prompt, actionUrl }) {
  const vr = new twilio.twiml.VoiceResponse();

  const gather = vr.gather({
    input: "speech",
    action: actionUrl,
    method: "POST",
    language: "en-AU",
    speechTimeout: "auto",
    timeout: 6,
    hints: "plumber, electrician, leak, blocked drain, hot water, repair, quote, Newcastle, tomorrow, urgent",
    profanityFilter: false
  });

  // Slightly slower + clear
  gather.say({ voice: "Polly.Olivia", language: "en-AU" }, prompt);

  // If no speech captured, loop once with a clearer prompt
  vr.redirect({ method: "POST" }, "/voice");

  return vr;
}

function baseUrl(req) {
  if (BASE_URL && BASE_URL.startsWith("http")) return BASE_URL;
  return `${req.protocol}://${req.get("host")}`;
}

function nextQuestion(sess) {
  // Ask ONE question at a time
  if (!sess.jobType) return { step: "jobType", q: "What job do you need help with? Plumber, electrician, or handyman?" };
  if (!sess.suburb) return { step: "suburb", q: "What suburb are you in?" };
  if (!sess.address) return { step: "address", q: "What’s the street address? You can say it slowly." };
  if (!sess.urgency) return { step: "urgency", q: "Is it urgent today, or standard?" };
  if (!sess.preferredTime) return { step: "preferredTime", q: "What time suits you? For example, tomorrow morning." };
  if (!sess.name) return { step: "name", q: "What’s your name?" };
  // optional upgrades
  if (!sess.budget) return { step: "budget", q: "Do you have a budget or want a quote? Say a number, or say skip." };
  // done
  return { step: "done", q: null };
}

// Very simple field assignment based on current step
function applyAnswer(sess, step, text) {
  const t = normalize(text);

  if (!t) return;

  // allow "skip" for optional budget
  const isSkip = /^(skip|no|nope|nah)$/i.test(t);

  switch (step) {
    case "jobType":
      sess.jobType = t;
      break;
    case "suburb":
      sess.suburb = t;
      break;
    case "address":
      sess.address = t;
      break;
    case "urgency":
      sess.urgency = /urgent|asap|today|now/i.test(t) ? "URGENT" : "Standard";
      break;
    case "preferredTime":
      sess.preferredTime = t;
      break;
    case "name":
      sess.name = t;
      break;
    case "budget":
      if (!isSkip) sess.budget = t;
      else sess.budget = "Skipped";
      break;
    default:
      setNote(sess, t);
  }
}

// -------------------- Routes --------------------
app.get("/health", (req, res) => res.json({ ok: true }));

// Test endpoint to verify calendar works
app.get("/test-booking", async (req, res) => {
  try {
    // dummy session
    const sess = {
      name: "Test Customer",
      jobType: "Handyman",
      suburb: "Newcastle",
      address: "123 Hunter St",
      urgency: "Standard",
      preferredTime: "Tomorrow morning",
      durationMins: 60,
      budget: "$200",
      notes: ["Auto test booking"]
    };

    const out = await createCalendarEvent(sess, "+61400000000");
    res.json({ ok: true, eventId: out.eventId, link: out.htmlLink });
  } catch (e) {
    res.json({ ok: false, error: String(e) });
  }
});

// Main Twilio webhook
app.post("/voice", async (req, res) => {
  try {
    const callSid = req.body.CallSid || "unknown";
    const from = req.body.From || "unknown";
    const speech = normalize(req.body.SpeechResult);

    const sess = getSession(callSid);

    // If we received speech, apply it to the previous step
    if (speech) {
      applyAnswer(sess, sess.step, speech);
      setNote(sess, `Caller said: ${speech}`);
    }

    // Determine next needed question
    const nxt = nextQuestion(sess);
    sess.step = nxt.step;

    // If finished, create booking + confirm
    if (nxt.step === "done") {
      const vr = new twilio.twiml.VoiceResponse();

      // create calendar event
      const { eventId, htmlLink } = await createCalendarEvent(sess, from);

      // confirm in voice
      vr.say({ voice: "Polly.Olivia", language: "en-AU" },
        `Done. You’re booked. We’ll call you to confirm. Goodbye.`
      );

      // OPTIONAL: you can SMS the link later if you add Twilio SMS
      // Clean up session
      sessions.delete(callSid);

      // Twilio needs XML
      res.type("text/xml").send(vr.toString());
      return;
    }

    // Otherwise ask the next question
    const actionUrl = `${baseUrl(req)}/voice`;

    // If speech was empty, give a clearer reprompt
    const prompt = speech
      ? nxt.q
      : "Sorry, I didn’t catch that. Please repeat clearly. " + nxt.q;

    const vr = twimlSayGather({ prompt, actionUrl });

    res.type("text/xml").send(vr.toString());
  } catch (e) {
    const vr = new twilio.twiml.VoiceResponse();
    vr.say("Sorry, something went wrong. Please try again later.");
    res.type("text/xml").send(vr.toString());
  }
});

// -------------------- Start --------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on ${PORT}`));
