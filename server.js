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

// ✅ NEW: natural-language time parsing ("tomorrow 5pm", "Thursday 11am", etc.)
const chrono = require("chrono-node");

const app = express();

// Twilio sends application/x-www-form-urlencoded
app.use(express.urlencoded({ extended: false }));

const VoiceResponse = twilio.twiml.VoiceResponse;

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
      suburb: "",
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
  return String(text)
    .trim()
    .replace(/\s+/g, " ");
}

function ask(twiml, prompt, actionUrl) {
  const gather = twiml.gather({
    input: "speech",
    action: actionUrl,
    method: "POST",
    timeout: 5,
    speechTimeout: "auto",
    language: "en-AU"
  });

  // Never pass empty text to say (prevents Twilio 13520)
  gather.say(prompt || "Sorry, can you repeat that?", {
    voice: "Polly.Amy",
    language: "en-AU"
  });
}

// Accept speech with step-specific tolerance.
// Suburb often comes back low confidence, so we allow it more.
function shouldReject(step, speech, confidence) {
  const s = speech.toLowerCase();

  // If they said literally nothing / noise
  if (!speech || speech.length < 2) return true;

  // Common garbage from STT
  if (s === "hello" || s === "hi" || s === "yeah" || s === "yes") {
    // allow "hello" only for first job step if they haven’t said job yet
    return step !== "job";
  }

  // Confidence tuning:
  // job: needs to be reasonable
  // suburb/name/time: allow lower confidence (Twilio often low here)
  const minConf = step === "job" ? 0.35 : 0.15;

  // If confidence is provided and very low AND the utterance is tiny, reject.
  if (typeof confidence === "number" && confidence > 0 && confidence < minConf) {
    if (speech.split(" ").length <= 2) return true;
  }

  return false;
}

// ✅ NEW: parse spoken times into a real Date (chrono-node)
function parseSpokenDateTime(text) {
  const cleaned = cleanSpeech(text);
  if (!cleaned) return null;

  // forwardDate: true pushes ambiguous refs to the future (e.g., "Monday" means next Monday)
  const dt = chrono.parseDate(cleaned, new Date(), { forwardDate: true });
  return dt || null;
}

function buildEventTimesFromSpeech(speechTime, durationMins = 60) {
  const start = parseSpokenDateTime(speechTime);
  if (!start) return null;

  const end = new Date(start.getTime() + durationMins * 60 * 1000);
  return { start, end };
}

// --------------------
// Google Calendar client
// --------------------
function parseGoogleServiceJson() {
  const raw = process.env.GOOGLE_SERVICE_JSON;
  if (!raw) throw new Error("Missing GOOGLE_SERVICE_JSON env variable");

  // People often paste either:
  // 1) real JSON object text: { "type": "...", ... }
  // 2) a JSON-stringified version (starts/ends with quotes) containing \n
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    // If they pasted raw JSON but with newlines mangled, still try:
    parsed = JSON.parse(raw.replace(/\r?\n/g, "\\n"));
  }

  // If it parsed into a STRING, parse again to get object
  if (typeof parsed === "string") {
    parsed = JSON.parse(parsed);
  }

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

  console.log(
    `CALLSID=${callSid} STEP=${session.step} Speech="${speech}" Confidence=${confidence}`
  );

  // If Twilio hits the webhook with no speech (start of call or timeout),
  // repeat the last prompt or ask the current step question.
  if (!speech) {
    session.tries += 1;

    // If they keep not responding, bail gracefully.
    if (session.tries >= 3) {
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
      suburb: "What suburb are you in?",
      name: "What is your name?",
      time: "What time would you like? For example, tomorrow at 5 p.m."
    };

    const prompt =
      session.lastPrompt || promptMap[session.step] || "Can you repeat that?";
    session.lastPrompt = prompt;

    ask(twiml, prompt, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  // We got speech → reset tries
  session.tries = 0;

  // Reject bad recognition for the current step
  if (shouldReject(session.step, speech, confidence)) {
    const repromptMap = {
      job: "Sorry — what job do you need help with?",
      suburb: "Sorry — what suburb are you in?",
      name: "Sorry — what is your name?",
      time: "Sorry — what time would you like? For example, tomorrow at 5 p.m."
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
      session.step = "suburb";
      session.lastPrompt = "What suburb are you in?";
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "suburb") {
      session.suburb = speech;
      session.step = "name";
      session.lastPrompt = "What is your name?";
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "name") {
      session.name = speech;
      session.step = "time";
      session.lastPrompt = "What time would you like? For example, tomorrow at 5 p.m.";
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "time") {
      session.time = speech;

      const summary = `${session.name} needs ${session.job} in ${session.suburb} at ${session.time}.`;

      // ✅ Parse time BEFORE confirming (so we can reprompt if parse fails)
      const times = buildEventTimesFromSpeech(session.time, 60);
      if (!times) {
        session.step = "time";
        session.lastPrompt =
          "Sorry — what time would you like? Please say something like: tomorrow at 5 p.m.";
        ask(twiml, session.lastPrompt, "/voice");
        return res.type("text/xml").send(twiml.toString());
      }

      twiml.say(`Booking confirmed. ${summary}`, {
        voice: "Polly.Amy",
        language: "en-AU"
      });

      // Create calendar event (safe)
      const calendarId = process.env.GOOGLE_CALENDAR_ID;
      if (!calendarId) {
        throw new Error("Missing GOOGLE_CALENDAR_ID env variable");
      }

      const calendar = getCalendarClient();

      const { start, end } = times;
      const timeZone = process.env.TIMEZONE || "Australia/Sydney";

      await calendar.events.insert({
        calendarId,
        requestBody: {
          summary: `${session.job} - ${session.name}`,
          description: summary,
          start: {
            dateTime: start.toISOString(),
            timeZone
          },
          end: {
            dateTime: end.toISOString(),
            timeZone
          }
        }
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

    twiml.say("Sorry, there was a system error. Please try again.", {
      voice: "Polly.Amy",
      language: "en-AU"
    });
    twiml.hangup();

    // Don't keep a broken session around
    resetSession(callSid);

    return res.type("text/xml").send(twiml.toString());
  }
});

// Health check
app.get("/", (req, res) => res.send("Voice bot running"));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("Server listening on", PORT));

