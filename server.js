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
  return String(text).trim().replace(/\s+/g, " ");
}

// Better “hearing” settings:
// - timeout: how long to wait for ANY speech to start (seconds)
// - speechTimeout: how long of silence ends the capture ("auto" ends too aggressively sometimes)
// - profanityFilter: avoid weird censoring artifacts (optional)
function ask(twiml, prompt, actionUrl) {
  const gather = twiml.gather({
    input: "speech",
    action: actionUrl,
    method: "POST",
    timeout: 12,          // was 5 — gives people time to start speaking
    speechTimeout: 2,     // allows short pauses without cutting off (more stable than "auto")
    language: "en-AU",
    profanityFilter: false
  });

  gather.say(prompt || "Sorry, can you repeat that?", {
    voice: "Polly.Amy",
    language: "en-AU"
  });
}

// Accept speech with step-specific tolerance.
// Suburb/name/time often come back low confidence.
function shouldReject(step, speech, confidence) {
  const s = speech.toLowerCase();

  // literally nothing / noise
  if (!speech || speech.length < 2) return true;

  // common garbage from STT
  const junk = new Set(["hello", "hi", "yeah", "yes", "yep", "nope", "okay", "ok"]);
  if (junk.has(s)) {
    // allow greeting only on first step if job isn't set yet
    return step !== "job" || !!speech && speech.length < 5;
  }

  // Tuned confidence:
  // job: be more strict
  // suburb/name/time: more forgiving
  const minConf = step === "job" ? 0.30 : 0.10;

  // If confidence exists & is very low AND utterance is tiny -> reject
  if (typeof confidence === "number" && confidence > 0 && confidence < minConf) {
    if (speech.split(" ").length <= 2) return true;
  }

  return false;
}

// --------------------
// Time parsing + timezone-safe formatting (calendar accuracy fix)
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

  console.log(
    `CALLSID=${callSid} STEP=${session.step} Speech="${speech}" Confidence=${confidence}`
  );

  // Handle no-speech hits (start of call / timeouts)
  if (!speech) {
    session.tries += 1;

    // was 3 — too aggressive. 6 gives a real chance without being annoying.
    if (session.tries >= 6) {
      twiml.say("All good — take your time. If you still need to book, call again when ready.", {
        voice: "Polly.Amy",
        language: "en-AU"
      });
      twiml.hangup();
      resetSession(callSid);
      return res.type("text/xml").send(twiml.toString());
    }

    const promptMap = {
      job: "What job do you need help with? You can speak in a full sentence.",
      suburb: "What suburb are you in? Take your time.",
      name: "What is your name? Say it clearly, like: Max Majerowski.",
      time: "What time would you like? For example: tomorrow at 5 p.m."
    };

    const prompt =
      session.lastPrompt ||
      promptMap[session.step] ||
      "Sorry — can you repeat that? Take your time.";

    session.lastPrompt = prompt;

    ask(twiml, prompt, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  // We got speech → reset tries
  session.tries = 0;

  if (shouldReject(session.step, speech, confidence)) {
    const repromptMap = {
      job: "Sorry — what job do you need help with? Say it like: I need help fixing a leaking sink.",
      suburb: "Sorry — what suburb are you in? Please say just the suburb name.",
      name: "Sorry — what is your name? Please say your first name.",
      time: "Sorry — what time would you like? Say: tomorrow at 5 p.m."
    };

    const reprompt = repromptMap[session.step] || "Sorry — can you repeat that? Take your time.";
    session.lastPrompt = reprompt;

    ask(twiml, reprompt, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    if (session.step === "job") {
      session.job = speech;
      session.step = "suburb";
      session.lastPrompt = "What suburb are you in? Take your time.";
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
      session.lastPrompt = "What time would you like? For example: tomorrow at 5 p.m.";
      ask(twiml, session.lastPrompt, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "time") {
      session.time = speech;

      const summary = `${session.name} needs ${session.job} in ${session.suburb} at ${session.time}.`;

      twiml.say(`Booking confirmed. ${summary}`, {
        voice: "Polly.Amy",
        language: "en-AU"
      });

      const calendarId = process.env.GOOGLE_CALENDAR_ID;
      if (!calendarId) throw new Error("Missing GOOGLE_CALENDAR_ID env variable");

      const calendar = getCalendarClient();
      const tz = process.env.TIMEZONE || "Australia/Sydney";

      const parsedStart = parseRequestedDateTime(session.time, tz);

      // Fallback if parse fails: now+10 minutes (more realistic than +5)
      const start = parsedStart ? new Date(parsedStart) : new Date(Date.now() + 10 * 60 * 1000);
      const end = new Date(start.getTime() + 60 * 60 * 1000);

      await calendar.events.insert({
        calendarId,
        requestBody: {
          summary: `${session.job} - ${session.name}`,
          description: summary,
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
