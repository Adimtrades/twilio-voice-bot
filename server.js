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

function ask(twiml, prompt, actionUrl) {
  const gather = twiml.gather({
    input: "speech",
    action: actionUrl,
    method: "POST",
    timeout: 5,
    speechTimeout: "auto",
    language: "en-AU"
  });

  gather.say(prompt || "Sorry, can you repeat that?", {
    voice: "Polly.Amy",
    language: "en-AU"
  });
}

function shouldReject(step, speech, confidence) {
  const s = speech.toLowerCase();

  if (!speech || speech.length < 2) return true;

  if (s === "hello" || s === "hi" || s === "yeah" || s === "yes") {
    return step !== "job";
  }

  const minConf = step === "job" ? 0.35 : 0.15;

  if (typeof confidence === "number" && confidence > 0 && confidence < minConf) {
    if (speech.split(" ").length <= 2) return true;
  }

  return false;
}

// --------------------
// Time parsing (calendar accuracy)
// --------------------
function parseRequestedDateTime(naturalText) {
  // chrono uses server locale, but works well for AU-style phrases like:
  // "5pm tomorrow", "Thursday 2pm", "next Monday at 11"
  const ref = new Date();
  const result = chrono.parseDate(naturalText, ref, { forwardDate: true });
  return result || null;
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
      suburb: "Sorry — what suburb are you in?",
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
      session.lastPrompt = "What time would you like?";
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

      // Parse their spoken time into a real datetime
      const parsedStart = parseRequestedDateTime(session.time);

      // Fallback if parse fails: now+5 minutes
      const start = parsedStart ? new Date(parsedStart) : new Date(Date.now() + 5 * 60 * 1000);
      const end = new Date(start.getTime() + 60 * 60 * 1000);

      await calendar.events.insert({
        calendarId,
        requestBody: {
          summary: `${session.job} - ${session.name}`,
          description: summary,
          start: {
            dateTime: start.toISOString(),
            timeZone: process.env.TIMEZONE || "Australia/Sydney"
          },
          end: {
            dateTime: end.toISOString(),
            timeZone: process.env.TIMEZONE || "Australia/Sydney"
          }
        }
      });

      resetSession(callSid);
      twiml.hangup();
      return res.type("text/xml").send(twiml.toString());
    }

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
