const express = require("express");
const twilio = require("twilio");
const { google } = require("googleapis");

const app = express();
app.use(express.urlencoded({ extended: false }));

const VoiceResponse = twilio.twiml.VoiceResponse;

// ===== MEMORY STORE =====
const sessions = new Map();

// ===== GOOGLE CALENDAR =====
function getCalendarClient() {
  if (!process.env.GOOGLE_SERVICE_JSON) {
    throw new Error("Missing GOOGLE_SERVICE_JSON env variable");
  }

  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_SERVICE_JSON),
    scopes: ["https://www.googleapis.com/auth/calendar"]
  });

  return google.calendar({ version: "v3", auth });
}

// ===== SPEECH HELPER =====
function gatherSpeech(twiml, text, actionUrl) {
  const gather = twiml.gather({
    input: "speech",
    action: actionUrl,
    method: "POST",
    timeout: 5,
    speechTimeout: "auto",
    language: "en-AU"
  });

  gather.say(text, { voice: "Polly.Amy", language: "en-AU" });
}

// ===== CLEAN SPEECH =====
function cleanSpeech(text) {
  if (!text) return "";
  return text.toLowerCase().replace(/[^\w\s]/g, "").trim();
}
// Ignore garbage or silence completely
if (!speech || speech.length < 2) {
  const twiml = new VoiceResponse();
  gatherSpeech(twiml, "Sorry, please say that again.", "/voice");
  return res.type("text/xml").send(twiml.toString());
}

// ===== MAIN VOICE ROUTE =====
app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  const from = req.body.From || "unknown";
  const speechRaw = req.body.SpeechResult || "";
  const confidence = Number(req.body.Confidence || 0);

  const speech = cleanSpeech(speechRaw);

  console.log("CALL FROM:", from);
  console.log("Speech:", speech, "Confidence:", confidence);

let session = sessions.get(from);

if (!session) {
  session = {
    step: "job",
    job: "",
    suburb: "",
    name: "",
    time: "",
    retries: 0
  };
}



  // ===== LOW CONFIDENCE FILTER =====
// Only reject if literally nothing heard
if (!speech || speech.length < 2) {
  gatherSpeech(twiml, "Sorry, I didn’t catch that. Please repeat.", "/voice");
  return res.type("text/xml").send(twiml.toString());
}

  try {

    // ===== JOB =====
 if (session.step === "job") {

  if (!session.job) {

    if (!speech) {
      gatherSpeech(twiml, "What job do you need help with?", "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    session.job = speech;
    session.step = "suburb";
    sessions.set(from, session);

    gatherSpeech(twiml, "What suburb are you in?", "/voice");
    return res.type("text/xml").send(twiml.toString());
  }
}


    // ===== SUBURB =====
   if (session.step === "suburb") {

  if (!speech) {
    session.retries++;

    if (session.retries >= 2) {
      twiml.say("I'll text you to finish booking.");
      sessions.delete(from);
      twiml.hangup();
      return res.type("text/xml").send(twiml.toString());
    }

    gatherSpeech(twiml, "Sorry, what suburb are you in?", "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  session.suburb = speech;
  session.retries = 0;
  session.step = "name";

  sessions.set(from, session);

  gatherSpeech(twiml, "What is your name?", "/voice");
  return res.type("text/xml").send(twiml.toString());
}

    // ===== NAME =====
    if (session.step === "name") {

      session.name = speech || session.name;
      session.step = "time";
      sessions.set(from, session);

      gatherSpeech(twiml, "When do you need this done?", "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    // ===== TIME =====
    if (session.step === "time") {

      session.time = speech || session.time;

      const summary =
        `${session.name} needs ${session.job} in ${session.suburb} at ${session.time}`;

      console.log("BOOKING:", summary);

      twiml.say(`Booking confirmed. ${summary}`);

      const calendar = getCalendarClient();

      await calendar.events.insert({
        calendarId: process.env.GOOGLE_CALENDAR_ID,
        requestBody: {
          summary: `${session.job} - ${session.name}`,
          description: summary,
          start: { dateTime: new Date().toISOString() },
          end: { dateTime: new Date(Date.now() + 3600000).toISOString() }
        }
      });

      sessions.delete(from);
      twiml.hangup();

      return res.type("text/xml").send(twiml.toString());
    }

  } catch (err) {
    console.error("VOICE ERROR:", err);
    twiml.say("Sorry, there was a system error.");
    return res.type("text/xml").send(twiml.toString());
  }

});

// ===== HEALTH CHECK =====
app.get("/", (req, res) => res.send("Voice bot running"));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("Server running on", PORT));
