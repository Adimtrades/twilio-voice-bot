require("dotenv").config();

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
    timeout: 4,
    speechTimeout: "auto",
    language: "en-AU"
  });

  gather.say(text, { voice: "Polly.Amy", language: "en-AU" });
}

// ===== CLEAN SPEECH =====
function cleanSpeech(text) {
  if (!text) return "";
  return text
    .toLowerCase()
    .replace(/[^\w\s]/g, "")
    .trim();
}

// ===== MAIN VOICE ROUTE =====
app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  const from = req.body.From;
  const speechRaw = req.body.SpeechResult || "";
  const confidence = Number(req.body.Confidence || 0);

  const speech = cleanSpeech(speechRaw);

  console.log("Speech:", speech, "Confidence:", confidence);

  // ===== GET SESSION =====
  let session = sessions.get(from) || {
    step: "job",
    job: "",
    suburb: "",
    name: "",
    time: ""
  };

  // ===== FILTER BAD INPUT =====
  if (confidence < 0.5 && speech.split(" ").length <= 2) {
    gatherSpeech(twiml, "Sorry, say that again.", "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  // ===== STATE MACHINE =====

  try {

    // ===== JOB =====
    if (session.step === "job") {

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

    // ===== SUBURB =====
    if (session.step === "suburb") {

      if (!speech) {
        gatherSpeech(twiml, "Sorry, what suburb?", "/voice");
        return res.type("text/xml").send(twiml.toString());
      }

      session.suburb = speech;
      session.step = "name";
      sessions.set(from, session);

      gatherSpeech(twiml, "What is your name?", "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    // ===== NAME =====
    if (session.step === "name") {

      if (!speech) {
        gatherSpeech(twiml, "Sorry, what is your name?", "/voice");
        return res.type("text/xml").send(twiml.toString());
      }

      session.name = speech;
      session.step = "time";
      sessions.set(from, session);

      gatherSpeech(twiml, "When do you need this done?", "/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    // ===== TIME =====
    if (session.step === "time") {

      if (!speech) {
        gatherSpeech(twiml, "What time suits you?", "/voice");
        return res.type("text/xml").send(twiml.toString());
      }

      session.time = speech;
      session.step = "confirm";
      sessions.set(from, session);

      const summary =
        `${session.name} needs ${session.job} in ${session.suburb} at ${session.time}`;

      twiml.say(`Booking confirmed. ${summary}`);

      // ===== CREATE CALENDAR EVENT =====
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
    console.error(err);
    twiml.say("Sorry, system error.");
    return res.type("text/xml").send(twiml.toString());
  }

});

// ===== TEST ROUTE =====
app.get("/", (req, res) => res.send("Voice bot running"));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("Server running", PORT));
