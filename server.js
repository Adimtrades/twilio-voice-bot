require("dotenv").config();

const express = require("express");
const twilio = require("twilio");
const { google } = require("googleapis");

const app = express();
app.use(express.urlencoded({ extended: false }));

const VoiceResponse = twilio.twiml.VoiceResponse;

const sessions = new Map();

/* ================= GOOGLE CALENDAR ================= */

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

/* ================= HELPERS ================= */

function cleanSpeech(text) {
  if (!text) return "";
  return text.toLowerCase().replace(/[^\w\s]/g, "").trim();
}

function gatherSpeech(twiml, text) {
  const gather = twiml.gather({
    input: "speech",
    action: "/voice",
    method: "POST",
    timeout: 5,
    speechTimeout: "auto",
    language: "en-AU"
  });

  gather.say(text, { voice: "Polly.Amy", language: "en-AU" });
}

/* ================= MAIN ROUTE ================= */

app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    const from = req.body.From || "unknown";

    const speechRaw = req.body.SpeechResult || "";
    const confidence = Number(req.body.Confidence || 0);

    const speech = cleanSpeech(speechRaw);

    console.log("CALL FROM:", from);
    console.log("Speech:", speech, "Confidence:", confidence);

    let session = sessions.get(from) || {
      step: "job",
      job: "",
      suburb: "",
      name: "",
      time: "",
      lastPrompt: ""
    };

    /* ===== BAD INPUT FILTER ===== */
    if (!speech && confidence < 0.4) {
      gatherSpeech(twiml, "Sorry, could you repeat that?");
      return res.type("text/xml").send(twiml.toString());
    }

    /* ================= JOB ================= */
    if (session.step === "job") {

      if (!speech) {
        gatherSpeech(twiml, "What job do you need help with?");
        return res.type("text/xml").send(twiml.toString());
      }

      session.job = speech;
      session.step = "suburb";
      sessions.set(from, session);

      gatherSpeech(twiml, "What suburb are you in?");
      return res.type("text/xml").send(twiml.toString());
    }

    /* ================= SUBURB ================= */
    if (session.step === "suburb") {

      if (!speech) {
        gatherSpeech(twiml, "Sorry, what suburb?");
        return res.type("text/xml").send(twiml.toString());
      }

      session.suburb = speech;
      session.step = "name";
      sessions.set(from, session);

      gatherSpeech(twiml, "What is your name?");
      return res.type("text/xml").send(twiml.toString());
    }

    /* ================= NAME ================= */
    if (session.step === "name") {

      if (!speech) {
        gatherSpeech(twiml, "Sorry, what is your name?");
        return res.type("text/xml").send(twiml.toString());
      }

      session.name = speech;
      session.step = "time";
      sessions.set(from, session);

      gatherSpeech(twiml, "When do you need this done?");
      return res.type("text/xml").send(twiml.toString());
    }

    /* ================= TIME ================= */
    if (session.step === "time") {

      if (!speech) {
        gatherSpeech(twiml, "What time suits you?");
        return res.type("text/xml").send(twiml.toString());
      }

      session.time = speech;

      const summary =
        `${session.name} needs ${session.job} in ${session.suburb} at ${session.time}`;

      console.log("BOOKING:", summary);

      twiml.say(`Booking confirmed. ${summary}`);

      try {
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

      } catch (calendarErr) {
        console.error("Calendar error:", calendarErr.message);
      }

      sessions.delete(from);
      twiml.hangup();

      return res.type("text/xml").send(twiml.toString());
    }

  } catch (err) {
    console.error("VOICE ERROR:", err);
    twiml.say("Sorry, system error.");
    return res.type("text/xml").send(twiml.toString());
  }
});

/* ================= HEALTH ================= */

app.get("/", (req, res) => res.send("Voice bot running"));

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("Server running on", PORT));
