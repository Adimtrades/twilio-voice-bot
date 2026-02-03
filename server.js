// server.js
const express = require("express");
const VoiceResponse = require("twilio").twiml.VoiceResponse;

const app = express();

// Twilio sends application/x-www-form-urlencoded
app.use(express.urlencoded({ extended: false }));

// In-memory call memory (ok for testing; use DB later)
const memory = new Map();

/**
 * Call OpenAI to generate the next short receptionist response.
 * Keeps replies short and asks ONE question at a time.
 */
async function askAI({ from, userText }) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return "System is not configured. Please try again later.";
  }

  const history = memory.get(from) || [];

  const messages = [
    {
      role: "system",
      content:
        "You are a phone receptionist for a tradie. Speak naturally. Keep replies under 10 words. Ask ONE question at a time. Goal: collect name, job type, suburb/address, urgency, and preferred time window. If audio unclear, ask them to repeat slowly.",
    },
    ...history,
    { role: "user", content: userText || "Caller said nothing." },
  ];

  try {
    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4.1-mini",
        messages,
        temperature: 0.3,
      }),
    });

    if (!resp.ok) {
      const errText = await resp.text();
      console.log("OpenAI error:", resp.status, errText);
      return "Sorry, try again. What do you need?";
    }

    const data = await resp.json();
    const aiText =
      data?.choices?.[0]?.message?.content?.trim() ||
      "Sorry—what do you need help with?";

    // Save short rolling history
    const newHistory = [
      ...history,
      { role: "user", content: userText || "" },
      { role: "assistant", content: aiText },
    ].slice(-10);

    memory.set(from, newHistory);

    return aiText;
  } catch (e) {
    console.log("OpenAI fetch failed:", e);
    return "Sorry, connection issue. Please repeat.";
  }
}

/**
 * Helper: creates a Gather (speech capture) with best phone settings.
 */
function makeGather(twiml) {
  return twiml.gather({
    input: "speech",
    action: "/listen",
    method: "POST",
    speechTimeout: "auto",
    enhanced: true,
    speechModel: "phone_call",
  });
}

// Health check
app.get("/", (req, res) => res.status(200).send("OK"));

// Entry: answer call + ask first question + listen
app.post("/voice", (req, res) => {
  const twiml = new VoiceResponse();

  twiml.say("Hi. What do you need help with?");
  makeGather(twiml);

  // If caller says nothing, re-prompt once
  twiml.say("Sorry, I didn't catch that. Please say it again.");
  makeGather(twiml);

  res.type("text/xml").send(twiml.toString());
});

// Listen: get speech text -> ask AI -> speak -> gather again
app.post("/listen", async (req, res) => {
  const twiml = new VoiceResponse();

  const from = req.body.From || "unknown";

  // Twilio speech text field:
  const speech = (req.body.SpeechResult || "").trim();
  const confidence = req.body.Confidence;

  console.log("From:", from, "SpeechResult:", speech, "Confidence:", confidence);

  // If speech is empty, ask to repeat
  if (!speech) {
    twiml.say("Sorry. Please repeat slowly.");
    makeGather(twiml);
    return res.type("text/xml").send(twiml.toString());
  }

  const reply = await askAI({ from, userText: speech });

  twiml.say(reply);
  makeGather(twiml);

  // safety re-prompt
  twiml.say("Sorry, say that again please.");
  makeGather(twiml);

  res.type("text/xml").send(twiml.toString());
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server running on port", PORT));
