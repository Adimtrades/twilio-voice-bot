const express = require("express");
const VoiceResponse = require("twilio").twiml.VoiceResponse;

const app = express();

// Twilio sends form-encoded by default
app.use(express.urlencoded({ extended: false }));

// Simple in-memory memory per caller (fine for testing)
const memory = new Map();

async function askAI({ from, userText }) {
  const history = memory.get(from) || [];

  const messages = [
    {
      role: "system",
      content:
        "You are a friendly phone receptionist for a tradie. Keep replies under 1 sentence. Ask ONE question at a time. Goal: collect name, job type, suburb/address, urgency, and preferred time window.",
    },
    ...history,
    { role: "user", content: userText || "Caller said nothing yet." },
  ];

  const resp = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "gpt-4.1-mini",
      messages,
      temperature: 0.4,
    }),
  });

  const data = await resp.json();
  const aiText = data?.choices?.[0]?.message?.content?.trim() || "Sorry, could you repeat that?";

  // Save short history
  const newHistory = [...history, { role: "user", content: userText }, { role: "assistant", content: aiText }].slice(-10);
  memory.set(from, newHistory);

  return aiText;
}

app.post("/voice", (req, res) => {
  const twiml = new VoiceResponse();

  twiml.say("Hi, thanks for calling. Tell me what you need help with.");

  twiml.gather({
    input: "speech",
    action: "/listen",
    method: "POST",
    speechTimeout: "auto",
  });

  res.type("text/xml").send(twiml.toString());
});

app.post("/listen", async (req, res) => {
  const twiml = new VoiceResponse();

  const from = req.body.From || "unknown";
  const said = req.body.SpeechResult || "";

  const reply = await askAI({ from, userText: said });

  twiml.say(reply);

  // Keep the conversation going
  twiml.gather({
    input: "speech",
    action: "/listen",
    method: "POST",
    speechTimeout: "auto",
  });

  res.type("text/xml").send(twiml.toString());
});

app.get("/", (req, res) => res.send("OK"));
app.listen(process.env.PORT || 3000);
