const express = require("express");
const fetch = require("node-fetch");
const twilio = require("twilio");

const VoiceResponse = twilio.twiml.VoiceResponse;

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

const memory = new Map();
const noInputCount = new Map(); // tracks silence per caller

/* ================= OPENAI ================= */

async function askAI(from, userText) {
  try {
    if (!process.env.OPENAI_API_KEY) {
      return "System not configured yet.";
    }

    const history = memory.get(from) || [];

    const messages = [
      {
        role: "system",
        content:
          "You are a phone receptionist for a tradie. Keep replies under 12 words. Ask one question at a time. Collect name, job type, suburb, urgency, and time.",
      },
      ...history,
      { role: "user", content: userText || "Caller said nothing" },
    ];

    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages,
        temperature: 0.4,
      }),
    });

    const json = await r.json();

    const reply =
      json?.choices?.[0]?.message?.content ||
      "Sorry, can you repeat that?";

    memory.set(from, [...messages.slice(-6)]);

    return String(reply).replace(/[<>]/g, "").slice(0, 250);
  } catch (e) {
    console.log("AI ERROR:", e);
    return "Sorry, system busy. Try again.";
  }
}

/* ================= HEALTH CHECK ================= */

app.get("/", (req, res) => {
  res.send("Voice bot running");
});

/* ================= VOICE ROUTE ================= */

app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
  const speech = (req.body?.SpeechResult || "").trim();
const from = req.body?.From || "unknown";
if (speech) noInputCount.set(from, 0);


    console.log("CALL FROM:", from);
    console.log("USER SAID:", speech);

    /* FIRST TIME — ASK QUESTION */
   if (!speech) {
  const fromKey = from;

  const count = (noInputCount.get(fromKey) || 0) + 1;
  noInputCount.set(fromKey, count);

  const gather = twiml.gather({
    input: "speech",
    action: "/voice",
    method: "POST",
    speechTimeout: "auto",
    timeout: 8, // was 5 (give them more time)
  });

  if (count === 1) {
    gather.say({ voice: "alice" }, "Hi, what do you need help with today?");
  } else if (count === 2) {
    gather.say({ voice: "alice" }, "No worries. Just say it in a short sentence.");
  } else {
    // stop looping forever
    twiml.say({ voice: "alice" }, "All good. Call back when ready. Goodbye.");
    twiml.hangup();
    res.type("text/xml").send(twiml.toString());
    return;
  }

  twiml.redirect({ method: "POST" }, "/voice");
  res.type("text/xml").send(twiml.toString());
  return;
}


      gather.say(
        { voice: "alice" },
        "Hi, what do you need help with today?"
      );

      twiml.redirect({ method: "POST" }, "/voice");

      res.type("text/xml").send(twiml.toString());
      return;
    }

    /* GET AI RESPONSE */
    const aiReply = await askAI(from, speech);

    twiml.say({ voice: "alice" }, aiReply);

    /* KEEP CONVERSATION GOING */
    const gather = twiml.gather({
      input: "speech",
      action: "/voice",
      method: "POST",
      speechTimeout: "auto",
      timeout: 5,
    });

    gather.say(
      { voice: "alice" },
      "Okay. What suburb are you located in?"
    );

    twiml.redirect({ method: "POST" }, "/voice");
  } catch (err) {
    console.log("VOICE ROUTE ERROR:", err);
    twiml.say(
      { voice: "alice" },
      "Sorry. Something went wrong. Please try again."
    );
  }

  res.type("text/xml").send(twiml.toString());
});

/* ================= START SERVER ================= */

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});
