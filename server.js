const express = require("express");
const VoiceResponse = require("twilio").twiml.VoiceResponse;

const app = express();

app.post("/voice", (req, res) => {
  const twiml = new VoiceResponse();

  twiml.say("Hi thanks for calling. How can I help today?");

  twiml.gather({
    input: "speech",
    action: "/listen",
    method: "POST"
  });

  res.type("text/xml");
  res.send(twiml.toString());
});

app.post("/listen", (req, res) => {
  const twiml = new VoiceResponse();

  twiml.say("Thanks. We received your request.");

  res.type("text/xml");
  res.send(twiml.toString());
});

app.listen(process.env.PORT || 3000);
