/* ============================================================================
SMS ROUTE (customer replies Y/N + QUOTE photos)
============================================================================ */
app.post("/sms", async (req, res) => {
  if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

  const tradie = getTradieConfig(req);
  const from = (req.body.From || "").trim();
  const body = (req.body.Body || "").trim();
  const bodyLower = body.toLowerCase();

  const twiml = new MessagingResponse();

  // 1) MMS media -> attach to quote lead
  const numMedia = Number(req.body.NumMedia || 0);
  if (numMedia > 0 && supaReady()) {
    const qKey = makeQuoteKey(tradie.key, from);
    const lead = await getQuoteLead(qKey);

    if (lead) {
      const urls = [];
      for (let i = 0; i < numMedia; i++) {
        const u = req.body[`MediaUrl${i}`];
        if (u) urls.push(String(u));
      }

      const existing = Array.isArray(lead.media_urls) ? lead.media_urls : [];
      const merged = [...existing, ...urls].slice(0, 30);

      await upsertQuoteLead(qKey, {
        ...lead,
        media_urls: merged,
        last_message: body,
        status: "MEDIA_RECEIVED"
      });

      await sendOwnerSms(
        tradie,
        `QUOTE PHOTOS RECEIVED 📸\nCaller: ${from}\nCount: ${urls.length}\nJob: ${lead.job || "-"}\nAddress: ${lead.address || "-"}\nNote: ${lead.note || "-"}`
      ).catch(() => {});

      twiml.message("Photos received ✅ Thanks — we’ll send your quote shortly.");
      return res.type("text/xml").send(twiml.toString());
    }
  }

  // 2) Y/N confirmations
  const pendingKey = makePendingKey(tradie.key, from);
  let pending = await getPendingConfirmationDb(pendingKey);
  if (!pending) pending = getPendingConfirmationMemory(pendingKey);

  if (pending) {
    const nice =
      `Caller: ${from}\n` +
      `Name: ${pending.name}\n` +
      `Job: ${pending.job}\n` +
      `Address: ${pending.address}\n` +
      `When: ${pending.when_text} (${pending.timezone || tradie.timezone})`;

    if (bodyLower === "y" || bodyLower === "yes" || bodyLower.startsWith("y ")) {
      await sendOwnerSms(tradie, `CUSTOMER CONFIRMED ✅\n${nice}`).catch(() => {});
      twiml.message("Confirmed ✅ Thanks — see you then.");

      await deletePendingConfirmationDb(pendingKey).catch(() => {});
      clearPendingConfirmationMemory(pendingKey);
      return res.type("text/xml").send(twiml.toString());
    }

    if (bodyLower === "n" || bodyLower === "no" || bodyLower.startsWith("n ")) {
      await sendOwnerSms(tradie, `CUSTOMER RESCHEDULE REQUEST ❗\n${nice}\nAction: Please call/text to reschedule.`).catch(() => {});
      twiml.message("No worries — we’ll contact you shortly to reschedule.");

      await deletePendingConfirmationDb(pendingKey).catch(() => {});
      clearPendingConfirmationMemory(pendingKey);
      return res.type("text/xml").send(twiml.toString());
    }

    twiml.message("Reply Y to confirm or N to reschedule.");
    return res.type("text/xml").send(twiml.toString());
  }

  // 3) Store quote notes if quote lead exists
  if (supaReady()) {
    const qKey = makeQuoteKey(tradie.key, from);
    const lead = await getQuoteLead(qKey);
    if (lead && body) {
      await upsertQuoteLead(qKey, {
        ...lead,
        last_message: body,
        status: lead.status || "OPEN"
      });
      twiml.message("Thanks — we received your message ✅");
      return res.type("text/xml").send(twiml.toString());
    }
  }

  twiml.message("Thanks — we received your message. If you need help, reply with your address or call us back.");
  return res.type("text/xml").send(twiml.toString());
}); // ✅ DO NOT LOSE THIS CLOSER

// Health check
app.get("/", (req, res) => res.send("Voice bot running"));

const PORT = process.env.PORT || 10000;

// ✅ optional: fail fast if file got truncated and Express never starts
if (!PORT) throw new Error("PORT missing");

app.listen(PORT, () => console.log("Server listening on", PORT));
