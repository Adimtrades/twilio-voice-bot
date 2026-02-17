// server.js — PATCHED FOR STRIPE WEBHOOK (raw body) + Twilio form parsing
// Node 18+

try { require("dotenv").config(); } catch (e) {}

const express = require("express");
const twilio = require("twilio");
const stripe = require("stripe")(process.env.STRIPE_SECRET_KEY || "");
const { google } = require("googleapis");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");

const app = express();
app.set("trust proxy", true);

/* ============================================================================
Process-level safety
============================================================================ */
process.on("unhandledRejection", (reason) => console.error("UNHANDLED REJECTION:", reason));
process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
  process.exit(1);
});

/* ============================================================================
RAW BODY CAPTURE (Twilio) + Stripe-safe parsing
IMPORTANT:
- Twilio needs urlencoded
- Stripe webhook MUST get raw body (application/json)
- So we SKIP express.json() for /stripe/webhook entirely
============================================================================ */
function rawBodySaver(req, res, buf) {
  try { req.rawBody = buf?.toString("utf8") || ""; } catch { req.rawBody = ""; }
}

// Twilio default: x-www-form-urlencoded
app.use(express.urlencoded({ extended: false, verify: rawBodySaver }));

// ✅ Stripe-safe JSON parsing: do NOT parse JSON on /stripe/webhook
app.use((req, res, next) => {
  if (req.originalUrl === "/stripe/webhook") return next();
  return express.json({ limit: "1mb" })(req, res, next);
});

const VoiceResponse = twilio.twiml.VoiceResponse;
const MessagingResponse = twilio.twiml.MessagingResponse;

/* ============================================================================
BASE URL helper
============================================================================ */
function getBaseUrl(req) {
  const envBase = (process.env.BASE_URL || "").trim().replace(/\/+$/, "");
  if (envBase) return envBase;

  const proto = (req.headers["x-forwarded-proto"] || "https").split(",")[0].trim();
  const host = (req.headers["x-forwarded-host"] || req.headers["host"] || "").split(",")[0].trim();
  return `${proto}://${host}`.replace(/\/+$/, "");
}

/* ============================================================================
(KEEP YOUR EXISTING CODE FROM HERE DOWN)
- Supabase helpers
- Tradie lookup + cache
- Twilio helpers
- Voice route /voice
- SMS route /sms
- etc...
============================================================================ */

// ---------------------------------------------------------------------------
// ✅ KEEP EVERYTHING YOU POSTED UNCHANGED… UNTIL YOU REACH STRIPE SECTION
// ---------------------------------------------------------------------------

// ... your existing code continues here ...

/* ============================================================================
Stripe SaaS: Checkout + Webhook + Portal + Verify
============================================================================ */
const STRIPE_PRICE_BASIC = process.env.STRIPE_PRICE_BASIC || "";
const STRIPE_PRICE_PRO = process.env.STRIPE_PRICE_PRO || "";
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || "";

function stripeReady() {
  return !!(process.env.STRIPE_SECRET_KEY && STRIPE_WEBHOOK_SECRET && (STRIPE_PRICE_BASIC || STRIPE_PRICE_PRO));
}

// (KEEP your /billing/checkout, /onboarding/verify, /onboarding/submit, /billing/portal unchanged)

// ✅ Stripe webhook MUST be RAW:
// IMPORTANT: This will now work because we SKIPPED express.json() above for this exact path.
app.post("/stripe/webhook", express.raw({ type: "application/json" }), async (req, res) => {
  try {
    if (!stripeReady()) return res.status(500).send("Stripe not configured");

    const sig = req.headers["stripe-signature"];
    let event;

    try {
      event = stripe.webhooks.constructEvent(req.body, sig, STRIPE_WEBHOOK_SECRET);
    } catch (err) {
      console.error("stripe webhook signature error", err?.message || err);
      return res.status(400).send(`Webhook Error: ${err.message}`);
    }

    // ✅ Debug so you can SEE it in Render logs
    console.log("✅ STRIPE WEBHOOK RECEIVED:", event.type);

    if (event.type === "checkout.session.completed") {
      // your existing handler code here (keep it)
    }

    if (event.type === "customer.subscription.deleted") {
      // your existing handler code here (keep it)
    }

    return res.json({ received: true });
  } catch (e) {
    console.error("stripe webhook error", e);
    return res.status(500).send("Server error");
  }
});

// Health check
app.get("/", (req, res) => res.status(200).send("Voice bot running (SaaS)"));

// Listen
const PORT = Number(process.env.PORT || 10000);
if (!PORT || Number.isNaN(PORT)) throw new Error("PORT missing/invalid");
app.listen(PORT, () => console.log("Server listening on", PORT));
