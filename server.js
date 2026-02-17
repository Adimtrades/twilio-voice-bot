// server.js (FULL FILE) — True SaaS: Stripe → Supabase → auto Twilio number provisioning + multi-tenant routing
// Node 18+
//
// WHAT THIS ADDS (True SaaS packaging):
// ✅ Stripe Checkout (subscriptions) + Stripe webhook activation
// ✅ Supabase "tradies" table as source of truth (no TRADIES_JSON needed, but still supported as fallback)
// ✅ Auto provision Twilio number per customer (buy number + set Voice/SMS webhook URLs)
// ✅ Onboarding verify endpoint (session_id) + onboarding submit endpoint to save tradie config
// ✅ Stripe Customer Portal endpoint (self-serve billing)
// ✅ Multi-tenant routing: incoming calls map by "To" (the Twilio number) → tradies record
//
// IMPORTANT:
// - You MUST create Supabase table: tradies
// - You MUST set Stripe webhook signing secret
// - You MUST have Twilio balance + a default country for number purchasing
//
// --------------------------
// REQUIRED ENV VARS
// --------------------------
// PORT=10000
// BASE_URL=https://your-render-url.onrender.com   (no trailing slash)
// TWILIO_ACCOUNT_SID=...
// TWILIO_AUTH_TOKEN=...
// TWILIO_SMS_FROM=+61... (optional; per-tenant number used once provisioned)
// STRIPE_SECRET_KEY=...
// STRIPE_WEBHOOK_SECRET=whsec_...
// STRIPE_PRICE_BASIC=price_...
// STRIPE_PRICE_PRO=price_...
// SUPABASE_URL=...
// SUPABASE_SERVICE_KEY=...
//
// OPTIONAL:
// TRADIES_JSON=... (fallback if Supabase not ready)
// TWILIO_BUY_COUNTRY=AU
// TWILIO_BUY_AREA_CODE=2   (Sydney example; optional)
// ADMIN_DASH_PASSWORD=...

try { require("dotenv").config(); } catch (e) {}

const express = require("express");
const twilio = require("twilio");
const stripe = require("stripe")(process.env.STRIPE_SECRET_KEY || "");
const { google } = require("googleapis");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");

// ✅ MUST exist before any routes
const app = express();
app.set("trust proxy", true);

/* ============================================================================
Process-level safety (Render-friendly logs)
============================================================================ */
process.on("unhandledRejection", (reason) => console.error("UNHANDLED REJECTION:", reason));
process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
  process.exit(1);
});

/* ============================================================================
RAW BODY CAPTURE (Twilio) + standard parsing
============================================================================ */
function rawBodySaver(req, res, buf) {
  try { req.rawBody = buf?.toString("utf8") || ""; } catch { req.rawBody = ""; }
}
// Twilio default: x-www-form-urlencoded
app.use(express.urlencoded({ extended: false, verify: rawBodySaver }));
// For normal JSON endpoints (NOT Stripe webhook)
app.use(express.json({ limit: "1mb" }));

const VoiceResponse = twilio.twiml.VoiceResponse;
const MessagingResponse = twilio.twiml.MessagingResponse;

/* ============================================================================
BASE URL (needed for SaaS webhook URLs)
============================================================================ */
function getBaseUrl(req) {
  // Prefer explicit BASE_URL for correctness (Render proxy, custom domains)
  const envBase = (process.env.BASE_URL || "").trim().replace(/\/+$/, "");
  if (envBase) return envBase;

  const proto = (req.headers["x-forwarded-proto"] || "https").split(",")[0].trim();
  const host = (req.headers["x-forwarded-host"] || req.headers["host"] || "").split(",")[0].trim();
  return `${proto}://${host}`.replace(/\/+$/, "");
}

/* ============================================================================
MULTI-TENANT SOURCE OF TRUTH
- Primary: Supabase "tradies" table
- Fallback: TRADIES_JSON
============================================================================ */
function parseTradiesJson() {
  const raw = process.env.TRADIES_JSON;
  if (!raw) return {};
  try { return JSON.parse(raw); }
  catch {
    try { return JSON.parse(raw.replace(/\r?\n/g, "")); }
    catch {
      console.warn("TRADIES_JSON parse failed. Check JSON formatting.");
      return {};
    }
  }
}
const TRADIES_FALLBACK = parseTradiesJson();

// Supabase (REST)
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || "";
const SUPABASE_PREFS_TABLE = process.env.SUPABASE_TABLE || "customer_prefs";
const SUPABASE_PENDING_TABLE = process.env.SUPABASE_PENDING_TABLE || "pending_confirmations";
const SUPABASE_METRICS_TABLE = process.env.SUPABASE_METRICS_TABLE || "metrics_daily";
const SUPABASE_QUOTES_TABLE = process.env.SUPABASE_QUOTES_TABLE || "quote_leads";
const SUPABASE_TRADIES_TABLE = process.env.SUPABASE_TRADIES_TABLE || "tradies";

function supaHeaders() {
  return {
    apikey: SUPABASE_SERVICE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
    "Content-Type": "application/json"
  };
}
function supaReady() {
  return !!(SUPABASE_URL && SUPABASE_SERVICE_KEY);
}
async function upsertRow(table, row) {
  if (!supaReady()) return false;
  const url = `${SUPABASE_URL}/rest/v1/${table}`;
  try {
    await fetch(url, {
      method: "POST",
      headers: { ...supaHeaders(), Prefer: "resolution=merge-duplicates" },
      body: JSON.stringify([row])
    });
    return true;
  } catch {
    return false;
  }
}
async function getOne(table, query) {
  if (!supaReady()) return null;
  const url = `${SUPABASE_URL}/rest/v1/${table}?${query}&limit=1`;
  try {
    const r = await fetch(url, { headers: supaHeaders() });
    const data = await r.json();
    return Array.isArray(data) && data[0] ? data[0] : null;
  } catch {
    return null;
  }
}
async function getMany(table, query) {
  if (!supaReady()) return [];
  const url = `${SUPABASE_URL}/rest/v1/${table}?${query}`;
  try {
    const r = await fetch(url, { headers: supaHeaders() });
    const data = await r.json();
    return Array.isArray(data) ? data : [];
  } catch {
    return [];
  }
}
async function delWhere(table, query) {
  if (!supaReady()) return false;
  const url = `${SUPABASE_URL}/rest/v1/${table}?${query}`;
  try {
    await fetch(url, { method: "DELETE", headers: supaHeaders() });
    return true;
  } catch {
    return false;
  }
}

// --- Tradies cache (fast routing) ---
const TRADIE_CACHE = new Map(); // key: lookupKey ("+612xxxx" or tradie_key) -> {data, exp}
const TRADIE_CACHE_TTL_MS = 30_000;

function cacheGet(k) {
  const item = TRADIE_CACHE.get(k);
  if (!item) return null;
  if (Date.now() > item.exp) { TRADIE_CACHE.delete(k); return null; }
  return item.data;
}
function cacheSet(k, data) {
  TRADIE_CACHE.set(k, { data, exp: Date.now() + TRADIE_CACHE_TTL_MS });
}

// Lookup order:
// 1) If req.query.tid present → tradie_key lookup
// 2) Else use Twilio inbound To number → match tradies.twilio_number (or voice_number)
// 3) Fallback: TRADIES_JSON/default
async function loadTradieRow(req) {
  const tid = (req.query.tid || req.body?.tid || "").trim();
  const to = (req.body?.To || req.query?.To || "").trim();

  if (supaReady()) {
    if (tid) {
      const k = `tid:${tid}`;
      const cached = cacheGet(k);
      if (cached) return cached;
      const row = await getOne(SUPABASE_TRADIES_TABLE, `tradie_key=eq.${encodeURIComponent(tid)}&select=*`);
      if (row) cacheSet(k, row);
      return row;
    }

    if (to) {
      const k = `to:${to}`;
      const cached = cacheGet(k);
      if (cached) return cached;
      // normalize "To" can be "+61..." etc
      const row = await getOne(
        SUPABASE_TRADIES_TABLE,
        `twilio_number=eq.${encodeURIComponent(to)}&select=*`
      );
      if (row) cacheSet(k, row);
      return row;
    }
  }

  // Fallback to TRADIES_JSON
  const key = (to || tid || "default").trim();
  const fallback = TRADIES_FALLBACK[key] || TRADIES_FALLBACK.default || null;
  if (!fallback) return null;
  return { tradie_key: key, ...fallback, _fallback: true };
}

function toBusinessDays(arrOrCsv) {
  if (Array.isArray(arrOrCsv) && arrOrCsv.length) return arrOrCsv.map(Number).filter(Boolean);
  const raw = String(arrOrCsv || "").trim();
  if (!raw) return (process.env.BUSINESS_DAYS || "1,2,3,4,5").split(",").map(x => Number(x.trim())).filter(Boolean);
  return raw.split(",").map(x => Number(x.trim())).filter(Boolean);
}

function normalizeTradieConfig(row) {
  const t = row || {};
  const businessDays = toBusinessDays(t.businessDays || t.business_days);

  const avgJobValue = Number(t.avgJobValue ?? t.avg_job_value ?? process.env.AVG_JOB_VALUE ?? 250);
  const closeRate = Number(t.closeRate ?? t.close_rate ?? process.env.CLOSE_RATE ?? 0.6);

  const slotMinutes = Number(t.slotMinutes ?? t.slot_minutes ?? process.env.SLOT_MINUTES ?? 60);
  const bufferMinutes = Number(t.bufferMinutes ?? t.buffer_minutes ?? process.env.BUFFER_MINUTES ?? 0);

  const bizName = t.bizName || t.biz_name || process.env.BIZ_NAME || "";
  const tone = t.tone || process.env.BOT_TONE || "friendly";
  const services = t.services || process.env.BOT_SERVICES || "";

  const timezone = t.timezone || process.env.TIMEZONE || "Australia/Sydney";

  // Per-tenant numbers:
  // - twilio_number: provisioned number (preferred for SMS/Voice)
  // - smsFrom fallback env
  const twilioNumber = t.twilio_number || t.twilioNumber || "";
  const smsFrom = twilioNumber || t.smsFrom || process.env.TWILIO_SMS_FROM || "";

  return {
    key: t.tradie_key || t.key || "default",

    status: t.status || "ACTIVE",
    plan: t.plan || "",

    ownerSmsTo: t.ownerSmsTo || t.owner_sms_to || process.env.OWNER_SMS_TO || "",
    smsFrom,

    // routing number
    twilioNumber,

    timezone,
    businessStartHour: Number(t.businessStartHour ?? t.business_start_hour ?? process.env.BUSINESS_START_HOUR ?? 7),
    businessEndHour: Number(t.businessEndHour ?? t.business_end_hour ?? process.env.BUSINESS_END_HOUR ?? 17),
    businessDays,

    calendarId: t.calendarId || t.calendar_id || process.env.GOOGLE_CALENDAR_ID || "",
    googleServiceJson: t.googleServiceJson || t.google_service_json || process.env.GOOGLE_SERVICE_JSON || "",

    avgJobValue,
    closeRate,
    slotMinutes,
    bufferMinutes,

    bizName,
    tone,
    services,

    // Stripe fields (optional)
    stripeCustomerId: t.stripe_customer_id || "",
    stripeSubscriptionId: t.stripe_subscription_id || "",
    email: t.email || ""
  };
}

async function getTradieConfig(req) {
  const row = await loadTradieRow(req);
  const cfg = normalizeTradieConfig(row || {});
  return cfg;
}

/* ============================================================================
Global controls
============================================================================ */
const MISSED_CALL_ALERT_TRIES = Number(process.env.MISSED_CALL_ALERT_TRIES || 2);
const MAX_SILENCE_TRIES = Number(process.env.MAX_SILENCE_TRIES || 10);
const CAL_RETRY_ATTEMPTS = Number(process.env.CAL_RETRY_ATTEMPTS || 3);
const GATHER_START_TIMEOUT = Number(process.env.GATHER_START_TIMEOUT || 25);
const GATHER_SPEECH_TIMEOUT = process.env.GATHER_SPEECH_TIMEOUT || "auto";
const DUP_WINDOW_DAYS = Number(process.env.DUP_WINDOW_DAYS || 14);

// Optional: Twilio signature validation
const REQUIRE_TWILIO_SIG = String(process.env.TWILIO_WEBHOOK_AUTH || "false").toLowerCase() === "true";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

// Admin endpoint
const ADMIN_DASH_PASSWORD = process.env.ADMIN_DASH_PASSWORD || "";

/* ============================================================================
LLM (optional)
============================================================================ */
const LLM_ENABLED = String(process.env.LLM_ENABLED || "false").toLowerCase() === "true";
const LLM_BASE_URL = process.env.LLM_BASE_URL || "https://api.openai.com/v1/responses";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const LLM_MODEL = process.env.LLM_MODEL || "gpt-5-mini";
const LLM_MAX_OUTPUT_TOKENS = Number(process.env.LLM_MAX_OUTPUT_TOKENS || 220);
const LLM_MAX_TURNS = Number(process.env.LLM_MAX_TURNS || 8);
const LLM_REQUIRE_FOR_OFFSCRIPT = String(process.env.LLM_REQUIRE_FOR_OFFSCRIPT || "true").toLowerCase() === "true";

function llmReady() {
  return LLM_ENABLED && OPENAI_API_KEY && LLM_BASE_URL;
}
function safeJsonParse(maybe) { try { return JSON.parse(maybe); } catch { return null; } }
function trimHistory(history, maxItems = 12) {
  const arr = Array.isArray(history) ? history : [];
  if (arr.length <= maxItems) return arr;
  return arr.slice(arr.length - maxItems);
}
function isOffScriptSpeech(speech) {
  if (!speech) return false;
  const s = String(speech).trim();
  const wordCount = s.split(/\s+/).filter(Boolean).length;
  if (wordCount >= 18) return true;
  if ((s.match(/\?/g) || []).length > 0) return true;
  const hasMulti = /(and|also|plus|another thing|by the way|actually|one more)/i.test(s);
  if (hasMulti && wordCount >= 10) return true;
  const hasStory = /(so|because|then|after|before|yesterday|last week|last time)/i.test(s) && wordCount >= 14;
  return hasStory;
}
function buildLlmSystemPrompt(tradie) {
  const biz = tradie.bizName ? `Business name: ${tradie.bizName}.` : "";
  const services = tradie.services ? `Services offered: ${tradie.services}.` : "";
  const tone = tradie.tone === "direct" ? "Be concise and direct." : "Be friendly, natural, human.";
  return (
`You are a voice receptionist for an Australian trades business. ${biz} ${services}
Goal: help the caller book or request a quote, while sounding natural.

Output MUST be STRICT JSON ONLY with this schema:
{
  "intent": "NEW_BOOKING" | "QUOTE" | "EMERGENCY" | "CANCEL_RESCHEDULE" | "EXISTING_CUSTOMER" | "UNKNOWN",
  "fields": {
    "job": string|null,
    "address": string|null,
    "name": string|null,
    "time_text": string|null,
    "access_note": string|null
  },
  "smalltalk_reply": string|null,
  "next_question": string,
  "suggested_step": "intent"|"job"|"address"|"name"|"access"|"time"|"pickSlot"|"confirm"
}

Rules:
- If caller asks pricing: intent=QUOTE.
- If caller mentions danger/urgent burst/leak/gas/fire: intent=EMERGENCY.
- If caller wants cancel/reschedule: intent=CANCEL_RESCHEDULE.
- If caller gives multiple details, fill all fields you can.
- Keep smalltalk_reply short (max 1 sentence) and then proceed with next_question.
${tone}`
  );
}
async function fetchJsonWithGuards(url, options, { retryOnce = true } = {}) {
  const attempt = async () => {
    const r = await fetch(url, options);
    const text = await r.text();
    if (!text || !String(text).trim()) {
      const err = new Error("Empty response body");
      err.status = r.status;
      err.body = "";
      throw err;
    }
    const parsed = safeJsonParse(text);
    if (!parsed) {
      const err = new Error("Non-JSON response body");
      err.status = r.status;
      err.body = text.slice(0, 500);
      throw err;
    }
    if (!r.ok) {
      const err = new Error(`HTTP ${r.status}`);
      err.status = r.status;
      err.body = text.slice(0, 500);
      throw err;
    }
    return parsed;
  };

  try {
    return await attempt();
  } catch (e) {
    if (retryOnce) {
      await new Promise((r) => setTimeout(r, 150));
      return attempt();
    }
    throw e;
  }
}
function extractResponseTextFromOpenAI(data) {
  if (data && typeof data.output_text === "string" && data.output_text.trim()) return data.output_text.trim();
  const text1 = data?.output?.[0]?.content?.[0]?.text;
  if (typeof text1 === "string" && text1.trim()) return text1.trim();
  const text2 = data?.choices?.[0]?.message?.content;
  if (typeof text2 === "string" && text2.trim()) return text2.trim();
  return "";
}
async function callLlm(tradie, session, userSpeech) {
  if (!llmReady()) return null;

  const history = trimHistory(session.history || [], 12);
  const input = [
    { role: "system", content: buildLlmSystemPrompt(tradie) },
    ...history.map(h => ({ role: h.role, content: h.content })),
    {
      role: "user",
      content:
        `Current step: ${session.step}\n` +
        `Known fields: job=${session.job||""} | address=${session.address||""} | name=${session.name||""} | time=${session.time||""} | access=${session.accessNote||""}\n` +
        `Caller said: ${userSpeech}`
    }
  ];

  const payload = { model: LLM_MODEL, max_output_tokens: LLM_MAX_OUTPUT_TOKENS, input };

  try {
    const data = await fetchJsonWithGuards(LLM_BASE_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify(payload)
    }, { retryOnce: true });

    const text = extractResponseTextFromOpenAI(data);
    const parsed = safeJsonParse(String(text || "").trim());
    if (!parsed) return null;
    if (!parsed.next_question || !parsed.suggested_step) return null;
    return parsed;
  } catch (e) {
    console.warn("LLM call failed:", e?.message || e);
    return null;
  }
}

/* ============================================================================
Twilio helpers + SaaS number provisioning
============================================================================ */
function getTwilioClient() {
  const sid = process.env.TWILIO_ACCOUNT_SID;
  const token = process.env.TWILIO_AUTH_TOKEN;
  if (!sid || !token) return null;
  return twilio(sid, token);
}
async function sendSms({ from, to, body }) {
  const client = getTwilioClient();
  if (!client) return console.warn("SMS skipped: Missing TWILIO creds");
  if (!from) return console.warn("SMS skipped: Missing smsFrom");
  if (!to) return console.warn("SMS skipped: missing 'to'");
  await client.messages.create({ from, to, body });
}
async function sendOwnerSms(tradie, body) {
  if (!tradie.ownerSmsTo) return;
  return sendSms({ from: tradie.smsFrom, to: tradie.ownerSmsTo, body });
}
async function sendCustomerSms(tradie, toCustomer, body) {
  return sendSms({ from: tradie.smsFrom, to: toCustomer, body });
}

const TWILIO_BUY_COUNTRY = (process.env.TWILIO_BUY_COUNTRY || "AU").trim();
const TWILIO_BUY_AREA_CODE = String(process.env.TWILIO_BUY_AREA_CODE || "").trim();

// Buy + configure a new Twilio number for tenant, store in Supabase.
// This runs after Stripe checkout completes OR after onboarding submit (your choice).
async function provisionTwilioNumberForTradie(tradieKey, reqForBaseUrl) {
  const client = getTwilioClient();
  if (!client) throw new Error("Twilio client not configured");

  const baseUrl = getBaseUrl(reqForBaseUrl);
  const voiceUrl = `${baseUrl}/voice?tid=${encodeURIComponent(tradieKey)}`;
  const smsUrl = `${baseUrl}/sms?tid=${encodeURIComponent(tradieKey)}`;

  // Search available local numbers
  let list;
  if (TWILIO_BUY_AREA_CODE) {
    list = await client.availablePhoneNumbers(TWILIO_BUY_COUNTRY).local.list({
      areaCode: TWILIO_BUY_AREA_CODE,
      smsEnabled: true,
      voiceEnabled: true,
      limit: 10
    });
  } else {
    list = await client.availablePhoneNumbers(TWILIO_BUY_COUNTRY).local.list({
      smsEnabled: true,
      voiceEnabled: true,
      limit: 10
    });
  }
  if (!list || !list.length) throw new Error("No Twilio numbers available to buy");

  const choice = list[0];
  const purchased = await client.incomingPhoneNumbers.create({
    phoneNumber: choice.phoneNumber,
    voiceUrl,
    voiceMethod: "POST",
    smsUrl,
    smsMethod: "POST"
  });

  // Persist
  if (supaReady()) {
    await upsertRow(SUPABASE_TRADIES_TABLE, {
      tradie_key: tradieKey,
      twilio_number: purchased.phoneNumber,
      twilio_incoming_sid: purchased.sid,
      updated_at: new Date().toISOString()
    });
  }

  // Warm cache
  cacheSet(`tid:${tradieKey}`, { tradie_key: tradieKey, twilio_number: purchased.phoneNumber });

  return { phoneNumber: purchased.phoneNumber, sid: purchased.sid };
}

/* ============================================================================
Webhook signature check (Twilio)
============================================================================ */
function validateTwilioSignature(req) {
  if (!REQUIRE_TWILIO_SIG) return true;
  if (!TWILIO_AUTH_TOKEN) return false;

  const signature = req.headers["x-twilio-signature"];
  if (!signature) return false;

  const proto = (req.headers["x-forwarded-proto"] || "https").split(",")[0].trim();
  const host = (req.headers["x-forwarded-host"] || req.headers["host"] || "").split(",")[0].trim();
  const url = `${proto}://${host}${req.originalUrl}`;

  try {
    return twilio.validateRequest(TWILIO_AUTH_TOKEN, signature, url, req.body);
  } catch {
    return false;
  }
}

/* ============================================================================
Stripe SaaS: Checkout + Webhook + Portal + Verify
============================================================================ */
const STRIPE_PRICE_BASIC = process.env.STRIPE_PRICE_BASIC || "";
const STRIPE_PRICE_PRO = process.env.STRIPE_PRICE_PRO || "";
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || "";

function stripeReady() {
  return !!(process.env.STRIPE_SECRET_KEY && STRIPE_WEBHOOK_SECRET && (STRIPE_PRICE_BASIC || STRIPE_PRICE_PRO));
}

// Create a Checkout session (subscription)
// POST /billing/checkout { plan: "basic"|"pro" }
app.post("/billing/checkout", async (req, res) => {
  try {
    if (!stripeReady()) return res.status(500).json({ ok: false, error: "Stripe not configured" });

    const plan = String(req.body.plan || "").toLowerCase().trim();
    const price =
      plan === "pro" ? STRIPE_PRICE_PRO :
      plan === "basic" ? STRIPE_PRICE_BASIC :
      "";

    if (!price) return res.status(400).json({ ok: false, error: "Invalid plan" });

    const base = getBaseUrl(req);
    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      payment_method_types: ["card"],
      line_items: [{ price, quantity: 1 }],
      allow_promotion_codes: true,
      customer_creation: "always",
      success_url: `${base}/onboarding/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${base}/onboarding/cancelled`,
      billing_address_collection: "auto"
    });

    return res.json({ ok: true, url: session.url });
  } catch (e) {
    console.error("checkout error", e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// Simple success page (you can replace with Carrd redirect)
app.get("/onboarding/success", async (req, res) => {
  const base = getBaseUrl(req);
  const sessionId = String(req.query.session_id || "").trim();
  if (!sessionId) return res.status(400).send("Missing session_id");

  // You can change this to redirect to your Carrd onboarding form:
  // e.g. `${YOUR_CARRD_URL}?session_id=${sessionId}`
  return res
    .status(200)
    .send(
      `<html><body style="font-family:Arial;padding:24px;">
        <h2>Payment received ✅</h2>
        <p>Next: complete setup.</p>
        <p><a href="${base}/onboarding/verify?session_id=${encodeURIComponent(sessionId)}">Continue setup</a></p>
      </body></html>`
    );
});
app.get("/onboarding/cancelled", (req, res) => res.status(200).send("Checkout cancelled."));

// Verify checkout session + return what you need (email, plan, customerId, subscriptionId, tradie_key)
// GET /onboarding/verify?session_id=...
app.get("/onboarding/verify", async (req, res) => {
  try {
    if (!stripeReady()) return res.status(500).json({ ok: false, error: "Stripe not configured" });

    const sessionId = String(req.query.session_id || "").trim();
    if (!sessionId) return res.status(400).json({ ok: false, error: "Missing session_id" });

    const sess = await stripe.checkout.sessions.retrieve(sessionId, { expand: ["subscription", "customer"] });
    if (!sess || sess.payment_status !== "paid") {
      return res.status(400).json({ ok: false, error: "Not paid (yet)" });
    }

    const sub = sess.subscription;
    const customer = sess.customer;

    // Determine plan from price id:
    const priceId = sub?.items?.data?.[0]?.price?.id || "";
    const plan =
      priceId === STRIPE_PRICE_PRO ? "PRO" :
      priceId === STRIPE_PRICE_BASIC ? "BASIC" :
      "UNKNOWN";

    // Create a stable tradie_key based on stripe customer id (or you can generate short key)
    const tradieKey = `t_${String(customer?.id || sess.customer || "").replace(/[^a-zA-Z0-9]/g, "").slice(-10)}`;

    // Ensure tradies row exists (inactive until onboarding submit if you want)
    if (supaReady()) {
      await upsertRow(SUPABASE_TRADIES_TABLE, {
        tradie_key: tradieKey,
        email: sess.customer_details?.email || "",
        plan,
        status: "PENDING_SETUP",
        stripe_customer_id: customer?.id || String(sess.customer || ""),
        stripe_subscription_id: sub?.id || "",
        updated_at: new Date().toISOString()
      });
      cacheSet(`tid:${tradieKey}`, { tradie_key: tradieKey });
    }

    return res.json({
      ok: true,
      tradie_key: tradieKey,
      email: sess.customer_details?.email || "",
      plan,
      stripe_customer_id: customer?.id || String(sess.customer || ""),
      stripe_subscription_id: sub?.id || "",
      next: "POST /onboarding/submit with session_id + config fields"
    });
  } catch (e) {
    console.error("verify error", e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// Submit onboarding config (store tradie config; provision number; activate)
// POST /onboarding/submit { session_id, ownerSmsTo, calendarId, timezone, bizName, services, tone, businessDays, businessStartHour, businessEndHour, avgJobValue, closeRate, slotMinutes, bufferMinutes, googleServiceJson }
app.post("/onboarding/submit", async (req, res) => {
  try {
    if (!stripeReady()) return res.status(500).json({ ok: false, error: "Stripe not configured" });
    if (!supaReady()) return res.status(500).json({ ok: false, error: "Supabase not configured" });

    const sessionId = String(req.body.session_id || "").trim();
    if (!sessionId) return res.status(400).json({ ok: false, error: "Missing session_id" });

    const sess = await stripe.checkout.sessions.retrieve(sessionId, { expand: ["subscription", "customer"] });
    if (!sess || sess.payment_status !== "paid") return res.status(400).json({ ok: false, error: "Not paid" });

    const sub = sess.subscription;
    const customer = sess.customer;
    const priceId = sub?.items?.data?.[0]?.price?.id || "";
    const plan =
      priceId === STRIPE_PRICE_PRO ? "PRO" :
      priceId === STRIPE_PRICE_BASIC ? "BASIC" :
      "UNKNOWN";

    const tradieKey = `t_${String(customer?.id || sess.customer || "").replace(/[^a-zA-Z0-9]/g, "").slice(-10)}`;

    // Persist config
    const row = {
      tradie_key: tradieKey,
      email: sess.customer_details?.email || "",
      plan,
      status: "ACTIVE",
      stripe_customer_id: customer?.id || String(sess.customer || ""),
      stripe_subscription_id: sub?.id || "",

      owner_sms_to: String(req.body.ownerSmsTo || "").trim(),
      calendar_id: String(req.body.calendarId || "").trim(),
      timezone: String(req.body.timezone || "Australia/Sydney").trim(),
      biz_name: String(req.body.bizName || "").trim(),
      services: String(req.body.services || "").trim(),
      tone: String(req.body.tone || "friendly").trim(),

      business_days: Array.isArray(req.body.businessDays) ? req.body.businessDays : String(req.body.businessDays || ""),
      business_start_hour: Number(req.body.businessStartHour ?? 7),
      business_end_hour: Number(req.body.businessEndHour ?? 17),

      avg_job_value: Number(req.body.avgJobValue ?? 250),
      close_rate: Number(req.body.closeRate ?? 0.6),

      slot_minutes: Number(req.body.slotMinutes ?? 60),
      buffer_minutes: Number(req.body.bufferMinutes ?? 0),

      google_service_json: String(req.body.googleServiceJson || "").trim(),
      updated_at: new Date().toISOString()
    };

    await upsertRow(SUPABASE_TRADIES_TABLE, row);

    // Provision Twilio number if missing
    const existing = await getOne(SUPABASE_TRADIES_TABLE, `tradie_key=eq.${encodeURIComponent(tradieKey)}&select=twilio_number`);
    let twilio_number = existing?.twilio_number || "";
    if (!twilio_number) {
      const provisioned = await provisionTwilioNumberForTradie(tradieKey, req);
      twilio_number = provisioned.phoneNumber;
    }

    // Send welcome SMS to owner
    const tradieCfg = await getTradieConfig({ query: { tid: tradieKey }, body: {} });
    await sendOwnerSms(tradieCfg, `You're live ✅\nPlan: ${plan}\nYour bot number: ${twilio_number}\nTest: call it now.\nSupport: reply here anytime.`).catch(()=>{});

    return res.json({ ok: true, tradie_key: tradieKey, twilio_number, status: "ACTIVE" });
  } catch (e) {
    console.error("onboarding submit error", e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// Customer Portal (self-serve billing)
// POST /billing/portal { tradie_key }  -> returns portal url
app.post("/billing/portal", async (req, res) => {
  try {
    if (!stripeReady()) return res.status(500).json({ ok: false, error: "Stripe not configured" });
    if (!supaReady()) return res.status(500).json({ ok: false, error: "Supabase not configured" });

    const tid = String(req.body.tradie_key || "").trim();
    if (!tid) return res.status(400).json({ ok: false, error: "Missing tradie_key" });

    const row = await getOne(SUPABASE_TRADIES_TABLE, `tradie_key=eq.${encodeURIComponent(tid)}&select=stripe_customer_id`);
    const customerId = row?.stripe_customer_id;
    if (!customerId) return res.status(400).json({ ok: false, error: "No stripe_customer_id" });

    const base = getBaseUrl(req);
    const portal = await stripe.billingPortal.sessions.create({
      customer: customerId,
      return_url: `${base}/`
    });

    return res.json({ ok: true, url: portal.url });
  } catch (e) {
    console.error("portal error", e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// Stripe webhook (activation + (optional) provisioning)
// IMPORTANT: must be RAW body, so we mount a raw middleware ONLY on this route.
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

    // Handle subscription checkout completion
    if (event.type === "checkout.session.completed") {
      const sess = event.data.object;
      // Only act on paid subscription checkouts
      if (sess?.mode === "subscription" && sess?.payment_status === "paid" && supaReady()) {
        const expanded = await stripe.checkout.sessions.retrieve(sess.id, { expand: ["subscription", "customer"] });
        const sub = expanded.subscription;
        const customer = expanded.customer;

        const priceId = sub?.items?.data?.[0]?.price?.id || "";
        const plan =
          priceId === STRIPE_PRICE_PRO ? "PRO" :
          priceId === STRIPE_PRICE_BASIC ? "BASIC" :
          "UNKNOWN";

        const tradieKey = `t_${String(customer?.id || expanded.customer || "").replace(/[^a-zA-Z0-9]/g, "").slice(-10)}`;

        await upsertRow(SUPABASE_TRADIES_TABLE, {
          tradie_key: tradieKey,
          email: expanded.customer_details?.email || "",
          plan,
          status: "PENDING_SETUP",
          stripe_customer_id: customer?.id || String(expanded.customer || ""),
          stripe_subscription_id: sub?.id || "",
          updated_at: new Date().toISOString()
        });

        cacheSet(`tid:${tradieKey}`, { tradie_key: tradieKey });

        // Optional: provision number immediately (some people prefer after onboarding submit)
        // If you want to provision here, uncomment:
        /*
        const row = await getOne(SUPABASE_TRADIES_TABLE, `tradie_key=eq.${encodeURIComponent(tradieKey)}&select=twilio_number`);
        if (!row?.twilio_number) {
          await provisionTwilioNumberForTradie(tradieKey, { headers: req.headers, originalUrl: "/", body: {}, query: {} });
        }
        */
      }
    }

    // Optional: handle subscription cancellation -> disable bot
    if (event.type === "customer.subscription.deleted") {
      const sub = event.data.object;
      const customerId = sub?.customer;
      if (customerId && supaReady()) {
        const row = await getOne(SUPABASE_TRADIES_TABLE, `stripe_customer_id=eq.${encodeURIComponent(customerId)}&select=tradie_key`);
        if (row?.tradie_key) {
          await upsertRow(SUPABASE_TRADIES_TABLE, {
            tradie_key: row.tradie_key,
            status: "PAST_DUE_OR_CANCELLED",
            updated_at: new Date().toISOString()
          });
          cacheSet(`tid:${row.tradie_key}`, { tradie_key: row.tradie_key, status: "PAST_DUE_OR_CANCELLED" });
        }
      }
    }

    return res.json({ received: true });
  } catch (e) {
    console.error("stripe webhook error", e);
    return res.status(500).send("Server error");
  }
});

/* ============================================================================
Customer memory notes
============================================================================ */
function makeCustomerKey(tradieKey, phone) {
  return `${tradieKey}::${phone || ""}`;
}
async function getCustomerNote(tradieKey, phone) {
  if (!phone) return null;
  const key = makeCustomerKey(tradieKey, phone);
  const row = await getOne(SUPABASE_PREFS_TABLE, `key=eq.${encodeURIComponent(key)}&select=preferred_note`);
  return row?.preferred_note ? String(row.preferred_note) : null;
}
async function setCustomerNote(tradieKey, phone, note) {
  if (!phone) return false;
  const key = makeCustomerKey(tradieKey, phone);
  return upsertRow(SUPABASE_PREFS_TABLE, {
    key,
    tradie_key: tradieKey,
    phone,
    preferred_note: note,
    updated_at: new Date().toISOString()
  });
}

/* ============================================================================
Pending confirmations (DB preferred)
============================================================================ */
function makePendingKey(tradieKey, customerPhone) {
  return `${tradieKey}::${customerPhone || ""}`;
}
async function upsertPendingConfirmationDb(key, payload) {
  return upsertRow(SUPABASE_PENDING_TABLE, { key, ...payload, updated_at: new Date().toISOString() });
}
async function getPendingConfirmationDb(key) {
  return getOne(SUPABASE_PENDING_TABLE, `key=eq.${encodeURIComponent(key)}&select=*`);
}
async function deletePendingConfirmationDb(key) {
  return delWhere(SUPABASE_PENDING_TABLE, `key=eq.${encodeURIComponent(key)}`);
}
const pendingConfirmations = new Map();
function setPendingConfirmationMemory(key, payload) { pendingConfirmations.set(key, { ...payload, createdAt: Date.now() }); }
function getPendingConfirmationMemory(key) { return pendingConfirmations.get(key) || null; }
function clearPendingConfirmationMemory(key) { pendingConfirmations.delete(key); }

/* ============================================================================
Quote leads
============================================================================ */
function makeQuoteKey(tradieKey, phone) {
  return `${tradieKey}::${phone || ""}`;
}
async function upsertQuoteLead(key, payload) {
  return upsertRow(SUPABASE_QUOTES_TABLE, { key, ...payload, updated_at: new Date().toISOString() });
}
async function getQuoteLead(key) {
  return getOne(SUPABASE_QUOTES_TABLE, `key=eq.${encodeURIComponent(key)}&select=*`);
}

/* ============================================================================
Analytics (metrics_daily)
============================================================================ */
function todayKey(tradieKey, tz) {
  const d = DateTime.now().setZone(tz).toFormat("yyyy-LL-dd");
  return `${tradieKey}::${d}`;
}
async function incMetric(tradie, fields) {
  if (!supaReady()) return false;
  const key = todayKey(tradie.key, tradie.timezone);
  const existing = await getOne(SUPABASE_METRICS_TABLE, `key=eq.${encodeURIComponent(key)}&select=*`);

  const base = existing || {
    key,
    tradie_key: tradie.key,
    date: DateTime.now().setZone(tradie.timezone).toFormat("yyyy-LL-dd"),
    calls_total: 0,
    missed_calls_saved: 0,
    bookings_created: 0,
    est_revenue: 0,
    updated_at: new Date().toISOString()
  };

  const next = {
    ...base,
    calls_total: Number(base.calls_total || 0) + Number(fields.calls_total || 0),
    missed_calls_saved: Number(base.missed_calls_saved || 0) + Number(fields.missed_calls_saved || 0),
    bookings_created: Number(base.bookings_created || 0) + Number(fields.bookings_created || 0),
    est_revenue: Number(base.est_revenue || 0) + Number(fields.est_revenue || 0),
    updated_at: new Date().toISOString()
  };

  return upsertRow(SUPABASE_METRICS_TABLE, next);
}

/* ============================================================================
Intent detection (heuristic fallback)
============================================================================ */
function detectIntent(text) {
  const t = (text || "").toLowerCase();
  const emergency = ["burst","flood","leak","gas","sparking","no power","smoke","fire","blocked","sewage","overflow","urgent","emergency","asap","now"];
  const quote = ["quote","pricing","how much","estimate","cost","rate"];
  const existing = ["i booked","already booked","existing","last time","repeat","returning"];
  const cancel = ["cancel","reschedule","change","move","postpone"];
  const has = (arr) => arr.some((w) => t.includes(w));
  if (has(cancel)) return "CANCEL_RESCHEDULE";
  if (has(emergency)) return "EMERGENCY";
  if (has(quote)) return "QUOTE";
  if (has(existing)) return "EXISTING_CUSTOMER";
  return "NEW_BOOKING";
}
function intentLabel(intent) {
  switch (intent) {
    case "EMERGENCY": return "Emergency";
    case "QUOTE": return "Quote request";
    case "EXISTING_CUSTOMER": return "Existing customer";
    case "CANCEL_RESCHEDULE": return "Cancel / reschedule";
    default: return "New booking";
  }
}

/* ============================================================================
Session store
============================================================================ */
const sessions = new Map();
function getSession(callSid, fromNumber = "") {
  if (!sessions.has(callSid)) {
    sessions.set(callSid, {
      step: "intent",
      intent: "NEW_BOOKING",
      job: "",
      address: "",
      name: "",
      time: "",
      bookedStartMs: null,
      accessNote: "",
      customerNote: null,
      proposedSlots: [],
      quoteKey: null,
      lastAskedField: "",
      lastPrompt: "",
      tries: 0,
      from: fromNumber || "",
      lastAtAddress: null,
      duplicateEvent: null,
      startedAt: Date.now(),
      _countedCall: false,
      history: [],
      llmTurns: 0,
      abuseStrikes: 0,
      accessEditMode: "replace"
    });
  } else {
    const s = sessions.get(callSid);
    if (fromNumber && !s.from) s.from = fromNumber;
  }
  return sessions.get(callSid);
}
function resetSession(callSid) { sessions.delete(callSid); }

/* ============================================================================
General helpers + validation
============================================================================ */
function cleanSpeech(text) { return text ? String(text).trim().replace(/\s+/g, " ") : ""; }
function addToHistory(session, role, content) {
  if (!session) return;
  session.history = trimHistory([...(session.history || []), { role, content }], 12);
}
function ask(twiml, prompt, actionUrl, options = {}) {
  const gather = twiml.gather({
    input: "speech dtmf",
    numDigits: 1,
    action: actionUrl,
    method: "POST",
    timeout: GATHER_START_TIMEOUT,
    speechTimeout: GATHER_SPEECH_TIMEOUT,
    actionOnEmptyResult: true,
    language: "en-AU",
    profanityFilter: false,
    ...options
  });
  gather.say(prompt || "Sorry, can you repeat that?", { voice: "Polly.Amy", language: "en-AU" });
  twiml.pause({ length: 1 });
}
function normStr(s) { return String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim(); }
function looksLikePhoneOrJunkName(s) {
  const t = String(s || "").trim();
  if (!t) return true;
  if (/\d{6,}/.test(t)) return true;
  if (t.split(/\s+/).length > 5) return true;
  return false;
}
function validateName(speech) {
  const s = String(speech || "").trim();
  if (!s || s.length < 2) return false;
  if (looksLikePhoneOrJunkName(s)) return false;
  if (/(tomorrow|today|am|pm|\d{1,2}:\d{2}|\d{1,2}\s?(am|pm))/i.test(s)) return false;
  return true;
}
function validateAddress(speech) {
  const s = String(speech || "").toLowerCase().trim();
  if (!s || s.length < 6) return false;
  const hasNum = /\d/.test(s);
  const addrHints = [" street"," st"," road"," rd"," avenue"," ave"," drive"," dr"," lane"," ln"," court"," ct"," crescent"," cr"," highway"," hwy"," place"," pl"," parade"," pde"," close"," cl"," terrace"," tce"," circuit"," cct"," way"];
  const hasHint = addrHints.some(h => s.includes(h));
  const hasUnit = /(unit|apt|apartment|lot|suite)\s*\d+/i.test(s);
  return (hasNum && (hasHint || hasUnit)) || (hasHint && s.split(" ").length >= 3);
}
function validateJob(speech) {
  const s = String(speech || "").trim();
  if (!s || s.length < 3) return false;
  if (s.split(/\s+/).length > 30) return true;
  if (/^(yes|no|yeah|nope|correct|wrong)$/i.test(s)) return false;
  return true;
}
function validateAccess(speech) {
  const s = String(speech || "").trim();
  if (!s) return true;
  if (/^(none|no|nope|nah)$/i.test(s)) return true;
  const sl = s.toLowerCase();
  if (/\bno\s+access\b/.test(sl)) return true;
  if (/\bno\s+access\s+notes\b/.test(sl)) return true;
  if (/\bnothing\b/.test(sl) && /\b(access|gate|code|parking|pet|dog|notes?)\b/.test(sl)) return true;
  return s.length >= 2;
}
function shouldReject(step, speech, confidence) {
  if (!speech || speech.length < 2) return true;
  const minConf =
    step === "address" ? 0.55 :
    step === "time" ? 0.45 :
    step === "name" ? 0.35 :
    step === "job" ? 0.25 :
    0.15;
  if (typeof confidence === "number" && confidence > 0 && confidence < minConf) {
    if (speech.split(" ").length <= 3) return true;
  }
  if (step === "address") return !validateAddress(speech);
  if (step === "name") return !validateName(speech);
  if (step === "job") return !validateJob(speech);
  if (step === "access") return !validateAccess(speech);
  return false;
}

/* ============================================================================
Time parsing
============================================================================ */
function normalizeTimeText(text, tz) {
  if (!text) return "";
  let t = String(text).toLowerCase().trim();
  t = t.replace(/\b(\d{1,2})\s*:\s*(\d{2})\s*p\.?\s*m\.?\b/g, "$1:$2pm");
  t = t.replace(/\b(\d{1,2})\s*:\s*(\d{2})\s*a\.?\s*m\.?\b/g, "$1:$2am");
  t = t.replace(/\b(\d{1,2})\s*p\.?\s*m\.?\b/g, "$1pm");
  t = t.replace(/\b(\d{1,2})\s*a\.?\s*m\.?\b/g, "$1am");
  t = t.replace(/\s+/g, " ");
  const now = DateTime.now().setZone(tz);
  if (t.includes("tomorrow")) {
    const d = now.plus({ days: 1 }).toFormat("cccc d LLL yyyy");
    t = t.replace(/\btomorrow\b/g, d);
  }
  if (t.includes("tonight") || t.includes("today")) {
    const d = now.toFormat("cccc d LLL yyyy");
    t = t.replace(/\btoday\b/g, d);
    t = t.replace(/\btonight\b/g, d);
  }
  return t;
}
function parseRequestedDateTime(naturalText, tz) {
  const ref = DateTime.now().setZone(tz).toJSDate();
  const norm = normalizeTimeText(naturalText, tz);
  const results = chrono.parse(norm, ref, { forwardDate: true });
  if (!results || results.length === 0) return null;
  const s = results[0].start;
  if (!s) return null;

  const year = s.get("year");
  const month = s.get("month");
  const day = s.get("day");
  const hour = s.get("hour");
  const minute = s.get("minute") ?? 0;
  if (!year || !month || !day || hour == null) return null;

  const dt = DateTime.fromObject({ year, month, day, hour, minute, second: 0, millisecond: 0 }, { zone: tz });
  return dt.isValid ? dt : null;
}
function toGoogleDateTime(dt) { return dt.toISO({ includeOffset: true, suppressMilliseconds: true }); }
function looksLikeAsap(text) {
  const t = (text || "").toLowerCase();
  return (t.includes("asap") || t.includes("anytime") || t.includes("whenever") || t.includes("dont care") || t.includes("don’t care") || t.includes("no preference") || t.includes("soon as possible") || t === "soon");
}
function formatForVoice(dt) { return dt.toFormat("ccc d LLL, h:mm a"); }
function isAfterHoursNow(tradie) {
  const now = DateTime.now().setZone(tradie.timezone);
  const isBizDay = tradie.businessDays.includes(now.weekday);
  const isBizHours = now.hour >= tradie.businessStartHour && now.hour < tradie.businessEndHour;
  return !(isBizDay && isBizHours);
}
function nextBusinessOpenSlot(tradie) {
  const tz = tradie.timezone;
  let dt = DateTime.now().setZone(tz);
  const isBizDay = tradie.businessDays.includes(dt.weekday);
  const isBizHours = dt.hour >= tradie.businessStartHour && dt.hour < tradie.businessEndHour;
  if (isBizDay && isBizHours) return dt.plus({ minutes: 10 }).startOf("minute");
  dt = dt.plus({ days: 1 }).startOf("day");
  while (!tradie.businessDays.includes(dt.weekday)) dt = dt.plus({ days: 1 }).startOf("day");
  return dt.set({ hour: tradie.businessStartHour, minute: 0, second: 0, millisecond: 0 });
}

/* ============================================================================
Interruption + edits
============================================================================ */
function detectYesNoFromDigits(d) { if (!d) return null; if (d === "1") return "YES"; if (d === "2") return "NO"; return null; }
function detectYesNo(text) {
  const t = (text || "").toLowerCase().trim();
  const yes = ["yes","yeah","yep","correct","that's right","that’s right","sounds good","ok","okay","confirm"];
  const no = ["no","nope","nah","wrong","not that"];
  if (yes.some(w => t === w || t.includes(w))) return "YES";
  if (no.some(w => t === w || t.includes(w))) return "NO";
  return null;
}
function detectCorrection(text) {
  const t = (text || "").toLowerCase();
  return (t.includes("actually") || t.includes("wait") || t.includes("sorry") || t.includes("i meant") || t.includes("correction") || t.includes("change that"));
}
function detectChangeFieldFromDigits(d) { if (!d) return null; if (d === "3") return "time"; if (d === "4") return "address"; if (d === "5") return "job"; if (d === "6") return "name"; return null; }
function detectChangeFieldFromSpeech(text) {
  const t = (text || "").toLowerCase();
  if (t.includes("access") || t.includes("gate") || t.includes("parking") || t.includes("dog") || t.includes("pet") || t.includes("code") || t.includes("notes")) return "access";
  if (t.includes("job")) return "job";
  if (t.includes("address")) return "address";
  if (t.includes("name")) return "name";
  if (t.includes("time") || t.includes("slot") || t.includes("date")) return "time";
  return null;
}
function wantsRepeatOptions(text) {
  const t = (text || "").toLowerCase();
  return t.includes("repeat") || t.includes("say that again") || t.includes("what were the options") || t.includes("options again");
}
function wantsHuman(text) {
  const t = (text || "").toLowerCase();
  return t.includes("human") || t.includes("person") || t.includes("owner") || t.includes("manager") || t.includes("someone real");
}

/* ============================================================================
Access notes normaliser
============================================================================ */
function interpretAccessUtterance(rawSpeech) {
  const s = String(rawSpeech || "").trim();
  const sl = s.toLowerCase();
  if (!s) return { kind: "SKIP" };
  if (/^(none|no|nope|nah|nothing)$/i.test(s)) return { kind: "CLEAR" };
  if (/\bno\s+access\b/.test(sl)) return { kind: "CLEAR" };
  if (/\bno\s+access\s+notes?\b/.test(sl)) return { kind: "CLEAR" };
  if (/\b(don't|dont)\s+change\b/.test(sl)) return { kind: "KEEP" };
  if (/\b(leave\s+it|keep\s+it|all\s+good|no\s+need)\b/.test(sl)) return { kind: "KEEP" };
  const mode = /\b(add|also|another|plus|and\s+also)\b/.test(sl) ? "append" : "replace";
  return { kind: "SET", mode, value: s };
}

/* ============================================================================
Profanity/abuse handling
============================================================================ */
function detectAbuse(text) {
  const t = (text || "").toLowerCase();
  const abusive = ["retard","retarded","idiot","stupid","moron","dumb","fuck you","f*** you","cunt","bitch","slut","kill yourself"];
  return abusive.some(w => t.includes(w));
}
function abuseReply(strikes) {
  if (strikes <= 1) return "I can help with that — let’s keep it respectful. ";
  if (strikes === 2) return "I’m here to help, but I can’t continue with abusive language. ";
  return "I’m going to end this call now. You can call back when you’re ready to book. ";
}

/* ============================================================================
Lightweight slot-fill
============================================================================ */
function trySlotFill(session, speech, tz) {
  const raw = String(speech || "").trim();
  if (!raw) return;
  const dt = parseRequestedDateTime(raw, tz);
  if (dt) { session.time = raw; session.bookedStartMs = dt.toMillis(); }
  if (validateAddress(raw)) session.address = session.address || raw;
  const m = raw.match(/my name is\s+(.+)/i);
  if (m && m[1]) {
    const nm = cleanSpeech(m[1]);
    if (validateName(nm)) session.name = session.name || nm;
  }
  if (/gate|code|parking|dog|pet|call on arrival|buzz|intercom/i.test(raw)) session.accessNote = session.accessNote || raw;
  if (!session.job && /(leak|blocked|hot water|air con|heater|toilet|sink|tap|power|switch|deck|til(e|es)|fence|roof|gutter|drain)/i.test(raw)) session.job = raw;
}

/* ============================================================================
Google Calendar
============================================================================ */
function parseGoogleServiceJson(raw) {
  if (!raw) throw new Error("Missing GOOGLE_SERVICE_JSON");
  let parsed;
  try { parsed = JSON.parse(raw); }
  catch { parsed = JSON.parse(raw.replace(/\r?\n/g, "\\n")); }
  if (typeof parsed === "string") parsed = JSON.parse(parsed);
  return parsed;
}
function getCalendarClient(tradie) {
  const credentials = parseGoogleServiceJson(tradie.googleServiceJson);
  const auth = new google.auth.GoogleAuth({ credentials, scopes: ["https://www.googleapis.com/auth/calendar"] });
  return google.calendar({ version: "v3", auth });
}
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
async function insertCalendarEventWithRetry(calendar, calendarId, requestBody) {
  let lastErr = null;
  for (let attempt = 1; attempt <= CAL_RETRY_ATTEMPTS; attempt++) {
    try { return await calendar.events.insert({ calendarId, requestBody }); }
    catch (err) {
      lastErr = err;
      console.error(`Calendar insert failed (attempt ${attempt}/${CAL_RETRY_ATTEMPTS})`, err?.message || err);
      if (attempt < CAL_RETRY_ATTEMPTS) await sleep(attempt === 1 ? 200 : 800);
    }
  }
  throw lastErr;
}
async function getLastBookingAtAddress(calendar, calendarId, tz, address) {
  const timeMin = DateTime.now().setZone(tz).minus({ days: 365 }).toISO();
  const timeMax = DateTime.now().setZone(tz).toISO();
  const resp = await calendar.events.list({ calendarId, q: address, timeMin, timeMax, singleEvents: true, orderBy: "startTime", maxResults: 30 });
  const items = resp?.data?.items || [];
  if (!items.length) return null;
  const addrN = normStr(address);
  const candidates = items.filter((ev) => {
    const locN = normStr(ev.location);
    const sumN = normStr(ev.summary);
    const descN = normStr(ev.description);
    return locN.includes(addrN) || sumN.includes(addrN) || descN.includes(addrN);
  });
  const last = (candidates.length ? candidates : items).sort((a, b) => {
    const aISO = a.start?.dateTime || a.start?.date || "";
    const bISO = b.start?.dateTime || b.start?.date || "";
    return String(aISO).localeCompare(String(bISO));
  }).pop();
  if (!last) return null;
  const whenISO = last.start?.dateTime || last.start?.date;
  const when = whenISO ? DateTime.fromISO(whenISO, { setZone: true }).setZone(tz) : null;
  return { summary: last.summary || "Previous booking", whenText: when ? when.toFormat("ccc d LLL yyyy") : "Unknown date" };
}
async function findDuplicate(calendar, calendarId, tz, name, address, startDt) {
  const t0 = startDt.minus({ days: DUP_WINDOW_DAYS });
  const t1 = startDt.plus({ days: DUP_WINDOW_DAYS });
  const resp = await calendar.events.list({
    calendarId,
    q: address,
    timeMin: t0.toISO(),
    timeMax: t1.toISO(),
    singleEvents: true,
    orderBy: "startTime",
    maxResults: 50
  });
  const items = resp?.data?.items || [];
  if (!items.length) return null;
  const nameN = normStr(name);
  const addrN = normStr(address);
  for (const ev of items) {
    const locN = normStr(ev.location);
    const sumN = normStr(ev.summary);
    const descN = normStr(ev.description);
    const addrMatch = locN.includes(addrN) || descN.includes(addrN) || sumN.includes(addrN);
    const nameMatch = sumN.includes(nameN) || descN.includes(nameN);
    if (addrMatch && nameMatch) {
      const whenISO = ev.start?.dateTime || ev.start?.date;
      const when = whenISO ? DateTime.fromISO(whenISO, { setZone: true }).setZone(tz) : null;
      return { id: ev.id, summary: ev.summary || "Existing booking", whenText: when ? when.toFormat("ccc d LLL, h:mm a") : "Unknown time" };
    }
  }
  return null;
}

/* ============================================================================
Scheduling: Free/Busy + next 3 slots
============================================================================ */
function overlaps(aStart, aEnd, bStart, bEnd) { return aStart < bEnd && bStart < aEnd; }
function isWithinBusinessHours(tradie, dt) {
  const inDay = tradie.businessDays.includes(dt.weekday);
  const inHours = dt.hour >= tradie.businessStartHour && dt.hour < tradie.businessEndHour;
  return inDay && inHours;
}
function slotEnd(tradie, startDt) { return startDt.plus({ minutes: tradie.slotMinutes }); }
async function getBusy(calendar, calendarId, tz, timeMinISO, timeMaxISO) {
  const resp = await calendar.freebusy.query({
    requestBody: { timeMin: timeMinISO, timeMax: timeMaxISO, timeZone: tz, items: [{ id: calendarId }] }
  });
  const cal = resp?.data?.calendars?.[calendarId];
  return Array.isArray(cal?.busy) ? cal.busy : [];
}
async function nextAvailableSlots(tradie, startSearchDt, count = 3) {
  if (!(tradie.calendarId && tradie.googleServiceJson)) return [];
  const tz = tradie.timezone;
  let start = startSearchDt;
  if (!start || !DateTime.isDateTime(start) || !start.isValid) start = DateTime.now().setZone(tz).plus({ minutes: 10 }).startOf("minute");
  else start = start.setZone(tz);

  const calendar = getCalendarClient(tradie);
  const searchEnd = start.plus({ days: 14 });

  const busy = await getBusy(calendar, tradie.calendarId, tz, start.toISO({ includeOffset: true }), searchEnd.toISO({ includeOffset: true }));
  const busyIntervals = busy.map(b => ({
    start: DateTime.fromISO(b.start, { setZone: true }).setZone(tz),
    end: DateTime.fromISO(b.end, { setZone: true }).setZone(tz)
  })).filter(x => x.start.isValid && x.end.isValid);

  const results = [];
  let cursor = start.startOf("minute");
  if (!isWithinBusinessHours(tradie, cursor)) cursor = nextBusinessOpenSlot(tradie);

  while (results.length < count && cursor < searchEnd) {
    if (!isWithinBusinessHours(tradie, cursor)) {
      cursor = cursor.plus({ days: 1 }).startOf("day");
      while (!tradie.businessDays.includes(cursor.weekday)) cursor = cursor.plus({ days: 1 }).startOf("day");
      cursor = cursor.set({ hour: tradie.businessStartHour, minute: 0, second: 0, millisecond: 0 });
      continue;
    }

    const startSlot = cursor;
    const endSlot = slotEnd(tradie, startSlot).plus({ minutes: tradie.bufferMinutes || 0 });

    if (endSlot.hour > tradie.businessEndHour || (endSlot.hour === tradie.businessEndHour && endSlot.minute > 0)) {
      cursor = cursor.plus({ days: 1 }).startOf("day");
      continue;
    }

    const clashes = busyIntervals.some(b => overlaps(startSlot.toMillis(), endSlot.toMillis(), b.start.toMillis(), b.end.toMillis()));
    if (!clashes) {
      results.push(startSlot);
      cursor = cursor.plus({ minutes: tradie.slotMinutes });
      continue;
    }
    cursor = cursor.plus({ minutes: 15 });
  }
  return results;
}
function slotsVoiceLine(slots, tz) {
  const parts = slots.slice(0, 3).map((dt, i) => {
    const label = i === 0 ? "First" : i === 1 ? "Second" : "Third";
    return `${label}: ${dt.setZone(tz).toFormat("ccc h:mm a")}`;
  });
  return parts.join(". ") + ".";
}
function pickSlotFromSpeechOrDigits(speech, digits) {
  if (digits === "1") return 0;
  if (digits === "2") return 1;
  if (digits === "3") return 2;
  const s = (speech || "").toLowerCase();
  if (s.includes("first") || s.includes("one") || s.includes("1")) return 0;
  if (s.includes("second") || s.includes("two") || s.includes("2")) return 1;
  if (s.includes("third") || s.includes("three") || s.includes("3")) return 2;
  return null;
}

/* ============================================================================
Handoff + missed revenue alerts
============================================================================ */
function flowProgress(session) {
  const parts = [];
  if (session.intent) parts.push(`Intent=${intentLabel(session.intent)}`);
  if (session.name) parts.push(`Name=${session.name}`);
  if (session.job) parts.push(`Job=${session.job}`);
  if (session.address) parts.push(`Address=${session.address}`);
  if (session.time) parts.push(`Time=${session.time}`);
  if (session.accessNote) parts.push(`AccessNote=${session.accessNote}`);
  return parts.join(" | ") || "No details captured";
}
async function missedRevenueAlert(tradie, session, reason) {
  await incMetric(tradie, { missed_calls_saved: 1, est_revenue: tradie.avgJobValue * tradie.closeRate });
  const body =
    `MISSED LEAD ALERT 💸\n` +
    `Reason: ${reason}\n` +
    `TradieKey: ${tradie.key}\n` +
    `Caller: ${session.from || "Unknown"}\n` +
    `${flowProgress(session)}\n` +
    `Action: Call/text back ASAP.`;
  await sendOwnerSms(tradie, body).catch(() => {});
}

/* ============================================================================
ADMIN DASH
============================================================================ */
app.get("/admin/metrics", async (req, res) => {
  const pw = String(req.query.pw || "");
  if (!ADMIN_DASH_PASSWORD || pw !== ADMIN_DASH_PASSWORD) return res.status(403).json({ error: "Forbidden" });

  const tid = String(req.query.tid || "default");
  const tradie = await getTradieConfig({ query: { tid }, body: {} });

  if (!supaReady()) return res.json({ ok: false, error: "Supabase not configured" });

  const end = DateTime.now().setZone(tradie.timezone);
  const start = end.minus({ days: 30 }).toFormat("yyyy-LL-dd");
  const endStr = end.toFormat("yyyy-LL-dd");

  const url = `${SUPABASE_URL}/rest/v1/${SUPABASE_METRICS_TABLE}?tradie_key=eq.${encodeURIComponent(tradie.key)}&date=gte.${start}&date=lte.${endStr}&select=*`;
  try {
    const r = await fetch(url, { headers: supaHeaders() });
    const rows = await r.json();

    const totals = (Array.isArray(rows) ? rows : []).reduce(
      (acc, x) => {
        acc.calls_total += Number(x.calls_total || 0);
        acc.missed_calls_saved += Number(x.missed_calls_saved || 0);
        acc.bookings_created += Number(x.bookings_created || 0);
        acc.est_revenue += Number(x.est_revenue || 0);
        return acc;
      },
      { calls_total: 0, missed_calls_saved: 0, bookings_created: 0, est_revenue: 0 }
    );

    return res.json({ ok: true, tradie_key: tradie.key, window: { start, end: endStr }, totals, rows });
  } catch (e) {
    return res.json({ ok: false, error: String(e?.message || e) });
  }
});

/* ============================================================================
VOICE ROUTE
============================================================================ */
app.post("/voice", async (req, res) => {
  if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

  const tradie = await getTradieConfig(req);
  // Hard stop if subscription disabled
  if (tradie.status && tradie.status !== "ACTIVE") {
    const twiml = new VoiceResponse();
    twiml.say("This service is currently unavailable.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.hangup();
    return res.type("text/xml").send(twiml.toString());
  }

  const twiml = new VoiceResponse();

  const callSid = req.body.CallSid || req.body.CallSID || "unknown";
  const fromNumber = req.body.From || "";
  const confidence = Number(req.body.Confidence || 0);
  const speechRaw = req.body.SpeechResult || "";
  const speech = cleanSpeech(speechRaw);
  const digits = String(req.body.Digits || "").trim();

  const session = getSession(callSid, fromNumber);

  if (!session._countedCall) {
    session._countedCall = true;
    await incMetric(tradie, { calls_total: 1 });
  }

  if (session.step && session.step !== "clarify") session.lastAskedField = session.step;

  console.log(`TID=${tradie.key} CALLSID=${callSid} TO=${req.body.To} FROM=${fromNumber} STEP=${session.step} Speech="${speech}" Digits="${digits}" Confidence=${confidence}`);

  if (speech) addToHistory(session, "user", speech);

  if (speech && detectAbuse(speech)) {
    session.abuseStrikes += 1;
    const prefix = abuseReply(session.abuseStrikes);
    if (session.abuseStrikes >= 3) {
      twiml.say(prefix, { voice: "Polly.Amy", language: "en-AU" });
      twiml.hangup();
      resetSession(callSid);
      return res.type("text/xml").send(twiml.toString());
    }
    const prompt = prefix + (session.lastPrompt || "How can we help today?");
    session.lastPrompt = prompt;
    addToHistory(session, "assistant", prompt);
    ask(twiml, prompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
    return res.type("text/xml").send(twiml.toString());
  }

  if (!speech && !digits) {
    session.tries += 1;

    if (session.tries >= MAX_SILENCE_TRIES) {
      await missedRevenueAlert(tradie, session, "Silent timeout");
      twiml.say("No worries. We'll call you back shortly.", { voice: "Polly.Amy", language: "en-AU" });
      twiml.hangup();
      resetSession(callSid);
      return res.type("text/xml").send(twiml.toString());
    }

    const promptMap = {
      intent: "How can we help today? You can say emergency, quote, reschedule, or new booking.",
      job: "What job do you need help with?",
      address: "What is the address?",
      name: "What is your name?",
      access: "Any access notes like gate code, parking, or pets? Say none if not.",
      time: "What time would you like?",
      pickSlot: "I’m booked then. Say first, second, or third, or say another time.",
      confirm: "Say yes to confirm, or say what you want to change — job, address, name, time, or access notes.",
      clarify: "No worries — what should I change? job, address, name, time, or access notes."
    };

    const prompt = session.lastPrompt || promptMap[session.step] || "Can you repeat that?";
    session.lastPrompt = prompt;
    addToHistory(session, "assistant", prompt);
    ask(twiml, prompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
    return res.type("text/xml").send(twiml.toString());
  }

  session.tries = 0;

  try {
    const tz = tradie.timezone;

    if (speech) trySlotFill(session, speech, tz);

    if (speech && wantsHuman(speech)) {
      await missedRevenueAlert(tradie, session, "Customer requested a human");
      twiml.say("No worries — we’ll have someone contact you shortly.", { voice: "Polly.Amy", language: "en-AU" });
      twiml.hangup();
      resetSession(callSid);
      return res.type("text/xml").send(twiml.toString());
    }

    const yn = detectYesNoFromDigits(digits) || detectYesNo(speech);
    const changeField = detectChangeFieldFromDigits(digits) || detectChangeFieldFromSpeech(speech);
    const corrected = speech ? detectCorrection(speech) : false;

    const shouldUseLlm =
      llmReady() &&
      session.llmTurns < LLM_MAX_TURNS &&
      (!LLM_REQUIRE_FOR_OFFSCRIPT || isOffScriptSpeech(speech));

    if (shouldUseLlm) {
      session.llmTurns += 1;
      const llm = await callLlm(tradie, session, speech);
      if (llm) {
        if (llm.intent && llm.intent !== "UNKNOWN") session.intent = llm.intent;
        const f = llm.fields || {};
        if (typeof f.job === "string" && f.job.trim().length >= 2) session.job = session.job || f.job.trim();
        if (typeof f.address === "string" && f.address.trim().length >= 4 && validateAddress(f.address)) session.address = session.address || f.address.trim();
        if (typeof f.name === "string" && f.name.trim().length >= 2 && validateName(f.name)) session.name = session.name || f.name.trim();
        if (typeof f.access_note === "string" && f.access_note.trim().length >= 2) session.accessNote = session.accessNote || f.access_note.trim();
        if (typeof f.time_text === "string" && f.time_text.trim().length >= 2) {
          session.time = session.time || f.time_text.trim();
          const dtGuess = parseRequestedDateTime(session.time, tz);
          if (dtGuess) session.bookedStartMs = session.bookedStartMs || dtGuess.toMillis();
        }

        const replyParts = [];
        if (llm.smalltalk_reply) replyParts.push(String(llm.smalltalk_reply).trim());
        if (llm.next_question) replyParts.push(String(llm.next_question).trim());
        const mergedPrompt = replyParts.filter(Boolean).join(" ");

        const suggested = String(llm.suggested_step || "").trim();
        const allowedSteps = new Set(["intent","job","address","name","access","time","pickSlot","confirm"]);
        if (allowedSteps.has(suggested)) {
          if (!["confirm","pickSlot"].includes(session.step)) session.step = suggested;
        }

        session.lastPrompt = mergedPrompt || session.lastPrompt;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }
    }

    const canGlobalInterrupt = !["intent", "clarify", "confirm", "pickSlot", "access"].includes(session.step);

    if (canGlobalInterrupt && (corrected || changeField)) {
      const target = changeField || session.lastAskedField || "clarify";
      session.step = target === "clarify" ? "clarify" : target;

      session.lastPrompt =
        session.step === "job" ? "Sure — what job do you need help with?"
        : session.step === "address" ? "Sure — what is the address?"
        : session.step === "name" ? "Sure — what is your name?"
        : session.step === "access" ? "Sure — what access notes should I add or update?"
        : session.step === "time" ? "Sure — what time would you like?"
        : "No worries — what should I change? job, address, name, time, or access notes?";

      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    if (canGlobalInterrupt && yn === "NO") {
      session.step = "clarify";
      session.lastPrompt = "No worries — what should I change? job, address, name, time, or access notes?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    if (session.step === "clarify") {
      const target = changeField || detectChangeFieldFromSpeech(speech) || session.lastAskedField || "";
      if (target) {
        session.step = target;
        session.lastPrompt =
          target === "job" ? "Sure — what job do you need help with?"
          : target === "address" ? "Sure — what is the address?"
          : target === "name" ? "Sure — what is your name?"
          : target === "access" ? "Sure — what access notes should I add or update?"
          : "Sure — what time would you like?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      session.lastPrompt = "Sorry — what should I change? job, address, name, time, or access notes?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    if (!digits && session.step !== "confirm" && session.step !== "pickSlot" && shouldReject(session.step, speech, confidence)) {
      session.lastPrompt =
        session.step === "address" ? "Sorry — can you say the full street address, like number and street name?"
        : session.step === "name" ? "Sorry — what name should I put this under?"
        : session.step === "job" ? "Sorry — what job do you need help with?"
        : session.step === "access" ? "Sorry — any access notes like gate code, parking, or pets? Say none if not."
        : "Sorry, can you repeat that?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: intent
    if (session.step === "intent") {
      session.intent = detectIntent(speech || "");
      if (session.from) session.customerNote = await getCustomerNote(tradie.key, session.from);

      if (session.intent === "CANCEL_RESCHEDULE") {
        session.step = "name";
        session.lastPrompt = "No worries. What is your name so we can reschedule you?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      if (session.intent === "QUOTE") {
        session.step = "job";
        session.lastPrompt = "Sure. What do you need a quote for?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      if (session.intent === "EMERGENCY") {
        session.step = "address";
        session.lastPrompt = "Understood. What is the address right now?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      session.step = "job";
      session.lastPrompt = "What job do you need help with?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: job
    if (session.step === "job") {
      if (speech) session.job = speech;
      session.step = "address";
      session.lastPrompt = "What is the address?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: address
    if (session.step === "address") {
      if (speech) session.address = speech;
      if (tradie.calendarId && tradie.googleServiceJson) {
        try {
          const calendar = getCalendarClient(tradie);
          session.lastAtAddress = await getLastBookingAtAddress(calendar, tradie.calendarId, tz, session.address);
        } catch {}
      }
      session.step = "name";
      session.lastPrompt = "What is your name?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: name
    if (session.step === "name") {
      if (speech) session.name = speech;

      if (session.intent === "CANCEL_RESCHEDULE") {
        await missedRevenueAlert(tradie, session, "Cancel/reschedule request");
        twiml.say("No worries. We'll contact you shortly to reschedule.", { voice: "Polly.Amy", language: "en-AU" });
        twiml.hangup();
        resetSession(callSid);
        return res.type("text/xml").send(twiml.toString());
      }

      session.step = "access";
      session.lastPrompt = "Any access notes like gate code, parking, or pets? Say none if not.";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: access
    if (session.step === "access") {
      const a = interpretAccessUtterance(speech);
      if (a.kind === "CLEAR") session.accessNote = "";
      else if (a.kind === "SET") {
        session.accessEditMode = a.mode || "replace";
        if (a.value) {
          session.accessNote =
            session.accessEditMode === "append" && session.accessNote
              ? `${session.accessNote} | ${a.value}`
              : a.value;
        }
      }

      if (session.intent === "QUOTE") {
        if (session.from) {
          const qKey = makeQuoteKey(tradie.key, session.from);
          session.quoteKey = qKey;
          const existingNote = session.customerNote ? `Existing note: ${session.customerNote}` : "";
          const combinedNote = [existingNote, session.accessNote].filter(Boolean).join(" | ");

          await upsertQuoteLead(qKey, {
            key: qKey,
            tradie_key: tradie.key,
            customer_phone: session.from,
            name: session.name || "",
            address: session.address || "",
            job: session.job || "",
            note: combinedNote,
            status: "OPEN",
            media_urls: []
          });

          await sendCustomerSms(
            tradie,
            session.from,
            `QUOTE REQUEST RECEIVED ✅\nReply to this SMS with photos/videos of the job.\nInclude any extra notes.\nWe’ll get back to you shortly.`
          ).catch(() => {});
        }

        twiml.say("Thanks. Please reply to the SMS with photos of the job and we’ll send a quote shortly.", { voice: "Polly.Amy", language: "en-AU" });
        twiml.hangup();
        resetSession(callSid);
        return res.type("text/xml").send(twiml.toString());
      }

      session.step = "time";
      session.lastPrompt = "What time would you like?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: time
    if (session.step === "time") {
      session.time = speech || session.time || "";

      let dt = null;
      if (session.bookedStartMs) dt = DateTime.fromMillis(session.bookedStartMs, { zone: tz });
      else {
        if (!looksLikeAsap(session.time)) dt = parseRequestedDateTime(session.time, tz);
        if (!dt && isAfterHoursNow(tradie)) dt = nextBusinessOpenSlot(tradie);
        if (!dt) dt = DateTime.now().setZone(tz).plus({ minutes: 10 }).startOf("minute");
      }

      if (tradie.calendarId && tradie.googleServiceJson) {
        const slots = await nextAvailableSlots(tradie, dt, 3);
        if (slots.length === 0) {
          await missedRevenueAlert(tradie, session, "No availability found (14d) — manual scheduling");
          twiml.say("Thanks. We’ll call you back shortly to lock in a time.", { voice: "Polly.Amy", language: "en-AU" });
          twiml.hangup();
          resetSession(callSid);
          return res.type("text/xml").send(twiml.toString());
        }

        const first = slots[0];
        const deltaMin = Math.abs(first.diff(dt, "minutes").minutes);
        if (deltaMin > 5) {
          session.proposedSlots = slots.map(x => x.toMillis());
          session.step = "pickSlot";
          session.lastPrompt = `We’re booked at that time. I can do: ${slotsVoiceLine(slots, tz)} Say first, second, or third — or tell me another time. (Or press 1, 2, or 3)`;
          addToHistory(session, "assistant", session.lastPrompt);
          ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
          return res.type("text/xml").send(twiml.toString());
        }
        dt = first;
      }

      session.bookedStartMs = dt.toMillis();
      session.duplicateEvent = null;

      if (tradie.calendarId && tradie.googleServiceJson) {
        try {
          const calendar = getCalendarClient(tradie);
          const dup = await findDuplicate(calendar, tradie.calendarId, tz, session.name, session.address, dt);
          if (dup) session.duplicateEvent = dup;
        } catch {}
      }

      const whenForVoice = formatForVoice(dt);
      const noteLine = session.customerNote ? ` I have a note on your account: ${session.customerNote}.` : "";
      const accessLine = session.accessNote ? ` Access notes: ${session.accessNote}.` : "";

      session.step = "confirm";
      session.lastPrompt =
        `Alright. I heard: ${session.job}, at ${session.address}, for ${session.name}, on ${whenForVoice}.` +
        `${noteLine}${accessLine} ` +
        `Is that correct? Say yes to confirm — or say what you want to change: job, address, name, time, or access notes. (Press 1 yes, 2 no)`;
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: pickSlot
    if (session.step === "pickSlot") {
      if (speech && wantsRepeatOptions(speech)) {
        const slots = (session.proposedSlots || []).map(ms => DateTime.fromMillis(ms, { zone: tz }));
        session.lastPrompt = `No worries. Options are: ${slotsVoiceLine(slots, tz)} Say first, second, or third — or tell me another time.`;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      const dtTry = speech ? parseRequestedDateTime(speech, tz) : null;
      if (dtTry) {
        session.step = "time";
        session.time = speech;
        session.bookedStartMs = dtTry.toMillis();
        session.lastPrompt = "Got it. Let me check that time.";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      const idx = pickSlotFromSpeechOrDigits(speech, digits);
      const slots = (session.proposedSlots || []).map(ms => DateTime.fromMillis(ms, { zone: tz }));

      if (idx == null || !slots[idx]) {
        session.lastPrompt = "Say first, second, or third. Or press 1, 2, or 3. Or tell me another time.";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      const chosen = slots[idx];
      session.bookedStartMs = chosen.toMillis();
      session.step = "confirm";

      const whenForVoice = formatForVoice(chosen);
      const noteLine = session.customerNote ? ` I have a note on your account: ${session.customerNote}.` : "";
      const accessLine = session.accessNote ? ` Access notes: ${session.accessNote}.` : "";

      session.lastPrompt =
        `Perfect. ${whenForVoice}.` +
        `${noteLine}${accessLine} ` +
        `Is that correct? Say yes to confirm — or say what you want to change: job, address, name, time, or access notes.`;
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: confirm
    if (session.step === "confirm") {
      const s = (speech || "").toLowerCase().trim();
      const keepTime = /(dont|don't)\s+change\s+the\s+time|keep\s+the\s+time|time\s+is\s+fine|leave\s+the\s+time/i.test(s);
      const yn2 = detectYesNoFromDigits(digits) || detectYesNo(speech);
      const changeField2 = detectChangeFieldFromSpeech(speech);

      if (changeField2) {
        session.step = changeField2;
        session.lastPrompt =
          changeField2 === "job" ? "Sure — what job should I put down instead?"
          : changeField2 === "address" ? "Sure — what’s the correct address?"
          : changeField2 === "name" ? "Sure — what name should I use?"
          : changeField2 === "access" ? "Sure — what access notes should I add or update?"
          : "Sure — what time would you like instead?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      const isYes = keepTime || yn2 === "YES" || s.includes("yes") || s.includes("yeah") || s.includes("yep") || s.includes("correct") || s.includes("confirm");
      const isNo = !keepTime && (yn2 === "NO" || s === "no" || s === "nope" || s.includes("wrong") || s.includes("not correct"));

      if (isNo) {
        session.step = "clarify";
        session.lastPrompt = "No worries — what should I change? job, address, name, time, or access notes?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      if (!isYes) {
        session.lastPrompt = "Sorry — say yes to confirm, or tell me what you want to change: job, address, name, time, or access notes.";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return res.type("text/xml").send(twiml.toString());
      }

      const start = DateTime.fromMillis(session.bookedStartMs || Date.now(), { zone: tz });
      const end = start.plus({ minutes: tradie.slotMinutes });
      const displayWhen = start.toFormat("ccc d LLL yyyy, h:mm a");
      const summaryText = `${session.name} needs ${session.job} at ${session.address}.`;

      await incMetric(tradie, { bookings_created: 1, est_revenue: tradie.avgJobValue });

      if (session.from && session.accessNote) await setCustomerNote(tradie.key, session.from, session.accessNote).catch(() => {});

      if (session.from) {
        const customerTxt =
          `Booking request received ✅\n` +
          `Type: ${intentLabel(session.intent)}\n` +
          `Job: ${session.job}\n` +
          `Address: ${session.address}\n` +
          `When: ${displayWhen}\n` +
          (session.accessNote ? `Access: ${session.accessNote}\n` : "") +
          `We’ll confirm shortly.\n` +
          `Reply Y to confirm / N to reschedule.`;

        sendCustomerSms(tradie, session.from, customerTxt).catch(() => {});

        const pendingKey = makePendingKey(tradie.key, session.from);
        const payload = {
          key: pendingKey,
          tradie_key: tradie.key,
          customer_phone: session.from,
          name: session.name,
          job: session.job,
          address: session.address,
          when_text: displayWhen,
          timezone: tz
        };

        const wroteDb = await upsertPendingConfirmationDb(pendingKey, payload);
        if (!wroteDb) setPendingConfirmationMemory(pendingKey, payload);
      }

      const historyLine = session.lastAtAddress ? `\nHistory: ${session.lastAtAddress.summary} on ${session.lastAtAddress.whenText}` : "";
      const memoryLine = session.customerNote ? `\nCustomer note (existing): ${session.customerNote}` : "";
      const accessLine = session.accessNote ? `\nAccess note (new): ${session.accessNote}` : "";

      if (tradie.calendarId && tradie.googleServiceJson) {
        const calendar = getCalendarClient(tradie);
        await insertCalendarEventWithRetry(calendar, tradie.calendarId, {
          summary: `${session.job} - ${session.name}`,
          description:
            `${summaryText}\n` +
            `Intent: ${intentLabel(session.intent)}\n` +
            `Caller: ${session.from || "Unknown"}\n` +
            `Spoken time: ${session.time}\n` +
            (session.customerNote ? `Customer note (existing): ${session.customerNote}\n` : "") +
            (session.accessNote ? `Access note (new): ${session.accessNote}\n` : ""),
          location: session.address,
          start: { dateTime: toGoogleDateTime(start), timeZone: tz },
          end: { dateTime: toGoogleDateTime(end), timeZone: tz }
        }).catch(async (calErr) => {
          await sendOwnerSms(
            tradie,
            `MANUAL FOLLOW-UP NEEDED (Calendar failed)\n` +
              `Intent: ${intentLabel(session.intent)}\n` +
              `Name: ${session.name}\n` +
              `Job: ${session.job}\n` +
              `Address: ${session.address}\n` +
              `Caller: ${session.from || "Unknown"}\n` +
              `Spoken: ${session.time}\n` +
              `Intended: ${displayWhen} (${tz})${historyLine}${memoryLine}${accessLine}\n` +
              `Reason: ${calErr?.message || calErr}`
          ).catch(() => {});
        });
      }

      await sendOwnerSms(
        tradie,
        `NEW ${intentLabel(session.intent).toUpperCase()} ✅\n` +
          `Name: ${session.name}\n` +
          `Job: ${session.job}\n` +
          `Address: ${session.address}\n` +
          `Caller: ${session.from || "Unknown"}\n` +
          `Booked: ${displayWhen} (${tz})${historyLine}${memoryLine}${accessLine}`
      ).catch(() => {});

      twiml.say(`Booked. Thanks ${session.name}. We will see you ${formatForVoice(start)}.`, { voice: "Polly.Amy", language: "en-AU" });

      resetSession(callSid);
      twiml.hangup();
      return res.type("text/xml").send(twiml.toString());
    }

    session.step = "intent";
    session.lastPrompt = "How can we help today? You can say emergency, quote, reschedule, or new booking.";
    addToHistory(session, "assistant", session.lastPrompt);
    ask(twiml, session.lastPrompt, "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
    return res.type("text/xml").send(twiml.toString());
  } catch (err) {
    console.error("VOICE ERROR:", err);
    twiml.say("Sorry, there was a system error. Please try again.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.hangup();
    resetSession(req.body.CallSid || req.body.CallSID || "unknown");
    return res.type("text/xml").send(twiml.toString());
  }
});

/* ============================================================================
SMS ROUTE (customer replies Y/N + QUOTE photos)
============================================================================ */
app.post("/sms", async (req, res) => {
  if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

  const tradie = await getTradieConfig(req);
  if (tradie.status && tradie.status !== "ACTIVE") {
    const twiml = new MessagingResponse();
    twiml.message("Service unavailable.");
    return res.type("text/xml").send(twiml.toString());
  }

  const from = (req.body.From || "").trim();
  const body = (req.body.Body || "").trim();
  const bodyLower = body.toLowerCase();

  const twiml = new MessagingResponse();

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

  if (supaReady()) {
    const qKey = makeQuoteKey(tradie.key, from);
    const lead = await getQuoteLead(qKey);
    if (lead && body) {
      await upsertQuoteLead(qKey, { ...lead, last_message: body, status: lead.status || "OPEN" });
      twiml.message("Thanks — we received your message ✅");
      return res.type("text/xml").send(twiml.toString());
    }
  }

  twiml.message("Thanks — we received your message. If you need help, reply with your address or call us back.");
  return res.type("text/xml").send(twiml.toString());
});

/* ============================================================================
Health check
============================================================================ */
app.get("/", (req, res) => res.status(200).send("Voice bot running (SaaS)"));

/* ============================================================================
Listen
============================================================================ */
const PORT = Number(process.env.PORT || 10000);
if (!PORT || Number.isNaN(PORT)) throw new Error("PORT missing/invalid");
app.listen(PORT, () => console.log("Server listening on", PORT));

