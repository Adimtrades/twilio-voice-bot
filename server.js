// server.js
// True SaaS: Stripe → Supabase → auto Twilio number provisioning + multi-tenant routing
// Node 18+
//
// ✅ Deploy-safe:
// - const app = express() before routes
// - app.listen(PORT)
// - raw body capture for Twilio signature validation
// - Stripe webhook uses express.raw({ type: "application/json" })
// - unhandled rejection + uncaught exception logging
//
// You MUST create Supabase tables (recommended):
// 1) tradies (source of truth)
// 2) customer_prefs (optional: customer memory)
// 3) pending_confirmations (optional: inbound SMS Y/N)
// 4) metrics_daily (optional: analytics)
// 5) quote_leads (optional: quote flow)
//
// Required ENV:
// PORT, BASE_URL, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN,
// STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET,
// STRIPE_PRICE_BASIC, STRIPE_PRICE_PRO,
// SUPABASE_URL, SUPABASE_SERVICE_KEY
//
// Optional ENV:
// TRADIES_JSON (fallback), TWILIO_BUY_COUNTRY=AU, TWILIO_BUY_AREA_CODE=2,
// OWNER_SMS_TO (fallback), TWILIO_SMS_FROM (fallback),
// REQUIRE_TWILIO_SIG=false,
// GOOGLE_CALENDAR_ID / GOOGLE_SERVICE_JSON (fallback per-tenant)
// LLM_ENABLED=false, OPENAI_API_KEY, LLM_BASE_URL, LLM_MODEL
// ADMIN_DASH_PASSWORD

try { require("dotenv").config(); } catch {}

const express = require("express");
const twilio = require("twilio");
const stripe = require("stripe")(process.env.STRIPE_SECRET_KEY || "");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");
const { google } = require("googleapis");

// ----------------------------------------------------------------------------
// App bootstrap
// ----------------------------------------------------------------------------
const app = express();
app.set("trust proxy", true);

// ----------------------------------------------------------------------------
// Process-level safety
// ----------------------------------------------------------------------------
process.on("unhandledRejection", (reason) => console.error("UNHANDLED REJECTION:", reason));
process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
  process.exit(1);
});

// ----------------------------------------------------------------------------
// RAW BODY CAPTURE (Twilio signature validation needs raw x-www-form-urlencoded)
// ----------------------------------------------------------------------------
function rawBodySaver(req, res, buf) {
  try { req.rawBody = buf?.toString("utf8") || ""; } catch { req.rawBody = ""; }
}

// Twilio posts x-www-form-urlencoded for Voice/SMS (but DON'T touch Stripe webhook raw body)
app.use((req, res, next) => {
  if (req.originalUrl === "/stripe/webhook") return next();
  return express.urlencoded({ extended: false, verify: rawBodySaver })(req, res, next);
});


// JSON for everything except Stripe webhook (must be raw)
app.use((req, res, next) => {
  if (req.originalUrl === "/stripe/webhook") return next();
  return express.json({ limit: "1mb" })(req, res, next);
});

const VoiceResponse = twilio.twiml.VoiceResponse;
const MessagingResponse = twilio.twiml.MessagingResponse;

// ----------------------------------------------------------------------------
// Helpers: Base URL
// ----------------------------------------------------------------------------
function getBaseUrl(req) {
  const envBase = (process.env.BASE_URL || "").trim().replace(/\/+$/, "");
  if (envBase) return envBase;

  const proto = (req.headers["x-forwarded-proto"] || "https").split(",")[0].trim();
  const host = (req.headers["x-forwarded-host"] || req.headers["host"] || "").split(",")[0].trim();
  return `${proto}://${host}`.replace(/\/+$/, "");
}

// ----------------------------------------------------------------------------
// Supabase REST helpers
// ----------------------------------------------------------------------------
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || "";

const SUPABASE_TRADIES_TABLE = process.env.SUPABASE_TRADIES_TABLE || "tradies";
const SUPABASE_PREFS_TABLE = process.env.SUPABASE_TABLE || "customer_prefs";
const SUPABASE_PENDING_TABLE = process.env.SUPABASE_PENDING_TABLE || "pending_confirmations";
const SUPABASE_METRICS_TABLE = process.env.SUPABASE_METRICS_TABLE || "metrics_daily";
const SUPABASE_QUOTES_TABLE = process.env.SUPABASE_QUOTES_TABLE || "quote_leads";

function supaReady() {
  return !!(SUPABASE_URL && SUPABASE_SERVICE_KEY);
}
function supaHeaders() {
  return {
    apikey: SUPABASE_SERVICE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
    "Content-Type": "application/json"
  };
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

// ----------------------------------------------------------------------------
// Multi-tenant source of truth: Supabase tradies, fallback TRADIES_JSON
// ----------------------------------------------------------------------------
function parseTradiesJson() {
  const raw = process.env.TRADIES_JSON;
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") return parsed;
    return {};
  } catch {
    return {};
  }
}
const TRADIES_FALLBACK = parseTradiesJson();

// Small routing cache (fast)
const TRADIE_CACHE = new Map(); // key -> {data, exp}
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

async function loadTradieRow(req) {
  const tid = String(req.query?.tid || req.body?.tid || "").trim();
  const to = String(req.body?.To || req.query?.To || "").trim();

  if (supaReady()) {
    if (tid) {
      const k = `tid:${tid}`;
      const cached = cacheGet(k);
      if (cached) return cached;
      const row = await getOne(
        SUPABASE_TRADIES_TABLE,
        `tradie_key=eq.${encodeURIComponent(tid)}&select=*`
      );
      if (row) cacheSet(k, row);
      return row;
    }

    if (to) {
      const k = `to:${to}`;
      const cached = cacheGet(k);
      if (cached) return cached;
      const row = await getOne(
        SUPABASE_TRADIES_TABLE,
        `twilio_number=eq.${encodeURIComponent(to)}&select=*`
      );
      if (row) cacheSet(k, row);
      return row;
    }
  }

  // Fallback
  const key = (tid || to || "default").trim();
  const fallback = TRADIES_FALLBACK[key] || TRADIES_FALLBACK.default || null;
  if (!fallback) return null;
  return { tradie_key: key, ...fallback, _fallback: true };
}

function toBusinessDays(arrOrCsv) {
  if (Array.isArray(arrOrCsv) && arrOrCsv.length) return arrOrCsv.map(Number).filter((n) => Number.isFinite(n));
  const raw = String(arrOrCsv || "").trim();
  if (!raw) return (process.env.BUSINESS_DAYS || "1,2,3,4,5").split(",").map((x) => Number(x.trim())).filter(Boolean);
  return raw.split(",").map((x) => Number(x.trim())).filter(Boolean);
}

function normalizeTradieConfig(row) {
  const t = row || {};

  const timezone = String(t.timezone || process.env.TIMEZONE || "Australia/Sydney");
  const businessDays = toBusinessDays(t.businessDays || t.business_days);

  const avgJobValue = Number(t.avgJobValue ?? t.avg_job_value ?? process.env.AVG_JOB_VALUE ?? 250);
  const closeRate = Number(t.closeRate ?? t.close_rate ?? process.env.CLOSE_RATE ?? 0.6);

  const slotMinutes = Number(t.slotMinutes ?? t.slot_minutes ?? process.env.SLOT_MINUTES ?? 60);
  const bufferMinutes = Number(t.bufferMinutes ?? t.buffer_minutes ?? process.env.BUFFER_MINUTES ?? 0);

  const bizName = String(t.bizName || t.biz_name || process.env.BIZ_NAME || "");
  const tone = String(t.tone || process.env.BOT_TONE || "friendly"); // friendly | direct
  const services = String(t.services || process.env.BOT_SERVICES || "");

  const twilioNumber = String(t.twilio_number || t.twilioNumber || "");
  const smsFrom = twilioNumber || String(t.smsFrom || process.env.TWILIO_SMS_FROM || "");

  return {
    key: String(t.tradie_key || t.key || "default"),
    status: String(t.status || "ACTIVE"),
    plan: String(t.plan || ""),

    ownerSmsTo: String(t.ownerSmsTo || t.owner_sms_to || process.env.OWNER_SMS_TO || ""),
    smsFrom,
    twilioNumber,

    timezone,
    businessDays,
    businessStartHour: Number(t.businessStartHour ?? t.business_start_hour ?? process.env.BUSINESS_START_HOUR ?? 7),
    businessEndHour: Number(t.businessEndHour ?? t.business_end_hour ?? process.env.BUSINESS_END_HOUR ?? 17),

    calendarId: String(t.calendarId || t.calendar_id || process.env.GOOGLE_CALENDAR_ID || ""),
    googleServiceJson: String(t.googleServiceJson || t.google_service_json || process.env.GOOGLE_SERVICE_JSON || ""),

    avgJobValue: Number.isFinite(avgJobValue) ? avgJobValue : 250,
    closeRate: Number.isFinite(closeRate) ? closeRate : 0.6,
    slotMinutes: Number.isFinite(slotMinutes) ? slotMinutes : 60,
    bufferMinutes: Number.isFinite(bufferMinutes) ? bufferMinutes : 0,

    bizName,
    tone,
    services,

    stripeCustomerId: String(t.stripe_customer_id || ""),
    stripeSubscriptionId: String(t.stripe_subscription_id || ""),
    email: String(t.email || "")
  };
}

async function getTradieConfig(req) {
  const row = await loadTradieRow(req);
  return normalizeTradieConfig(row || {});
}

// ----------------------------------------------------------------------------
// Global controls
// ----------------------------------------------------------------------------
const REQUIRE_TWILIO_SIG = String(process.env.REQUIRE_TWILIO_SIG || "false").toLowerCase() === "true";
const MISSED_CALL_ALERT_TRIES = Number(process.env.MISSED_CALL_ALERT_TRIES || 2);
const MAX_SILENCE_TRIES = Number(process.env.MAX_SILENCE_TRIES || 10);
const CAL_RETRY_ATTEMPTS = Number(process.env.CAL_RETRY_ATTEMPTS || 3);
const DUP_WINDOW_DAYS = Number(process.env.DUP_WINDOW_DAYS || 2);

const ADMIN_DASH_PASSWORD = process.env.ADMIN_DASH_PASSWORD || "";

// ----------------------------------------------------------------------------
// Optional LLM (off-script helper)
// ----------------------------------------------------------------------------
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
function safeJsonParse(maybe) {
  try { return JSON.parse(maybe); } catch { return null; }
}
function trimHistory(history, maxItems = 12) {
  const arr = Array.isArray(history) ? history : [];
  if (arr.length <= maxItems) return arr;
  return arr.slice(arr.length - maxItems);
}
function isOffScriptSpeech(speech) {
  if (!speech) return false;
  const s = String(speech).trim();
  const wordCount = s.split(/\s+/).filter(Boolean).length;
  const long = wordCount >= 18;
  const questiony = /\?|\b(can you|do you|what is|how do|why)\b/i.test(s);
  const hasStory = /(so|because|then|after|before|yesterday|last week|last time)/i.test(s) && wordCount >= 14;
  return long || questiony || hasStory;
}
function buildLlmSystemPrompt(tradie) {
  const biz = tradie.bizName ? `Business name: ${tradie.bizName}.` : "";
  const services = tradie.services ? `Services offered: ${tradie.services}.` : "";
  const tone = tradie.tone === "direct" ? "Tone: direct and efficient." : "Tone: friendly, calm, efficient.";

  return (
`You are a voice receptionist for an Australian trades business. ${biz} ${services}
Goal: help the caller book or request a quote, while sounding natural.

You must:
- Extract booking fields from the user's latest speech if present.
- If the user goes off-script, answer briefly and steer back to booking.
- Ask ONE best next question at a time.
- Do NOT invent details. If uncertain, ask.

Output MUST be STRICT JSON ONLY with this schema:
{
 "intent": "NEW_BOOKING" | "QUOTE" | "EMERGENCY" | "CANCEL_RESCHEDULE" | "EXISTING_CUSTOMER" | "UNKNOWN",
 "fields": {
   "job": string|null,
   "address": string|null,
   "name": string|null,
   "time_text": string|null,
   "access": string|null
 },
 "smalltalk_reply": string|null,
 "next_question": string,
 "suggested_step": "intent"|"job"|"address"|"name"|"access"|"time"|"confirm"
}

Rules:
- Emergency words: burst/leak/gas/fire/smoke/sparking/no power/overflow/sewage -> intent=EMERGENCY.
- If cancel/reschedule -> intent=CANCEL_RESCHEDULE.
- If quote -> intent=QUOTE.
- If they're returning / already booked -> intent=EXISTING_CUSTOMER.
- time_text is natural (e.g. "tomorrow at 3").
${tone}`
  );
}

/**
 * Guarded JSON fetch (fixes partial JSON / unexpected end)
 */
async function fetchJsonWithGuards(url, options, { retryOnce = true } = {}) {
  const attempt = async () => {
    const r = await fetch(url, options);
    const ct = (r.headers.get("content-type") || "").toLowerCase();
    const text = await r.text();
    let data = null;
    try {
      data = ct.includes("application/json") ? JSON.parse(text) : JSON.parse(text);
    } catch (e) {
      const err = new Error("LLM response not valid JSON");
      err.status = r.status;
      err.body = text?.slice(0, 2000);
      throw err;
    }
    if (!r.ok) {
      const err = new Error(`HTTP ${r.status}`);
      err.status = r.status;
      err.body = text?.slice(0, 2000);
      throw err;
    }
    return data;
  };

  try {
    return await attempt();
  } catch (e) {
    if (retryOnce) return await fetchJsonWithGuards(url, options, { retryOnce: false });
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
    ...history.map((h) => ({ role: h.role, content: h.content })),
    { role: "user", content: String(userSpeech || "") }
  ];

  const payload = { model: LLM_MODEL, max_output_tokens: LLM_MAX_OUTPUT_TOKENS, input };

  try {
    const data = await fetchJsonWithGuards(LLM_BASE_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify(payload)
    });

    const text = extractResponseTextFromOpenAI(data);
    const parsed = safeJsonParse(String(text || "").trim());
    if (!parsed?.next_question || !parsed?.suggested_step) return null;
    return parsed;
  } catch (e) {
    console.warn("LLM call failed (guarded):", e?.message || e, e?.status ? `status=${e.status}` : "", e?.body ? `body=${e.body}` : "");
    return null;
  }
}

// ----------------------------------------------------------------------------
// Twilio helpers + SaaS number provisioning
// ----------------------------------------------------------------------------
function getTwilioClient() {
  const sid = process.env.TWILIO_ACCOUNT_SID;
  const token = process.env.TWILIO_AUTH_TOKEN;
  if (!sid || !token) return null;
  return twilio(sid, token);
}

async function sendSms({ from, to, body }) {
  const client = getTwilioClient();
  if (!client) return console.warn("SMS skipped: Missing TWILIO creds");
  if (!from) return console.warn("SMS skipped: Missing smsFrom/TWILIO_SMS_FROM");
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

async function provisionTwilioNumberForTradie(tradieKey, reqForBaseUrl) {
  const client = getTwilioClient();
  if (!client) throw new Error("Twilio client not configured");

  const baseUrl = getBaseUrl(reqForBaseUrl);
  const voiceUrl = `${baseUrl}/voice?tid=${encodeURIComponent(tradieKey)}`;
  const smsUrl = `${baseUrl}/sms?tid=${encodeURIComponent(tradieKey)}`;

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
  if (!list?.length) throw new Error("No Twilio numbers available to buy");

  const choice = list[0];
  const purchased = await client.incomingPhoneNumbers.create({
    phoneNumber: choice.phoneNumber,
    voiceUrl,
    voiceMethod: "POST",
    smsUrl,
    smsMethod: "POST"
  });

  if (supaReady()) {
    await upsertRow(SUPABASE_TRADIES_TABLE, {
      tradie_key: tradieKey,
      twilio_number: purchased.phoneNumber,
      twilio_incoming_sid: purchased.sid,
      updated_at: new Date().toISOString()
    });
  }

  cacheSet(`tid:${tradieKey}`, { tradie_key: tradieKey, twilio_number: purchased.phoneNumber });

  return { phoneNumber: purchased.phoneNumber, sid: purchased.sid };
}

// ----------------------------------------------------------------------------
// Twilio signature validation
// ----------------------------------------------------------------------------
function validateTwilioSignature(req) {
  if (!REQUIRE_TWILIO_SIG) return true;
  const token = process.env.TWILIO_AUTH_TOKEN;
  if (!token) return false;

  const signature = req.headers["x-twilio-signature"];
  if (!signature) return false;

  const base = getBaseUrl(req);
  const path = req.originalUrl || req.url || "/";
  const url = `${base}${path}`;

  // Twilio signs the POST params for x-www-form-urlencoded
  const params = { ...(req.body || {}) };

  try {
    return twilio.validateRequest(token, signature, url, params);
  } catch {
    return false;
  }
}

// ----------------------------------------------------------------------------
// Stripe SaaS: Checkout + Webhook + Portal + Onboarding
// ----------------------------------------------------------------------------
const STRIPE_PRICE_BASIC = process.env.STRIPE_PRICE_BASIC || "";
const STRIPE_PRICE_PRO = process.env.STRIPE_PRICE_PRO || "";
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || "";

function stripeReady() {
  return !!(process.env.STRIPE_SECRET_KEY && STRIPE_WEBHOOK_SECRET && (STRIPE_PRICE_BASIC || STRIPE_PRICE_PRO));
}

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

// Simple success page (swap for Carrd redirect if you want)
app.get("/onboarding/success", (req, res) => {
  const base = getBaseUrl(req);
  const sessionId = String(req.query.session_id || "").trim();
  if (!sessionId) return res.status(400).send("Missing session_id");

  return res.status(200).send(
    `<html><body style="font-family:Arial;padding:24px;">
      <h2>Payment received ✅</h2>
      <p>Next: complete setup.</p>
      <p><a href="${base}/onboarding/verify?session_id=${encodeURIComponent(sessionId)}">Continue setup</a></p>
    </body></html>`
  );
});
app.get("/onboarding/cancelled", (req, res) => res.status(200).send("Checkout cancelled."));

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

    const priceId = sub?.items?.data?.[0]?.price?.id || "";
    const plan =
      priceId === STRIPE_PRICE_PRO ? "PRO" :
      priceId === STRIPE_PRICE_BASIC ? "BASIC" :
      "UNKNOWN";

    const tradieKey = `t_${String(customer?.id || sess.customer || "").replace(/[^a-zA-Z0-9]/g, "").slice(-10)}`;

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

    const tradieCfg = await getTradieConfig({ query: { tid: tradieKey }, body: {} });
    await sendOwnerSms(tradieCfg, `You're live ✅\nPlan: ${plan}\nYour bot number: ${twilio_number}\nTest: call it now.\nSupport: reply here anytime.`).catch(() => {});

    return res.json({ ok: true, tradie_key: tradieKey, twilio_number, status: "ACTIVE" });
  } catch (e) {
    console.error("onboarding submit error", e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// POST /billing/portal { tradie_key }
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

// Stripe webhook (RAW)
// IMPORTANT: must be raw JSON or signature breaks
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

    if (event.type === "checkout.session.completed") {
      const sess = event.data.object;

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
      }
    }

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

// ----------------------------------------------------------------------------
// Customer memory + pending confirmation + metrics (DB-backed optional)
// ----------------------------------------------------------------------------
const pendingMemory = new Map(); // fallback: key -> payload
function makePendingKey(tradieKey, from) {
  return `${tradieKey}::${String(from || "").trim()}`;
}

async function setCustomerNote(tradieKey, from, note) {
  if (!supaReady()) return false;
  const row = { tradie_key: tradieKey, customer_phone: from, note: String(note || ""), updated_at: new Date().toISOString() };
  return upsertRow(SUPABASE_PREFS_TABLE, row);
}
async function getCustomerNote(tradieKey, from) {
  if (!supaReady()) return null;
  const r = await getOne(
    SUPABASE_PREFS_TABLE,
    `tradie_key=eq.${encodeURIComponent(tradieKey)}&customer_phone=eq.${encodeURIComponent(from)}&select=note`
  );
  return r?.note || null;
}

async function setPendingConfirmationDb(pendingKey, payload) {
  if (!supaReady()) return false;
  const [tradie_key, customer_phone] = pendingKey.split("::");
  return upsertRow(SUPABASE_PENDING_TABLE, {
    tradie_key,
    customer_phone,
    payload,
    updated_at: new Date().toISOString()
  });
}
async function getPendingConfirmationDb(pendingKey) {
  if (!supaReady()) return null;
  const [tradie_key, customer_phone] = pendingKey.split("::");
  return getOne(
    SUPABASE_PENDING_TABLE,
    `tradie_key=eq.${encodeURIComponent(tradie_key)}&customer_phone=eq.${encodeURIComponent(customer_phone)}&select=payload`
  );
}
async function deletePendingConfirmationDb(pendingKey) {
  if (!supaReady()) return false;
  const [tradie_key, customer_phone] = pendingKey.split("::");
  return delWhere(
    SUPABASE_PENDING_TABLE,
    `tradie_key=eq.${encodeURIComponent(tradie_key)}&customer_phone=eq.${encodeURIComponent(customer_phone)}`
  );
}

function setPendingConfirmationMemory(pendingKey, payload) { pendingMemory.set(pendingKey, payload); }
function getPendingConfirmationMemory(pendingKey) { return pendingMemory.get(pendingKey) || null; }
function clearPendingConfirmationMemory(pendingKey) { pendingMemory.delete(pendingKey); }

function todayKey(tz) {
  return DateTime.now().setZone(tz || "UTC").toFormat("yyyy-LL-dd");
}
async function incMetric(tradie, partial) {
  if (!supaReady()) return false;
  const day = todayKey(tradie.timezone);
  const row = {
    tradie_key: tradie.key,
    day,
    ...Object.fromEntries(Object.entries(partial || {}).map(([k, v]) => [k, Number(v || 0)])),
    updated_at: new Date().toISOString()
  };
  // resolution=merge-duplicates assumes unique (tradie_key, day) constraint in DB
  return upsertRow(SUPABASE_METRICS_TABLE, row);
}

// ----------------------------------------------------------------------------
// Quote flow (lead + SMS “send photos”)
// ----------------------------------------------------------------------------
function makeQuoteKey(tradieKey, from) {
  const s = `${tradieKey}:${from}:${Date.now()}`;
  return Buffer.from(s).toString("base64url").slice(0, 24);
}
async function createQuoteLead(tradie, session) {
  if (!supaReady()) return false;
  return upsertRow(SUPABASE_QUOTES_TABLE, {
    tradie_key: tradie.key,
    quote_key: session.quoteKey,
    customer_phone: session.from || "",
    job: session.job || "",
    address: session.address || "",
    name: session.name || "",
    access_note: session.accessNote || "",
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString()
  });
}

// ----------------------------------------------------------------------------
// Admin: metrics
// ----------------------------------------------------------------------------
app.get("/admin/metrics", async (req, res) => {
  const pw = String(req.query.pw || "");
  if (!ADMIN_DASH_PASSWORD || pw !== ADMIN_DASH_PASSWORD) return res.status(403).json({ error: "Forbidden" });

  const tid = String(req.query.tid || "default").trim();
  const tradie = await getTradieConfig({ query: { tid }, body: {} });

  if (!supaReady()) return res.json({ ok: false, error: "Supabase not configured" });

  const days = Number(req.query.days || 30);
  const since = DateTime.now().setZone(tradie.timezone).minus({ days: Math.max(1, days) }).toFormat("yyyy-LL-dd");
  const rows = await getMany(
    SUPABASE_METRICS_TABLE,
    `tradie_key=eq.${encodeURIComponent(tradie.key)}&day=gte.${encodeURIComponent(since)}&select=*`
  );

  return res.json({ ok: true, tradie_key: tradie.key, rows });
});

// ----------------------------------------------------------------------------
// Conversation / session store (in-memory)
// ----------------------------------------------------------------------------
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
      silenceTries: 0,
      from: fromNumber,

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
    if (!s.from && fromNumber) s.from = fromNumber;
  }
  return sessions.get(callSid);
}
function resetSession(callSid) { sessions.delete(callSid); }

function addToHistory(session, role, content) {
  if (!session) return;
  session.history = trimHistory([...(session.history || []), { role, content }], 12);
}

// ----------------------------------------------------------------------------
// General helpers + validation
// ----------------------------------------------------------------------------
function cleanSpeech(text) {
  if (!text) return "";
  return String(text).trim().replace(/\s+/g, " ");
}

function ask(twiml, prompt, actionUrl, options = {}) {
  const gather = twiml.gather({
    input: "speech dtmf",
    speechTimeout: "auto",
    speechModel: "phone_call",
    enhanced: true,
    action: actionUrl,
    method: "POST",
    profanityFilter: false,
    ...options
  });

  gather.say(prompt || "Sorry, can you repeat that?", { voice: "Polly.Amy", language: "en-AU" });
  twiml.pause({ length: 1 });
}

function normStr(s) {
  return String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

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
  const hasNum = /\d{1,5}/.test(s);
  const hasHint = /(st|street|rd|road|ave|avenue|dr|drive|cres|crescent|ct|court|pl|place|terrace|tce|lane|ln|way|circuit|cct|nsw|vic|qld|sa|wa|tas|act|nt)\b/i.test(s);
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
  if (!s) return true; // optional
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

// ----------------------------------------------------------------------------
// Intent detection (heuristic fallback)
// ----------------------------------------------------------------------------
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

// ----------------------------------------------------------------------------
// Time parsing
// ----------------------------------------------------------------------------
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
    t = t.replace("tomorrow", d);
  }
  if (t.includes("today")) {
    const d = now.toFormat("cccc d LLL yyyy");
    t = t.replace("today", d);
  }
  return t;
}

function parseRequestedDateTime(naturalText, tz) {
  const ref = DateTime.now().setZone(tz).toJSDate();
  const norm = normalizeTimeText(naturalText, tz);

  const results = chrono.parse(norm, ref, { forwardDate: true });
  if (!results?.length) return null;

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

function toGoogleDateTime(dt) {
  return dt.toISO({ includeOffset: true, suppressMilliseconds: true });
}

function looksLikeAsap(text) {
  const t = (text || "").toLowerCase();
  return (
    t.includes("asap") ||
    t.includes("anytime") ||
    t.includes("whenever") ||
    t.includes("dont care") ||
    t.includes("don’t care") ||
    t.includes("no preference") ||
    t.includes("soon as possible") ||
    t === "soon"
  );
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

// ----------------------------------------------------------------------------
// Interruption + edits
// ----------------------------------------------------------------------------
function detectYesNoFromDigits(d) {
  if (!d) return null;
  if (d === "1") return "YES";
  if (d === "2") return "NO";
  return null;
}
function detectYesNo(text) {
  const t = (text || "").toLowerCase().trim();
  const yes = ["yes","yeah","yep","correct","that's right","that’s right","sounds good","ok","okay","confirm"];
  const no = ["no","nope","nah","wrong","not right","don’t","dont"];

  if (yes.some((w) => t === w || t.includes(w))) return "YES";
  if (no.some((w) => t === w || t.includes(w))) return "NO";
  return null;
}
function detectCorrection(text) {
  const t = (text || "").toLowerCase();
  return t.includes("actually") || t.includes("wait") || t.includes("sorry") || t.includes("i meant") || t.includes("correction") || t.includes("change that");
}
function detectChangeFieldFromDigits(d) {
  if (!d) return null;
  if (d === "3") return "time";
  if (d === "4") return "address";
  if (d === "5") return "job";
  if (d === "6") return "name";
  return null;
}
function detectChangeFieldFromSpeech(text) {
  const t = (text || "").toLowerCase();
  if (t.includes("access") || t.includes("gate") || t.includes("parking") || t.includes("dog") || t.includes("pet") || t.includes("code") || t.includes("notes")) return "access";
  if (t.includes("address") || t.includes("location") || t.includes("where")) return "address";
  if (t.includes("name")) return "name";
  if (t.includes("job") || t.includes("issue") || t.includes("problem") || t.includes("quote for")) return "job";
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

// ----------------------------------------------------------------------------
// Access notes normaliser (fixes “stuck on access notes”)
// ----------------------------------------------------------------------------
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

// ----------------------------------------------------------------------------
// Profanity/abuse handling
// ----------------------------------------------------------------------------
function detectAbuse(text) {
  const t = (text || "").toLowerCase();
  const abusive = ["retard","retarded","idiot","stupid","moron","dumb","fuck you","f*** you","cunt","bitch","slut","kill yourself"];
  return abusive.some((w) => t.includes(w));
}
function abuseReply(strikes) {
  if (strikes <= 1) return "I can help with that — let’s keep it respectful. ";
  if (strikes === 2) return "I’m here to help, but I can’t continue with abusive language. ";
  return "I can’t continue this call. Please call back when you’re ready. ";
}

// ----------------------------------------------------------------------------
// Lightweight slot-fill (if caller blurts multiple fields)
// ----------------------------------------------------------------------------
function trySlotFill(session, speech, tz) {
  const raw = String(speech || "").trim();
  if (!raw) return;

  const dt = parseRequestedDateTime(raw, tz);
  if (dt) {
    session.time = raw;
    session.bookedStartMs = dt.toMillis();
  }

  if (validateAddress(raw)) session.address = session.address || raw;

  const m = raw.match(/my name is\s+(.+)/i);
  if (m?.[1]) {
    const nm = cleanSpeech(m[1]);
    if (validateName(nm)) session.name = session.name || nm;
  }

  if (/gate|code|parking|dog|pet|call on arrival|buzz|intercom/i.test(raw)) {
    session.accessNote = session.accessNote || raw;
  }

  if (!session.job && /(leak|blocked|hot water|air con|heater|toilet|sink|tap|power|switch|deck|til(e|es)|fence|roof|gutter|drain)/i.test(raw)) {
    session.job = raw;
  }
}

// ----------------------------------------------------------------------------
// Google Calendar helpers
// ----------------------------------------------------------------------------
function parseGoogleServiceJson(raw) {
  if (!raw) throw new Error("Missing GOOGLE_SERVICE_JSON env/config");
  let parsed;
  try { parsed = JSON.parse(raw); }
  catch { parsed = JSON.parse(raw.replace(/\r?\n/g, "\\n")); }
  if (typeof parsed === "string") parsed = JSON.parse(parsed);
  return parsed;
}
function getCalendarClient(tradie) {
  const credentials = parseGoogleServiceJson(tradie.googleServiceJson);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/calendar"]
  });
  return google.calendar({ version: "v3", auth });
}
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

async function insertCalendarEventWithRetry(calendar, calendarId, requestBody) {
  let lastErr = null;
  for (let attempt = 1; attempt <= CAL_RETRY_ATTEMPTS; attempt++) {
    try {
      return await calendar.events.insert({ calendarId, requestBody });
    } catch (err) {
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

  const resp = await calendar.events.list({
    calendarId,
    q: address,
    timeMin,
    timeMax,
    singleEvents: true,
    orderBy: "startTime",
    maxResults: 30
  });

  const items = resp?.data?.items || [];
  if (!items.length) return null;

  const addrN = normStr(address);
  const candidates = items.filter((ev) => {
    const locN = normStr(ev.location);
    const sumN = normStr(ev.summary);
    const descN = normStr(ev.description);
    return locN.includes(addrN) || sumN.includes(addrN) || descN.includes(addrN);
  });

  const last = (candidates.length ? candidates : items)
    .sort((a, b) => {
      const aISO = a.start?.dateTime || a.start?.date || "";
      const bISO = b.start?.dateTime || b.start?.date || "";
      return String(aISO).localeCompare(String(bISO));
    })
    .pop();

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
    timeMin: t0.toISO({ includeOffset: true }),
    timeMax: t1.toISO({ includeOffset: true }),
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

// ----------------------------------------------------------------------------
// Scheduling: Free/Busy + next slots
// ----------------------------------------------------------------------------
function overlaps(aStart, aEnd, bStart, bEnd) { return aStart < bEnd && bStart < aEnd; }

function isWithinBusinessHours(tradie, dt) {
  const inDay = tradie.businessDays.includes(dt.weekday);
  const inHours = dt.hour >= tradie.businessStartHour && dt.hour < tradie.businessEndHour;
  return inDay && inHours;
}
function slotEnd(tradie, startDt) { return startDt.plus({ minutes: tradie.slotMinutes }); }

async function getBusy(calendar, calendarId, tz, timeMinISO, timeMaxISO) {
  const resp = await calendar.freebusy.query({
    requestBody: {
      timeMin: timeMinISO,
      timeMax: timeMaxISO,
      timeZone: tz,
      items: [{ id: calendarId }]
    }
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

  const busy = await getBusy(
    calendar,
    tradie.calendarId,
    tz,
    start.toISO({ includeOffset: true }),
    searchEnd.toISO({ includeOffset: true })
  );

  const busyIntervals = busy
    .map((b) => ({
      start: DateTime.fromISO(b.start, { setZone: true }).setZone(tz),
      end: DateTime.fromISO(b.end, { setZone: true }).setZone(tz)
    }))
    .filter((x) => x.start.isValid && x.end.isValid);

  const results = [];
  let cursor = start.startOf("minute");

  if (!isWithinBusinessHours(tradie, cursor)) cursor = nextBusinessOpenSlot(tradie);

  while (results.length < count && cursor < searchEnd) {
    if (!isWithinBusinessHours(tradie, cursor)) {
      cursor = nextBusinessOpenSlot(tradie);
      continue;
    }

    const end = slotEnd(tradie, cursor);
    const hasOverlap = busyIntervals.some((b) => overlaps(cursor, end, b.start, b.end));

    if (!hasOverlap) results.push(cursor);

    // step in 15-minute increments for more natural offering
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

// ----------------------------------------------------------------------------
// Missed revenue alert + flow progress
// ----------------------------------------------------------------------------
function flowProgress(session) {
  const parts = [];
  if (session.intent) parts.push(`Intent=${session.intent}`);
  if (session.job) parts.push(`Job=${session.job}`);
  if (session.address) parts.push(`Address=${session.address}`);
  if (session.name) parts.push(`Name=${session.name}`);
  if (session.time) parts.push(`Time=${session.time}`);
  if (session.accessNote) parts.push(`AccessNote=${session.accessNote}`);
  return parts.join(" | ") || "No details captured";
}

async function missedRevenueAlert(tradie, session, reason) {
  await incMetric(tradie, {
    missed_calls_saved: 1,
    est_revenue: tradie.avgJobValue * tradie.closeRate
  });

  const body =
`MISSED LEAD ALERT 💸
Reason: ${reason}
TradieKey: ${tradie.key}
Caller: ${session.from || "Unknown"}
${flowProgress(session)}
Action: Call/text back ASAP.`;

  await sendOwnerSms(tradie, body).catch(() => {});
}

// ----------------------------------------------------------------------------
// VOICE ROUTE
// ----------------------------------------------------------------------------
app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

    const tradie = await getTradieConfig(req);

    // Hard stop if disabled
    if (tradie.status && tradie.status !== "ACTIVE") {
      twiml.say("This service is currently unavailable.", { voice: "Polly.Amy", language: "en-AU" });
      twiml.hangup();
      return res.type("text/xml").send(twiml.toString());
    }

    const tz = tradie.timezone;

    const callSid = req.body.CallSid || req.body.CallSID || "unknown";
    const fromNumber = (req.body.From || "").trim();

    const speech = cleanSpeech(req.body.SpeechResult || req.body.speechResult || "");
    const digits = String(req.body.Digits || "").trim();
    const confidence = req.body.Confidence ? Number(req.body.Confidence) : null;

    const session = getSession(callSid, fromNumber);

    // Count inbound call once for analytics
    if (!session._countedCall) {
      session._countedCall = true;
      await incMetric(tradie, { calls_total: 1 }).catch(() => {});
    }

    if (speech) addToHistory(session, "user", speech);

    console.log(`TID=${tradie.key} CALLSID=${callSid} TO=${req.body.To} FROM=${fromNumber} STEP=${session.step} Speech="${speech}" Digits="${digits}" Confidence=${confidence}`);

    // Abuse handling
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

      const actionUrl = "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      ask(twiml, prompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    // Handle silence
    if (!speech && !digits) {
      session.silenceTries += 1;
      if (session.silenceTries >= MAX_SILENCE_TRIES) {
        await missedRevenueAlert(tradie, session, "Caller silent / dropped").catch(() => {});
        twiml.say("No worries. We’ll call you back shortly.", { voice: "Polly.Amy", language: "en-AU" });
        twiml.hangup();
        resetSession(callSid);
        return res.type("text/xml").send(twiml.toString());
      }

      const prompt = session.lastPrompt || "Sorry, I didn’t catch that. How can we help today?";
      session.lastPrompt = prompt;
      addToHistory(session, "assistant", prompt);

      const actionUrl = "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      ask(twiml, prompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    } else {
      session.silenceTries = 0;
    }

    // Optional: early “human” request
    if (speech && wantsHuman(speech)) {
      await missedRevenueAlert(tradie, session, "Caller requested human").catch(() => {});
      twiml.say("No worries. I’ll get someone to call you back shortly.", { voice: "Polly.Amy", language: "en-AU" });
      twiml.hangup();
      resetSession(callSid);
      return res.type("text/xml").send(twiml.toString());
    }

    // Slot-fill from rambles
    if (speech) trySlotFill(session, speech, tz);

    // Load customer note once
    if (session.from && session.customerNote == null) {
      session.customerNote = await getCustomerNote(tradie.key, session.from).catch(() => null);
    }

    // LLM assist (optional, off-script only by default)
    const shouldUseLlm =
      llmReady() &&
      session.llmTurns < LLM_MAX_TURNS &&
      (LLM_REQUIRE_FOR_OFFSCRIPT ? isOffScriptSpeech(speech) : true);

    if (shouldUseLlm) {
      session.llmTurns += 1;
      const llm = await callLlm(tradie, session, speech);

      if (llm) {
        if (llm.intent && llm.intent !== "UNKNOWN") session.intent = llm.intent;

        const f = llm.fields || {};
        if (typeof f.job === "string" && f.job.trim().length >= 2) session.job = session.job || f.job.trim();
        if (typeof f.address === "string" && f.address.trim().length >= 4 && validateAddress(f.address)) session.address = session.address || f.address.trim();
        if (typeof f.name === "string" && validateName(f.name)) session.name = session.name || f.name.trim();
        if (typeof f.access === "string" && validateAccess(f.access)) session.accessNote = session.accessNote || f.access.trim();
        if (typeof f.time_text === "string" && f.time_text.trim().length >= 2) {
          session.time = session.time || f.time_text.trim();
          const dtTry = parseRequestedDateTime(session.time, tz);
          if (dtTry) session.bookedStartMs = session.bookedStartMs || dtTry.toMillis();
        }

        // Use llm next_question to steer
        const mergedPrompt = (llm.smalltalk_reply ? `${llm.smalltalk_reply} ` : "") + String(llm.next_question || "How can we help today?");
        session.lastPrompt = mergedPrompt;
        addToHistory(session, "assistant", mergedPrompt);

        // We *do not* blindly set step from LLM if we already have a stricter flow.
        // But we can gently nudge:
        if (llm.suggested_step && session.step === "intent") session.step = llm.suggested_step;

        const actionUrl = "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
        ask(twiml, mergedPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }
    }

    // Global interrupts (context-aware) — do NOT hijack access step
    const yn = detectYesNoFromDigits(digits) || detectYesNo(speech);
    const corrected = speech ? detectCorrection(speech) : false;
    const changeField = detectChangeFieldFromDigits(digits) || detectChangeFieldFromSpeech(speech);
    const canGlobalInterrupt = !["intent", "clarify", "confirm", "pickSlot", "access"].includes(session.step);

    if (canGlobalInterrupt && (corrected || changeField)) {
      session.step = "clarify";
      session.lastPrompt = changeField
        ? `Sure — what’s the correct ${changeField}?`
        : "No worries — what should I change? job, address, name, time, or access notes?";
      addToHistory(session, "assistant", session.lastPrompt);

      const actionUrl = "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      ask(twiml, session.lastPrompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    if (canGlobalInterrupt && yn === "NO") {
      session.step = "clarify";
      session.lastPrompt = "No worries — what should I change? job, address, name, time, or access notes?";
      addToHistory(session, "assistant", session.lastPrompt);

      const actionUrl = "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      ask(twiml, session.lastPrompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    // ------------------------------------------------------------------------
    // MAIN FLOW
    // ------------------------------------------------------------------------
    const actionUrl = "/voice" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");

    // STEP: intent
    if (session.step === "intent") {
      session.intent = detectIntent(speech);

      // Emergency short-circuit
      if (session.intent === "EMERGENCY") {
        session.step = "address";
        session.lastPrompt = "Understood. What is the address right now?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      // Cancel/reschedule
      if (session.intent === "CANCEL_RESCHEDULE") {
        session.step = "name";
        session.lastPrompt = "No worries. What is your name so we can reschedule you?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      // Quote
      if (session.intent === "QUOTE") {
        session.step = "job";
        session.lastPrompt = "Sure. What do you need a quote for?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      // Normal booking
      session.step = "job";
      session.lastPrompt = "What job do you need help with?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: clarify (edit any field)
    if (session.step === "clarify") {
      const target = detectChangeFieldFromDigits(digits) || detectChangeFieldFromSpeech(speech);
      if (target) {
        session.step = target;
        session.lastPrompt =
          target === "job" ? "Sure — what’s the job?"
          : target === "address" ? "Sure — what’s the correct address?"
          : target === "name" ? "Sure — what name should I use?"
          : target === "access" ? "Sure — what access notes should I add or update?"
          : "Sure — what time would you like?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      session.lastPrompt = "Sorry — what should I change? job, address, name, time, or access notes?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: job
    if (session.step === "job") {
      if (speech) session.job = speech;

      if (shouldReject("job", session.job, confidence)) {
        session.lastPrompt = "Sorry — what job do you need help with?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      session.step = "address";
      session.lastPrompt = "What is the address?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: address
    if (session.step === "address") {
      if (speech) session.address = speech;

      if (shouldReject("address", session.address, confidence)) {
        session.lastPrompt = "Sorry — what is the full address?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      // address history
      if (tradie.calendarId && tradie.googleServiceJson) {
        try {
          const calendar = getCalendarClient(tradie);
          session.lastAtAddress = await getLastBookingAtAddress(calendar, tradie.calendarId, tz, session.address);
        } catch {}
      }

      session.step = "name";
      session.lastPrompt = "What is your name?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: name
    if (session.step === "name") {
      if (speech) session.name = speech;

      if (shouldReject("name", session.name, confidence)) {
        session.lastPrompt = "Sorry — what name should I put the booking under?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      session.step = "access";
      session.lastPrompt = "Any access notes like gate code, parking, or pets? Say none if not.";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: access (fixed)
    if (session.step === "access") {
      const a = interpretAccessUtterance(speech);

      if (a.kind === "CLEAR") session.accessNote = "";
      else if (a.kind === "KEEP") { /* keep */ }
      else if (a.kind === "SET") {
        session.accessEditMode = a.mode || "replace";
        if (a.value) {
          session.accessNote =
            session.accessEditMode === "append" && session.accessNote
              ? `${session.accessNote} | ${a.value}`
              : a.value;
        }
      } // SKIP -> do nothing

      // QUOTE flow: create lead + SMS ask for photos
      if (session.intent === "QUOTE") {
        if (session.from) {
          session.quoteKey = session.quoteKey || makeQuoteKey(tradie.key, session.from);
          await createQuoteLead(tradie, session).catch(() => {});
          await incMetric(tradie, { quotes_created: 1 }).catch(() => {});

          const link = `Reply to this SMS with photos, or describe the job. Ref: ${session.quoteKey}`;
          await sendCustomerSms(tradie, session.from, `Thanks ${session.name || ""}. To quote faster, please reply with photos. ${link}`.trim()).catch(() => {});
          await sendOwnerSms(tradie, `NEW QUOTE LEAD 📸\nFrom: ${session.from}\nName: ${session.name}\nAddress: ${session.address}\nJob: ${session.job}\nAccess: ${session.accessNote || "None"}\nRef: ${session.quoteKey}`).catch(() => {});
        }

        twiml.say("Thanks. We’ve sent you a text — reply with photos and we’ll get back to you shortly.", { voice: "Polly.Amy", language: "en-AU" });
        twiml.hangup();
        resetSession(callSid);
        return res.type("text/xml").send(twiml.toString());
      }

      session.step = "time";
      session.lastPrompt = "What time would you like?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
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

      // If calendar configured, offer next 3 slots if requested time is busy
      if (tradie.calendarId && tradie.googleServiceJson) {
        const slots = await nextAvailableSlots(tradie, dt, 3);

        if (slots.length === 0) {
          await missedRevenueAlert(tradie, session, "No availability found (14d) — manual scheduling").catch(() => {});
          twiml.say("Thanks. We’ll call you back shortly to lock in a time.", { voice: "Polly.Amy", language: "en-AU" });
          twiml.hangup();
          resetSession(callSid);
          return res.type("text/xml").send(twiml.toString());
        }

        const first = slots[0];
        const deltaMin = Math.abs(first.diff(dt, "minutes").minutes);

        // If chosen time isn't close to available, switch to pickSlot
        if (deltaMin > 5) {
          session.proposedSlots = slots.map((x) => x.toMillis());
          session.step = "pickSlot";
          session.lastPrompt = `We’re booked at that time. I can do: ${slotsVoiceLine(slots, tz)} Say first, second, or third — or tell me another time. (Or press 1, 2, or 3)`;
          addToHistory(session, "assistant", session.lastPrompt);
          ask(twiml, session.lastPrompt, actionUrl);
          return res.type("text/xml").send(twiml.toString());
        }

        dt = first;
      }

      session.bookedStartMs = dt.toMillis();
      session.step = "confirm";

      const whenText = formatForVoice(dt);

      // Duplicate detection (calendar)
      if (tradie.calendarId && tradie.googleServiceJson) {
        try {
          const calendar = getCalendarClient(tradie);
          const dup = await findDuplicate(calendar, tradie.calendarId, tz, session.name, session.address, dt);
          session.duplicateEvent = dup;
        } catch {}
      }

      if (session.duplicateEvent) {
        await sendOwnerSms(tradie, `DUPLICATE BOOKING FLAG ⚠️\nCaller: ${session.from}\nLooks like: ${session.duplicateEvent.summary} at ${session.duplicateEvent.whenText}\nNew request: ${whenText}\n${flowProgress(session)}`).catch(() => {});
      }

      const noteLine = session.customerNote ? `I see a note on your file. ` : "";
      const accessLine = session.accessNote ? `Access notes: ${session.accessNote}. ` : "";

      session.lastPrompt =
`Great. I’ve got ${session.name}, ${session.address}, for ${session.job}, at ${whenText}. ${noteLine}${accessLine}
Is that correct? Say yes to confirm — or say what you want to change: job, address, name, time, or access notes. (Press 1 yes, 2 no)`;

      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: pickSlot
    if (session.step === "pickSlot") {
      if (speech && wantsRepeatOptions(speech)) {
        const slots = (session.proposedSlots || []).map((ms) => DateTime.fromMillis(ms, { zone: tz }));
        session.lastPrompt = `No worries. Options are: ${slotsVoiceLine(slots, tz)} Say first, second, or third — or tell me another time.`;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      const idx = pickSlotFromSpeechOrDigits(speech, digits);

      // If they said a NEW time instead of first/second/third
      const dtTry = speech ? parseRequestedDateTime(speech, tz) : null;
      if (dtTry) {
        session.bookedStartMs = dtTry.toMillis();
        session.step = "time";
        session.lastPrompt = "Got it. Let me check that time.";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      const slots = (session.proposedSlots || []).map((ms) => DateTime.fromMillis(ms, { zone: tz }));
      if (idx == null || !slots[idx]) {
        session.lastPrompt = "Say first, second, or third. Or press 1, 2, or 3. Or tell me another time.";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      const chosen = slots[idx];
      session.bookedStartMs = chosen.toMillis();
      session.step = "confirm";

      const whenText = formatForVoice(chosen);
      const noteLine = session.customerNote ? `I see a note on your file. ` : "";
      const accessLine = session.accessNote ? `Access notes: ${session.accessNote}. ` : "";

      session.lastPrompt =
`Great. I’ve got ${session.name}, ${session.address}, for ${session.job}, at ${whenText}. ${noteLine}${accessLine}
Is that correct? Say yes to confirm — or say what you want to change: job, address, name, time, or access notes.`;

      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return res.type("text/xml").send(twiml.toString());
    }

    // STEP: confirm
    if (session.step === "confirm") {
      const yn2 = detectYesNoFromDigits(digits) || detectYesNo(speech);
      const changeField2 = detectChangeFieldFromSpeech(speech);

      if (changeField2) {
        session.step = changeField2;
        session.lastPrompt =
          changeField2 === "job" ? "Sure — what’s the job?"
          : changeField2 === "address" ? "Sure — what’s the correct address?"
          : changeField2 === "name" ? "Sure — what name should I use?"
          : changeField2 === "access" ? "Sure — what access notes should I add or update?"
          : "Sure — what time would you like instead?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      if (yn2 === "NO") {
        session.step = "clarify";
        session.lastPrompt = "No worries — what should I change? job, address, name, time, or access notes?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      if (yn2 !== "YES") {
        session.lastPrompt = "Sorry — say yes to confirm, or tell me what you want to change: job, address, name, time, or access notes.";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return res.type("text/xml").send(twiml.toString());
      }

      // Create calendar event (if configured), else fallback “manual booking”
      const startDt = session.bookedStartMs
        ? DateTime.fromMillis(session.bookedStartMs, { zone: tz })
        : DateTime.now().setZone(tz).plus({ minutes: 10 }).startOf("minute");
      const endDt = startDt.plus({ minutes: tradie.slotMinutes });

      // Save access note as customer memory (optional)
      if (session.from && session.accessNote) {
        await setCustomerNote(tradie.key, session.from, session.accessNote).catch(() => {});
      }

      // Pending confirmation (for inbound SMS Y/N)
      const pendingKey = makePendingKey(tradie.key, session.from);
      const payload = {
        tradie_key: tradie.key,
        from: session.from,
        name: session.name,
        address: session.address,
        job: session.job,
        access: session.accessNote || "",
        startISO: startDt.toISO(),
        endISO: endDt.toISO()
      };

      let wroteDb = false;
      if (supaReady()) wroteDb = await setPendingConfirmationDb(pendingKey, payload).catch(() => false);
      if (!wroteDb) setPendingConfirmationMemory(pendingKey, payload);

      // Owner alert + analytics
      await incMetric(tradie, { bookings_created: 1, est_revenue: tradie.avgJobValue }).catch(() => {});

      const historyLine = session.lastAtAddress ? `\nHistory: ${session.lastAtAddress.summary} on ${session.lastAtAddress.whenText}` : "";
      const memoryLine = session.customerNote ? `\nCustomer note (existing): ${session.customerNote}` : "";
      const accessLine2 = session.accessNote ? `\nAccess note (new): ${session.accessNote}` : "";

      await sendOwnerSms(tradie,
`NEW BOOKING ✅
Name: ${session.name}
Phone: ${session.from}
Address: ${session.address}
Job: ${session.job}
Time: ${formatForVoice(startDt)}
Confirm: customer will reply Y/N
${historyLine}${memoryLine}${accessLine2}`.trim()).catch(() => {});

      // Insert calendar event if configured
      if (tradie.calendarId && tradie.googleServiceJson) {
        try {
          const calendar = getCalendarClient(tradie);
          await insertCalendarEventWithRetry(calendar, tradie.calendarId, {
            summary: `${session.name} — ${session.job}`.slice(0, 120),
            location: session.address,
            description: [
              `Caller: ${session.from}`,
              session.customerNote ? `Customer note: ${session.customerNote}` : null,
              session.accessNote ? `Access notes: ${session.accessNote}` : null
            ].filter(Boolean).join("\n"),
            start: { dateTime: toGoogleDateTime(startDt), timeZone: tz },
            end: { dateTime: toGoogleDateTime(endDt), timeZone: tz }
          });
        } catch (e) {
          console.warn("Calendar insert failed, continuing:", e?.message || e);
          await missedRevenueAlert(tradie, session, "Calendar insert failed — manual follow-up").catch(() => {});
        }
      }

      // Customer SMS: confirm Y/N
      if (session.from) {
        await sendCustomerSms(
          tradie,
          session.from,
          `Booked: ${formatForVoice(startDt)} at ${session.address} for ${session.job}. Reply Y to confirm or N to reschedule.`
        ).catch(() => {});
      }

      twiml.say("All set. We’ve sent you a text to confirm. Thanks!", { voice: "Polly.Amy", language: "en-AU" });
      twiml.hangup();
      resetSession(callSid);
      return res.type("text/xml").send(twiml.toString());
    }

    // Fallback
    session.step = "intent";
    session.lastPrompt = "How can we help today? You can say emergency, quote, reschedule, or new booking.";
    addToHistory(session, "assistant", session.lastPrompt);
    ask(twiml, session.lastPrompt, actionUrl);
    return res.type("text/xml").send(twiml.toString());
  } catch (err) {
    console.error("VOICE ERROR:", err);
    twiml.say("Sorry, there was a system error. Please try again.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.hangup();
    return res.type("text/xml").send(twiml.toString());
  }
});

// ----------------------------------------------------------------------------
// SMS ROUTE (customer replies Y/N + QUOTE photos)
// ----------------------------------------------------------------------------
app.post("/sms", async (req, res) => {
  try {
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

    // If MMS photos are included, forward owner the media URLs
    const numMedia = Number(req.body.NumMedia || 0);
    if (numMedia > 0) {
      const urls = [];
      for (let i = 0; i < numMedia; i++) {
        const u = req.body[`MediaUrl${i}`];
        if (u) urls.push(u);
      }
      if (urls.length) {
        await sendOwnerSms(tradie, `QUOTE PHOTOS 📸\nFrom: ${from}\n${urls.join("\n")}`).catch(() => {});
      }
    }

    // Pending confirmation handling (Y/N)
    const pendingKey = makePendingKey(tradie.key, from);
    const dbRow = await getPendingConfirmationDb(pendingKey).catch(() => null);
    const payload = dbRow?.payload || getPendingConfirmationMemory(pendingKey);

    const twiml = new MessagingResponse();

    if (payload) {
      const nice = `Name: ${payload.name}\nAddress: ${payload.address}\nJob: ${payload.job}\nTime: ${payload.startISO}`;

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

      // Anything else: treat as message to owner
      await sendOwnerSms(tradie, `CUSTOMER MESSAGE 💬\nFrom: ${from}\nMessage: ${body}\n\nPending booking:\n${nice}`).catch(() => {});
      twiml.message("Thanks — we’ll get back to you shortly.");
      return res.type("text/xml").send(twiml.toString());
    }

    // No pending confirmation: treat as inbound quote / general message
    await sendOwnerSms(tradie, `INBOUND SMS 💬\nFrom: ${from}\nMessage: ${body || "(no text)"}${numMedia ? `\nMedia: ${numMedia} attached` : ""}`).catch(() => {});
    twiml.message("Thanks — we’ve received your message.");
    return res.type("text/xml").send(twiml.toString());
  } catch (e) {
    console.error("SMS ERROR:", e);
    const twiml = new MessagingResponse();
    twiml.message("Sorry — system error. Please try again.");
    return res.type("text/xml").send(twiml.toString());
  }
});

// ----------------------------------------------------------------------------
// Health check
// ----------------------------------------------------------------------------
app.get("/", (req, res) => res.status(200).send("Voice bot running (SaaS)"));

// ---- Google Form → Server webhook (single, safe endpoint) ----
app.post("/form/submit", express.json({ limit: "1mb" }), async (req, res) => {
try {
const expected = (process.env.FORM_WEBHOOK_SECRET || "").trim();
const got = (req.get("x-form-secret") || "").trim();

if (!expected) return res.status(500).send("Missing FORM_WEBHOOK_SECRET on server");
if (got !== expected) return res.status(401).send("Bad form secret");

const payload = req.body || {};
const answers = payload.answers || {};

const pick = (...keys) => {
for (const k of keys) {
const v = answers[k];
if (v != null && String(v).trim() !== "") return String(v).trim();
}
return null;
};

const business_name = pick("Business Name", "business name", "Company Name");
const owner_mobile = pick("Owner Mobile Number", "Owner Mobile", "Owner Phone", "Mobile", "Phone");
const plan = pick("Plan") || "unknown";
const service_offered = pick("Service Offered", "Service", "Trade");
const calendar_email = pick("Google Calendar Email", "Calendar Email", "Email", "Email Address");
const business_hours = pick("Business Hours", "Hours");
const timezone = pick("Time zone", "Timezone", "time zone") || "Australia/Sydney";
const notes = pick("Anything else we should know", "Notes", "Anything else");

const { createClient } = require("@supabase/supabase-js");
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

const leadRow = {
created_at: new Date().toISOString(),
submitted_at: payload.submitted_at || new Date().toISOString(),
form_id: payload.form_id || null,
response_id: payload.response_id || null,
sheet_id: payload.sheet_id || null,

business_name,
owner_mobile,
plan,
service_offered,
calendar_email,
business_hours,
timezone,
notes,

raw_answers: answers,
status: "new"
};

const ins = await supabase.from("onboarding_leads").insert([leadRow]).select().single();

if (ins.error) {
console.error("Supabase insert error:", ins.error);
return res.status(500).json({ ok: false, error: ins.error.message });
}

return res.status(200).json({ ok: true, lead_id: ins.data?.id || null });
} catch (err) {
console.error("POST /form/submit error:", err);
return res.status(500).send("Server error");
}
});

    await upsertRow("onboarding_leads", row);

    // Optional SMS notify you
    if (process.env.OWNER_SMS_TO && process.env.TWILIO_SMS_FROM) {
      await sendSms({
        from: process.env.TWILIO_SMS_FROM,
        to: process.env.OWNER_SMS_TO,
        body: `New onboarding form: ${business_name || email || "Unknown"} (${service_offered || "service"})`
      }).catch(() => {});
    }

    return res.status(200).json({ ok: true });
  } catch (err) {
    console.error("POST /form/submit error:", err);
    return res.status(500).send("Server error");
  }
});

// ----------------------------------------------------------------------------
// Listen
// ----------------------------------------------------------------------------
const PORT = Number(process.env.PORT || 10000);
if (!PORT || Number.isNaN(PORT)) throw new Error("PORT missing/invalid");

app.listen(PORT, () => console.log("Server listening on", PORT));

