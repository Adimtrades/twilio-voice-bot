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
const nodemailer = require("nodemailer");
const stripe = require("stripe")(process.env.STRIPE_SECRET_KEY || "");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");
const { google } = require("googleapis");
const { createClient } = require("@supabase/supabase-js");
const OpenAI = require("openai");

const BOT_VERSION = process.env.BOT_VERSION || process.env.npm_package_version || "1.0.0";
const LOG_TAG = `[bot:${BOT_VERSION}]`;

const requiredEnv = [
  "TWILIO_ACCOUNT_SID",
  "TWILIO_AUTH_TOKEN",
  "SUPABASE_URL",
  "SUPABASE_SERVICE_KEY",
  "OPENAI_API_KEY"
];

requiredEnv.forEach((key) => {
  if (!process.env[key]) {
    console.warn(`${LOG_TAG} Missing ENV variable: ${key}`);
  }
});

console.log(`${LOG_TAG} Server booting`);

const DEV_MODE = process.env.DEV_MODE === "true";

if (DEV_MODE) {
  console.log(`${LOG_TAG} ⚠️ Running in DEV_MODE`);
}

// Start Claw bot in background
require('./claw');
// ----------------------------------------------------------------------------
// App bootstrap
// ----------------------------------------------------------------------------
const app = express();
app.set("trust proxy", true);
app.set("strict routing", false);

// ----------------------------------------------------------------------------
// Process-level safety
// ----------------------------------------------------------------------------
process.on("unhandledRejection", (reason) => console.error("UNHANDLED REJECTION:", reason));
process.on("uncaughtException", async (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
  try {
    if (supabase) {
      await supabase.from("error_logs").insert([{
        error_type: "uncaughtException",
        message: err?.message || String(err),
        stack: err?.stack || null,
        created_at: new Date().toISOString()
      }]);
    }
  } catch (logErr) {
    console.error("FAILED_TO_LOG_UNCAUGHT_EXCEPTION", logErr);
  }
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

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    version: BOT_VERSION,
    time: new Date().toISOString(),
    uptimeSeconds: Math.floor(process.uptime())
  });
});

const BASE_URL = process.env.BASE_URL || "http://localhost:3000";
const GOOGLE_REDIRECT_URI = `${BASE_URL}/google/callback`;

const oauth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  GOOGLE_REDIRECT_URI
);

app.get("/google/auth", async (req, res) => {
  try {
    const tradieId = String(req.query.tradieId || "").trim();
    if (!tradieId) {
      return res.status(400).json({ error: "tradieId is required" });
    }

    if (!process.env.GOOGLE_CLIENT_ID || !process.env.GOOGLE_CLIENT_SECRET) {
      console.error("GOOGLE_AUTH_ENV_MISSING", {
        hasClientId: !!process.env.GOOGLE_CLIENT_ID,
        hasClientSecret: !!process.env.GOOGLE_CLIENT_SECRET
      });
      return res.status(500).json({ error: "Google OAuth is not configured" });
    }

    const url = oauth2Client.generateAuthUrl({
      access_type: "offline",
      prompt: "consent",
      scope: [
        "https://www.googleapis.com/auth/calendar.readonly",
        "https://www.googleapis.com/auth/calendar.events"
      ],
      state: tradieId
    });

    return res.redirect(url);
  } catch (error) {
    console.error("GOOGLE_AUTH_ROUTE_ERROR", error);
    return res.status(500).json({ error: "Unable to start Google authorization" });
  }
});

app.get("/google/callback", async (req, res) => {
  try {
    const code = String(req.query.code || "").trim();
    const tradieId = String(req.query.state || "").trim();

    if (!code || !tradieId) {
      return res.status(400).json({ error: "Missing code or tradieId" });
    }

    if (!supabase) {
      console.error("GOOGLE_CALLBACK_SUPABASE_NOT_READY");
      return res.status(500).json({ error: "Database not configured" });
    }

    const { tokens } = await oauth2Client.getToken(code);
    const refreshToken = String(tokens?.refresh_token || "").trim();

    const updatePayload = { google_connected: true };
    if (refreshToken) {
      updatePayload.google_refresh_token = refreshToken;
    } else {
      console.error("GOOGLE_CALLBACK_REFRESH_TOKEN_MISSING", { tradieId });
    }

    const { error } = await supabase
      .from(SUPABASE_TRADIES_TABLE)
      .update(updatePayload)
      .eq("id", tradieId);

    if (error) {
      console.error("GOOGLE_CALLBACK_SUPABASE_UPDATE_ERROR", {
        tradieId,
        message: error.message,
        code: error.code || null
      });
      return res.status(500).json({ error: "Unable to save Google connection" });
    }

    return res.status(200).send("Google connected successfully.");
  } catch (error) {
    console.error("GOOGLE_CALLBACK_ERROR", error);
    return res.status(500).json({ error: "Google connection failed" });
  }
});

const VoiceResponse = twilio.twiml.VoiceResponse;
const MessagingResponse = twilio.twiml.MessagingResponse;
const MAX_NO_SPEECH_RETRIES = Number(process.env.MAX_NO_SPEECH_RETRIES || 2);
const CALENDAR_OP_TIMEOUT_MS = Number(process.env.CALENDAR_OP_TIMEOUT_MS || 8000);
const CALENDAR_CHECK_TIMEOUT_MS = Number(process.env.CALENDAR_CHECK_TIMEOUT_MS || 5000);
const VOICE_GATHER_TIMEOUT_SECONDS = Number(process.env.VOICE_GATHER_TIMEOUT_SECONDS || 6);
const SESSION_TTL_MS = Number(process.env.VOICE_SESSION_TTL_MS || 30 * 60 * 1000);
const WEBHOOK_IDEMPOTENCY_TTL_MS = Number(process.env.WEBHOOK_IDEMPOTENCY_TTL_MS || 7000);
const MIN_SPEECH_CONFIDENCE = Number(process.env.MIN_SPEECH_CONFIDENCE || 0.45);

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
const SUPABASE_TRADIE_ACCOUNTS_TABLE = process.env.SUPABASE_TRADIE_ACCOUNTS_TABLE || "tradie_accounts";
const supabase = (SUPABASE_URL && SUPABASE_SERVICE_KEY)
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY)
  : null;

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

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

function normalizePhoneE164AU(phone) {
  const raw = String(phone || "").trim();
  if (!raw) return "";

  const digits = raw.replace(/\D/g, "");
  if (!digits) return "";

  if (raw.startsWith("+")) {
    return `+${digits}`;
  }

  if (digits.startsWith("61")) {
    return `+${digits}`;
  }

  if (digits.startsWith("0")) {
    return `+61${digits.slice(1)}`;
  }

  return `+61${digits}`;
}

async function lookupTradieAccountByCalledNumber({ supabase, calledNumber }) {
  const normalizedCalledNumber = normalizePhoneE164AU(calledNumber);
  const digitsOnly = normalizedCalledNumber.replace(/^\+/, "");
  if (!normalizedCalledNumber) return { account: null, normalizedCalledNumber };

  const { data, error } = await supabase
    .from(SUPABASE_TRADIE_ACCOUNTS_TABLE)
    .select("tradie_id,tradie_key,calendar_email,calendar_id,timezone")
    .or([
      `twilio_number.eq.${normalizedCalledNumber}`,
      `twilio_number.eq.${digitsOnly}`,
      `phone_number.eq.${normalizedCalledNumber}`,
      `phone_number.eq.${digitsOnly}`
    ].join(","))
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`TRADIE_ACCOUNT_LOOKUP_BY_NUMBER_FAILED: ${error.message}`);
  }

  return { account: data || null, normalizedCalledNumber };
}

async function resolveCalendarIdFromCalledNumber({ supabase, calledNumber }) {
  const normalizedCalledNumber = normalizePhoneE164AU(calledNumber);
  if (!normalizedCalledNumber) throw new Error("MISSING_CALLED_NUMBER");

  console.log("TRADE_LOOKUP_START", { calledNumber: normalizedCalledNumber });

  const { account: accountByNumber } = await lookupTradieAccountByCalledNumber({
    supabase,
    calledNumber: normalizedCalledNumber
  });

  let lookupPath = "tradie_accounts.called_number";
  let account = accountByNumber;

  if (!account?.tradie_id) {
    const { data: defaultAccount, error: defaultErr } = await supabase
      .from(SUPABASE_TRADIE_ACCOUNTS_TABLE)
      .select("tradie_id,tradie_key,calendar_email,calendar_id,timezone")
      .eq("tradie_key", "default")
      .maybeSingle();

    if (defaultErr) {
      throw new Error(`TRADIE_ACCOUNT_LOOKUP_BY_TRADIE_KEY_FAILED: ${defaultErr.message}`);
    }
    account = defaultAccount || null;
    lookupPath = "tradie_accounts.tradie_key=default";
  }

  if (!account?.tradie_id) {
    throw new Error(`NO_CALLED_NUMBER_MATCH: ${normalizedCalledNumber}`);
  }

  const resolvedTradieId = String(account.tradie_id || "").trim();
  const resolvedTradieKey = String(account.tradie_key || "").trim() || null;

  const { data: tradieRow, error: tradieErr } = await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .select("id,tradie_key")
    .eq("id", resolvedTradieId)
    .maybeSingle();

  if (tradieErr) throw new Error(`TRADIE_LOOKUP_FAILED: ${tradieErr.message}`);

  const resolvedCalendarEmail = String(account.calendar_email || account.calendar_id || "").trim();

  console.log("TRADE_LOOKUP_RESOLVED", {
    calledNumber: normalizedCalledNumber,
    lookupPath,
    tradie_key: String(tradieRow?.tradie_key || resolvedTradieKey || "").trim() || null,
    tradie_id: String(tradieRow?.id || resolvedTradieId || "").trim() || null
  });

  if (!resolvedCalendarEmail) {
    throw new Error(`MISSING_TRADIE_CALENDAR_EMAIL for tradie_key=${resolvedTradieKey || "unknown"} tradie_id=${resolvedTradieId || "unknown"}`);
  }

  return resolvedCalendarEmail;
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

async function updateTradieSubscriptionByCustomerId(customerId, updates = {}) {
  if (!supabase || !customerId) return false;

  const payload = { updated_at: new Date().toISOString() };
  for (const [key, value] of Object.entries(updates)) {
    if (value !== undefined) payload[key] = value;
  }

  const { error } = await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .update(payload)
    .eq("stripe_customer_id", customerId);

  if (error) {
    console.error("subscription status update error", error);
    return false;
  }

  return true;
}

function mapSubscriptionStatusFromStripe(stripeStatus = "") {
  const status = String(stripeStatus || "").toLowerCase();
  if (["active", "trialing"].includes(status)) return "ACTIVE";
  if (["past_due", "unpaid", "incomplete", "incomplete_expired"].includes(status)) return "PAST_DUE";
  if (["canceled"].includes(status)) return "CANCELLED";
  return "ACTIVE";
}

function mapStripeStatusToLocalStatus(stripeStatus = "") {
  return mapSubscriptionStatusFromStripe(stripeStatus);
}

function mapWebhookStatus(stripeStatus = "") {
  const status = String(stripeStatus || "").toLowerCase();
  if (["active", "trialing"].includes(status)) return "active";
  if (["past_due", "unpaid", "incomplete", "incomplete_expired"].includes(status)) return "past_due";
  if (["canceled"].includes(status)) return "cancelled";
  return "active";
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
  const tradieId = String(req.query?.tradie_id || req.body?.tradie_id || "").trim();
  const to = normalizePhoneE164AU(req.body?.To || req.query?.To || "");

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

    if (tradieId) {
      const k = `id:${tradieId}`;
      const cached = cacheGet(k);
      if (cached) return cached;
      const row = await getOne(
        SUPABASE_TRADIES_TABLE,
        `id=eq.${encodeURIComponent(tradieId)}&select=*`
      );
      if (row) cacheSet(k, row);
      return row;
    }

    if (to) {
      const k = `to:${to}`;
      const cached = cacheGet(k);
      if (cached) return cached;
      const digitsOnly = to.replace(/^\+/, "");
      const row = await getOne(
        SUPABASE_TRADIES_TABLE,
        `or=(twilio_number.eq.${encodeURIComponent(to)},twilio_phone_number.eq.${encodeURIComponent(to)},twilio_number.eq.${encodeURIComponent(digitsOnly)},twilio_phone_number.eq.${encodeURIComponent(digitsOnly)})&select=*`
      );
      if (row) cacheSet(k, row);
      return row;
    }
  }

  // Fallback
  const key = (tid || tradieId || to || "default").trim();
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

  const twilioNumber = String(t.twilio_phone_number || t.twilio_number || t.twilioNumber || "");
  const smsFrom = twilioNumber || String(t.smsFrom || process.env.TWILIO_SMS_FROM || "");

  return {
    id: t.id || null,
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

    calendarId: String(t.calendarId || t.calendar_id || t.calendar_email || process.env.GOOGLE_CALENDAR_ID || ""),
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
function getTwilioClient({ accountSid, authToken } = {}) {
  const sid = accountSid || process.env.TWILIO_ACCOUNT_SID;
  const token = authToken || process.env.TWILIO_AUTH_TOKEN;
  if (!sid || !token) return null;
  return twilio(sid, token, accountSid ? { accountSid } : undefined);
}

function getSmtpTransporter() {
  const host = String(process.env.SMTP_HOST || "").trim();
  const port = Number(process.env.SMTP_PORT || 0);
  const user = String(process.env.SMTP_USER || "").trim();
  const pass = String(process.env.SMTP_PASS || "").trim();
  if (!host || !port || !user || !pass) return null;
  return nodemailer.createTransport({
    host,
    port,
    secure: port === 465,
    auth: { user, pass }
  });
}

async function sendTradieEmail(to, subject, text, html = "") {
  const from = String(process.env.FROM_EMAIL || "").trim();
  if (!from || !to) {
    console.warn("Email skipped: sender/recipient missing", { to: to || "" });
    return false;
  }

  const sendgridApiKey = String(process.env.SENDGRID_API_KEY || "").trim();
  if (sendgridApiKey) {
    const resp = await fetch("https://api.sendgrid.com/v3/mail/send", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${sendgridApiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        personalizations: [{ to: [{ email: to }] }],
        from: { email: from },
        subject,
        content: html
          ? [{ type: "text/plain", value: text }, { type: "text/html", value: html }]
          : [{ type: "text/plain", value: text }]
      })
    });

    if (!resp.ok) {
      const errBody = await resp.text().catch(() => "");
      throw new Error(`SendGrid send failed: ${resp.status} ${errBody}`);
    }
    return true;
  }

  const transporter = getSmtpTransporter();
  if (!transporter) {
    console.warn("Email skipped: SMTP not configured", { to: to || "" });
    return false;
  }

  await transporter.sendMail({ from, to, subject, text, ...(html ? { html } : {}) });
  return true;
}

async function getLatestActiveTwilioNumberForTradie(tradieId) {
  if (!supaReady() || !supabase || !tradieId) return "";

  const baseQuery = () => supabase
    .from("twilio_numbers")
    .select("phone_number,created_at")
    .eq("assigned_tradie_id", tradieId)
    .order("created_at", { ascending: false })
    .limit(1);

  let { data, error } = await baseQuery().eq("status", "active");
  if (error) {
    ({ data, error } = await baseQuery());
  }
  if (error) {
    console.error("twilio number lookup failed", { tradie_id: tradieId, error: error.message || error });
    return "";
  }

  return String(data?.[0]?.phone_number || "").trim();
}

function buildActivationEmailContent({ customerName, businessName, provisionedPhoneNumber, baseUrl, tradieId }) {
  const safeCustomerName = customerName || "there";
  const safeBusinessName = businessName || "Your Business";
  const safePhoneNumber = provisionedPhoneNumber || "Not available";
  const serviceAccountEmail = "twilio-voice@twilio-voice-booking.iam.gserviceaccount.com";
  const safeBaseUrl = String(baseUrl || "https://twilio-voice-bot-w9gq.onrender.com").replace(/\/+$/, "");
  const connectGoogleCalendarLink = `${safeBaseUrl}/google/auth?tradieId=${encodeURIComponent(String(tradieId || ""))}`;

  const text =
`Hi ${safeCustomerName},

Your AI booking assistant is now live.

📞 Your dedicated booking number:
${safePhoneNumber}

Save this number as "${safeBusinessName} Bookings".

To enable automatic bookings into your Google Calendar, please complete this quick step:

1. Open Google Calendar
2. Click Settings (⚙ icon, top right)
3. Select your main calendar (left sidebar)
4. Click “Share with specific people”
5. Add this email:
${serviceAccountEmail}
6. Set permission to:
"Make changes to events"
7. Click Send

Once done, your assistant will automatically create bookings in your calendar.

Connect Google Calendar
${connectGoogleCalendarLink}

No other setup is required.

If you need help, simply reply to this email and we’ll assist immediately.

Thanks,
AdimTrades Automation`;

  const html = `<p>Hi ${safeCustomerName},</p>
<p>Your AI booking assistant is now live.</p>
<p>📞 <strong>Your dedicated booking number:</strong><br/>${safePhoneNumber}</p>
<p>Save this number as "${safeBusinessName} Bookings".</p>
<p>To enable automatic bookings into your Google Calendar, please complete this quick step:</p>
<ol>
  <li>Open Google Calendar</li>
  <li>Click Settings (⚙ icon, top right)</li>
  <li>Select your main calendar (left sidebar)</li>
  <li>Click “Share with specific people”</li>
  <li>Add this email:<br/><strong>${serviceAccountEmail}</strong></li>
  <li>Set permission to:<br/>"Make changes to events"</li>
  <li>Click Send</li>
</ol>
<p>Once done, your assistant will automatically create bookings in your calendar.</p>
<p><strong>Connect Google Calendar</strong><br/><a href="${connectGoogleCalendarLink}">Connect Google Calendar</a></p>
<p>No other setup is required.</p>
<p>If you need help, simply reply to this email and we’ll assist immediately.</p>
<p>Thanks,<br/>AdimTrades Automation</p>`;

  return { text, html };
}

async function sendActivationEmailForTradie({ tradie, provisionedPhoneNumber = "", emailFallback = "", source = "", reqForBaseUrl = null }) {
  const recipient = String(tradie?.email || emailFallback || "").trim();
  if (!recipient) {
    console.warn("activation email skipped: missing recipient", { tradie_id: tradie?.id || "", source });
    return;
  }

  const twilioNumber = String(provisionedPhoneNumber || "").trim() || await getLatestActiveTwilioNumberForTradie(tradie.id);
  const fallbackTriggered = !twilioNumber;
  const customerName = String(tradie?.customer_name || tradie?.name || tradie?.owner_name || tradie?.business_name || "there").trim();
  const businessName = String(tradie?.business_name || tradie?.biz_name || tradie?.bizName || "Your Business").trim();
  const fallbackBaseUrl = "https://twilio-voice-bot-w9gq.onrender.com";
  const reqBaseUrl = reqForBaseUrl?.get ? `https://${reqForBaseUrl.get("host")}` : fallbackBaseUrl;
  const baseUrl = process.env.BASE_URL || reqBaseUrl || fallbackBaseUrl;

  console.log("activation email target", { to: recipient, source });
  console.log("activation email context", { tradie_id: tradie.id, stripe_customer_id: tradie.stripe_customer_id || "", source });
  console.log("activation email number", { twilioNumber: twilioNumber || "", fallbackTriggered, source });

  const { text, html } = buildActivationEmailContent({
    customerName,
    businessName,
    provisionedPhoneNumber: twilioNumber,
    baseUrl,
    tradieId: tradie?.id || ""
  });
  try {
    await sendTradieEmail(recipient, "Your Booking Number is Ready 🚀 (Action Required)", text, html);
    console.log("Activation email sent with booking number and calendar instructions", {
      tradie_id: tradie.id,
      source
    });
  } catch (err) {
    console.error("activation email send failed", { tradie_id: tradie.id, source, error: err?.message || err });
  }
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
const TWILIO_VOICE_URL = "https://twilio-voice-bot-w9gq.onrender.com/twilio/voice";
const TWILIO_SMS_URL = "https://twilio-voice-bot-w9gq.onrender.com/twilio/sms";

function planFromPriceId(priceId = "") {
  if (priceId === STRIPE_PRICE_PRO) return "PRO";
  if (priceId === STRIPE_PRICE_BASIC) return "BASIC";
  return "UNKNOWN";
}

async function ensureTradieByStripeCustomer({ customerId, subscriptionId, plan, email }) {
  if (!supaReady() || !supabase || !customerId) return null;

  console.log("stripe provisioning step: lookup tradie", { customerId, subscriptionId: subscriptionId || "" });
  let tradie = await getTradieByStripeRefs({ customerId, subscriptionId });
  if (tradie?.id) return tradie;

  const generatedKey = `t_${String(customerId).replace(/[^a-zA-Z0-9]/g, "").slice(-10)}`;
  const payload = {
    tradie_key: generatedKey,
    email: email || "",
    stripe_customer_id: customerId,
    stripe_subscription_id: subscriptionId || null,
    plan: plan || "UNKNOWN",
    status: "active",
    subscription_status: "active",
    updated_at: new Date().toISOString()
  };

  console.log("stripe provisioning step: create tradie", { customerId, tradie_key: generatedKey });
  const { data, error } = await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .upsert(payload, { onConflict: "stripe_customer_id" })
    .select("*")
    .single();

  if (error) {
    console.error("stripe provisioning step: create tradie failed", error);
    return null;
  }
  return data;
}

async function getOrCreateTradieForStripeEvent({ customerId, subscriptionId, plan, email }) {
  const tradie = await ensureTradieByStripeCustomer({ customerId, subscriptionId, plan, email });
  if (!tradie?.id) {
    console.error("stripe provisioning step: tradie not found/created", { customerId, subscriptionId: subscriptionId || "" });
    return null;
  }
  return tradie;
}

async function ensureTwilioNumberProvisionedForTradie(tradie, reqForBaseUrl) {
  if (!tradie?.id) return null;
  if (tradie.twilio_phone_number || tradie.twilio_number) {
    console.log("stripe provisioning step: existing twilio number found", {
      tradie_id: tradie.id,
      phone: tradie.twilio_phone_number || tradie.twilio_number
    });
    return {
      skipped: true,
      phoneNumber: tradie.twilio_phone_number || tradie.twilio_number,
      sid: tradie.twilio_phone_sid || tradie.twilio_incoming_sid || ""
    };
  }
  return provisionTwilioNumberForTradie(tradie, reqForBaseUrl);
}

async function getTradieByStripeRefs({ customerId, subscriptionId } = {}) {
  if (!supaReady() || !supabase) return null;

  if (subscriptionId) {
    const { data, error } = await supabase
      .from(SUPABASE_TRADIES_TABLE)
      .select("*")
      .eq("stripe_subscription_id", subscriptionId)
      .maybeSingle();
    if (!error && data) return data;
  }

  if (customerId) {
    const { data, error } = await supabase
      .from(SUPABASE_TRADIES_TABLE)
      .select("*")
      .eq("stripe_customer_id", customerId)
      .maybeSingle();
    if (!error && data) return data;
  }

  return null;
}

async function getTradieProvisioningRecord(tradieKey) {
  if (!supaReady() || !supabase || !tradieKey) return null;
  const { data } = await supabase
    .from(SUPABASE_TRADIE_ACCOUNTS_TABLE)
    .select("*")
    .eq("tradie_key", tradieKey)
    .maybeSingle();
  return data || null;
}

async function provisionTwilioNumberForTradie(tradie, reqForBaseUrl) {
  const tradieId = String(tradie?.id || "").trim();
  const tradieKey = String(tradie?.tradie_key || tradieId).trim();
  if (!tradieId) throw new Error("Cannot provision: missing tradie id");

  const existingProvisioning = await getTradieProvisioningRecord(tradieKey);
  if (tradie?.twilio_phone_number || tradie?.twilio_number || existingProvisioning?.twilio_phone_number || existingProvisioning?.provisioning_status === "PROVISIONED") {
    console.log("twilio provisioning skipped: already provisioned", { tradie_id: tradieId, stripe_customer_id: tradie?.stripe_customer_id || "" });
    return {
      skipped: true,
      phoneNumber: tradie?.twilio_phone_number || tradie?.twilio_number || existingProvisioning?.twilio_phone_number,
      sid: tradie?.twilio_phone_sid || tradie?.twilio_incoming_sid || existingProvisioning?.twilio_incoming_sid || "",
      subaccountSid: existingProvisioning?.twilio_subaccount_sid || ""
    };
  }

  console.log("twilio provisioning started", { tradie_id: tradieId, stripe_customer_id: tradie?.stripe_customer_id || "" });

  const twilioClient = getTwilioClient();
  if (!twilioClient) throw new Error("Twilio client not configured");

  const tryClaimNumberBySid = async (numberSid, phoneNumber) => {
    const { data, error } = await supabase
      .from("twilio_numbers")
      .update({ assigned_tradie_id: tradieId })
      .eq("sid", numberSid)
      .is("assigned_tradie_id", null)
      .select("sid, phone_number")
      .maybeSingle();

    if (error) throw error;
    if (!data?.sid) return null;
    return { sid: data.sid, phoneNumber: data.phone_number || phoneNumber };
  };

  let allocated = null;

  const incoming = await twilioClient.incomingPhoneNumbers.list({ limit: 200 });
  const { data: assignedRows, error: assignedErr } = await supabase
    .from("twilio_numbers")
    .select("sid")
    .not("assigned_tradie_id", "is", null);
  if (assignedErr) throw assignedErr;

  const assignedSet = new Set((assignedRows || []).map((row) => row.sid).filter(Boolean));
  const freeIncoming = incoming.filter((num) => !assignedSet.has(num.sid));

  for (const num of freeIncoming) {
    const { error: upsertErr } = await supabase
      .from("twilio_numbers")
      .upsert({ sid: num.sid, phone_number: num.phoneNumber, assigned_tradie_id: null }, { onConflict: "sid" });
    if (upsertErr) throw upsertErr;

    allocated = await tryClaimNumberBySid(num.sid, num.phoneNumber);
    if (allocated) break;
  }

  if (!allocated) {
    const searchParams = {
      smsEnabled: true,
      voiceEnabled: true,
      limit: 1
    };
    if (TWILIO_BUY_AREA_CODE) searchParams.areaCode = TWILIO_BUY_AREA_CODE;

    const list = await twilioClient.availablePhoneNumbers(TWILIO_BUY_COUNTRY).local.list(searchParams);
    if (!list?.length) throw new Error("No AU numbers available to buy (Twilio search returned 0).");

    const purchased = await twilioClient.incomingPhoneNumbers.create({
      phoneNumber: list[0].phoneNumber,
      voiceUrl: TWILIO_VOICE_URL,
      voiceMethod: "POST",
      smsUrl: TWILIO_SMS_URL,
      smsMethod: "POST"
    });

    const { error: insertErr } = await supabase
      .from("twilio_numbers")
      .upsert({ sid: purchased.sid, phone_number: purchased.phoneNumber, assigned_tradie_id: null }, { onConflict: "sid" });
    if (insertErr) throw insertErr;

    allocated = await tryClaimNumberBySid(purchased.sid, purchased.phoneNumber);
    if (!allocated) throw new Error("Failed to claim purchased Twilio number");
  }

  await twilioClient.incomingPhoneNumbers(allocated.sid).update({
    voiceUrl: TWILIO_VOICE_URL,
    voiceMethod: "POST",
    smsUrl: TWILIO_SMS_URL,
    smsMethod: "POST"
  });

  if (supaReady() && supabase) {
    await supabase.from(SUPABASE_TRADIE_ACCOUNTS_TABLE).upsert({
      tradie_id: tradieId,
      twilio_phone_number: allocated.phoneNumber,
      twilio_incoming_sid: allocated.sid,
      twilio_subaccount_sid: null,
      provisioning_status: "PROVISIONED",
      provisioned_at: new Date().toISOString(),
      stripe_subscription_id: tradie.stripe_subscription_id || null
    }, { onConflict: "tradie_id" });

    await supabase.from(SUPABASE_TRADIES_TABLE)
      .update({
        twilio_phone_number: allocated.phoneNumber,
        twilio_phone_sid: allocated.sid,
        twilio_number: allocated.phoneNumber,
        twilio_incoming_sid: allocated.sid,
        updated_at: new Date().toISOString()
      })
      .eq("id", tradieId);
  }

  if (tradieKey) {
    cacheSet(`tid:${tradieKey}`, { tradie_key: tradieKey, twilio_number: allocated.phoneNumber });
  }

  console.log("twilio provisioning success", { tradie_id: tradieId, stripe_customer_id: tradie?.stripe_customer_id || "" });
  return { skipped: false, phoneNumber: allocated.phoneNumber, sid: allocated.sid, subaccountSid: "" };
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
      const tradieForProvision = await getTradieByStripeRefs({
        customerId: row.stripe_customer_id,
        subscriptionId: row.stripe_subscription_id
      });
      if (tradieForProvision?.id) {
        const provisioned = await provisionTwilioNumberForTradie(tradieForProvision, req);
        twilio_number = provisioned.phoneNumber;
      }
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

async function markProvisioningFailure(tradie, reason) {
  if (!supaReady() || !supabase || !tradie?.id) return;
  await supabase.from(SUPABASE_TRADIE_ACCOUNTS_TABLE).upsert({
    tradie_id: tradie.id,
    provisioning_status: "FAILED",
    updated_at: new Date().toISOString()
  }, { onConflict: "tradie_id" });

  const failText =
`Hi ${tradie.business_name || "there"},

Your subscription is active, but we hit a setup issue while provisioning your AI receptionist phone number.
Our team has been notified and will follow up shortly.

If you need help now, reply to this email and our support team will assist.`;

  await sendTradieEmail(tradie.email, "Setup update for your AI Receptionist", failText).catch(() => {});
  console.error("twilio provisioning failure", {
    tradie_id: tradie.id,
    stripe_customer_id: tradie.stripe_customer_id || "",
    reason: String(reason || "unknown")
  });
}

async function ensureProvisioningForActiveSubscription({ customerId, subscriptionId, status, reqForBaseUrl }) {
  if (status !== "ACTIVE") return;
  const tradie = await getTradieByStripeRefs({ customerId, subscriptionId });
  if (!tradie?.id) return;

  const existingProvisioning = await getTradieProvisioningRecord(tradie.tradie_key || tradie.id);
  if (tradie?.twilio_phone_number || tradie?.twilio_number || existingProvisioning?.twilio_phone_number || existingProvisioning?.provisioning_status === "PROVISIONED") {
    console.log("twilio provisioning skipped", { tradie_id: tradie.id, stripe_customer_id: customerId || "" });
    return;
  }

  try {
    const provisioned = await provisionTwilioNumberForTradie(tradie, reqForBaseUrl);
    if (provisioned?.skipped) return;
    await sendActivationEmailForTradie({
      tradie,
      provisionedPhoneNumber: provisioned?.phoneNumber || "",
      source: "ensureProvisioningForActiveSubscription",
      reqForBaseUrl
    });
  } catch (err) {
    await markProvisioningFailure(tradie, err?.message || err);
  }
}

// Stripe webhook (RAW)
// IMPORTANT: must be raw JSON or signature breaks
async function syncActiveSubscriptionAndProvision({ customerId, subscriptionId, plan, email, reqForBaseUrl, sourceEvent }) {
  if (!customerId) return;

  console.log("stripe provisioning step: begin", { sourceEvent, customerId, subscriptionId: subscriptionId || "", plan: plan || "UNKNOWN" });
  const tradie = await getOrCreateTradieForStripeEvent({ customerId, subscriptionId, plan, email });
  if (!tradie?.id) return;

  const finalPlan = plan || tradie.plan || "UNKNOWN";
  await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .update({
      status: "active",
      subscription_status: "active",
      stripe_subscription_id: subscriptionId || tradie.stripe_subscription_id || null,
      plan: finalPlan,
      updated_at: new Date().toISOString()
    })
    .eq("id", tradie.id);
  console.log("stripe provisioning step: tradie marked active", { tradie_id: tradie.id, plan: finalPlan });

  const refreshedTradie = await getTradieByStripeRefs({ customerId, subscriptionId: subscriptionId || tradie.stripe_subscription_id || "" });
  const provisioned = await ensureTwilioNumberProvisionedForTradie(refreshedTradie || tradie, reqForBaseUrl);
  if (!provisioned) return;

  if (provisioned.skipped) {
    console.log("stripe provisioning step: number already exists, skipping buy", { tradie_id: tradie.id, phone: provisioned.phoneNumber || "" });
    return;
  }

  console.log("stripe provisioning step: number provisioned", { tradie_id: tradie.id, phone: provisioned.phoneNumber });
  await sendActivationEmailForTradie({
    tradie: refreshedTradie || tradie,
    provisionedPhoneNumber: provisioned.phoneNumber || "",
    emailFallback: email,
    source: "syncActiveSubscriptionAndProvision:new",
    reqForBaseUrl
  });
}

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

    console.log("stripe webhook received", { eventType: event.type, eventId: event.id });
    const stripeObj = event.data.object || {};

    if (event.type === "checkout.session.completed") {
      const sess = stripeObj;
      if (sess?.mode === "subscription") {
        console.log("stripe webhook step: checkout.session.completed subscription", { sessionId: sess.id, payment_status: sess.payment_status || "" });
        const expanded = await stripe.checkout.sessions.retrieve(sess.id, { expand: ["subscription", "customer"] });
        const subscriptionId = String(expanded?.subscription?.id || expanded?.subscription || "").trim();
        const customerId = String(expanded?.customer?.id || expanded?.customer || "").trim();
        const priceId = expanded?.subscription?.items?.data?.[0]?.price?.id || "";
        const plan = planFromPriceId(priceId);
        const email = expanded?.customer_details?.email || expanded?.customer_email || "";
        const tradieKey = `t_${String(customerId).replace(/[^a-zA-Z0-9]/g, "").slice(-10)}`;

        if (supaReady()) {
          await upsertRow(SUPABASE_TRADIES_TABLE, {
            tradie_key: tradieKey,
            email,
            plan,
            status: "active",
            subscription_status: "active",
            stripe_customer_id: customerId,
            stripe_subscription_id: subscriptionId,
            updated_at: new Date().toISOString()
          });
          cacheSet(`tid:${tradieKey}`, { tradie_key: tradieKey, stripe_customer_id: customerId });
        }

        (async () => {
          try {
            console.log("AUTO_PROVISION start", tradieKey);
            const existing = await getOne(
              SUPABASE_TRADIES_TABLE,
              `tradie_key=eq.${encodeURIComponent(tradieKey)}&select=*`
            );
            if (!existing) return;

            if (existing.twilio_number || existing.twilio_phone_number) {
              console.log("AUTO_PROVISION already has number", existing.twilio_number || existing.twilio_phone_number);
            } else {
              const provisioned = await provisionTwilioNumberForTradie(existing, req);
              const phoneNumber = provisioned?.phoneNumber || "";
              if (phoneNumber) {
                console.log("AUTO_PROVISION provisioned", phoneNumber);
                await upsertRow(SUPABASE_TRADIES_TABLE, {
                  tradie_key: tradieKey,
                  twilio_number: phoneNumber,
                  twilio_incoming_sid: provisioned?.sid || null,
                  updated_at: new Date().toISOString()
                });
              }
            }

            const fresh = await getOne(
              SUPABASE_TRADIES_TABLE,
              `tradie_key=eq.${encodeURIComponent(tradieKey)}&select=*`
            );
            if (!fresh) return;
            const twilioNumber = String(fresh.twilio_number || fresh.twilio_phone_number || "").trim();
            if (!twilioNumber) return;

            const tradieCfg = await getTradieConfig({ query: { tid: tradieKey }, body: {} });
            await sendOwnerSms(
              tradieCfg,
              `Your AI receptionist is live ✅\nYour bot number: ${twilioNumber}\nNext: call it for a quick test, then forward your current business line.`
            ).catch(() => {});

            // Email send is handled in syncActiveSubscriptionAndProvision to avoid duplicates.
          } catch (err) {
            console.error("AUTO_PROVISION failed", err);
          }
        })();

        await syncActiveSubscriptionAndProvision({
          customerId,
          subscriptionId,
          plan,
          email,
          reqForBaseUrl: req,
          sourceEvent: event.type
        });
      }
    }

    if (event.type === "invoice.payment_succeeded") {
      const customerId = String(stripeObj?.customer || "").trim();
      const subscriptionId = String(stripeObj?.subscription || "").trim();
      console.log("stripe webhook step: invoice.payment_succeeded", { customerId, subscriptionId });

      let plan = "UNKNOWN";
      if (subscriptionId) {
        const sub = await stripe.subscriptions.retrieve(subscriptionId, { expand: ["items.data.price"] });
        plan = planFromPriceId(sub?.items?.data?.[0]?.price?.id || "");
      }

      await syncActiveSubscriptionAndProvision({
        customerId,
        subscriptionId,
        plan,
        email: stripeObj?.customer_email || "",
        reqForBaseUrl: req,
        sourceEvent: event.type
      });
    }

    if (event.type === "customer.subscription.updated") {
      const customerId = String(stripeObj?.customer || "").trim();
      const subscriptionId = String(stripeObj?.id || "").trim();
      const status = mapWebhookStatus(stripeObj?.status);
      console.log("stripe webhook step: customer.subscription.updated", { customerId, subscriptionId, status });
      if (customerId) {
        await updateTradieSubscriptionByCustomerId(customerId, {
          subscription_status: status,
          stripe_subscription_id: subscriptionId || undefined,
          status
        });
      }
    }

    if (event.type === "customer.subscription.deleted") {
      const customerId = String(stripeObj?.customer || "").trim();
      const subscriptionId = String(stripeObj?.id || "").trim();
      console.log("stripe webhook step: customer.subscription.deleted", { customerId, subscriptionId });
      if (customerId) {
        await updateTradieSubscriptionByCustomerId(customerId, {
          subscription_status: "cancelled",
          stripe_subscription_id: subscriptionId || undefined,
          status: "cancelled"
        });
      }
    }

    if (event.type === "invoice.payment_failed") {
      const customerId = String(stripeObj?.customer || "").trim();
      const subscriptionId = String(stripeObj?.subscription || "").trim();
      console.log("stripe webhook step: invoice.payment_failed", { customerId, subscriptionId });
      if (customerId) {
        await updateTradieSubscriptionByCustomerId(customerId, {
          subscription_status: "past_due",
          stripe_subscription_id: subscriptionId || undefined,
          status: "past_due"
        });
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
const processSpeechLocks = new Map();

function cleanupMaps() {
  const now = Date.now();
  for (const [sid, data] of sessions.entries()) {
    if (!data?.updatedAt || (now - data.updatedAt) > SESSION_TTL_MS) sessions.delete(sid);
  }
  for (const [key, exp] of processSpeechLocks.entries()) {
    if (!exp || now > exp) processSpeechLocks.delete(key);
  }
}

function getSession(callSid, fromNumber = "") {
  cleanupMaps();
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
      retryCount: 0,
      retryCountByStep: {},
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
  const active = sessions.get(callSid);
  active.updatedAt = Date.now();
  return sessions.get(callSid);
}
function resetSession(callSid) { sessions.delete(callSid); }


// idempotency lock: suppress duplicate CallSid+step webhook processing within short TTL
function acquireProcessSpeechLock(callSid, step) {
  cleanupMaps();
  const key = `${callSid}:${step}`;
  if (processSpeechLocks.has(key)) return false;
  processSpeechLocks.set(key, Date.now() + WEBHOOK_IDEMPOTENCY_TTL_MS);
  return true;
}

function isLowConfidence(confidence) {
  return typeof confidence === "number" && confidence > 0 && confidence < MIN_SPEECH_CONFIDENCE;
}

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

async function withTimeout(promise, ms, label = "operation") {
  let timer;
  const timeoutErr = new Error(`${label} timed out after ${ms}ms`);
  timeoutErr.code = "TIMEOUT";
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => reject(timeoutErr), ms);
  });

  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    clearTimeout(timer);
  }
}

function logVoiceStep(req, { callSid = "unknown", step = "unknown", speech = "", retryCount = 0 } = {}) {
  console.log(
    `VOICE callSid=${callSid} step=${step} method=${req.method} path=${req.path} speechLen=${speech.length} retryCount=${retryCount}`
  );
}

function voiceActionUrl(req) {
  return "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
}

function buildInitialVoiceTwiml(req, greetingText = "Hi. What would you like help with today?") {
  const twiml = new VoiceResponse();
  const gather = twiml.gather({
    input: "speech",
    timeout: VOICE_GATHER_TIMEOUT_SECONDS,
    speechTimeout: "auto",
    action: voiceActionUrl(req),
    method: "POST",
    language: "en-AU"
  });
  gather.say(greetingText, { voice: "Polly.Amy", language: "en-AU" });
  return twiml;
}

function handleVoiceEntry(req, res) {
  const twiml = buildInitialVoiceTwiml(req);
  const callSid = resolveCallSid(req);
  const fromNumber = (req.body?.From || req.query?.From || "").trim();
  const session = getSession(callSid, fromNumber);
  session.step = "initial";
  session.retryCount = 0;
  session.hasEnteredVoice = true;
  session.lastNoSpeechFallback = false;
  session.silenceTries = 0;
  logVoiceStep(req, { callSid, step: "initial", speech: "", retryCount: 0 });
  return sendVoiceTwiml(res, twiml);
}

function ask(twiml, prompt, actionUrl, options = {}) {
  const gather = twiml.gather({
    input: "speech",
    speechTimeout: "auto",
    action: actionUrl,
    method: "POST",
    language: "en-AU",
    timeout: VOICE_GATHER_TIMEOUT_SECONDS,
    ...options
  });

  gather.say(prompt || "Sorry, can you repeat that?", { voice: "Polly.Amy", language: "en-AU" });
  twiml.pause({ length: 1 });
}

function getRetryCountForStep(session, step) {
  if (!session.retryCountByStep || typeof session.retryCountByStep !== "object") {
    session.retryCountByStep = {};
  }
  return Number(session.retryCountByStep[step] || 0);
}

function resetRetryCountForStep(session, step) {
  if (!session.retryCountByStep || typeof session.retryCountByStep !== "object") {
    session.retryCountByStep = {};
  }
  session.retryCountByStep[step] = 0;
}

function incrementRetryCountForStep(session, step) {
  if (!session.retryCountByStep || typeof session.retryCountByStep !== "object") {
    session.retryCountByStep = {};
  }
  session.retryCountByStep[step] = Number(session.retryCountByStep[step] || 0) + 1;
  return session.retryCountByStep[step];
}

function repeatLastStepPrompt(req, twiml, session, step, reason = "NO_SPEECH") {
  const retryCount = incrementRetryCountForStep(session, step);
  const actionUrl = voiceActionUrl(req);
  const basePrompt = session.lastPrompt || "Could you repeat that?";

  if (retryCount <= MAX_NO_SPEECH_RETRIES) {
    const prompt = `No worries — take your time. ${basePrompt}`;
    session.lastPrompt = basePrompt;
    console.log(`STEP=${step} speech='' interpreted='${reason}' retryCount=${retryCount}`);
    ask(twiml, prompt, actionUrl, { input: "speech", timeout: 6, speechTimeout: "auto" });
    return { handled: true };
  }

  if (retryCount === MAX_NO_SPEECH_RETRIES + 1) {
    session.lastStepBeforeFallback = step;
    session.promptBeforeFallback = basePrompt;
    session.step = "sms_fallback_offer";
    session.lastPrompt = "I didn’t catch that. You can say it again, or say ‘text me’ and I’ll send an SMS link.";
    console.log(`STEP=${step} speech='' interpreted='${reason}' retryCount=${retryCount}`);
    ask(twiml, session.lastPrompt, actionUrl, { input: "speech", timeout: 6, speechTimeout: "auto" });
    return { handled: true };
  }

  console.log(`STEP=${step} speech='' interpreted='${reason}' retryCount=${retryCount}`);
  ask(twiml, `No worries — take your time. ${basePrompt}`, actionUrl, { input: "speech", timeout: 6, speechTimeout: "auto" });
  return { handled: true };
}

function keepCallAliveForProcessing(req, twiml, message = "One moment while I check that for you.") {
  twiml.say(message, { voice: "Polly.Amy", language: "en-AU" });
  const checkUrl = "/check-availability" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
  twiml.redirect({ method: "POST" }, checkUrl);
}

function sendVoiceTwiml(res, twiml, fallbackMessage = "Sorry, there was a temporary issue. Please try again.") {
  const responseTwiml = twiml instanceof VoiceResponse ? twiml : new VoiceResponse();
  let xml = "";

  try {
    xml = String(responseTwiml.toString() || "").trim();
  } catch {
    xml = "";
  }

  if (!xml) {
    const fallback = new VoiceResponse();
    fallback.say(fallbackMessage, { voice: "Polly.Amy", language: "en-AU" });
    xml = fallback.toString();
  }

  return res.type("text/xml").send(xml);
}

async function performCalendarAvailabilityCheck({ tradie, dt, tz, callSid }) {
  const startedAt = Date.now();
  console.log("CALENDAR_CHECK_START", {
    callSid,
    tradieKey: tradie?.key || "unknown",
    requestedDtISO: dt?.toISO?.() || null
  });

  try {
    const slots = await withTimeout(
      nextAvailableSlots(tradie, dt, 3),
      CALENDAR_CHECK_TIMEOUT_MS,
      "calendar availability"
    );
    const responseMs = Date.now() - startedAt;
    console.log("CALENDAR_CHECK_DONE", {
      callSid,
      tradieKey: tradie?.key || "unknown",
      responseMs,
      available: Array.isArray(slots) && slots.length > 0
    });
    return { ok: true, slots };
  } catch (err) {
    const responseMs = Date.now() - startedAt;
    if (err?.code === "TIMEOUT") {
      console.error("CALENDAR_CHECK_TIMEOUT", {
        callSid,
        tradieKey: tradie?.key || "unknown",
        responseMs,
        timeoutMs: CALENDAR_CHECK_TIMEOUT_MS,
        error: err?.message || String(err)
      });
    } else {
      console.error("CALENDAR_CHECK_ERROR", {
        callSid,
        tradieKey: tradie?.key || "unknown",
        responseMs,
        error: err?.message || String(err)
      });
    }
    return { ok: false, slots: [], error: err };
  }
}

function normStr(s) {
  return String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

const ALLOWED_INITIAL_INTENTS = new Map([
  ["booking", "NEW_BOOKING"],
  ["reschedule", "CANCEL_RESCHEDULE"],
  ["cancel", "CANCEL_RESCHEDULE"],
  ["quote", "QUOTE"],
  ["support", "EXISTING_CUSTOMER"],
  ["admin", "EXISTING_CUSTOMER"],
  ["pricing", "QUOTE"]
]);

function normalizeIntentSpeech(text) {
  return String(text || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\s]/g, "")
    .replace(/\s+/g, " ");
}

function resolveCallSid(req) {
  const fromBody = String(req?.body?.CallSid || req?.body?.CallSID || "").trim();
  const fromQuery = String(req?.query?.CallSid || req?.query?.CallSID || "").trim();
  if (fromBody) return fromBody;
  if (fromQuery) return fromQuery;
  return `missing-callsid-${String(req?.body?.From || req?.query?.From || "unknown").trim() || "unknown"}`;
}

function validateName(speech) {
  const s = String(speech || "").trim();
  return s.length >= 2;
}
function validateAddress(speech) {
  const s = String(speech || "").trim();
  return s.length >= 5;
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
  if (/^(none|no|nope|nah|skip)$/i.test(s)) return true;

  const sl = s.toLowerCase();
  if (/\bno\s+access\b/.test(sl)) return true;
  if (/\bno\s+access\s+notes\b/.test(sl)) return true;
  if (/\bnothing\b/.test(sl) && /\b(access|gate|code|parking|pet|dog|notes?)\b/.test(sl)) return true;

  return s.length >= 2;
}

// confidence gating: only reject when speech is empty, low confidence, or invalid for the current step
function shouldReject(step, speech, confidence) {
  if (!speech || speech.length < 2) return true;
  if (isLowConfidence(confidence)) return true;

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
  const no = ["no","nope","nah","wrong","not right","don’t","dont","change","edit"];

  if (yes.some((w) => t === w || t.includes(w))) return "YES";
  if (no.some((w) => t === w || t.includes(w))) return "NO";
  return null;
}
function detectGlobalVoiceOverride(text) {
  const t = (text || "").toLowerCase();
  if (!t) return null;
  if (t.includes("start over")) return "START_OVER";
  if (t.includes("cancel") || t.includes("goodbye")) return "CANCEL";
  if (t.includes("operator")) return "OPERATOR";
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

async function resolveCalendarTarget(tradie, context = {}) {
  const fallbackCalendarId = String(tradie?.calendarId || "").trim();
  if (fallbackCalendarId && fallbackCalendarId.toLowerCase() !== "default") {
    return {
      calendarId: fallbackCalendarId,
      source: "tradie-config",
      timezone: String(tradie?.timezone || "Australia/Sydney")
    };
  }
  if (fallbackCalendarId.toLowerCase() === "default") {
    console.warn("CALENDAR_TARGET_DEFAULT_REJECTED", {
      bookingId: context.bookingId || null,
      callSid: context.callSid || null,
      tradieKey: tradie?.key || null
    });
  }

  if (supabase && (tradie?.id || tradie?.key)) {
    const query = supabase
      .from(SUPABASE_TRADIE_ACCOUNTS_TABLE)
      .select("calendar_email,calendar_id,timezone")
      .limit(1);

    const scoped = tradie?.key
      ? query.eq("tradie_key", tradie.key)
      : query.eq("tradie_id", tradie.id);

    const { data, error } = await scoped.single();
    if (error && error.code !== "PGRST116") {
      console.error("CALENDAR_TARGET_LOOKUP_ERROR", {
        bookingId: context.bookingId || null,
        callSid: context.callSid || null,
        message: error.message,
        code: error.code || null
      });
    }

    const calendarId = String(data?.calendar_email || data?.calendar_id || "").trim();
    if (calendarId) {
      return {
        calendarId,
        source: "tradie_accounts",
        timezone: String(data?.timezone || tradie?.timezone || "Australia/Sydney")
      };
    }
  }

  console.warn("CALENDAR_TARGET_MISSING", {
    bookingId: context.bookingId || null,
    callSid: context.callSid || null,
    tradieKey: tradie?.key || null,
    tradieId: tradie?.id || null
  });
  return { calendarId: "", source: "none", timezone: String(tradie?.timezone || "Australia/Sydney") };
}

async function createBookingCalendarEvent({ tradie, booking, context = {} }) {
  let calendarId = "";
  const calledNumber = String(context.calledNumber || "").trim();

  try {
    if (supabase && calledNumber) {
      calendarId = await resolveCalendarIdFromCalledNumber({ supabase, calledNumber });
    } else {
      const target = await resolveCalendarTarget(tradie, context);
      calendarId = target.calendarId;
    }
  } catch (err) {
    console.error("CALENDAR_ID_RESOLVE_ERROR", {
      message: err?.message || String(err),
      bookingId: booking.bookingId || context.bookingId || null,
      callSid: context.callSid || booking.callSid || null,
      calledNumber: calledNumber || null
    });
    return { ok: false, reason: "missing_calendar", error: err };
  }

  if (!calendarId) return { ok: false, reason: "missing_calendar" };
  if (String(calendarId).toLowerCase().includes("adimtrades")) {
    throw new Error(`SAFETY_BLOCK: Refusing to insert into admin calendar: ${calendarId}`);
  }

  let calendar;
  try {
    calendar = getCalendarClient(tradie);
  } catch (err) {
    console.error("CALENDAR_EVENT_CREATE_ERROR", {
      message: err?.message || String(err),
      stack: err?.stack || null,
      calendarId
    });
    return { ok: false, reason: "insert_failed", error: err };
  }

  const tz = String(tradie?.timezone || "Australia/Sydney");
  const startDt = DateTime.fromISO(booking.startISO, { zone: tz });
  const endDt = DateTime.fromISO(booking.endISO, { zone: tz });
  const customerName = booking.name || "Unknown";
  const customerPhone = booking.phone || "Unknown";
  const address = booking.address || "Unknown";
  const job = booking.job || "Unknown";
  const startISO = toGoogleDateTime(startDt);
  const endISO = toGoogleDateTime(endDt);
  const requestBody = {
    summary: `${customerName} — ${job}`,
    location: address,
    description: `Name: ${customerName}\nPhone: ${customerPhone}\nAddress: ${address}\nJob: ${job}`,
    start: { dateTime: startISO, timeZone: "Australia/Sydney" },
    end: { dateTime: endISO, timeZone: "Australia/Sydney" }
  };

  console.log("CALENDAR_INSERT_START", {
    callSid: context.callSid || booking.callSid || null,
    calledNumber,
    calendarId
  });

  try {
    const resp = await withTimeout(
      insertCalendarEventWithRetry(calendar, calendarId, requestBody),
      CALENDAR_OP_TIMEOUT_MS,
      "calendar insert"
    );
    const eventId = resp?.data?.id || null;
    const htmlLink = resp?.data?.htmlLink || null;
    console.log("CALENDAR_INSERT_OK", { eventId, htmlLink });
    return { ok: true, eventId, htmlLink, calendarId };
  } catch (err) {
    console.error("CALENDAR_EVENT_CREATE_ERROR", {
      message: err?.message || String(err),
      stack: err?.stack || null,
      calendarId
    });
    return { ok: false, reason: "insert_failed", error: err };
  }
}

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
// VOICE ROUTES
// ----------------------------------------------------------------------------
app.post("/voice", async (req, res) => {
  try {
    if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

    if (supabase) {
      await supabase.from("calls").insert([{
        call_sid: resolveCallSid(req),
        from_number: String(req.body?.From || "").trim() || null,
        to_number: String(req.body?.To || "").trim() || null,
        created_at: new Date().toISOString()
      }]).catch(() => {});
    }

    return handleVoiceEntry(req, res);
  } catch (e) {
    console.error("/voice error", e);
    const twiml = new VoiceResponse();
    twiml.say("Sorry, there was a temporary issue. Please try again.", { voice: "Polly.Amy", language: "en-AU" });
    return sendVoiceTwiml(res, twiml);
  }
});

app.post("/process", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

    const tradie = await getTradieConfig(req);

    // Hard stop if disabled
    if (tradie.status && tradie.status !== "ACTIVE") {
      twiml.say("This service is currently unavailable.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, twiml);
    }

    const tz = tradie.timezone;

    const callSid = resolveCallSid(req);
    const fromNumber = (req.body.From || "").trim();

    const hasSpeechField = Object.prototype.hasOwnProperty.call(req.body || {}, "SpeechResult") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "speechResult");
    const speech = cleanSpeech(req.body.SpeechResult || req.body.speechResult || "");
    const digits = String(req.body.Digits || "").trim();
    const confidenceRaw = req.body.Confidence;
    const hasConfidence = confidenceRaw !== undefined && confidenceRaw !== null && String(confidenceRaw).trim() !== "";
    const confidence = hasConfidence ? Number(confidenceRaw) : null;

    const session = getSession(callSid, fromNumber);

    // state persistence: CallSid keyed session is authoritative for the current step
    const authoritativeStep = session.step || "intent";

    // idempotency lock: ignore duplicate webhooks for the same CallSid+step within a short TTL
    if (hasSpeechField && !acquireProcessSpeechLock(callSid, authoritativeStep)) {
      console.log(`IDEMPOTENT_DUPLICATE TID=${tradie.key} CALLSID=${callSid} STEP=${authoritativeStep}`);
      const replayPrompt = session.lastPrompt || "No worries — take your time. Please say that again.";
      ask(twiml, replayPrompt, voiceActionUrl(req), { input: "speech", timeout: 6, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    // Count inbound call once for analytics
    if (!session._countedCall) {
      session._countedCall = true;
      await incMetric(tradie, { calls_total: 1 }).catch(() => {});
    }

    if (!hasSpeechField && !session.awaitingCalendarCheck) {
      session.hasEnteredVoice = true;
      session.step = "initial";
      session.lastNoSpeechFallback = false;
      session.retryCount = 0;
      console.log(`INITIAL_CALL TID=${tradie.key} CALLSID=${callSid} FROM=${fromNumber} VIA=/process`);
      logVoiceStep(req, { callSid, step: session.step, speech, retryCount: session.silenceTries || 0 });
      return sendVoiceTwiml(res, buildInitialVoiceTwiml(req));
    }

    if (speech) {
      console.log(`SPEECH_RECEIVED TID=${tradie.key} CALLSID=${callSid} FROM=${fromNumber} Speech="${speech}"`);
      session.lastNoSpeechFallback = false;
      addToHistory(session, "user", speech);
    }

    const globalOverride = detectGlobalVoiceOverride(speech);
    if (globalOverride === "START_OVER") {
      session.step = "intent";
      session.lastPrompt = "No worries, starting over. What would you like help with today?";
      resetRetryCountForStep(session, "intent");
      ask(twiml, session.lastPrompt, voiceActionUrl(req), { input: "speech", timeout: 7, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }
    if (globalOverride === "CANCEL") {
      await missedRevenueAlert(tradie, session, "Caller said cancel").catch(() => {});
      twiml.say("No problem. I’ve cancelled this request. Goodbye.", { voice: "Polly.Amy", language: "en-AU" });
      twiml.hangup();
      resetSession(callSid);
      return sendVoiceTwiml(res, twiml);
    }
    if (globalOverride === "OPERATOR") {
      await missedRevenueAlert(tradie, session, "Caller requested operator").catch(() => {});
      twiml.say("No worries. I’ll ask someone to call you back shortly.", { voice: "Polly.Amy", language: "en-AU" });
      resetSession(callSid);
      return sendVoiceTwiml(res, twiml);
    }

    console.log(`TID=${tradie.key} CALLSID=${callSid} TO=${req.body.To} FROM=${fromNumber} STEP=${session.step} Speech="${speech}" Digits="${digits}" Confidence=${confidence}`);
    logVoiceStep(req, { callSid, step: session.step, speech, retryCount: session.silenceTries || 0 });

    // Abuse handling
    if (speech && detectAbuse(speech)) {
      session.abuseStrikes += 1;
      const prefix = abuseReply(session.abuseStrikes);

      if (session.abuseStrikes >= 3) {
        const prompt = `${prefix} If you'd still like help, please tell me what you need.`;
        session.lastPrompt = prompt;
        addToHistory(session, "assistant", prompt);
        ask(twiml, prompt, "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return sendVoiceTwiml(res, twiml);
      }

      const prompt = prefix + (session.lastPrompt || "How can we help today?");
      session.lastPrompt = prompt;
      addToHistory(session, "assistant", prompt);

      const actionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      ask(twiml, prompt, actionUrl);
      return sendVoiceTwiml(res, twiml);
    }

    // Handle no-speech timeout from <Gather> callback only
    if (!speech && !digits && hasSpeechField) {
      console.log(`NO_SPEECH_TIMEOUT TID=${tradie.key} CALLSID=${callSid} FROM=${fromNumber}`);
      session.silenceTries += 1;
      session.lastNoSpeechFallback = true;
      const repeated = repeatLastStepPrompt(req, twiml, session, session.step, "NO_SPEECH");
      if (repeated.shouldReset) resetSession(callSid);
      return sendVoiceTwiml(res, twiml);
    } else {
      session.silenceTries = 0;
      if (speech || digits) {
        session.retryCount = 0;
        session.lastNoSpeechFallback = false;
      }
    }

    // confidence gating: do not advance when Twilio speech confidence is too low
    if (speech && isLowConfidence(confidence)) {
      const currentStep = session.step || "intent";
      console.log(`LOW_CONFIDENCE TID=${tradie.key} CALLSID=${callSid} STEP=${currentStep} confidence=${confidence}`);
      session.lastPrompt = "Sorry, I didn’t quite catch that. Please say it again.";
      ask(twiml, session.lastPrompt, voiceActionUrl(req), { input: "speech", timeout: 6, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    resetRetryCountForStep(session, session.step);

    if (session.step === "sms_fallback_offer") {
      const ynFallback = detectYesNo(speech);
      const wantsText = /\btext me\b/i.test(speech || "");
      if (ynFallback === "YES" || wantsText) {
        console.log(`STEP=sms_fallback_offer speech='${speech}' interpreted='YES|SEND_SMS' retryCount=${getRetryCountForStep(session, "sms_fallback_offer")}`);
        if (session.from) {
          await sendCustomerSms(tradie, session.from, "No worries — use this link to finish your booking: https://example.com/booking").catch(() => {});
        }
        session.step = session.lastStepBeforeFallback || "intent";
        session.lastPrompt = "Done. I’ve sent an SMS link. You can also keep going here — please say your answer again.";
        ask(twiml, session.lastPrompt, voiceActionUrl(req), { input: "speech", timeout: 6, speechTimeout: "auto" });
        return sendVoiceTwiml(res, twiml);
      }

      if (ynFallback === "NO") {
        console.log(`STEP=sms_fallback_offer speech='${speech}' interpreted='NO|DECLINED_SMS' retryCount=${getRetryCountForStep(session, "sms_fallback_offer")}`);
        session.step = session.lastStepBeforeFallback || "intent";
        const resumePrompt = `No worries — take your time. ${session.promptBeforeFallback || "Please say that again."}`;
        ask(twiml, resumePrompt, voiceActionUrl(req), { input: "speech", timeout: 6, speechTimeout: "auto" });
        return sendVoiceTwiml(res, twiml);
      }

      const repeatedFallback = repeatLastStepPrompt(req, twiml, session, "sms_fallback_offer", speech ? "UNCLEAR" : "NO_SPEECH");
      return sendVoiceTwiml(res, twiml);
    }

    // Optional: early “human” request
    if (speech && wantsHuman(speech)) {
      await missedRevenueAlert(tradie, session, "Caller requested human").catch(() => {});
      twiml.say("No worries. I’ll get someone to call you back shortly.", { voice: "Polly.Amy", language: "en-AU" });
      resetSession(callSid);
      return sendVoiceTwiml(res, twiml);
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

        const actionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
        ask(twiml, mergedPrompt, actionUrl);
        return sendVoiceTwiml(res, twiml);
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

      const actionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      ask(twiml, session.lastPrompt, actionUrl);
      return sendVoiceTwiml(res, twiml);
    }

    if (canGlobalInterrupt && yn === "NO") {
      session.step = "clarify";
      session.lastPrompt = "No worries — what should I change? job, address, name, time, or access notes?";
      addToHistory(session, "assistant", session.lastPrompt);

      const actionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      ask(twiml, session.lastPrompt, actionUrl);
      return sendVoiceTwiml(res, twiml);
    }

    // ------------------------------------------------------------------------
    // MAIN FLOW
    // ------------------------------------------------------------------------
    const actionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");

    // STEP: intent
    if (session.step === "intent") {
      const normalized = normalizeIntentSpeech(speech);
      const mappedIntent = ALLOWED_INITIAL_INTENTS.get(normalized);

      // If caller says a supported intent keyword, follow existing routing.
      if (mappedIntent) {
        session.intent = mappedIntent;

        // Cancel/reschedule
        if (session.intent === "CANCEL_RESCHEDULE") {
          session.step = "name";
          session.lastPrompt = "No worries. What is your name so we can reschedule you?";
          addToHistory(session, "assistant", session.lastPrompt);
          ask(twiml, session.lastPrompt, actionUrl);
          return sendVoiceTwiml(res, twiml);
        }

        // Quote
        if (session.intent === "QUOTE") {
          session.step = "job";
          session.lastPrompt = "Sure. What do you need a quote for?";
          addToHistory(session, "assistant", session.lastPrompt);
          ask(twiml, session.lastPrompt, actionUrl);
          return sendVoiceTwiml(res, twiml);
        }

        // Support/admin/existing or booking defaults to normal booking flow.
        session.step = "job";
        session.lastPrompt = "What job do you need help with?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return sendVoiceTwiml(res, twiml);
      }

      // Otherwise treat first answer as the job description and continue.
      if (speech) session.job = speech;
      session.intent = "NEW_BOOKING";
      session.step = "address";
      session.lastPrompt = `Got it — ${session.job}. What’s the address for the job?`;
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { input: "speech" });
      return sendVoiceTwiml(res, twiml);
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
        return sendVoiceTwiml(res, twiml);
      }

      session.lastPrompt = "Sorry — what should I change? job, address, name, time, or access notes?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { input: "speech", timeout: 7, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: job
    if (session.step === "job") {
      if (speech) session.job = speech;

      if (shouldReject("job", session.job, confidence)) {
        session.lastPrompt = "Sorry — what job do you need help with?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return sendVoiceTwiml(res, twiml);
      }

      session.step = "address";
      session.lastPrompt = "What is the address?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: address
    if (session.step === "address") {
      if (speech) session.address = speech;

      if (shouldReject("address", session.address, confidence)) {
        session.lastPrompt = "Sorry — what is the full address?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return sendVoiceTwiml(res, twiml);
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
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: name
    if (session.step === "name") {
      if (speech) session.name = speech;

      if (shouldReject("name", session.name, confidence)) {
        session.lastPrompt = "Sorry — what name should I put the booking under?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return sendVoiceTwiml(res, twiml);
      }

      session.step = "access";
      session.lastPrompt = "Any access notes like gate code, parking, or pets? Say none if not.";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return sendVoiceTwiml(res, twiml);
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
        resetSession(callSid);
        return sendVoiceTwiml(res, twiml);
      }

      session.step = "time";
      session.lastPrompt = "What time would you like?";
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl);
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: time
    if (session.step === "time") {
      session.time = speech || session.time || "";

      let dt = null;
      if (session.bookedStartMs) dt = DateTime.fromMillis(session.bookedStartMs, { zone: tz });
      else {
        if (!looksLikeAsap(session.time)) dt = parseRequestedDateTime(session.time, tz);
        if (!dt && speech && !looksLikeAsap(session.time)) {
          session.lastPrompt = "Sorry, I didn’t quite catch that time. Please say it again, for example: tomorrow at 2 pm.";
          ask(twiml, session.lastPrompt, actionUrl, { input: "speech", timeout: 6, speechTimeout: "auto" });
          return sendVoiceTwiml(res, twiml);
        }
        if (!dt && isAfterHoursNow(tradie)) dt = nextBusinessOpenSlot(tradie);
        if (!dt) dt = DateTime.now().setZone(tz).plus({ minutes: 10 }).startOf("minute");
      }

      if (tradie.calendarId && tradie.googleServiceJson) {
        session.bookedStartMs = dt.toMillis();
        session.calendarCheck = {
          requestedDtISO: dt.toISO(),
          attempts: 0
        };
        keepCallAliveForProcessing(req, twiml, "Okay, just a moment while I check that for you.");
        return sendVoiceTwiml(res, twiml);
      }

      session.bookedStartMs = dt.toMillis();
      session.step = "confirm";

      const whenText = formatForVoice(dt);

      // Duplicate detection (calendar)
      if (tradie.calendarId && tradie.googleServiceJson) {
        try {
          const calendar = getCalendarClient(tradie);
          const dup = await withTimeout(
            findDuplicate(calendar, tradie.calendarId, tz, session.name, session.address, dt),
            CALENDAR_OP_TIMEOUT_MS,
            "calendar duplicate check"
          );
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
Is that correct? Please say 'yes' to confirm or 'no' to change it.`;

      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { input: "speech", timeout: 7, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: pickSlot
    if (session.step === "pickSlot") {
      if (speech && wantsRepeatOptions(speech)) {
        const slots = (session.proposedSlots || []).map((ms) => DateTime.fromMillis(ms, { zone: tz }));
        session.lastPrompt = `No worries. Options are: ${slotsVoiceLine(slots, tz)} Say first, second, or third — or tell me another time.`;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return sendVoiceTwiml(res, twiml);
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
        return sendVoiceTwiml(res, twiml);
      }

      const slots = (session.proposedSlots || []).map((ms) => DateTime.fromMillis(ms, { zone: tz }));
      if (idx == null || !slots[idx]) {
        session.lastPrompt = "Say first, second, or third. Or press 1, 2, or 3. Or tell me another time.";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl);
        return sendVoiceTwiml(res, twiml);
      }

      const chosen = slots[idx];
      session.bookedStartMs = chosen.toMillis();
      session.step = "confirm";

      const whenText = formatForVoice(chosen);
      const noteLine = session.customerNote ? `I see a note on your file. ` : "";
      const accessLine = session.accessNote ? `Access notes: ${session.accessNote}. ` : "";

      session.lastPrompt =
`Great. I’ve got ${session.name}, ${session.address}, for ${session.job}, at ${whenText}. ${noteLine}${accessLine}
Is that correct? Please say 'yes' to confirm or 'no' to change it.`;

      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { input: "speech", timeout: 7, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: confirm
    if (session.step === "confirm") {
      const confirmConfidenceTooLow = typeof confidence === "number" && confidence > 0 && confidence < 0.45;
      const yn2 = detectYesNo(speech);

      if (!speech || confirmConfidenceTooLow || !yn2) {
        const interpreted = !speech ? "NO_SPEECH" : (confirmConfidenceTooLow ? "UNCLEAR" : "UNCLEAR");
        console.log(`STEP=confirm speech='${speech}' interpreted='${interpreted}' retryCount=${getRetryCountForStep(session, "confirm") + 1}`);
        const repeatedConfirm = repeatLastStepPrompt(req, twiml, session, "confirm", interpreted);
        if (repeatedConfirm.shouldReset) resetSession(callSid);
        return sendVoiceTwiml(res, twiml);
      }

      resetRetryCountForStep(session, "confirm");
      if (yn2 === "NO") {
        console.log(`STEP=confirm speech='${speech}' interpreted='NO' retryCount=${getRetryCountForStep(session, "confirm")}`);
        session.step = "clarify";
        session.lastPrompt = "No worries — what should I change? job, address, name, time, or access notes?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { input: "speech", timeout: 7, speechTimeout: "auto" });
        return sendVoiceTwiml(res, twiml);
      }

      console.log(`STEP=confirm speech='${speech}' interpreted='YES' retryCount=${getRetryCountForStep(session, "confirm")}`);

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
      const bookingId = `${callSid}:${startDt.toMillis()}`;
      const payload = {
        bookingId,
        callSid,
        tradie_key: tradie.key,
        from: session.from,
        name: session.name,
        address: session.address,
        job: session.job,
        access: session.accessNote || "",
        startISO: startDt.toISO(),
        endISO: endDt.toISO(),
        timezone: tz
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

      const eventResult = await createBookingCalendarEvent({
        tradie,
        booking: {
          bookingId,
          callSid,
          name: session.name,
          phone: session.from,
          address: session.address,
          job: session.job,
          startISO: startDt.toISO(),
          endISO: endDt.toISO()
        },
        context: { bookingId, callSid, calledNumber: req.body.To }
      });

      let customerCalendarNotice = "";
      if (!eventResult.ok && eventResult.reason === "missing_calendar") {
        await sendOwnerSms(tradie, "Calendar not connected yet — please share your calendar.").catch(() => {});
        customerCalendarNotice = " Calendar not connected yet — please share your calendar.";
      }
      if (!eventResult.ok && eventResult.reason === "insert_failed") {
        customerCalendarNotice = " I couldn’t write to the calendar yet, but I’ve saved the booking and will text you.";
        await missedRevenueAlert(tradie, session, "Calendar insert failed — manual follow-up").catch(() => {});
      }

      // Customer SMS: confirm Y/N
      if (session.from) {
        await sendCustomerSms(
          tradie,
          session.from,
          `Booked: ${formatForVoice(startDt)} at ${session.address} for ${session.job}. Reply Y to confirm or N to reschedule.`
        ).catch(() => {});
      }

      twiml.say(`All set. We’ve sent you a text to confirm. Thanks!${customerCalendarNotice}`, { voice: "Polly.Amy", language: "en-AU" });
      resetSession(callSid);
      return sendVoiceTwiml(res, twiml);
    }

    // Fallback for missing/unknown step
    session.step = "intent";
    session.lastPrompt = "Hi. What would you like help with today?";
    addToHistory(session, "assistant", session.lastPrompt);
    ask(twiml, session.lastPrompt, actionUrl, { input: "speech" });
    return sendVoiceTwiml(res, twiml);
  } catch (err) {
    console.error("VOICE ERROR:", err);
    twiml.say("Sorry, there was a system error. Please try again.", { voice: "Polly.Amy", language: "en-AU" });
    return sendVoiceTwiml(res, twiml);
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

    if (supabase) {
      await supabase.from("messages").insert([{
        from_number: from || null,
        to_number: String(req.body.To || "").trim() || null,
        message: body || null,
        message_sid: String(req.body.MessageSid || "").trim() || null,
        created_at: new Date().toISOString()
      }]).catch(() => {});
    }

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
        const eventResult = await createBookingCalendarEvent({
          tradie,
          booking: {
            bookingId: payload.bookingId,
            callSid: payload.callSid,
            name: payload.name,
            phone: from,
            address: payload.address,
            job: payload.job,
            startISO: payload.startISO,
            endISO: payload.endISO
          },
          context: {
            bookingId: payload.bookingId,
            callSid: payload.callSid,
            calledNumber: req.body.To
          }
        });

        await sendOwnerSms(tradie, `CUSTOMER CONFIRMED ✅
${nice}`).catch(() => {});

        if (!eventResult.ok && eventResult.reason === "missing_calendar") {
          await sendOwnerSms(tradie, "Calendar not connected yet — please share your calendar.").catch(() => {});
          twiml.message("Confirmed ✅ Calendar not connected yet — please share your calendar.");
        } else if (!eventResult.ok) {
          twiml.message("Confirmed ✅ I couldn’t write to the calendar yet, but I’ve saved the booking and will text you.");
        } else {
          twiml.message("Confirmed ✅ Thanks — see you then.");
        }

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

app.post("/twilio/voice", async (req, res) => {
  try {
    if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");
    return handleVoiceEntry(req, res);
  } catch (e) {
    console.error("/twilio/voice error", e);
    const twiml = new VoiceResponse();
    twiml.say("Sorry, we hit a temporary issue. Please try again.", { voice: "Polly.Amy", language: "en-AU" });
    return sendVoiceTwiml(res, twiml);
  }
});

app.all("/voice/inbound", (req, res) => {
  return handleVoiceEntry(req, res);
});

app.post("/twilio/status", (req, res) => {
  console.log("twilio status callback", {
    callSid: String(req.body?.CallSid || ""),
    callStatus: String(req.body?.CallStatus || ""),
    to: String(req.body?.To || ""),
    from: String(req.body?.From || "")
  });
  return res.status(200).send("ok");
});

app.post("/twilio/sms", (req, res) => {
  return res.redirect(307, "/sms");
});

app.post("/next-step", async (req, res) => {
  try {
    if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");
    const twiml = new VoiceResponse();
    const processUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
    twiml.redirect({ method: "POST" }, processUrl);
    return sendVoiceTwiml(res, twiml);
  } catch (e) {
    console.error("/next-step error", e);
    const twiml = new VoiceResponse();
    twiml.say("Sorry, there was a temporary issue. Please tell me what job you need help with.", { voice: "Polly.Amy", language: "en-AU" });
    const processUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
    twiml.redirect({ method: "POST" }, processUrl);
    return sendVoiceTwiml(res, twiml);
  }
});

app.post("/time", async (req, res) => {
  try {
    if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");
    const twiml = new VoiceResponse();
    const actionUrl = voiceActionUrl(req);
    const gather = twiml.gather({
      input: "speech",
      timeout: 7,
      speechTimeout: "auto",
      action: actionUrl,
      method: "POST",
      language: "en-AU"
    });
    gather.say("That time is not available. Would you like another time?", { voice: "Polly.Amy", language: "en-AU" });
    return sendVoiceTwiml(res, twiml);
  } catch (e) {
    console.error("/time error", e);
    const twiml = new VoiceResponse();
    twiml.say("Sorry, there was a temporary issue. Please tell me another time.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.redirect({ method: "POST" }, voiceActionUrl(req));
    return sendVoiceTwiml(res, twiml);
  }
});

app.post("/confirm", async (req, res) => {
  try {
    if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");
    const twiml = new VoiceResponse();
    const actionUrl = voiceActionUrl(req);
    const gather = twiml.gather({
      input: "speech",
      timeout: 7,
      speechTimeout: "auto",
      action: actionUrl,
      method: "POST",
      language: "en-AU"
    });
    gather.say("Good news — that time is available. Would you like me to book it?", { voice: "Polly.Amy", language: "en-AU" });
    return sendVoiceTwiml(res, twiml);
  } catch (e) {
    console.error("/confirm error", e);
    const twiml = new VoiceResponse();
    twiml.say("Sorry, there was a temporary issue. Let’s continue.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.redirect({ method: "POST" }, voiceActionUrl(req));
    return sendVoiceTwiml(res, twiml);
  }
});

app.post("/check-availability", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    if (!validateTwilioSignature(req)) return res.status(403).send("Forbidden");

    const tradie = await getTradieConfig(req);
    const tz = tradie.timezone;
    const callSid = resolveCallSid(req);
    const fromNumber = (req.body.From || "").trim();
    const session = getSession(callSid, fromNumber);
    if (!(tradie.calendarId && tradie.googleServiceJson)) {
      twiml.redirect({ method: "POST" }, "/confirm" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return sendVoiceTwiml(res, twiml);
    }

    const bookedStartMs = Number(session.bookedStartMs || 0);
    const dt = bookedStartMs ? DateTime.fromMillis(bookedStartMs, { zone: tz }) : null;
    if (!dt || !dt.isValid) {
      session.step = "time";
      twiml.redirect({ method: "POST" }, "/time" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return sendVoiceTwiml(res, twiml);
    }

    session.step = "time";
    session.calendarCheck = session.calendarCheck || {};
    session.calendarCheck.attempts = Number(session.calendarCheck.attempts || 0);

    session.calendarCheck.attempts += 1;
    const result = await performCalendarAvailabilityCheck({ tradie, dt, tz, callSid });
    if (result.ok) {
      const slots = Array.isArray(result.slots) ? result.slots : [];
      const requestedDt = DateTime.fromISO(session.calendarCheck.requestedDtISO || dt.toISO(), { zone: tz });

      if (slots.length === 0) {
        session.bookedStartMs = null;
        session.lastPrompt = "That time is not available. Would you like another time?";
        twiml.say("That time is not available. Would you like another time?", { voice: "Polly.Amy", language: "en-AU" });
        twiml.redirect({ method: "POST" }, "/time" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return sendVoiceTwiml(res, twiml);
      }

      const first = slots[0];
      const deltaMin = Math.abs(first.diff(requestedDt, "minutes").minutes);
      if (deltaMin > 5) {
        session.bookedStartMs = null;
        session.lastPrompt = "That time is not available. Would you like another time?";
        twiml.say("That time is not available. Would you like another time?", { voice: "Polly.Amy", language: "en-AU" });
        twiml.redirect({ method: "POST" }, "/time" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
        return sendVoiceTwiml(res, twiml);
      }

      session.calendarCheck = null;
      session.bookedStartMs = first.toMillis();
      session.step = "confirm";
      session.lastPrompt = "Good news — that time is available. Would you like me to book it?";
      twiml.say("Good news — that time is available. Would you like me to book it?", { voice: "Polly.Amy", language: "en-AU" });
      twiml.redirect({ method: "POST" }, "/confirm" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return sendVoiceTwiml(res, twiml);
    }

    if (session.calendarCheck.attempts === 1) {
      twiml.say("I’m having trouble checking the calendar right now. Let me try that again.", { voice: "Polly.Amy", language: "en-AU" });
      twiml.redirect({ method: "POST" }, "/check-availability" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return sendVoiceTwiml(res, twiml);
    }

    session.calendarCheck = null;
    twiml.say("Please try again later or reply to our SMS confirmation.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.hangup();
    return sendVoiceTwiml(res, twiml);
  } catch (e) {
    console.error("/check-availability error", e);
    twiml.say("Please try again later or reply to our SMS confirmation.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.hangup();
    return sendVoiceTwiml(res, twiml);
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
console.log("FORM HIT", new Date().toISOString());
console.log("secret header:", req.get("x-form-secret"));
console.log("body keys:", Object.keys(req.body || {}));
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

if (!supabase) return res.status(500).send("Supabase not configured");

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


app.post("/debug/create-test-event", express.json({ limit: "256kb" }), async (req, res) => {
  const expected = String(process.env.DEBUG_CALENDAR_SECRET || "").trim();
  const got = String(req.get("x-debug-secret") || "").trim();
  if (!expected) return res.status(500).json({ ok: false, error: "Missing DEBUG_CALENDAR_SECRET" });
  if (!got || got !== expected) return res.status(401).json({ ok: false, error: "Unauthorized" });

  try {
    const tradie = await getTradieConfig(req);
    const requestedCalendarId = String(req.body?.calendarId || "").trim();
    const now = DateTime.now().setZone(tradie.timezone || "Australia/Sydney");
    const startDt = now.plus({ minutes: 5 }).startOf("minute");
    const endDt = startDt.plus({ minutes: Number(req.body?.durationMinutes || tradie.slotMinutes || 60) });
    const bookingId = `debug:${Date.now()}`;

    if (requestedCalendarId) tradie.calendarId = requestedCalendarId;

    const result = await createBookingCalendarEvent({
      tradie,
      booking: {
        bookingId,
        callSid: "debug",
        name: "Debug Tester",
        phone: String(req.body?.phone || "debug"),
        address: String(req.body?.address || "Debug Address"),
        job: "TEST EVENT",
        summary: "TEST EVENT",
        startISO: startDt.toISO(),
        endISO: endDt.toISO()
      },
      context: { bookingId, callSid: "debug" }
    });

    if (!result.ok) return res.status(500).json({ ok: false, result });
    return res.json({ ok: true, result });
  } catch (err) {
    console.error("CALENDAR_EVENT_CREATE_ERROR", {
      message: err?.message || String(err),
      stack: err?.stack || null,
      calendarId: String(req.body?.calendarId || "") || null
    });
    return res.status(500).json({ ok: false, error: err?.message || "failed" });
  }
});

// ----------------------------------------------------------------------------
// Error handler
// ----------------------------------------------------------------------------
app.use((err, req, res, next) => {
  console.error("UNHANDLED_ROUTE_ERROR", err);
  if (res.headersSent) return next(err);
  return res.status(500).json({ error: "Internal Server Error" });
});

// ----------------------------------------------------------------------------
// Listen
// ----------------------------------------------------------------------------
const PORT = Number(process.env.PORT || 10000);
if (!PORT || Number.isNaN(PORT)) throw new Error("PORT missing/invalid");

if (require.main === module) {
  app.listen(PORT, () => console.log("Server listening on", PORT));
}

module.exports = app;
