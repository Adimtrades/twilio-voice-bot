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
// LLM_ENABLED=false, OPENAI_API_KEY, LLM_BASE_URL, LLM_MODEL
// GOOGLE_SERVICE_JSON, GOOGLE_SERVICE_JSON_FILE, GOOGLE_APPLICATION_CREDENTIALS
// ADMIN_DASH_PASSWORD, ADMIN_ALERT_NUMBER

try { require("dotenv").config(); } catch {}
const express = require("express");
const twilio = require("twilio");
const nodemailer = require("nodemailer");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const stripe = require("stripe")(process.env.STRIPE_SECRET_KEY || "");
const chrono = require("chrono-node");
const { DateTime } = require("luxon");
const { google } = require("googleapis");
const { createClient } = require("@supabase/supabase-js");
const OpenAI = require("openai");
const behaviouralEngine = require("./behaviouralEngine");

const BOT_VERSION = process.env.BOT_VERSION || process.env.npm_package_version || "1.0.0";
const LOG_TAG = `[bot:${BOT_VERSION}]`;

function readGoogleServiceAccount() {
  const parseServiceJson = (rawJson, sourceLabel) => {
    try {
      return JSON.parse(rawJson);
    } catch (error) {
      throw new Error(`Invalid Google service account JSON from ${sourceLabel}: ${error?.message || error}`);
    }
  };

  const googleServiceJson = String(process.env.GOOGLE_SERVICE_JSON || "").trim();
  if (googleServiceJson) {
    if (googleServiceJson.startsWith("{")) {
      console.log(`${LOG_TAG} Google service account loaded from GOOGLE_SERVICE_JSON inline JSON`);
      return parseServiceJson(googleServiceJson, "GOOGLE_SERVICE_JSON");
    }

    const envPath = googleServiceJson;
    const fileContents = fs.readFileSync(envPath, "utf8");
    console.log(`${LOG_TAG} Google service account loaded from GOOGLE_SERVICE_JSON file path`);
    return parseServiceJson(fileContents, `GOOGLE_SERVICE_JSON file (${envPath})`);
  }

  const googleServiceJsonFile = String(process.env.GOOGLE_SERVICE_JSON_FILE || "").trim();
  if (googleServiceJsonFile) {
    const resolvedPath = path.resolve(__dirname, googleServiceJsonFile);
    const fileContents = fs.readFileSync(resolvedPath, "utf8");
    console.log(`${LOG_TAG} Google service account loaded from GOOGLE_SERVICE_JSON_FILE`);
    return parseServiceJson(fileContents, `GOOGLE_SERVICE_JSON_FILE (${resolvedPath})`);
  }

  const googleApplicationCredentials = String(process.env.GOOGLE_APPLICATION_CREDENTIALS || "").trim();
  if (googleApplicationCredentials) {
    const fileContents = fs.readFileSync(googleApplicationCredentials, "utf8");
    console.log(`${LOG_TAG} Google service account loaded from GOOGLE_APPLICATION_CREDENTIALS`);
    return parseServiceJson(fileContents, `GOOGLE_APPLICATION_CREDENTIALS (${googleApplicationCredentials})`);
  }

  throw new Error("Missing GOOGLE_SERVICE_JSON env/config");
}

readGoogleServiceAccount();

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

app.use((req, res, next) => {
  console.log("REQ", req.method, req.originalUrl);
  next();
});

// ----------------------------------------------------------------------------
// Process-level safety
// ----------------------------------------------------------------------------
process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

process.on("unhandledRejection", (err) => {
  console.error("UNHANDLED PROMISE:", err);
});

process.on("SIGTERM", () => {
  console.log("Graceful shutdown start");
  process.exit(0);
});

setInterval(() => {
  const m = process.memoryUsage();
  console.log("MEM:", Math.round(m.rss / 1024 / 1024), "MB");
}, 60000);

const safeAsync = (fn) => (req, res, next) =>
  Promise.resolve(fn(req, res, next)).catch((err) => {
    console.error("ROUTE ERROR:", err);
    if (res.headersSent) return;
    const isSmsLike = (req.originalUrl || "").includes("/sms");
    const fallbackTwiml = isSmsLike
      ? "<Response><Message>Sorry, we are experiencing a temporary issue. Let’s continue.</Message></Response>"
      : "<Response><Say>Sorry, we are experiencing a temporary issue. Let’s continue.</Say></Response>";
    res.type("text/xml");
    res.send(fallbackTwiml);
  });

function wrapRouteMethod(method) {
  return (routePath, ...handlers) => {
    const wrapped = handlers.map((handler) => (typeof handler === "function" ? safeAsync(handler) : handler));
    return method(routePath, ...wrapped);
  };
}

app.get = wrapRouteMethod(app.get.bind(app));
app.post = wrapRouteMethod(app.post.bind(app));

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

app.use((req, res, next) => {
  const callSid = String(req.body?.CallSid || req.query?.CallSid || "").trim();
  if (callSid && sessions && sessions.has(callSid)) {
    const session = sessions.get(callSid);
    if (session) session.lastActivity = Date.now();
  }
  next();
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    version: BOT_VERSION,
    time: new Date().toISOString(),
    uptimeSeconds: Math.floor(process.uptime())
  });
});

app.get("/debug/google-config", (req, res) => {
  const secret = String(req.query.secret || "").trim();
  if (secret !== process.env.DEBUG_CALENDAR_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  return res.json({
    hasClientId: !!process.env.GOOGLE_CLIENT_ID,
    hasClientSecret: !!process.env.GOOGLE_CLIENT_SECRET,
    redirectUri: process.env.GOOGLE_REDIRECT_URI || process.env.GOOGLE_REDIRECT_URL || null,
    clientIdPrefix: (process.env.GOOGLE_CLIENT_ID || "").slice(0, 12),
  });
});

const BASE_URL = process.env.BASE_URL || "http://localhost:3000";
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || `${BASE_URL}/api/google/callback`;

const oauth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  GOOGLE_REDIRECT_URI
);

app.get("/api/google/connect", async (req, res) => {
  try {
    const tradieId = req.query.tradieId;
    if (!tradieId) {
      return res.status(400).send("Missing tradieId");
    }

    const authUrl = oauth2Client.generateAuthUrl({
      access_type: "offline",
      prompt: "consent",
      scope: "https://www.googleapis.com/auth/calendar",
      state: tradieId
    });

    return res.redirect(authUrl);

  } catch (err) {
    console.error("Google Auth Error:", err);
    return res.status(500).send("Auth failed");
  }
});

app.get("/api/google/callback", async (req, res) => {
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
    const accessToken = String(tokens?.access_token || "").trim();
    const expiryDate = Number(tokens?.expiry_date || 0);

    const tradiesUpdate = {
      google_refresh_token: refreshToken || null,
      google_access_token: accessToken || null,
      google_expiry_date: expiryDate ? new Date(expiryDate).toISOString() : null,
      google_connected: true,
      calendar_id: "primary",
      updated_at: new Date().toISOString()
    };

    const { error: tradiesError } = await supabase
      .from(SUPABASE_TRADIES_TABLE)
      .update(tradiesUpdate)
      .eq("id", tradieId);

    if (tradiesError) {
      console.error("GOOGLE_CALLBACK_TRADIES_UPDATE_ERROR", {
        tradieId,
        message: tradiesError.message,
        code: tradiesError.code || null
      });
      return res.status(500).json({ error: "Unable to save Google connection" });
    }

    return res.send(`
  <!DOCTYPE html>
  <html>
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Calendar Connected</title>
    <style>
      body { font-family: sans-serif; display: flex; align-items: center;
             justify-content: center; min-height: 100vh; margin: 0;
             background: #f0fdf4; }
      .card { background: white; border-radius: 12px; padding: 40px;
              text-align: center; max-width: 400px; box-shadow: 0 4px 24px rgba(0,0,0,0.08); }
      .tick { font-size: 48px; margin-bottom: 16px; }
      h1 { color: #16a34a; font-size: 24px; margin: 0 0 12px; }
      p { color: #555; line-height: 1.6; margin: 0; }
    </style>
  </head>
  <body>
    <div class="card">
      <div class="tick">✅</div>
      <h1>Calendar Connected!</h1>
      <p>Your AI phone bot is now set up and ready to take bookings.<br><br>
      Callers can now book jobs directly through your phone number.</p>
    </div>
  </body>
  </html>
`);
  } catch (error) {
    console.error("GOOGLE_CALLBACK_ERROR", {
      message: error?.message || String(error),
      stack: error?.stack || null,
      code: error?.code || null,
      status: error?.status || error?.response?.status || null,
      data: error?.response?.data || null
    });
    return res.status(500).json({
      error: "Google connection failed",
      detail: error?.message || String(error)
    });
  }
});

app.get("/google/auth", (req, res) => res.redirect(302, `/api/google/connect?tradieId=${encodeURIComponent(String(req.query.tradieId || ""))}`));
app.get("/google/callback", (req, res) => {
  const params = new URLSearchParams(req.query || {}).toString();
  return res.redirect(302, `/api/google/callback${params ? `?${params}` : ""}`);
});
app.get("/auth/google", (req, res) => res.redirect(302, `/api/google/connect?tradieId=${encodeURIComponent(String(req.query.tradieId || ""))}`));
app.get("/auth/google/callback", (req, res) => {
  const params = new URLSearchParams(req.query || {}).toString();
  return res.redirect(302, `/api/google/callback${params ? `?${params}` : ""}`);
});

function collectRoutesFromStack(stack, basePath = "") {
  const routes = [];

  for (const layer of stack || []) {
    if (layer.route?.path) {
      const methods = Object.keys(layer.route.methods || {}).filter((method) => layer.route.methods[method]);
      for (const method of methods) {
        routes.push({
          method: method.toUpperCase(),
          path: `${basePath}${layer.route.path}`
        });
      }
      continue;
    }

    if (layer.name === "router" && layer.handle?.stack) {
      const mountPath = layer.path || "";
      routes.push(...collectRoutesFromStack(layer.handle.stack, `${basePath}${mountPath}`));
    }
  }

  return routes;
}

app.get("/debug/routes", (req, res) => {
  const stack = app.router?.stack || app._router?.stack || [];
  const routes = collectRoutesFromStack(stack)
    .filter((route) => route.path)
    .map((route) => ({
      method: route.method,
      path: route.path.startsWith("/") ? route.path : `/${route.path}`
    }));

  return res.json({ routes });
});

const VoiceResponse = twilio.twiml.VoiceResponse;
const MessagingResponse = twilio.twiml.MessagingResponse;
const MAX_NO_SPEECH_RETRIES = Number(process.env.MAX_NO_SPEECH_RETRIES || 2);
const CALENDAR_OP_TIMEOUT_MS = Number(process.env.CALENDAR_OP_TIMEOUT_MS || 8000);
const CALENDAR_CHECK_TIMEOUT_MS = Number(process.env.CALENDAR_CHECK_TIMEOUT_MS || 5000);
const VOICE_GATHER_TIMEOUT_SECONDS = Number(process.env.VOICE_GATHER_TIMEOUT_SECONDS || 6);
const SESSION_TTL_MS = Number(process.env.VOICE_SESSION_TTL_MS || 30 * 60 * 1000);
const WEBHOOK_IDEMPOTENCY_TTL_MS = Number(process.env.WEBHOOK_IDEMPOTENCY_TTL_MS || 7000);
const MIN_SPEECH_CONFIDENCE = Number(process.env.MIN_SPEECH_CONFIDENCE || 0.15);

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

function isValidE164(phone) {
  return /^\+[1-9]\d{7,14}$/.test(String(phone || "").trim());
}

function hashOnboardingToken(token) {
  return crypto.createHash("sha256").update(String(token || "")).digest("hex");
}

function generateOnboardingToken() {
  return crypto.randomBytes(32).toString("hex");
}

async function lookupTradieAccountByCalledNumber({ supabase, calledNumber }) {
  const normalizedCalledNumber = normalizePhoneE164AU(calledNumber);
  const digitsOnly = normalizedCalledNumber.replace(/^\+/, "");
  if (!normalizedCalledNumber) return { account: null, normalizedCalledNumber };

  const { data, error } = await supabase
    .from(SUPABASE_TRADIE_ACCOUNTS_TABLE)
    .select("tradie_id")
    .or([
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

function parseGoogleServiceJson() {
  const rawValue = process.env.GOOGLE_SERVICE_JSON;
  if (typeof rawValue !== "string" || !rawValue.trim()) {
    throw new Error("Missing required env variable: GOOGLE_SERVICE_JSON");
  }

  const value = rawValue.trim();
  const isInlineJson = value.startsWith("{");
  let jsonText = value;

  if (!isInlineJson) {
    if (!fs.existsSync(value)) {
      throw new Error(`GOOGLE_SERVICE_JSON file not found: ${value}`);
    }

    jsonText = fs.readFileSync(value, "utf8");
  }

  try {
    return JSON.parse(jsonText);
  } catch (err) {
    const parseReason = err?.message || String(err);
    const source = isInlineJson ? "GOOGLE_SERVICE_JSON env value" : `GOOGLE_SERVICE_JSON file (${value})`;
    throw new Error(`Invalid JSON in ${source}: ${parseReason}`);
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

  const twilioNumber = String(t.twilio_phone_number || t.twilio_number || t.twilioNumber || t.twilio_to || "");
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

    calendarId: String(t.calendarId || t.calendar_id || ""),
    googleRefreshToken: String(t.googleRefreshToken || t.google_refresh_token || ""),

    avgJobValue: Number.isFinite(avgJobValue) ? avgJobValue : 250,
    closeRate: Number.isFinite(closeRate) ? closeRate : 0.6,
    slotMinutes: Number.isFinite(slotMinutes) ? slotMinutes : 60,
    bufferMinutes: Number.isFinite(bufferMinutes) ? bufferMinutes : 0,

    bizName,
    botName: String(t.botName || t.bot_name || process.env.BOT_NAME || "Alex"),
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
function buildLlmSystemPrompt(tradie, session = {}) {
  const biz = tradie.bizName ? `Business name: ${tradie.bizName}.` : "";
  const services = tradie.services ? `Services offered: ${tradie.services}.` : "";

  const collected = {
    job: session.job || null,
    address: session.address || null,
    name: session.name || null,
    time: session.time || null,
    access: session.accessNote || null
  };

  const offScriptCount = Number(session.offScriptCount || 0);
  const callerTone = session.detectedTone || "casual";
  const toneInstruction = callerTone === "formal"
    ? "The caller is speaking formally. Be professional, precise, use full sentences, avoid contractions."
    : "The caller is casual. Be warm, conversational, use contractions, keep it natural and relaxed.";
  const abuseStrikes = Number(session.abuseStrikes || 0);
  const currentStep = session.step || "intent";

  const stepQuestions = {
    intent: "What do you need help with today?",
    job: "What job do you need help with?",
    address: "What is the address?",
    name: "What is your name?",
    access: "Any access notes like gate code, parking, or pets?",
    time: "What time would you like?",
    confirm: "Is that correct?"
  };

  const nextQuestion = stepQuestions[currentStep] || "How can I help you?";

  return `You are an AI assistant helping a human receptionist handle difficult callers for a trades business. ${biz} ${services}

YOUR ONLY JOB IN THIS FUNCTION:
The main booking script is handled by separate code. You are ONLY called when a caller goes off-script, gets emotional, angry, confused, or starts rambling.
Your job is to produce a SHORT acknowledgement (smalltalk_reply) that a real human receptionist would say to calm the caller and bring them back on track.
After your acknowledgement the system will automatically ask the next booking question: "${nextQuestion}"
So do NOT include the next booking question in your smalltalk_reply — it will be appended automatically.

COLLECTED SO FAR: ${JSON.stringify(collected)}
CURRENT STEP: ${currentStep}
OFF SCRIPT COUNT THIS CALL: ${offScriptCount}
ABUSE STRIKES: ${abuseStrikes}

YOUR PERSONALITY:
- Warm, calm, human, Australian
- Never robotic or corporate
- Never say "I understand your frustration" — mirror their words instead
- Never apologise more than once per call
- Keep smalltalk_reply to 1-2 short sentences maximum

TONE MATCHING:
${toneInstruction}
Never use a tone that conflicts with how the caller is speaking.
If caller uses formal language like "I would like to enquire" match that register.
If caller is casual like "yeah mate" or "heaps good" match that energy.

OFF-SCRIPT TECHNIQUE:
When caller is angry or upset:
1. Mirror one specific thing they said using their own words (1 sentence)
2. Validate briefly (1 sentence)
Example: caller says "I have been waiting three weeks"
smalltalk_reply: "Three weeks with no update — that is not on and I get why you are frustrated."

When caller is rambling:
Pick one word or phrase from what they said and mirror it briefly.
Example: smalltalk_reply: "Sounds like it has been a rough one."

When caller is confused:
Clarify in one plain sentence only.

COMPETITOR AND PRICE OBJECTION HANDLING:
If caller mentions a competitor by name or says phrases like
"the last guy charged less", "I can get it cheaper elsewhere",
"another company quoted me less", "your prices are too high":
- Never mention or badmouth any competitor
- Acknowledge briefly in one sentence: "Totally fair to shop around."
- Pivot immediately to booking: "What sets us apart is we show up
  on time and get it done right. Let me get you locked in."
- Never get into a price debate on the call
- If they keep pushing on price say exactly:
  "I can note that for the tradie and they can discuss pricing
  options with you when they arrive."
- Always bridge back to the next missing booking field immediately
  after acknowledging the objection

When offScriptCount >= 3:
Skip mirror and validation. Just say: "I really want to get this sorted for you."

When offScriptCount >= 5:
Say: "Let me get someone to call you back — I just need a couple of details."

When abuseStrikes >= 3:
Say: "I want to help — let us keep it respectful and get this sorted."

EMERGENCY — if caller mentions burst pipe, gas leak, flooding, sparking, fire, sewage, no power:
smalltalk_reply: "That sounds urgent — I am getting someone to you now."
Set intent to EMERGENCY.

OUTPUT FORMAT — respond ONLY with valid JSON, no markdown, no text outside JSON:
{
  "intent": "NEW_BOOKING" | "QUOTE" | "EMERGENCY" | "CANCEL_RESCHEDULE" | "EXISTING_CUSTOMER" | "UNKNOWN",
  "fields": {
    "job": string|null,
    "address": string|null,
    "name": string|null,
    "time_text": string|null,
    "access": string|null
  },
  "emotion": "neutral" | "angry" | "confused" | "urgent" | "upset" | "rambling",
  "off_script": boolean,
  "smalltalk_reply": string|null,
  "suggested_step": "intent"|"job"|"address"|"name"|"access"|"time"|"confirm",
  "urgency_score": number from 1 to 10,
  "next_question": string
}

URGENCY SCORING GUIDE:
Rate every call from 1 to 10:
1-3: General enquiry, future booking, quote request
4-6: Needs booking this week, some inconvenience mentioned
7-8: Urgent job, significant problem, customer stressed
9-10: Emergency — burst pipe, gas leak, flooding, no power,
      fire risk, sewage, sparking, customer in crisis
Always include urgency_score in your JSON response.`;
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

async function safeLLMCall(payload) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 9000);

  try {
    return await openai.chat.completions.create({
      ...payload,
      signal: controller.signal
    });
  } catch (e) {
    console.error("LLM FAIL:", e);
    return null;
  } finally {
    clearTimeout(timeout);
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

  const history = trimHistory(session.history || [], 8);
  const systemPrompt = buildLlmSystemPrompt(tradie, session);

  const messages = [
    { role: "system", content: systemPrompt },
    ...history.map((h) => ({ role: h.role, content: h.content })),
    { role: "user", content: String(userSpeech || "") }
  ];

  try {
    const data = await safeLLMCall({
      model: "gpt-4o",
      max_tokens: 200,
      temperature: 0.6,
      messages
    });

    if (!data) return null;

    const text = data?.choices?.[0]?.message?.content || "";
    const clean = text.replace(/```json|```/g, "").trim();

    let parsed = null;
    try { parsed = JSON.parse(clean); } catch { return null; }
    if (!parsed) return null;

    if (parsed.off_script) {
      session.offScriptCount = Number(session.offScriptCount || 0) + 1;
    }

    // Detect caller tone from speech
    const formalSignals = /\b(would like|I wish to|enquire|regarding|furthermore|I require|please advise)\b/i;
    if (formalSignals.test(String(userSpeech || ""))) {
      session.detectedTone = "formal";
    } else if (!session.detectedTone || session.detectedTone === "casual") {
      session.detectedTone = "casual";
    }

    // Process urgency score
    if (typeof parsed.urgency_score === "number") {
      session.urgencyScore = parsed.urgency_score;
      if (parsed.urgency_score >= 8 && !session.urgencyAlertSent) {
        session.urgencyAlertSent = true;
        const capturedKey = session.tradieKey || "";
        const capturedFrom = session.from || "";
        const capturedJob = session.job || "unknown";
        const capturedAddress = session.address || "unknown";
        const score = parsed.urgency_score;
        (async () => {
          try {
            const t = await getTradieConfig({ query: { tid: capturedKey }, body: {} });
            if (t) {
              await sendOwnerSms(t,
                `🚨 URGENT CALL (${score}/10)\nCaller: ${capturedFrom}\nJob: ${capturedJob}\nAddress: ${capturedAddress}\nAnswer ASAP.`
              ).catch(() => {});
            }
          } catch {}
        })();
      }
    }
    if (parsed.emotion && parsed.emotion !== "neutral") {
      session.lastEmotion = parsed.emotion;
    }

    return parsed;
  } catch (e) {
    console.warn("LLM call failed:", e?.message || e);
    return null;
  }
}

// 
// Twilio helpers + SaaS number provisioning
// 
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
  const from = String(process.env.EMAIL_FROM || process.env.FROM_EMAIL || "").trim();
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

async function sendGoogleCalendarConnectEmailForTradie({ tradie, source = "" }) {
  const tradieId = String(tradie?.id || "").trim();
  const recipient = String(tradie?.email || "").trim();
  const googleConnected = String(tradie?.google_refresh_token || "").trim().length > 0;

  if (!tradieId) {
    console.error("google connect email skipped: missing tradie id", { source });
    return false;
  }

  if (!recipient) {
    console.log("google connect email skipped: missing recipient", { tradie_id: tradieId, source });
    return false;
  }

  if (googleConnected) {
    console.log("google connect email skipped: already connected", { tradie_id: tradieId, source });
    return false;
  }

  const connectLink = `${BASE_URL.replace(/\/+$/, "")}/api/google/connect?tradieId=${encodeURIComponent(tradieId)}`;
  const subject = "Connect your Google Calendar";
  const text =
`Hi,

Connect your Google Calendar so your AI phone bot can manage bookings for you.

${connectLink}

This lets the phone bot check availability and create bookings.`;
  const html = `<p>Hi,</p>
<p>Connect your Google Calendar so your AI phone bot can manage bookings for you.</p>
<p><a href="${connectLink}" style="display:inline-block;padding:10px 16px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;">Connect Google Calendar</a></p>
<p>If the button doesn't work, copy and paste this link into your browser:<br/><a href="${connectLink}">${connectLink}</a></p>
<p>This lets the phone bot check availability and create bookings.</p>`;

  try {
    const sent = await sendTradieEmail(recipient, subject, text, html);
    if (sent) {
      console.log("google connect email sent", { tradie_id: tradieId, to: recipient, source });
      return true;
    }
    console.log("google connect email not sent (email transport unavailable)", { tradie_id: tradieId, source });
    return false;
  } catch (error) {
    console.error("google connect email failed", { tradie_id: tradieId, source, error: error?.message || error });
    return false;
  }
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
  const safeBaseUrl = String(baseUrl || "https://twilio-voice-bot-w9gq.onrender.com").replace(/\/+$/, "");
  const connectGoogleCalendarLink = `${safeBaseUrl}/api/google/connect?tradieId=${encodeURIComponent(String(tradieId || ""))}`;

  const text =
`Hi ${safeCustomerName},

Your AI booking assistant is now live and ready to take calls.

YOUR DEDICATED BOOKING NUMBER:
${safePhoneNumber}

Share this number with your customers so they can book jobs directly.
Save it as "${safeBusinessName} Bookings".

CONNECT YOUR GOOGLE CALENDAR:
Your phone bot needs access to your Google Calendar to check
availability and create bookings automatically.

Click here to connect:
${connectGoogleCalendarLink}

Once connected your assistant will automatically manage bookings
in your primary calendar with no extra setup needed.

If you need any further assistance please email us at:
adimtrades@gmail.com

Thanks,
AdimTrades Automation`;

  const html = `
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f5;font-family:sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f5;padding:32px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
        
        <!-- Header -->
        <tr><td style="background:#2563eb;padding:32px;text-align:center;">
          <h1 style="margin:0;color:#ffffff;font-size:22px;">Your AI Booking Assistant is Live 🚀</h1>
        </td></tr>

        <!-- Body -->
        <tr><td style="padding:32px;">
          <p style="margin:0 0 16px;color:#374151;font-size:16px;">Hi ${safeCustomerName},</p>
          <p style="margin:0 0 24px;color:#374151;font-size:15px;">
            Your AI phone bot is set up and ready to take bookings for <strong>${safeBusinessName}</strong>.
          </p>

          <!-- Phone number box -->
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0fdf4;border:2px solid #16a34a;border-radius:10px;margin-bottom:28px;">
            <tr><td style="padding:20px;text-align:center;">
              <p style="margin:0 0 6px;color:#16a34a;font-size:13px;font-weight:bold;text-transform:uppercase;letter-spacing:1px;">Your Dedicated Booking Number</p>
              <p style="margin:0;color:#111827;font-size:28px;font-weight:bold;letter-spacing:2px;">${safePhoneNumber}</p>
              <p style="margin:8px 0 0;color:#6b7280;font-size:13px;">Share this with your customers — save it as "<strong>${safeBusinessName} Bookings</strong>"</p>
            </td></tr>
          </table>

          <!-- Calendar connect -->
          <p style="margin:0 0 8px;color:#111827;font-size:15px;font-weight:bold;">Connect Your Google Calendar</p>
          <p style="margin:0 0 16px;color:#6b7280;font-size:14px;">
            Your bot needs access to your Google Calendar to check availability and create bookings automatically.
          </p>
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
            <tr><td align="center">
              <a href="${connectGoogleCalendarLink}" style="display:inline-block;padding:14px 32px;background:#2563eb;color:#ffffff;border-radius:8px;text-decoration:none;font-weight:bold;font-size:15px;">
                Connect Google Calendar
              </a>
            </td></tr>
          </table>
          <p style="margin:0 0 24px;color:#9ca3af;font-size:13px;text-align:center;">
            If the button does not work, copy and paste this link:<br>
            <a href="${connectGoogleCalendarLink}" style="color:#2563eb;word-break:break-all;">${connectGoogleCalendarLink}</a>
          </p>

          <!-- Divider -->
          <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 24px;">

          <!-- Support -->
          <p style="margin:0;color:#6b7280;font-size:14px;text-align:center;">
            Need further assistance? Email us at<br>
            <a href="mailto:adimtrades@gmail.com" style="color:#2563eb;font-weight:bold;">adimtrades@gmail.com</a>
          </p>
        </td></tr>

        <!-- Footer -->
        <tr><td style="background:#f9fafb;padding:20px;text-align:center;">
          <p style="margin:0;color:#9ca3af;font-size:12px;">AdimTrades Automation</p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>`;

  return { text, html };
}

async function sendActivationEmailForTradie({ tradie, provisionedPhoneNumber = "", emailFallback = "", source = "", reqForBaseUrl = null }) {
  const recipient = String(tradie?.email || emailFallback || "").trim();
  if (!recipient) {
    console.warn("activation email skipped: missing recipient", { tradie_id: tradie?.id || "", source });
    return;
  }

  // Hard stop — never send activation email twice
  const { data: sentCheck } = await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .select("activation_email_sent")
    .eq("id", tradie.id)
    .single();

  if (sentCheck?.activation_email_sent === true) {
    console.log("activation email skipped: already sent", { tradie_id: tradie.id, source });
    return;
  }

  // Mark as sent immediately before sending to prevent race condition
  await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .update({ activation_email_sent: true, updated_at: new Date().toISOString() })
    .eq("id", tradie.id);

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
    plan: plan || "starter",
    status: "ACTIVE",
    subscription_status: "ACTIVE",
    calendar_id: "primary",
    google_connected: false,
    is_active: true,
    owner_email: email || "",
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

  await sendGoogleCalendarConnectEmailForTradie({
    tradie: data,
    source: "ensureTradieByStripeCustomer:create"
  });

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

  const existingAssigned = await getExistingTwilioNumberForTradie(tradie.id).catch(() => null);
  if (existingAssigned?.phoneNumber) {
    return {
      skipped: true,
      phoneNumber: existingAssigned.phoneNumber,
      sid: existingAssigned.sid || ""
    };
  }

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

  // Check if tradie already has a number — skip if so
  const { data: existingTradie } = await supabase
    .from("tradies")
    .select("twilio_number, twilio_to")
    .eq("id", tradieId)
    .single();

  if (existingTradie?.twilio_number) {
    console.log(`PROVISION_SKIP tradie=${tradieId} already has number=${existingTradie.twilio_number}`);
    return existingTradie.twilio_number;
  }

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
    const phoneNumber = allocated.phoneNumber;
    let existingNumber = null;
    try {
      const { data: numRow } = await supabase
        .from("twilio_numbers")
        .select("id")
        .eq("phone_number", phoneNumber)
        .single();
      existingNumber = numRow;
    } catch {}

    if (!existingNumber) {
      await supabase
        .from("twilio_numbers")
        .insert({ phone_number: phoneNumber, tradie_id: tradieId });
    } else {
      // Update existing row to point to this tradie
      await supabase
        .from("twilio_numbers")
        .update({ tradie_id: tradieId, updated_at: new Date().toISOString() })
        .eq("phone_number", phoneNumber);
    }

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
        twilio_to: allocated.phoneNumber,
        twilio_incoming_sid: allocated.sid,
        updated_at: new Date().toISOString()
      })
      .eq("id", tradieId);

    await supabase
      .from("tradies")
      .update({ twilio_to: allocated.phoneNumber, updated_at: new Date().toISOString() })
      .eq("id", tradieId);

    console.log(`TWILIO_TO_SYNCED tradieId=${tradieId} number=${allocated.phoneNumber}`);
  }

  if (tradieKey) {
    cacheSet(`tid:${tradieKey}`, { tradie_key: tradieKey, twilio_number: allocated.phoneNumber });
  }

  console.log("twilio provisioning success", { tradie_id: tradieId, stripe_customer_id: tradie?.stripe_customer_id || "" });
  return { skipped: false, phoneNumber: allocated.phoneNumber, sid: allocated.sid, subaccountSid: "" };
}

//
// Twilio signature validation
// 
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

// 
// Stripe SaaS: Checkout + Webhook + Portal + Onboarding
// 
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

// POST /onboarding/submit { session_id, ownerSmsTo, calendarId, timezone, bizName, services, tone, businessDays, businessStartHour, businessEndHour, avgJobValue, closeRate, slotMinutes, bufferMinutes }
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


const onboardingStartRateLimit = new Map();
const ONBOARDING_START_RATE_LIMIT_WINDOW_MS = Number(process.env.ONBOARDING_START_RATE_LIMIT_WINDOW_MS || 10 * 60 * 1000);
const ONBOARDING_START_RATE_LIMIT_MAX = Number(process.env.ONBOARDING_START_RATE_LIMIT_MAX || 5);
const ONBOARDING_TOKEN_TTL_HOURS = Number(process.env.ONBOARDING_TOKEN_TTL_HOURS || 24);

function getOnboardingStartRateLimitKey(req, email) {
  const ip = String(req.ip || req.headers["x-forwarded-for"] || "unknown").split(",")[0].trim();
  return `${ip}:${String(email || "").toLowerCase()}`;
}

function isOnboardingStartRateLimited(req, email) {
  const now = Date.now();
  const key = getOnboardingStartRateLimitKey(req, email);
  const existing = onboardingStartRateLimit.get(key);
  if (!existing || now > existing.resetAt) {
    onboardingStartRateLimit.set(key, { count: 1, resetAt: now + ONBOARDING_START_RATE_LIMIT_WINDOW_MS });
    return false;
  }

  existing.count += 1;
  onboardingStartRateLimit.set(key, existing);
  return existing.count > ONBOARDING_START_RATE_LIMIT_MAX;
}

async function getLeadByEmail(email) {
  const normalizedEmail = String(email || "").trim().toLowerCase();
  if (!normalizedEmail) return null;

  const { data, error } = await supabase
    .from("onboarding_leads")
    .select("id,email,phone,onboarding_token_hash,token_expires_at,created_at,completed,onboarding_email_sent_at,tradie_id")
    .eq("email", normalizedEmail)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw new Error(`ONBOARDING_LEAD_LOOKUP_FAILED: ${error.message}`);
  return data || null;
}

async function ensureTradieForOnboarding({ email, phone }) {
  const normalizedEmail = String(email || "").trim().toLowerCase();
  const { data: existing, error: existingError } = await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .select("*")
    .eq("email", normalizedEmail)
    .order("updated_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (existingError) {
    throw new Error(`TRADIE_LOOKUP_FOR_ONBOARDING_FAILED: ${existingError.message}`);
  }

  if (existing?.id) {
    const updatePayload = {
      owner_mobile: phone,
      owner_sms_to: phone,
      updated_at: new Date().toISOString()
    };

    const { data: updated, error: updateError } = await supabase
      .from(SUPABASE_TRADIES_TABLE)
      .update(updatePayload)
      .eq("id", existing.id)
      .select("*")
      .single();

    if (updateError) throw new Error(`TRADIE_UPDATE_FOR_ONBOARDING_FAILED: ${updateError.message}`);
    return updated;
  }

  const generatedKey = `t_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
  const payload = {
    tradie_key: generatedKey,
    email: normalizedEmail,
    owner_mobile: phone,
    owner_sms_to: phone,
    status: "PENDING_SETUP",
    subscription_status: "pending",
    updated_at: new Date().toISOString()
  };

  const { data, error } = await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .insert(payload)
    .select("*")
    .single();

  if (error) throw new Error(`TRADIE_CREATE_FOR_ONBOARDING_FAILED: ${error.message}`);
  return data;
}

async function getExistingTwilioNumberForTradie(tradieId) {
  if (!tradieId) return null;
  const { data, error } = await supabase
    .from("twilio_numbers")
    .select("sid,phone_number,created_at")
    .eq("assigned_tradie_id", tradieId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw new Error(`TWILIO_NUMBER_LOOKUP_FAILED: ${error.message}`);
  if (!data?.phone_number) return null;
  return { sid: data.sid || "", phoneNumber: data.phone_number };
}

async function ensureSingleTwilioNumberForTradie(tradie, reqForBaseUrl) {
  const existing = await getExistingTwilioNumberForTradie(tradie?.id);
  if (existing?.phoneNumber) {
    return { skipped: true, phoneNumber: existing.phoneNumber, sid: existing.sid || "" };
  }
  return ensureTwilioNumberProvisionedForTradie(tradie, reqForBaseUrl);
}

function buildOnboardingEmail({ assignedNumber, frontendBaseUrl, rawToken }) {
  const setupLink = `${String(frontendBaseUrl || BASE_URL).replace(/\/+$/, "")}/setup?token=${encodeURIComponent(rawToken)}`;
  const subject = "Your AI Receptionist Setup Link";
  const text = `Hi,

Your AI receptionist system is almost ready.

Phone number assigned:
${assignedNumber}

Complete your setup here:
${setupLink}

This link expires in 24 hours.

If you did not request this setup, ignore this email.`;
  const html = `<p>Hi,</p><p>Your AI receptionist system is almost ready.</p><p><strong>Phone number assigned:</strong><br/>${assignedNumber}</p><p>Complete your setup here:<br/><a href="${setupLink}">${setupLink}</a></p><p>This link expires in 24 hours.</p><p>If you did not request this setup, ignore this email.</p>`;
  return { subject, text, html };
}

app.post("/onboarding/start", async (req, res) => {
  try {
    if (!supaReady() || !supabase) return res.status(500).json({ ok: false, error: "Supabase not configured" });

    const email = String(req.body?.email || "").trim().toLowerCase();
    const phone = normalizePhoneE164AU(req.body?.phone);

    if (!email) return res.status(400).json({ ok: false, error: "Missing email" });
    if (!phone || !isValidE164(phone)) return res.status(400).json({ ok: false, error: "Invalid phone. Use E.164 format." });

    if (isOnboardingStartRateLimited(req, email)) {
      return res.status(429).json({ ok: false, error: "Too many onboarding requests. Please retry later." });
    }

    const tradie = await ensureTradieForOnboarding({ email, phone });
    const twilioProvision = await ensureSingleTwilioNumberForTradie(tradie, req);
    const existingLead = await getLeadByEmail(email);

    if (existingLead?.onboarding_email_sent_at) {
      return res.status(200).json({ ok: true, message: "Onboarding email already sent.", twilio_number: twilioProvision.phoneNumber, email_sent: false });
    }

    const rawToken = generateOnboardingToken();
    const tokenHash = hashOnboardingToken(rawToken);
    const tokenExpiry = new Date(Date.now() + (ONBOARDING_TOKEN_TTL_HOURS * 60 * 60 * 1000)).toISOString();

    const leadPayload = {
      email,
      phone,
      onboarding_token_hash: tokenHash,
      token_expires_at: tokenExpiry,
      completed: false,
      tradie_id: tradie.id,
      onboarding_email_sent_at: null
    };

    let lead;
    if (existingLead?.id) {
      const { data, error } = await supabase
        .from("onboarding_leads")
        .update({ ...leadPayload, created_at: existingLead.created_at || new Date().toISOString() })
        .eq("id", existingLead.id)
        .select("id,email,phone,token_expires_at,completed")
        .single();
      if (error) throw new Error(`ONBOARDING_LEAD_UPDATE_FAILED: ${error.message}`);
      lead = data;
    } else {
      const { data, error } = await supabase
        .from("onboarding_leads")
        .insert(leadPayload)
        .select("id,email,phone,token_expires_at,completed")
        .single();
      if (error) throw new Error(`ONBOARDING_LEAD_CREATE_FAILED: ${error.message}`);
      lead = data;
    }

    const frontendUrl = process.env.FRONTEND_URL || process.env.BASE_URL || BASE_URL;
    const { subject, text, html } = buildOnboardingEmail({
      assignedNumber: twilioProvision.phoneNumber || "Not available",
      frontendBaseUrl: frontendUrl,
      rawToken
    });

    const sent = await sendTradieEmail(email, subject, text, html);
    if (!sent) {
      return res.status(503).json({ ok: false, error: "Email transport unavailable" });
    }

    await supabase
      .from("onboarding_leads")
      .update({ onboarding_email_sent_at: new Date().toISOString() })
      .eq("id", lead.id);

    return res.status(200).json({ ok: true, lead_id: lead.id, twilio_number: twilioProvision.phoneNumber, expires_at: lead.token_expires_at, email_sent: true });
  } catch (error) {
    console.error("onboarding start error", error);
    return res.status(500).json({ ok: false, error: String(error?.message || error) });
  }
});

app.post("/onboarding/complete", async (req, res) => {
  try {
    if (!supaReady() || !supabase) return res.status(500).json({ ok: false, error: "Supabase not configured" });

    const token = String(req.body?.token || "").trim();
    if (!token) return res.status(400).json({ ok: false, error: "Missing token" });

    const tokenHash = hashOnboardingToken(token);
    const { data: lead, error: leadError } = await supabase
      .from("onboarding_leads")
      .select("id,email,tradie_id,token_expires_at,completed")
      .eq("onboarding_token_hash", tokenHash)
      .limit(1)
      .maybeSingle();

    if (leadError) throw new Error(`ONBOARDING_TOKEN_LOOKUP_FAILED: ${leadError.message}`);
    if (!lead?.id) return res.status(400).json({ ok: false, error: "Invalid token" });
    if (lead.completed) return res.status(400).json({ ok: false, error: "Token already used" });
    if (new Date(lead.token_expires_at).getTime() < Date.now()) return res.status(400).json({ ok: false, error: "Token expired" });

    const nowIso = new Date().toISOString();
    const { data: completedLead, error: completeError } = await supabase
      .from("onboarding_leads")
      .update({ completed: true, completed_at: nowIso, onboarding_token_hash: null })
      .eq("id", lead.id)
      .eq("completed", false)
      .select("id,email,tradie_id,completed")
      .single();

    if (completeError) throw new Error(`ONBOARDING_COMPLETE_UPDATE_FAILED: ${completeError.message}`);

    const tradieTarget = completedLead.tradie_id
      ? supabase.from(SUPABASE_TRADIES_TABLE).update({ status: "ACTIVE", subscription_status: "active", updated_at: nowIso }).eq("id", completedLead.tradie_id)
      : supabase.from(SUPABASE_TRADIES_TABLE).update({ status: "ACTIVE", subscription_status: "active", updated_at: nowIso }).eq("email", completedLead.email);

    const { error: tradieUpdateError } = await tradieTarget;
    if (tradieUpdateError) throw new Error(`TRADIE_ACTIVATION_FAILED: ${tradieUpdateError.message}`);

    return res.status(200).json({ ok: true, activated: true });
  } catch (error) {
    console.error("onboarding complete error", error);
    return res.status(500).json({ ok: false, error: String(error?.message || error) });
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

  // Hard lock — prevent double provisioning from concurrent webhooks
  const lockKey = `provisioning_lock_${customerId}`;
  if (global[lockKey]) {
    console.log(`PROVISION_LOCK_SKIP customerId=${customerId} already provisioning`);
    return;
  }
  global[lockKey] = true;
  setTimeout(() => { delete global[lockKey]; }, 30000);

  console.log("stripe provisioning step: begin", { sourceEvent, customerId, subscriptionId: subscriptionId || "", plan: plan || "UNKNOWN" });
  const tradie = await getOrCreateTradieForStripeEvent({ customerId, subscriptionId, plan, email });
  if (!tradie?.id) return;

  // Prevent double provisioning — if number already assigned skip
  if (tradie.twilio_number) {
    console.log(`PROVISION_SKIP tradie=${tradie.id} already provisioned with ${tradie.twilio_number}`);
    return;
  }

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

    // Dedup — skip if same event processed within last 60 seconds
    const eventNow = Date.now();
    const lastSeen = processedStripeEvents.get(event.id);
    if (lastSeen && (eventNow - lastSeen) < 60000) {
      console.log(`STRIPE_WEBHOOK_DEDUP skipped eventId=${event.id}`);
      return res.json({ received: true, deduped: true });
    }
    processedStripeEvents.set(event.id, eventNow);
    // Cleanup old entries every 100 events
    if (processedStripeEvents.size > 100) {
      for (const [eid, ts] of processedStripeEvents.entries()) {
        if (eventNow - ts > 120000) processedStripeEvents.delete(eid);
      }
    }
    const stripeObj = event.data.object || {};

    if (event.type === "checkout.session.completed") {
      const sess = stripeObj;
      if (sess?.mode === "subscription") {
        console.log("stripe webhook step: checkout.session.completed subscription", { sessionId: sess.id, payment_status: sess.payment_status || "" });
        const expanded = await stripe.checkout.sessions.retrieve(sess.id, { expand: ["subscription", "customer"] });
        const subscriptionId = String(expanded?.subscription?.id || expanded?.subscription || "").trim();
        const customerId = String(expanded?.customer?.id || expanded?.customer || "").trim();
        const subscription = expanded?.subscription;
        // Try every possible location Stripe puts plan info
        const plan =
          expanded?.metadata?.plan ||
          subscription?.metadata?.plan ||
          expanded?.metadata?.tier ||
          subscription?.metadata?.tier ||
          subscription?.items?.data?.[0]?.price?.lookup_key ||
          subscription?.items?.data?.[0]?.price?.nickname ||
          subscription?.items?.data?.[0]?.plan?.nickname ||
          "starter";
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

        try {
          await syncActiveSubscriptionAndProvision({
            customerId,
            subscriptionId,
            plan,
            email,
            reqForBaseUrl: req,
            sourceEvent: event.type
          });
        } catch (provisionErr) {
          console.error("PROVISION_ERROR", provisionErr?.message || provisionErr);
          // Always return 200 to Stripe so it does not retry endlessly
        }
      }
    }

    if (event.type === "customer.subscription.created") {
      console.log("SUBSCRIPTION_CREATED_SKIP provisioning handled by checkout.session.completed");
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

        // Win-back SMS 24 hours after cancellation
        setTimeout(async () => {
          try {
            const cancelledTradie = await getTradieByStripeRefs({ customerId, subscriptionId });
            if (!cancelledTradie) return;
            const t = normalizeTradieConfig(cancelledTradie);
            if (!t.ownerSmsTo) return;
            // Check they have not resubscribed
            const fresh = await getOne(
              SUPABASE_TRADIES_TABLE,
              `stripe_customer_id=eq.${encodeURIComponent(customerId)}&select=status,subscription_status`
            );
            if (
              String(fresh?.status || "").toUpperCase() === "ACTIVE" ||
              String(fresh?.subscription_status || "").toUpperCase() === "ACTIVE"
            ) return;
            await sendSms({
              from: t.smsFrom || process.env.TWILIO_SMS_FROM || "",
              to: t.ownerSmsTo,
              body: `Hi — we noticed you cancelled your AI booking assistant. We would love to have you back. Use code COMEBACK20 for 20% off your first month when you resubscribe: ${process.env.BASE_URL || "https://twilio-voice-bot-w9gq.onrender.com"}/pricing`
            });
            console.log(`WIN_BACK_SMS_SENT customerId=${customerId}`);
          } catch (wbErr) {
            console.error("WIN_BACK_SMS_ERROR", wbErr?.message || wbErr);
          }
        }, 24 * 60 * 60 * 1000);
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

        try {
          const failedTradie = await getTradieByStripeRefs({ customerId, subscriptionId });
          if (failedTradie) {
            const t = normalizeTradieConfig(failedTradie);
            if (t.ownerSmsTo) {
              await sendSms({
                from: t.smsFrom || process.env.TWILIO_SMS_FROM || "",
                to: t.ownerSmsTo,
                body: `⚠️ Payment failed for your AI booking assistant. Please update your payment details to keep your booking number active. Contact us at adimtrades@gmail.com if you need help.`
              }).catch(() => {});
            }
            if (t.email) {
              await sendTradieEmail(
                t.email,
                "⚠️ Payment failed — action required",
                `Your payment failed. Please update your billing details to keep your AI booking number active. Contact us at adimtrades@gmail.com`,
                `<p>Your payment failed. Please update your billing details to keep your AI booking number active.</p><p>Contact us at <a href="mailto:adimtrades@gmail.com">adimtrades@gmail.com</a> if you need help.</p>`
              ).catch(() => {});
            }
            console.log(`PAYMENT_FAILED_ALERT_SENT customerId=${customerId}`);
          }
        } catch (pfErr) {
          console.error("PAYMENT_FAILED_ALERT_ERROR", pfErr?.message || pfErr);
        }
      }
    }

    return res.json({ received: true });
  } catch (e) {
    console.error("stripe webhook error", e);
    trackError("Stripe webhook");
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

// 
// Quote flow (lead + SMS “send photos”)
// 
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

// 
// Admin: metrics
// 
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

app.post("/admin/provision", async (req, res) => {
  try {
    const pw = String(req.body?.pw || req.query?.pw || "");
    if (!ADMIN_DASH_PASSWORD || pw !== ADMIN_DASH_PASSWORD) {
      return res.status(403).json({ error: "Forbidden" });
    }

    if (!supabase) return res.status(500).json({ error: "Supabase not configured" });

    const email = String(req.body?.email || "").trim().toLowerCase();
    const tradieId = String(req.body?.tradie_id || "").trim();

    let tradie = null;

    if (tradieId) {
      const { data } = await supabase.from(SUPABASE_TRADIES_TABLE).select("*").eq("id", tradieId).maybeSingle();
      tradie = data;
    } else if (email) {
      const { data } = await supabase.from(SUPABASE_TRADIES_TABLE).select("*").eq("email", email).order("updated_at", { ascending: false }).limit(1).maybeSingle();
      tradie = data;
    }

    if (!tradie?.id) return res.status(404).json({ error: "Tradie not found" });

    await supabase.from(SUPABASE_TRADIES_TABLE).update({
      status: "active",
      subscription_status: "active",
      calendar_id: tradie.calendar_id || "primary",
      google_connected: tradie.google_connected || false,
      updated_at: new Date().toISOString()
    }).eq("id", tradie.id);

    let phoneNumber = tradie.twilio_number || tradie.twilio_phone_number || "";
    let provisioned = null;
    if (!phoneNumber) {
      provisioned = await provisionTwilioNumberForTradie(tradie, req);
      phoneNumber = provisioned?.phoneNumber || "";
    }

    const { data: freshTradie } = await supabase.from(SUPABASE_TRADIES_TABLE).select("*").eq("id", tradie.id).maybeSingle();

    if (freshTradie?.email) {
      await sendActivationEmailForTradie({
        tradie: freshTradie,
        provisionedPhoneNumber: phoneNumber,
        source: "admin/provision",
        reqForBaseUrl: req
      });
    }

    if (!freshTradie?.google_refresh_token) {
      await sendGoogleCalendarConnectEmailForTradie({
        tradie: freshTradie,
        source: "admin/provision:calendar-connect"
      });
    }

    return res.json({
      ok: true,
      tradie_id: tradie.id,
      tradie_key: tradie.tradie_key,
      email: tradie.email,
      phone_number: phoneNumber,
      provisioned_new: !!(provisioned && !provisioned.skipped),
      google_connected: freshTradie?.google_connected || false,
      calendar_id: freshTradie?.calendar_id || "primary"
    });
  } catch (err) {
    console.error("ADMIN_PROVISION_ERROR", err);
    return res.status(500).json({ error: err?.message || "failed" });
  }
});

// 
// Conversation / session store (in-memory)
// 
const sessions = new Map();

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of sessions.entries()) {
    if (!v || now - Number(v.lastActivity || 0) > 1000 * 60 * 30) {
      sessions.delete(k);
    }
  }
}, 1000 * 60 * 5);

const rate = new Map();

function rateLimit(phone) {
  const now = Date.now();
  const arr = rate.get(phone) || [];
  const fresh = arr.filter((t) => now - t < 10000);
  fresh.push(now);
  rate.set(phone, fresh);
  return fresh.length > 8;
}


setInterval(() => {
  const now = Date.now();
  for (const [phone, stamps] of rate.entries()) {
    const fresh = (stamps || []).filter((t) => now - t < 10000);
    if (!fresh.length) {
      rate.delete(phone);
    } else {
      rate.set(phone, fresh);
    }
  }
}, 30000);
// In-memory caller blacklist — persists for life of server process
// Owner SMSes "BLOCK +61xxxxxxxxx" to add, "UNBLOCK +61xxxxxxxxx" to remove
const callerBlacklist = new Map(); // key: "tradieKey::phoneNumber" -> true
// Stripe webhook dedup — prevent double processing within 60 seconds
const processedStripeEvents = new Map(); // eventId -> timestamp
// Error rate monitoring — alert if more than 10 errors in one hour
const errorRateTracker = { count: 0, windowStart: Date.now(), alerted: false };

function trackError(context = "") {
  const now = Date.now();
  // Reset window every hour
  if (now - errorRateTracker.windowStart > 60 * 60 * 1000) {
    errorRateTracker.count = 0;
    errorRateTracker.windowStart = now;
    errorRateTracker.alerted = false;
  }
  errorRateTracker.count += 1;
  if (errorRateTracker.count >= 10 && !errorRateTracker.alerted) {
    errorRateTracker.alerted = true;
    const alertBody = `🚨 ERROR SPIKE: ${errorRateTracker.count} errors in the last hour on your AI booking bot. Context: ${context || "unknown"}. Check Render logs.`;
    // Fire and forget
    (async () => {
      try {
        await sendSms({
          from: process.env.TWILIO_SMS_FROM || "",
          to: process.env.ADMIN_ALERT_NUMBER || "",
          body: alertBody
        });
        // Also send email
        if (typeof sendTradieEmail === "function") {
          await sendTradieEmail(
            "adimtrades@gmail.com",
            "🚨 Bot Error Spike Alert",
            alertBody,
            `<p>${alertBody}</p>`
          ).catch(() => {});
        }
      } catch {}
    })();
  }
}
const processSpeechLocks = new Map();

function cleanupMaps() {
  // Cap sessions at 500 — evict oldest first to prevent memory leak
  if (sessions.size > 500) {
    const sorted = [...sessions.entries()].sort((a, b) => (a[1].updatedAt || 0) - (b[1].updatedAt || 0));
    const toDelete = sorted.slice(0, sessions.size - 500);
    for (const [sid] of toDelete) sessions.delete(sid);
  }
  const now = Date.now();
  for (const [sid, data] of sessions.entries()) {
    if (!data?.updatedAt || (now - data.updatedAt) > SESSION_TTL_MS) {
      // Send recovery SMS if call had data but never completed
      if (
        data &&
        data.from &&
        !data.bookingConfirmed &&
        !data.recoverySMSSent &&
        (data.job || data.address || data.name)
      ) {
        data.recoverySMSSent = true;
        const capturedKey = data.tradieKey || "";
        const capturedFrom = data.from || "";
        const capturedJob = data.job || "";
        const capturedAddress = data.address || "";
        const capturedName = data.name || "";
        // Fire and forget — never await in cleanup
        (async () => {
          try {
            const t = await getTradieConfig({ query: { tid: capturedKey }, body: {} });
            if (!t || !t.twilioNumber) return;
            const collected = [
              capturedJob ? `Job: ${capturedJob}` : "",
              capturedAddress ? `Address: ${capturedAddress}` : "",
              capturedName ? `Name: ${capturedName}` : ""
            ].filter(Boolean).join(", ");
            await sendCustomerSms(
              t,
              capturedFrom,
              `Hi${capturedName ? " " + capturedName : ""} — looks like we got cut off. We had: ${collected}. Call us back on ${t.twilioNumber} to complete your booking.`
            ).catch(() => {});
            await sendOwnerSms(
              t,
              `⚠️ INCOMPLETE BOOKING\nCaller: ${capturedFrom}\n${collected}\nCall dropped before confirmation.`
            ).catch(() => {});
          } catch {}
        })();
      }
      sessions.delete(sid);
    }
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
      lastActivity: Date.now(),
      _countedCall: false,

      history: [],
      llmTurns: 0,
      abuseStrikes: 0,
      offScriptCount: 0,
      lastEmotion: "neutral",
      calendarCheckAnnounced: false,
      confirmPromptSent: false,

      lastTwimlXml: null,
      _usedAffirmations: [],
      addressConfirmed: false,
      addressReadBack: false,
      suburb: "",
      bookingRef: "",
      totalFailedAttempts: 0,
      transferredToOwner: false,
      bookingConfirmed: false,
      callFlow: {
        bookingConfirmed: false,
        calendarEventCreated: false,
        userFinalAcknowledgement: false,
        hangupAllowed: false
      },
      recoverySMSSent: false,
      tradieKey: "",
      urgencyScore: 0,
      urgencyAlertSent: false,
      detectedTone: "casual",
      pendingStep: "",
      unclearTurns: 0,
      toneMode: "balanced",
      memoryHooks: {
        timePreference: "",
        urgencyLevel: "",
        emotionalStress: "",
        budgetConcern: ""
      },
      conversation_state: behaviouralEngine.createConversationState(),
      conversationState: {
        currentStep: "intro",
        collectedFields: {
          name: "",
          job: "",
          time: "",
          urgency: "",
          address: "",
          access: "",
          accessNotes: ""
        },
        lastBotQuestion: "",
        lastQuestionAsked: "",
        awaitingUserResponse: false,
        lastQuestionTimestamp: 0,
        minQuestionGapMs: 1200,
        confirmationPending: false,
        calendarEventPending: true,
        callLockedToFlow: true,
        smartModeActive: true
      },

      accessEditMode: "replace",
      callbackRequested: false
    });
  } else {
    const s = sessions.get(callSid);
      if (!s.from && fromNumber) s.from = fromNumber;
  }
  const active = sessions.get(callSid);
  if (!active.conversation_state) active.conversation_state = behaviouralEngine.createConversationState();
  if (!active.conversationState || typeof active.conversationState !== "object") {
    active.conversationState = {
      currentStep: "intro",
      collectedFields: {
        name: "",
        job: "",
        time: "",
        urgency: "",
        address: "",
        access: "",
        accessNotes: ""
      },
      lastBotQuestion: "",
      lastQuestionAsked: "",
      awaitingUserResponse: false,
      lastQuestionTimestamp: 0,
      minQuestionGapMs: 1200,
      confirmationPending: false,
      calendarEventPending: true,
      callLockedToFlow: true,
      smartModeActive: true
    };
  }
  ensureCallFlow(active);
  active.updatedAt = Date.now();
  active.lastActivity = Date.now();
  return sessions.get(callSid);
}
function resetSession(callSid) { sessions.delete(callSid); }

const LOCKED_FLOW_STEPS = ["intro", "name", "job", "time", "urgency", "address", "access", "confirm", "calendar", "close"];

function syncConversationStateFromSession(session) {
  if (!session?.conversationState) return;
  session.conversationState.collectedFields = {
    name: session.name || "",
    job: session.job || "",
    time: session.time || "",
    urgency: String(session.urgencyScore || ""),
    address: session.address || "",
    access: session.accessNote || "",
    accessNotes: session.accessNote || ""
  };
  session.conversationState.confirmationPending = session.step === "confirm";
}

function setLockedFlowStep(session, nextStep, reason = "") {
  const flow = session?.conversationState;
  if (!flow) return;
  const current = flow.currentStep || "intro";
  const fromIdx = LOCKED_FLOW_STEPS.indexOf(current);
  const toIdx = LOCKED_FLOW_STEPS.indexOf(nextStep);
  if (flow.callLockedToFlow && fromIdx >= 0 && toIdx >= 0 && toIdx < fromIdx) {
    console.log(`FLOW_GUARD_BLOCK callSid=${session.callSid || "unknown"} from=${current} to=${nextStep} reason=${reason}`);
    return;
  }
  if (current !== nextStep) {
    console.log(`FLOW_STEP_CHANGE callSid=${session.callSid || "unknown"} from=${current} to=${nextStep} reason=${reason}`);
    flow.currentStep = nextStep;
  }
}

function buildObjectivePrompt(step, collectedFields = {}) {
  const firstName = (collectedFields.name || "").split(" ")[0] || "";
  if (step === "intro") return "Hi, what do you need help with today?";
  if (step === "name") return "Great — what is your name?";
  if (step === "job") return firstName ? `Thanks ${firstName} — what exactly needs fixing?` : "What exactly needs fixing?";
  if (step === "time") return "What day and time works best for you?";
  if (step === "urgency") return "How urgent is this — emergency, today, or can it wait a bit?";
  if (step === "address") return "What is the address for the job?";
  if (step === "access") return "Any access notes like gate code, parking, or pets? Say none if not.";
  if (step === "confirm") return "Just to confirm — does that all sound right so we can lock this in?";
  if (step === "calendar") return "Great — I am locking this into the calendar now.";
  return "Just checking you're still there — would you like to continue booking?";
}

function mapLegacyStepToLockedStep(step) {
  if (!step || step === "intent") return "intro";
  if (["name"].includes(step)) return "name";
  if (["job"].includes(step)) return "job";
  if (["time", "pickSlot"].includes(step)) return "time";
  if (["address", "address_confirm"].includes(step)) return "address";
  if (["access"].includes(step)) return "access";
  if (["confirm"].includes(step)) return "confirm";
  return "intro";
}


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

// 
// General helpers + validation
// 
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

function buildInitialVoiceTwiml(req, greetingText = "Hi, what do you need help with today?", tradie = {}) {
  const twiml = new VoiceResponse();
  const gather = twiml.gather({
    input: "speech",
    timeout: VOICE_GATHER_TIMEOUT_SECONDS,
    speechTimeout: "auto",
    action: voiceActionUrl(req),
    method: "POST",
    language: "en-AU",
    speechModel: "phone_call",
    enhanced: true,
    hints: buildSpeechHints(tradie)
  });
  gather.say(greetingText, { voice: "Polly.Amy", language: "en-AU" });
  return twiml;
}

function handleVoiceEntry(req, res) {
  const rawTradie = req._tradieConfig || {};
  const bizName = rawTradie?.bizName || "";
  const botName = rawTradie?.botName || "Alex";
  const tradieTimezone = rawTradie?.timezone || "Australia/Sydney";
  const timeGreeting = getTimeOfDayGreeting(tradieTimezone);
  const isHoliday = isAustralianPublicHoliday(tradieTimezone);
  const holidayNote = isHoliday ? " Hope you are enjoying the public holiday." : "";
  let greeting;
  if (typeof isReturning !== "undefined" && isReturning && returningName && bizName) {
    greeting = `${timeGreeting} ${returningName}, welcome back to ${bizName}.${holidayNote} What do you need help with today?`;
  } else if (typeof isReturning !== "undefined" && isReturning && returningName) {
    greeting = `${timeGreeting} ${returningName}, good to hear from you again.${holidayNote} What do you need help with today?`;
  } else if (bizName) {
    greeting = `${timeGreeting}, thanks for calling ${bizName}. This is ${botName}.${holidayNote} What do you need help with today?`;
  } else {
    greeting = `${timeGreeting}.${holidayNote} What do you need help with today?`;
  }
  const twiml = buildInitialVoiceTwiml(req, greeting, rawTradie);
  const callSid = resolveCallSid(req);
  const fromNumber = (req.body?.From || req.query?.From || "").trim();
  const session = getSession(callSid, fromNumber);
  session.step = "intent";
  session.retryCount = 0;
  session.hasEnteredVoice = true;
  session.lastNoSpeechFallback = false;
  session.silenceTries = 0;
  logVoiceStep(req, { callSid, step: "intent", speech: "", retryCount: 0 });
  return sendVoiceTwiml(res, twiml);
}

function buildSpeechHints(tradie) {
  const base = "yes, no, confirm, cancel, reschedule, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday, morning, afternoon, today, tomorrow, next week, Street, Road, Avenue, Drive, Close, Court, Place, Lane, Crescent, Boulevard, Way, urgent, emergency, none, quote";

  const services = String(tradie?.services || tradie?.tone || "").toLowerCase();
  const bizName = String(tradie?.bizName || "").toLowerCase();
  const combined = services + " " + bizName;

  const plumbing = "plumber, plumbing, blocked drain, leaking tap, burst pipe, hot water, gas leak, toilet, sink, tap, pipe, sewage, overflow, cistern, valve, flexi hose, water heater";
  const electrical = "electrician, electrical, rewire, switchboard, power point, light fitting, circuit breaker, safety switch, no power, sparking, short circuit, LED downlight, smoke alarm";
  const building = "builder, carpenter, deck, pergola, fence, fencing, retaining wall, cladding, framing, structural, renovation, extension";
  const tiling = "tiler, tiling, tiles, bathroom, kitchen splashback, grout, waterproofing, wall tiles, floor tiles";
  const painting = "painter, painting, interior, exterior, render, texture coat, feature wall, touch up";
  const roofing = "roofer, roofing, leaking roof, gutter, downpipe, ridge cap, fascia, soffit, colorbond, terracotta";
  const aircon = "air conditioning, aircon, split system, ducted, reverse cycle, gas heater, evaporative, service, regas, not cooling, not heating";
  const landscaping = "landscaper, landscaping, lawn, turf, retaining wall, garden bed, irrigation, mulch, instant lawn, tree removal, stump grinding";
  const concreting = "concreter, concreting, driveway, slab, path, footpath, exposed aggregate, reinforced";

  let hints = base;
  if (!combined.trim() || combined.trim().length < 3) {
    hints += ", " + plumbing + ", " + electrical + ", " + building;
  } else {
    if (/plumb|drain|pipe|tap|water|gas/.test(combined)) hints += ", " + plumbing;
    if (/electr|power|light|switch|wire/.test(combined)) hints += ", " + electrical;
    if (/build|carp|deck|fence|pergola|renov/.test(combined)) hints += ", " + building;
    if (/tile|tiling|bathroom|kitchen/.test(combined)) hints += ", " + tiling;
    if (/paint/.test(combined)) hints += ", " + painting;
    if (/roof|gutter/.test(combined)) hints += ", " + roofing;
    if (/air|aircon|hvac|heat|cool/.test(combined)) hints += ", " + aircon;
    if (/landscap|lawn|garden|turf/.test(combined)) hints += ", " + landscaping;
    if (/concret|driv|slab|path/.test(combined)) hints += ", " + concreting;
  }
  return hints;
}

function applyTonePolishSafe(session, responseText) {
  try {
    const shorten = Number(session?.conversation_state?.urgency_score || 0) >= 7;
    return behaviouralEngine.tonePolish(responseText, { shorten });
  } catch {
    return String(responseText || "").trim();
  }
}

function compressBotResponse(text = "") {
  const original = String(text || "").replace(/\s+/g, " ").trim();
  if (!original) return "";

  const fillers = [
    /thanks for that information[, ]*/gi,
    /that really helps[, ]*/gi,
    /just so i can\s*/gi,
    /to make sure we\s*/gi,
    /if that's okay\.?/gi
  ];

  let compressed = original;
  fillers.forEach((pattern) => {
    compressed = compressed.replace(pattern, "");
  });

  compressed = compressed.replace(/\s+/g, " ").trim();
  const questionIndex = compressed.indexOf("?");
  if (questionIndex >= 0) {
    const questionSentence = compressed.slice(0, questionIndex + 1).trim();
    compressed = questionSentence;
  }

  const isConfirmationSummary = /just to confirm|confirm\?/i.test(compressed);
  if (!isConfirmationSummary && compressed.length > 140) {
    compressed = compressed.slice(0, 140).trim();
  }

  if (compressed && compressed !== original) {
    console.log("RESPONSE_COMPRESSED", { beforeLength: original.length, afterLength: compressed.length });
  }

  return compressed || original;
}

function isFieldValid(field, value, session = {}) {
  const text = String(value || "").trim();
  if (field === "name") return text.length >= 2;
  if (field === "job") return text.length >= 3;
  if (field === "time") return !!(session.bookedStartMs || text.length >= 3);
  if (field === "urgency") return text.length > 0;
  if (field === "address") return text.length >= 6;
  if (field === "access") return text.length >= 0;
  return false;
}

function maybeSkipCollectedField(session, step) {
  const stepToField = {
    name: "name",
    job: "job",
    time: "time",
    urgency: "urgency",
    address: "address",
    access: "access"
  };
  const field = stepToField[step];
  if (!field) return false;
  const fields = session?.conversationState?.collectedFields || {};
  const value = field === "access" ? (session.accessNote ?? fields.access ?? "") : fields[field];
  if (!isFieldValid(field, value, session)) return false;
  console.log(`FIELD_ALREADY_COLLECTED_SKIP step=${step} field=${field}`);
  if (step === "job") session.step = "address";
  else if (step === "address") session.step = "name";
  else if (step === "name") session.step = "access";
  else if (step === "access") session.step = "time";
  else if (step === "time") session.step = "confirm";
  return true;
}

function ask(twiml, prompt, actionUrl, options = {}) {
  const session = options?.session;
  const polishedPrompt = compressBotResponse(applyTonePolishSafe(session, prompt));
  const activeStep = session?.step || "";
  const now = Date.now();
  if (session?.conversationState) {
    const minGap = Number(session.conversationState.minQuestionGapMs || 1200);
    const isSameQuestion = session.conversationState.lastQuestionAsked === activeStep;
    const gapMs = now - Number(session.conversationState.lastQuestionTimestamp || 0);
    if (isSameQuestion && gapMs < minGap) {
      console.log(`QUESTION_LOCKED step=${activeStep} gapMs=${gapMs}`);
      const lockPrompt = "I didn’t quite catch that — could you repeat it?";
      const gatherLocked = twiml.gather({
        input: "speech",
        speechTimeout: "auto",
        action: actionUrl,
        method: "POST",
        language: "en-AU",
        timeout: VOICE_GATHER_TIMEOUT_SECONDS,
        speechModel: "phone_call",
        enhanced: true,
        hints: buildSpeechHints(typeof tradie !== "undefined" ? tradie : {}),
        ...options
      });
      gatherLocked.say(lockPrompt, { voice: "Polly.Amy", language: "en-AU" });
      twiml.pause({ length: 1 });
      return;
    }
    session.conversationState.lastQuestionAsked = activeStep;
    session.conversationState.lastQuestionTimestamp = now;
    session.conversationState.awaitingUserResponse = true;
    session.conversationState.lastBotQuestion = polishedPrompt;
  }
  const gather = twiml.gather({
    input: "speech",
    speechTimeout: "auto",
    action: actionUrl,
    method: "POST",
    language: "en-AU",
    timeout: VOICE_GATHER_TIMEOUT_SECONDS,
    speechModel: "phone_call",
    enhanced: true,
    hints: buildSpeechHints(typeof tradie !== "undefined" ? tradie : {}),
    ...options
  });

  gather.say(polishedPrompt || "Sorry, can you repeat that?", { voice: "Polly.Amy", language: "en-AU" });
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

async function repeatLastStepPrompt(req, res, twiml, session, step, reason = "NO_SPEECH", tradie = {}) {
  // Increment total failed attempts across entire call
  session.totalFailedAttempts = Number(session.totalFailedAttempts || 0) + 1;

  // After 7 total failures transfer to owner number
  if (session.totalFailedAttempts >= 7 && !session.transferredToOwner) {
    session.transferredToOwner = true;
    const ownerNumber = tradie?.ownerSmsTo || process.env.OWNER_SMS_TO || "";
    if (ownerNumber) {
      console.log(`TRANSFER_TO_OWNER after 7 failed attempts owner=${ownerNumber}`);
      try {
        await sendOwnerSms(tradie,
          `📞 CALL TRANSFER\nCaller ${session.from || "unknown"} could not be understood after 7 attempts.\nJob hint: ${session.job || "unknown"}`
        );
      } catch {}
      const transferTwiml = new VoiceResponse();
      transferTwiml.say(
        "No worries — let me get someone on the line for you right now.",
        { voice: "Polly.Amy", language: "en-AU" }
      );
      const dial = transferTwiml.dial({ timeout: 30, callerId: req.body?.To || "" });
      dial.number(ownerNumber);
      transferTwiml.say(
        "Sorry we missed you — please call back and we will get you sorted.",
        { voice: "Polly.Amy", language: "en-AU" }
      );
      res.type("text/xml").send(transferTwiml.toString());
      return { handled: true, transferred: true };
    }
  }
  const retryCount = incrementRetryCountForStep(session, step);
  const actionUrl = voiceActionUrl(req);
  const basePrompt = session.lastPrompt || "Could you repeat that?";

  if (retryCount <= MAX_NO_SPEECH_RETRIES) {
    console.log(`RETRY_VALIDATION_TRIGGERED step=${step} reason=${reason} attempt=${retryCount}`);
    let prompt = "I didn’t quite catch that — could you repeat it?";
    if (retryCount === 2) {
      prompt = "Just briefly — could you repeat that one more time?";
    } else if (retryCount >= 3) {
      prompt = step === "time"
        ? "Try a clear time like: tomorrow at 2 PM."
        : "Please repeat in a few words so I can lock this in.";
    }
    session.lastPrompt = basePrompt;
    console.log(`STEP=${step} speech='' interpreted='${reason}' retryCount=${retryCount}`);
    ask(twiml, prompt, actionUrl, { session, input: "speech", timeout: 6, speechTimeout: "auto" });
    return { handled: true };
  }

  if (retryCount === MAX_NO_SPEECH_RETRIES + 1) {
    session.lastStepBeforeFallback = step;
    session.promptBeforeFallback = basePrompt;
    session.step = "sms_fallback_offer";
    session.lastPrompt = "I didn’t catch that. You can say it again, or say ‘text me’ and I’ll send an SMS link.";
    console.log(`STEP=${step} speech='' interpreted='${reason}' retryCount=${retryCount}`);
    ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 6, speechTimeout: "auto" });
    return { handled: true };
  }

  console.log(`STEP=${step} speech='' interpreted='${reason}' retryCount=${retryCount}`);
  const silenceRecovery = session.from
    ? "Still there? Take your time — I am here when you are ready."
    : "No worries — I am still here. What do you need help with today?";
  ask(twiml, silenceRecovery, actionUrl, { session, input: "speech", timeout: 10, speechTimeout: "auto" });
  return { handled: true };
}

function keepCallAliveForProcessing(req, twiml, message = "") {
  if (message) {
    twiml.say(message, { voice: "Polly.Amy", language: "en-AU" });
  }
  const checkUrl = "/check-availability" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
  twiml.redirect({ method: "POST" }, checkUrl);
}

function canHangUp(session) {
  if (!session || !session.callFlow) return false;

  return (
    session.callFlow.bookingConfirmed === true &&
    session.callFlow.calendarEventCreated === true &&
    session.callFlow.userFinalAcknowledgement === true
  );
}

function ensureCallFlow(session) {
  if (!session || typeof session !== "object") return;
  if (!session.callFlow || typeof session.callFlow !== "object") {
    session.callFlow = {
      bookingConfirmed: false,
      calendarEventCreated: false,
      userFinalAcknowledgement: false,
      hangupAllowed: false
    };
  }
  session.callFlow.hangupAllowed = canHangUp(session);
}

function sendVoiceTwiml(res, twiml, fallbackMessage = "Sorry, still here with you. What do you need help with today?") {
  const responseTwiml = twiml instanceof VoiceResponse ? twiml : new VoiceResponse();
  let xml = "";

  try {
    xml = String(responseTwiml.toString() || "").trim();
  } catch {
    xml = "";
  }

  if (!xml || xml === '<?xml version="1.0" encoding="UTF-8"?><Response/>') {
    const fallback = new VoiceResponse();
    const gather = fallback.gather({
      input: "speech",
      timeout: 8,
      speechTimeout: "auto",
      action: "/process",
      method: "POST",
      language: "en-AU"
    });
    gather.say(fallbackMessage, { voice: "Polly.Amy", language: "en-AU" });
    xml = fallback.toString();
  }

  try {
    const callSid = res.locals?.callSid;
    const session = callSid ? sessions.get(callSid) : null;
    if (session) {
      ensureCallFlow(session);
      console.log("FLOW STATUS:", session.callFlow);
    }

    const hangupAllowed = canHangUp(session);
    const hasGatherOrRedirect = xml.includes("<Gather") || xml.includes("<Redirect");
    const hasHangup = xml.includes("<Hangup");

    if (hasHangup && !hangupAllowed) {
      console.log("HANGUP BLOCKED — booking flow incomplete");
      xml = '<?xml version="1.0" encoding="UTF-8"?><Response><Say>Before we finish, I just need to confirm a few final details.</Say><Gather input="speech" action="/process" method="POST" speechTimeout="auto"><Say>Let’s continue.</Say></Gather></Response>';
    }

    if (!hangupAllowed && !hasGatherOrRedirect && !xml.includes("<Hangup")) {
      const failsafeSuffix = '<Pause length="1"/><Gather input="speech" action="/process" method="POST" speechTimeout="auto"><Say>Is there anything else you would like help with before we finalise your booking?</Say></Gather>';
      xml = xml.includes("</Response>")
        ? xml.replace("</Response>", `${failsafeSuffix}</Response>`)
        : `${xml}${failsafeSuffix}`;
    }
  } catch (flowErr) {
    console.error("FLOW_GUARD_TWIML_PATCH_ERROR", flowErr?.message || flowErr);
  }

  // Cache last response for idempotency replay
  try {
    const callSid = res.locals?.callSid;
    if (callSid && sessions.has(callSid)) {
      sessions.get(callSid).lastTwimlXml = xml;
    }
  } catch {}

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
  if (!s) return false;
  // Accept any single word of 2+ chars as a valid name
  // Short names like "Tim", "Jo", "Al" are all valid
  return s.length >= 2 && !/^(yes|no|yeah|nope|correct|wrong|none|skip)$/i.test(s);
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

  // For address and time steps never reject on confidence alone
  // as these fields contain numbers and street names that score low
  if (step === "address" || step === "time") {
    return !speech || speech.trim().length < 3;
  }

  // For name step only reject if completely empty
  if (step === "name") {
    return !speech || speech.trim().length < 2;
  }

  if (isLowConfidence(confidence)) return true;

  if (step === "job") return !validateJob(speech);
  if (step === "access") return !validateAccess(speech);

  return false;
}

// 
// Intent detection (heuristic fallback)
// 
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

function detectConversationState(text = "") {
  const t = String(text || "").toLowerCase().trim();
  const has = (arr) => arr.some((w) => t.includes(w));

  const states = [];
  if (!t) return { primaryState: "confusion", states: ["confusion"], offTopic: false };

  if (has(["book", "booking", "come out", "send someone", "appointment", "slot", "available"])) states.push("booking_intent");
  if (has(["too expensive", "too high", "cheaper", "price", "cost", "quote me better", "can't afford", "budget"])) states.push("price_objection");
  if (has(["don't understand", "confused", "what do you mean", "not sure", "say that again", "unclear"])) states.push("confusion");
  if (has(["asap", "right now", "urgent", "today", "immediately", "getting worse", "emergency"])) states.push("urgency");
  if (has(["stressed", "overwhelmed", "anxious", "panicking", "really upset", "freaking out"])) states.push("emotional_distress");
  if (has(["anyone else", "other company", "shopping around", "compare", "another quote", "another company quoted"])) states.push("comparison_shopping");
  if (has(["listen", "just do it", "stop", "i need", "i want", "now", "don't waste my time"])) states.push("dominant_customer");
  if (has(["whatever works", "you decide", "not sure", "i guess", "anything is fine", "up to you"])) states.push("passive_customer");
  if (has(["exactly", "itemise", "breakdown", "how long", "process", "warranty", "steps"])) states.push("analytical_customer");
  if (has(["are you real", "can i trust", "do you actually", "licensed", "insured", "prove", "who are you"])) states.push("trust_testing");

  const isStory = t.split(/\s+/).filter(Boolean).length > 22 && has(["then", "after that", "last week", "my partner", "my neighbour", "anyway"]);
  if (isStory) states.push("ranting_storytelling");

  const priority = [
    "emotional_distress", "urgency", "price_objection", "trust_testing", "comparison_shopping",
    "dominant_customer", "analytical_customer", "passive_customer", "ranting_storytelling", "booking_intent", "confusion"
  ];
  const primaryState = priority.find((state) => states.includes(state)) || "confusion";
  const offTopic = states.includes("ranting_storytelling") && !states.includes("booking_intent");
  return { primaryState, states: states.length ? states : ["confusion"], offTopic };
}

function loss_aversion_line() {
  return "If we leave it too long, this can become more costly to fix.";
}

function social_proof_line() {
  return "We handle this type of issue often.";
}

function authority_recommendation_line() {
  return "Best next step is to lock a time and inspect it properly.";
}

function scarcity_line() {
  return "Today is filling quickly, so securing a slot now is safest.";
}

function micro_commitment_question(type = "time") {
  return behaviouralEngine.generateGuidedChoice(type);
}

function trust_reassurance_line() {
  return pickRandom([
    "We handle this type of issue often.",
    "This will be quick to inspect.",
    "We'll make this easy."
  ]);
}

function toneModifierForState(session, detectedState) {
  if (detectedState === "dominant_customer") return "concise";
  if (detectedState === "emotional_distress") return "supportive";
  if (detectedState === "analytical_customer") return "factual";
  if (detectedState === "passive_customer") return "guiding";
  return session?.toneMode || "balanced";
}

function updateConversationMemory(session, text, conversationState) {
  if (!session) return;
  session.memoryHooks = session.memoryHooks || {
    timePreference: "",
    urgencyLevel: "",
    emotionalStress: "",
    budgetConcern: ""
  };

  const t = String(text || "").toLowerCase();
  if (/\b(morning|afternoon|before\s?noon|after\s?noon|after work|this evening|tonight)\b/.test(t)) {
    session.memoryHooks.timePreference = RegExp.$1;
  }
  if (conversationState?.states?.includes("urgency")) session.memoryHooks.urgencyLevel = "high";
  if (conversationState?.states?.includes("emotional_distress")) session.memoryHooks.emotionalStress = "high";
  if (/\b(budget|can't afford|too expensive|cheap|cheaper|price)\b/.test(t)) session.memoryHooks.budgetConcern = "high";
}

function buildTaskControlPrompt(session, step) {
  if (step === "job") return "What job do you need help with?";
  if (step === "address" || step === "address_confirm") return "What is the address for the job?";
  if (step === "name") return "What name should I put the booking under?";
  if (step === "access") return "Any access notes like gate code, parking, or pets?";
  if (step === "time" || step === "pickSlot") {
    if (session?.memoryHooks?.timePreference) return `Noted ${session.memoryHooks.timePreference}. ${micro_commitment_question()}`;
    return micro_commitment_question();
  }
  if (step === "confirm") return "Does that all sound right so we can lock this in?";
  return "What do you need help with today?";
}

function buildOffTopicRecoveryResponse(session, conversationState, step) {
  const reaction = conversationState.primaryState === "emotional_distress"
    ? "That sounds stressful."
    : "I hear you.";
  const controlPrompt = buildTaskControlPrompt(session, step);
  return `${reaction} ${trust_reassurance_line()} Let's get this sorted — ${controlPrompt}`.trim();
}

function composeAdaptivePrompt(session, basePrompt, conversationState, step) {
  const raw = String(basePrompt || "").trim();
  if (!raw) return raw;

  const state = conversationState?.primaryState || "confusion";
  const toneMode = toneModifierForState(session, state);
  session.toneMode = toneMode;

  const prefix = toneMode === "concise"
    ? "Understood."
    : toneMode === "supportive"
      ? "You're doing the right thing by calling."
      : toneMode === "factual"
        ? "Just to confirm details clearly."
        : toneMode === "guiding"
          ? "No worries, I'll guide this step by step."
          : getAffirmation(session);

  const inserts = [];
  const behaviourState = session?.conversation_state || {};

  if (state === "price_objection") inserts.push(authority_recommendation_line());
  if (state === "comparison_shopping") inserts.push(social_proof_line());
  if (state === "urgency") inserts.push(scarcity_line());
  if (state === "trust_testing" || behaviourState.resistance_type === "trust" || behaviourState.commitment_stage === "info") {
    inserts.push(behaviouralEngine.addTrustSignal(behaviourState.commitment_stage === "decision" ? "decision" : "early"));
  }

  if (behaviourState.urgency_score >= 6 && !behaviourState.loss_framing_used) {
    const problem = behaviourState.last_customer_problem_summary || "this issue";
    inserts.push(behaviouralEngine.applyLossFraming(problem));
    behaviourState.loss_framing_used = true;
  }

  if (step === "time" || step === "pickSlot") {
    const memoryLine = behaviouralEngine.memoryRecallResponse(behaviourState, "we can prioritise that window");
    if (memoryLine) inserts.push(memoryLine);
  }

  if (state === "analytical_customer" && step === "confirm") inserts.push("Just to confirm —");

  const problemSummary = behaviourState.last_customer_problem_summary || session?.job || "the issue";
  const guidedBase = behaviouralEngine.reflectAndGuide(problemSummary, raw);
  const combined = `${prefix} ${inserts.join(" ")} ${guidedBase}`.replace(/\s+/g, " ").trim();
  return applyTonePolishSafe(session, combined);
}


// 
// Time parsing
// 
function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function getAffirmation(session) {
  const used = session._usedAffirmations || [];
  const all = ["Perfect.", "Got it.", "Lovely.", "No worries.", "Brilliant.", "Good one.", "Great stuff.", "Noted."];
  const available = all.filter(a => !used.includes(a));
  const pool = available.length > 0 ? available : all;
  const pick = pickRandom(pool);
  session._usedAffirmations = [...used.slice(-4), pick];
  return pick;
}

function getTimeOfDayGreeting(tz) {
  const hour = DateTime.now().setZone(tz || "Australia/Sydney").hour;
  if (hour < 12) return "Good morning";
  if (hour < 17) return "Good afternoon";
  return "Thanks for calling this evening";
}

function getJobEmpathy(job) {
  const j = String(job || "").toLowerCase();
  if (/burst pipe|flooding|leak.*bad|emergency|gas leak|sparking|no power/.test(j))
    return "Oh no — let us get that sorted fast.";
  if (/blocked drain|blocked toilet|overflow/.test(j))
    return "That is never fun — let us get someone out to you.";
  if (/hot water|no hot water/.test(j))
    return "No hot water is rough — let us fix that quickly.";
  if (/broken|not working|damaged/.test(j))
    return "That sounds frustrating — let us get it sorted.";
  return "";
}

function getCalendarWaitMessage() {
  return pickRandom([
    "Just checking the calendar — won't be a moment.",
    "Bear with me one sec while I check availability.",
    "Let me just check that time for you.",
    "One moment — just pulling up the calendar.",
    "Just a sec — checking that now."
  ]);
}

function isAustralianPublicHoliday(tz) {
  const now = DateTime.now().setZone(tz || "Australia/Sydney");
  const month = now.month;
  const day = now.day;
  // Fixed Australian public holidays
  const fixed = [
    [1, 1],   // New Year's Day
    [1, 26],  // Australia Day
    [4, 25],  // Anzac Day
    [12, 25], // Christmas Day
    [12, 26], // Boxing Day
  ];
  return fixed.some(([m, d]) => m === month && d === day);
}

function normaliseSpokenNumber(text) {
  if (!text) return text;
  const words = {
    "zero": "0", "one": "1", "two": "2", "three": "3", "four": "4",
    "five": "5", "six": "6", "seven": "7", "eight": "8", "nine": "9",
    "ten": "10", "eleven": "11", "twelve": "12", "thirteen": "13",
    "fourteen": "14", "fifteen": "15", "sixteen": "16", "seventeen": "17",
    "eighteen": "18", "nineteen": "19", "twenty": "20", "thirty": "30",
    "forty": "40", "fifty": "50", "sixty": "60", "seventy": "70",
    "eighty": "80", "ninety": "90", "hundred": "100"
  };
  let t = String(text).toLowerCase();
  for (const [word, num] of Object.entries(words)) {
    t = t.replace(new RegExp(`\\b${word}\\b`, "g"), num);
  }
  return t;
}

function normaliseAussieSlang(text, tz) {
  if (!text) return text;
  let t = String(text).toLowerCase();
  // Time slang
  t = t.replace(/\bthis arvo\b/g, "this afternoon");
  t = t.replace(/\barvo\b/g, "afternoon");
  t = t.replace(/\bsmoko\b/g, "10am");
  t = t.replace(/\bhalf two\b/g, "2:30pm");
  t = t.replace(/\bhalf three\b/g, "3:30pm");
  t = t.replace(/\bhalf four\b/g, "4:30pm");
  t = t.replace(/\bhalf five\b/g, "5:30pm");
  t = t.replace(/\bhalf six\b/g, "6:30pm");
  t = t.replace(/\bhalf seven\b/g, "7:30am");
  t = t.replace(/\bhalf eight\b/g, "8:30am");
  t = t.replace(/\bhalf nine\b/g, "9:30am");
  t = t.replace(/\bhalf ten\b/g, "10:30am");
  t = t.replace(/\bhalf eleven\b/g, "11:30am");
  t = t.replace(/\bhalf twelve\b/g, "12:30pm");
  t = t.replace(/\barvie\b/g, "afternoon");
  t = t.replace(/\btomoz\b/g, "tomorrow");
  t = t.replace(/\bnext week sometime\b/g, "next week");
  return t;
}

function wrapSsml(text, rate = "medium", pitch = "medium") {
  return `<speak><prosody rate="${rate}" pitch="${pitch}">${text}</prosody></speak>`;
}

function saySlowly(twiml, text) {
  twiml.say({ voice: "Polly.Amy", language: "en-AU" }, wrapSsml(text, "slow", "low"));
}

function sayWarm(twiml, text) {
  twiml.say({ voice: "Polly.Amy", language: "en-AU" }, wrapSsml(text, "medium", "medium"));
}

function pickRandom(arr) {
  if (!Array.isArray(arr) || arr.length === 0) return "";
  return arr[Math.floor(Math.random() * arr.length)];
}

function getAffirmation(session) {
  const used = Array.isArray(session._usedAffirmations) ? session._usedAffirmations : [];
  const all = [
    "Perfect.", "Got it.", "Lovely.", "No worries.", "Brilliant.",
    "Good one.", "Great stuff.", "Noted.", "Wonderful.", "Sounds good."
  ];
  const available = all.filter(a => !used.includes(a));
  const pool = available.length > 0 ? available : all;
  const pick = pickRandom(pool);
  session._usedAffirmations = [...used.slice(-4), pick];
  return pick;
}

function getTimeOfDayGreeting(tz) {
  const hour = DateTime.now().setZone(tz || "Australia/Sydney").hour;
  if (hour < 12) return "Good morning";
  if (hour < 17) return "Good afternoon";
  return "Thanks for calling this evening";
}

function getJobEmpathy(job) {
  const j = String(job || "").toLowerCase();
  if (/burst pipe|flooding|gas leak|sparking|no power|emergency/.test(j))
    return "Oh no — let us get that sorted fast.";
  if (/blocked drain|blocked toilet|overflow/.test(j))
    return "That is never fun — let us get someone out.";
  if (/no hot water|hot water/.test(j))
    return "No hot water is rough — let us fix that quickly.";
  if (/broken|not working|damaged/.test(j))
    return "That sounds frustrating — let us get it sorted.";
  return "";
}

function getCalendarWaitMessage() {
  return pickRandom([
    "Just checking the calendar — won't be a moment.",
    "Bear with me one sec while I check availability.",
    "Let me just pull up the calendar for you.",
    "One moment — just checking that now.",
    "Just a sec — looking at availability."
  ]);
}

function isAustralianPublicHoliday(tz) {
  const now = DateTime.now().setZone(tz || "Australia/Sydney");
  const m = now.month;
  const d = now.day;
  const fixed = [[1,1],[1,26],[4,25],[12,25],[12,26]];
  return fixed.some(([fm, fd]) => fm === m && fd === d);
}

function normaliseSpokenNumber(text) {
  if (!text) return text;
  const map = {
    zero:"0",one:"1",two:"2",three:"3",four:"4",five:"5",six:"6",
    seven:"7",eight:"8",nine:"9",ten:"10",eleven:"11",twelve:"12",
    thirteen:"13",fourteen:"14",fifteen:"15",sixteen:"16",seventeen:"17",
    eighteen:"18",nineteen:"19",twenty:"20",thirty:"30",forty:"40",
    fifty:"50",sixty:"60",seventy:"70",eighty:"80",ninety:"90"
  };
  let t = String(text).toLowerCase();
  for (const [w, n] of Object.entries(map)) {
    t = t.replace(new RegExp(`\b${w}\b`, "g"), n);
  }
  return t;
}

function normaliseAussieSlang(text) {
  if (!text) return text;
  let t = String(text).toLowerCase();
  t = t.replace(/\bthis arvo\b/g, "this afternoon");
  t = t.replace(/\barvo\b/g,      "afternoon");
  t = t.replace(/\barvie\b/g,     "afternoon");
  t = t.replace(/\bsmoko\b/g,     "10am");
  t = t.replace(/\btomoz\b/g,     "tomorrow");
  t = t.replace(/\bhalf two\b/g,  "2:30pm");
  t = t.replace(/\bhalf three\b/g,"3:30pm");
  t = t.replace(/\bhalf four\b/g, "4:30pm");
  t = t.replace(/\bhalf five\b/g, "5:30pm");
  t = t.replace(/\bhalf six\b/g,  "6:30pm");
  t = t.replace(/\bhalf seven\b/g,"7:30am");
  t = t.replace(/\bhalf eight\b/g,"8:30am");
  t = t.replace(/\bhalf nine\b/g, "9:30am");
  t = t.replace(/\bhalf ten\b/g,  "10:30am");
  t = t.replace(/\bhalf eleven\b/g,"11:30am");
  t = t.replace(/\bhalf twelve\b/g,"12:30pm");
  return t;
}

function generateBookingRef() {
  return Math.random().toString(36).substring(2, 8).toUpperCase();
}

function normalizeTimeText(text, tz) {
  if (!text) return "";
  let t = String(text).toLowerCase().trim();

  // Normalise Australian slang and spoken numbers first
  t = normaliseAussieSlang(t);
  t = normaliseSpokenNumber(t);

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

  // If text contains a day name but no time, default to 9am
  const dayOnly = /\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|next week|tomorrow)\b/i.test(t);
  const hasTime = /\b\d{1,2}(:\d{2})?\s*(am|pm)\b|\b(morning|afternoon|evening|midday|noon|midnight)\b/i.test(t);
  if (dayOnly && !hasTime) {
    t = t + " at 9am";
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

// 
// Interruption + edits
// 
function detectYesNoFromDigits(d) {
  if (!d) return null;
  if (d === "1") return "YES";
  if (d === "2") return "NO";
  return null;
}
function detectYesNo(text) {
  const t = (text || "").toLowerCase().trim();
  const yes = ["yes","yeah","yep","yup","sure","ok","okay","correct","that's right","thats right","sounds good","absolutely","for sure","definitely","go ahead","go for it","that’s right","confirm"];
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

// 
// Access notes normaliser (fixes “stuck on access notes”)
// 
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

// 
// Profanity/abuse handling
// 
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

// 
// Lightweight slot-fill (if caller blurts multiple fields)
// 
function trySlotFill(session, speech, tz) {
  const raw = String(speech || "").trim();
  if (!raw) return;

  const dt = parseRequestedDateTime(raw, tz);
  if (dt) {
    session.time = raw;
    session.bookedStartMs = dt.toMillis();
  }

  if (validateAddress(raw)) session.address = session.address || raw;

  // Extract suburb mentioned in passing during other steps
  if (!session.suburb) {
    const suburbMatch = raw.match(/\bin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b/);
    if (suburbMatch?.[1] && suburbMatch[1].length > 3) {
      session.suburb = suburbMatch[1];
    }
  }

  const m = raw.match(/my name is\s+(.+)/i);
  if (m?.[1]) {
    const nm = cleanSpeech(m[1]);
    if (validateName(nm)) session.name = session.name || nm;
  }

  if (/gate|code|parking|dog|pet|call on arrival|buzz|intercom/i.test(raw)) {
    session.accessNote = session.accessNote || raw;
  }

  // Multi job capture — join both if caller mentions two jobs
  const jobPattern = /(leak|blocked|hot water|air con|heater|toilet|sink|tap|power|switch|deck|tile|tiles|fence|roof|gutter|drain|pipe|painting|electrical|plumbing)/gi;
  const jobMatches = raw.match(jobPattern);
  if (!session.job && jobMatches && jobMatches.length >= 2) {
    session.job = raw;
  } else if (!session.job && jobMatches && jobMatches.length === 1) {
    session.job = raw;
  }
}

// 
// Google Calendar helpers
// 
async function getTradieGoogleAuthByPhone(phone) {
  const normalizedPhone = normalizePhoneE164AU(phone);
  const digitsOnly = normalizedPhone.replace(/^\+/, "");

  if (!supabase) throw new Error("Database not configured");
  if (!normalizedPhone) throw new Error("Missing phone");

  const { data: tradieByNumber, error: tradieError } = await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .select("id,google_refresh_token")
    .or([
      `twilio_number.eq.${normalizedPhone}`,
      `twilio_phone_number.eq.${normalizedPhone}`,
      `twilio_to.eq.${normalizedPhone}`,
      `twilio_number.eq.${digitsOnly}`,
      `twilio_phone_number.eq.${digitsOnly}`,
      `twilio_to.eq.${digitsOnly}`
    ].join(","))
    .limit(1)
    .maybeSingle();

  if (tradieError) {
    throw new Error(`TRADIE_PHONE_LOOKUP_FAILED: ${tradieError.message}`);
  }

  if (tradieByNumber?.id) {
    return {
      tradieId: String(tradieByNumber.id || "").trim(),
      googleRefreshToken: String(tradieByNumber.google_refresh_token || "").trim()
    };
  }

  const { account, normalizedCalledNumber } = await lookupTradieAccountByCalledNumber({
    supabase,
    calledNumber: normalizedPhone
  });

  if (!account?.tradie_id) {
    throw new Error(`NO_TRADIE_FOR_PHONE: ${normalizedCalledNumber || normalizedPhone}`);
  }

  const tradieId = String(account.tradie_id || "").trim();
  const { data: tradieById, error: tradieByIdError } = await supabase
    .from(SUPABASE_TRADIES_TABLE)
    .select("id,google_refresh_token")
    .eq("id", tradieId)
    .maybeSingle();

  if (tradieByIdError) {
    throw new Error(`TRADIE_ID_LOOKUP_FAILED: ${tradieByIdError.message}`);
  }

  return {
    tradieId,
    googleRefreshToken: String(tradieById?.google_refresh_token || "").trim()
  };
}

async function getCalendarClient(identifier) {
  try {
    const rawIdentifier = String(identifier || "").trim();
    let authDetails = null;

    if (/^\+?\d{8,}$/.test(rawIdentifier.replace(/\s+/g, ""))) {
      authDetails = await getTradieGoogleAuthByPhone(rawIdentifier);
    } else if (rawIdentifier) {
      const { data, error } = await supabase
        .from(SUPABASE_TRADIES_TABLE)
        .select("id,google_refresh_token")
        .eq("id", rawIdentifier)
        .maybeSingle();
      if (error) throw new Error(`TRADIE_LOOKUP_FOR_AUTH_FAILED: ${error.message}`);
      authDetails = {
        tradieId: String(data?.id || rawIdentifier).trim(),
        googleRefreshToken: String(data?.google_refresh_token || "").trim()
      };
    }

    if (!authDetails) throw new Error("Missing tradie identifier");
    if (!authDetails.googleRefreshToken) throw new Error("Google OAuth not connected for this tradie");

    const auth = new google.auth.OAuth2(
      process.env.GOOGLE_CLIENT_ID,
      process.env.GOOGLE_CLIENT_SECRET,
      process.env.GOOGLE_REDIRECT_URI
    );

    auth.setCredentials({
      refresh_token: authDetails.googleRefreshToken
    });

    return { calendar: google.calendar({ version: "v3", auth }), tradieId: authDetails.tradieId };
  } catch (error) {
    console.error("GET_CALENDAR_CLIENT_ERROR", {
      identifier,
      message: error?.message || String(error)
    });
    throw error;
  }
}
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

async function safeSupabaseWrite(writePromise, context = "") {
  try {
    const result = await writePromise;
    if (result?.error) {
      console.error("DB WRITE FAIL:", context, result.error);
    }
    return result;
  } catch (error) {
    console.error("DB WRITE FAIL:", context, error);
    return { error };
  }
}


async function resolveCalendarTarget(tradie, context = {}) {
  const fallbackCalendarId = String(tradie?.calendarId || "").trim();
  if (fallbackCalendarId) {
    return {
      calendarId: fallbackCalendarId,
      source: "tradie-config",
      timezone: String(tradie?.timezone || "Australia/Sydney")
    };
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
  const calledNumber = String(context.calledNumber || "").trim();
  const calendarTarget = await resolveCalendarTarget(tradie, context);
  const calendarId = String(calendarTarget.calendarId || "").trim();
  if (!calendarId) {
    return { ok: false, reason: "missing_calendar_id" };
  }

  let calendar;
  try {
    const { calendar: calendarClient, tradieId } = await getCalendarClient(calledNumber || tradie?.twilioNumber || tradie?.twilio_number || tradie?.twilio_phone_number || "");
    calendar = calendarClient;
    if (!tradie.id && tradieId) tradie.id = tradieId;
  } catch (err) {
    console.error("CALENDAR_EVENT_CREATE_ERROR", {
      message: err?.message || String(err),
      stack: err?.stack || null,
      calendarId,
      calledNumber: calledNumber || null
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
    description: `Name: ${customerName}
Phone: ${customerPhone}
Address: ${address}
Job: ${job}`,
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
  for (let i = 0; i < 3; i++) {
    try {
      return await calendar.events.insert({ calendarId, requestBody });
    } catch (e) {
      lastErr = e;
      console.log("Calendar retry", i);
      if (i < 2) await new Promise((r) => setTimeout(r, 1500));
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
  if (!tradie.googleRefreshToken) return [];
  const calendarId = String(tradie.calendarId || "").trim();
  if (!calendarId) return [];
  const tz = tradie.timezone;

  let start = startSearchDt;
  if (!start || !DateTime.isDateTime(start) || !start.isValid) start = DateTime.now().setZone(tz).plus({ minutes: 10 }).startOf("minute");
  else start = start.setZone(tz);

  const { calendar } = await getCalendarClient(tradie.id);
  const searchEnd = start.plus({ days: 14 });

  const busy = await getBusy(
    calendar,
    calendarId,
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
    if (!validateTwilioSignature(req)) {
      const deniedTwiml = new VoiceResponse();
      deniedTwiml.say("Sorry, we are experiencing a temporary issue. Let’s continue.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, deniedTwiml);
    }

    if (supabase) {
      await safeSupabaseWrite(supabase.from("calls").insert([{
        call_sid: resolveCallSid(req),
        from_number: String(req.body?.From || "").trim() || null,
        to_number: String(req.body?.To || "").trim() || null,
        created_at: new Date().toISOString()
      }]), "calls.insert");
    }

    return handleVoiceEntry(req, res);
  } catch (e) {
    console.error("/voice error", e);
    const twiml = new VoiceResponse();
    const gather = twiml.gather({
      input: "speech",
      timeout: 8,
      speechTimeout: "auto",
      action: "/process",
      method: "POST",
      language: "en-AU"
    });
    gather.say("Hi — still here with you. What do you need help with today?", { voice: "Polly.Amy", language: "en-AU" });
    return sendVoiceTwiml(res, twiml);
  }
});

app.post("/process", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    if (!validateTwilioSignature(req)) {
      twiml.say("Sorry, we are experiencing a temporary issue. Let’s continue.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, twiml);
    }

    const tradie = await getTradieConfig(req);

    // Hard stop if disabled
    if (tradie.status && tradie.status !== "ACTIVE") {
      twiml.say("This service is currently unavailable.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, twiml);
    }

    // Check in-memory blacklist
    const callerNumber = String(req.body?.From || "").trim();
    if (callerNumber) {
      const blCheckKey = `${tradie.key}::${callerNumber}`;
      if (callerBlacklist.has(blCheckKey)) {
        console.log(`BLACKLISTED_CALLER_BLOCKED tradie=${tradie.key} number=${callerNumber}`);
        console.log(`HANGUP_PREVENTED reason=blacklist callSid=${resolveCallSid(req)}`);
        twiml.say("Sorry, we are unable to take your call at this time. I can ask someone to call you back.", { voice: "Polly.Amy", language: "en-AU" });
        return sendVoiceTwiml(res, twiml);
      }
    }

    // If tradie is active but Google Calendar not connected yet
    // give caller a friendly setup message instead of crashing
    if (!tradie.googleRefreshToken || tradie.googleConnected === false) {
      twiml.say("Hi, thanks for calling. We are just setting up your booking system — please try again in a few minutes and we will be ready to take your booking.", { voice: "Polly.Amy", language: "en-AU" });
      console.log(`HANGUP_PREVENTED reason=calendar_not_connected callSid=${resolveCallSid(req)}`);
      return sendVoiceTwiml(res, twiml);
    }

    // After hours detection — log and alert owner but keep bot running
    const nowForHours = DateTime.now().setZone(tradie.timezone || "Australia/Sydney");
    const callHour = nowForHours.hour;
    const callDay = nowForHours.weekday; // 1=Mon 7=Sun
    const bizDays = tradie.businessDays || [1, 2, 3, 4, 5];
    const bizStart = tradie.businessStartHour || 7;
    const bizEnd = tradie.businessEndHour || 17;
    const isInBizHours = bizDays.includes(callDay) &&
                         callHour >= bizStart &&
                         callHour < bizEnd;
    if (!isInBizHours) {
      const afterHoursFrom = String(req.body?.From || "").trim();
      console.log(`AFTER_HOURS_CALL tradie=${tradie.key} from=${afterHoursFrom} hour=${callHour}`);
      try {
        await incMetric(tradie, { after_hours_calls: 1 }).catch(() => {});
        await sendOwnerSms(tradie,
          `🌙 AFTER HOURS CALL\nCaller: ${afterHoursFrom}\nTime: ${nowForHours.toFormat("HH:mm")} (${tradie.timezone || "AEST"})\nBot is still attempting to take the booking.`
        ).catch(() => {});
      } catch {}
      // Store flag so confirm step can add reassurance message
      req._isAfterHours = true;
      // Do NOT hang up — let bot continue to take booking after hours
    }

    const tz = tradie.timezone;

    const callSid = resolveCallSid(req);
    const fromNumber = (req.body.From || "").trim();
    res.locals.callSid = callSid;

    if (fromNumber && rateLimit(fromNumber)) {
      twiml.say("Thanks for your patience — we are handling a lot right now. I can still help you book this in.", { voice: "Polly.Amy", language: "en-AU" });
      twiml.pause({ length: 1 });
    }

    // Block any POST that arrives after session was reset on confirmed booking
    const existingSession = sessions.get(callSid);
    if (!existingSession && req.body?.SpeechResult) {
      const doneTwiml = new VoiceResponse();
      console.log(`HANGUP_PREVENTED reason=post_reset_webhook callSid=${callSid}`);
      const keepAlivePrompt = "Just checking you're still there — would you like to continue booking?";
      ask(doneTwiml, keepAlivePrompt, voiceActionUrl(req), { input: "speech", timeout: 8, speechTimeout: "auto" });
      return res.type("text/xml").send(doneTwiml.toString());
    }

    const hasSpeechField = Object.prototype.hasOwnProperty.call(req.body || {}, "SpeechResult") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "speechResult");
    const speech = cleanSpeech(req.body.SpeechResult || req.body.speechResult || "");
    const digits = String(req.body.Digits || "").trim();
    const confidenceRaw = req.body.Confidence;
    const hasConfidence = confidenceRaw !== undefined && confidenceRaw !== null && String(confidenceRaw).trim() !== "";
    const confidence = hasConfidence ? Number(confidenceRaw) : null;

    const session = getSession(callSid, fromNumber);
    if (session.processing === true) {
      console.log("FLOW LOCK — duplicate webhook");
      return sendVoiceTwiml(res, twiml);
    }
    session.processing = true;

    try {
    session.callSid = callSid;
    session.tradieKey = session.tradieKey || tradie.key;
    const detectedConversationState = detectConversationState(speech || "");
    updateConversationMemory(session, speech, detectedConversationState);
    syncConversationStateFromSession(session);
    session.conversationState.smartModeActive = true;
    session.conversationState.callLockedToFlow = true;
    setLockedFlowStep(session, mapLegacyStepToLockedStep(session.step), "sync_from_legacy_step");

    try {
      session.conversation_state = behaviouralEngine.updateConversationState(
        session.conversation_state,
        speech || "",
        session.step || "intent"
      );
      if (process.env.DEBUG_BEHAVIOUR === "true") {
        console.log("BEHAVIOUR_STATE", {
          callSid,
          urgency: session.conversation_state?.urgency_score,
          trust: session.conversation_state?.trust_score,
          resistance: session.conversation_state?.resistance_type,
          commitment: session.conversation_state?.commitment_stage
        });
      }
    } catch (behaviourErr) {
      if (process.env.DEBUG_BEHAVIOUR === "true") {
        console.error("BEHAVIOUR_STATE_UPDATE_ERROR", behaviourErr?.message || behaviourErr);
      }
    }

    // state persistence: CallSid keyed session is authoritative for the current step
    const authoritativeStep = session.step || "intent";

    // idempotency lock: ignore duplicate webhooks for the same CallSid+step within a short TTL
    if (hasSpeechField && !acquireProcessSpeechLock(callSid, authoritativeStep)) {
      console.log(`IDEMPOTENT_DUPLICATE TID=${tradie.key} CALLSID=${callSid} STEP=${authoritativeStep}`);
      if (session.lastTwimlXml) {
        return res.type("text/xml").send(session.lastTwimlXml);
      }
      return sendVoiceTwiml(res, new VoiceResponse());
    }

    // Count inbound call once for analytics
    if (!session._countedCall) {
      session._countedCall = true;
      await incMetric(tradie, { calls_total: 1 }).catch(() => {});
    }

    if (!hasSpeechField && !session.awaitingCalendarCheck) {
      if (session.hasEnteredVoice) {
        const actionUrl = voiceActionUrl(req);
        const twimlRepeat = new VoiceResponse();
        const repeatPrompt = session.lastPrompt || "What do you need help with today?";
        ask(twimlRepeat, repeatPrompt, actionUrl, { input: "speech", timeout: 7, speechTimeout: "auto" });
        return sendVoiceTwiml(res, twimlRepeat);
      }
      session.hasEnteredVoice = true;
      session.step = "intent";
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
    if (speech || digits) {
      session.conversationState.awaitingUserResponse = false;
    }

    if (
      session.conversationState.awaitingUserResponse &&
      hasSpeechField &&
      !speech &&
      !digits &&
      (Date.now() - Number(session.conversationState.lastQuestionTimestamp || 0)) < Number(session.conversationState.minQuestionGapMs || 1200)
    ) {
      console.log(`QUESTION_LOCKED step=${session.conversationState.lastQuestionAsked || session.step || "unknown"} reason=no_valid_user_data`);
      session.lastPrompt = "I didn’t quite catch that — could you repeat it?";
      ask(twiml, session.lastPrompt, voiceActionUrl(req), { session, input: "speech", timeout: 6, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    const globalOverride = detectGlobalVoiceOverride(speech);
    if (globalOverride === "START_OVER") {
      console.log(`FLOW_GUARD_BLOCK callSid=${callSid} reason=start_over_blocked`);
      const retryPrompt = buildObjectivePrompt(session.conversationState.currentStep, session.conversationState.collectedFields);
      session.lastPrompt = retryPrompt;
      ask(twiml, retryPrompt, voiceActionUrl(req), { session, input: "speech", timeout: 7, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }
    if (globalOverride === "CANCEL") {
      await missedRevenueAlert(tradie, session, "Caller said cancel").catch(() => {});
      twiml.say("No problem. I’ve cancelled this request. Goodbye.", { voice: "Polly.Amy", language: "en-AU" });
      console.log(`HANGUP_PREVENTED reason=cancel_requested callSid=${callSid}`);
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

      if (session.abuseStrikes >= 5) {
        console.log(`HANGUP_PREVENTED reason=abuse_threshold callSid=${callSid}`);
        twiml.say("Let us keep this respectful so I can help with your booking. Are you ready to continue?", { voice: "Polly.Amy", language: "en-AU" });
        ask(twiml, "Would you like to continue booking?", voiceActionUrl(req), { input: "speech", timeout: 7, speechTimeout: "auto" });
        return sendVoiceTwiml(res, twiml);
      }

      const abuseActionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      const abusePrompt = prefix + (session.lastPrompt || "How can we help today?");
      session.lastPrompt = abusePrompt;
      addToHistory(session, "assistant", abusePrompt);
      ask(twiml, abusePrompt, abuseActionUrl);
      return sendVoiceTwiml(res, twiml);
    }

    // Handle no-speech timeout from <Gather> callback only
    if (!speech && !digits && hasSpeechField) {
      console.log(`NO_SPEECH_TIMEOUT TID=${tradie.key} CALLSID=${callSid} FROM=${fromNumber}`);
      session.silenceTries += 1;
      session.lastNoSpeechFallback = true;
      const repeated = await repeatLastStepPrompt(req, res, twiml, session, session.step, "NO_SPEECH", tradie);
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
      session.unclearTurns = Number(session.unclearTurns || 0) + 1;
      session.lastPrompt = "Sorry, I didn’t quite catch that. Please say it again.";
      ask(twiml, session.lastPrompt, voiceActionUrl(req), { input: "speech", timeout: 6, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    if (!speech || detectedConversationState.primaryState === "confusion") {
      session.unclearTurns = Number(session.unclearTurns || 0) + 1;
    } else {
      session.unclearTurns = 0;
    }

    if (session.unclearTurns >= 3) {
      session.unclearTurns = 0;
      const controlPrompt = "Let's get a time secured first so this doesn't get worse. " + micro_commitment_question("time");
      session.step = "time";
      session.lastPrompt = controlPrompt;
      addToHistory(session, "assistant", controlPrompt);
      ask(twiml, controlPrompt, voiceActionUrl(req), { input: "speech", timeout: 7, speechTimeout: "auto" });
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

      const repeatedFallback = await repeatLastStepPrompt(req, res, twiml, session, "sms_fallback_offer", speech ? "UNCLEAR" : "NO_SPEECH", tradie);
      return sendVoiceTwiml(res, twiml);
    }

    // Callback request detection
    const wantsCallback = /\b(call me back|ring me back|call me later|get someone to call|have someone call|callback|call back)\b/i.test(speech || "");
    if (wantsCallback && !session.callbackRequested) {
      session.callbackRequested = true;
      const callbackTime = session.time || "as soon as possible";
      await sendOwnerSms(tradie,
        `📞 CALLBACK REQUEST\nCaller: ${session.from || "unknown"}\nName: ${session.name || "unknown"}\nTime: ${callbackTime}\nJob: ${session.job || "unknown"}\nPlease call them back.`
      ).catch(() => {});
      twiml.say(
        "No worries — I will let the team know to call you back. Is there a good time that suits you?",
        { voice: "Polly.Amy", language: "en-AU" }
      );
      ask(twiml, "What time works best for a callback?", voiceActionUrl(req));
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
    const isOffScript = isOffScriptSpeech(speech) ||
      (session.lastEmotion && session.lastEmotion !== "neutral") ||
      detectAbuse(speech);

    const shouldUseLlm =
      llmReady() &&
      session.llmTurns < LLM_MAX_TURNS &&
      !!speech &&
      isOffScript &&
      !["confirm", "pickSlot", "intent", "initial"].includes(session.step) &&
      !session.conversationState.smartModeActive;

    if (!session.conversationState.smartModeActive && speech && (detectedConversationState.offTopic || detectedConversationState.primaryState === "ranting_storytelling" || detectedConversationState.primaryState === "emotional_distress")) {
      const fallbackQuestion = buildTaskControlPrompt(session, session.step || "intent");
      const redirected = behaviouralEngine.redirectOffTopic(session.conversation_state, fallbackQuestion);
      const controlledPrompt = composeAdaptivePrompt(session, redirected, detectedConversationState, session.step || "intent");
      session.lastPrompt = controlledPrompt;
      addToHistory(session, "assistant", controlledPrompt);
      ask(twiml, controlledPrompt, voiceActionUrl(req), { input: "speech", timeout: 7, speechTimeout: "auto", session });
      return sendVoiceTwiml(res, twiml);
    }

    if (!session.conversationState.smartModeActive && speech && ["price_objection", "comparison_shopping", "trust_testing", "dominant_customer", "analytical_customer", "passive_customer", "urgency"].includes(detectedConversationState.primaryState)) {
      const controlPrompt = buildTaskControlPrompt(session, session.step || "intent");
      const resistance = session.conversation_state?.resistance_type;
      const objection = behaviouralEngine.objectionResponse(resistance);
      const trustSignal = behaviouralEngine.addTrustSignal(session.conversation_state?.commitment_stage === "decision" ? "decision" : "early");
      const adapted = composeAdaptivePrompt(session, `${objection} ${trustSignal} ${controlPrompt}`.trim(), detectedConversationState, session.step || "intent");
      session.lastPrompt = adapted;
      addToHistory(session, "assistant", adapted);
      ask(twiml, adapted, voiceActionUrl(req), { input: "speech", timeout: 7, speechTimeout: "auto", session });
      return sendVoiceTwiml(res, twiml);
    }

    if (shouldUseLlm) {
      session.llmTurns += 1;
      const llm = await callLlm(tradie, session, speech);

      if (llm) {
        // LLM used for off-script acknowledgement only
        // Combine LLM acknowledgement with the ORIGINAL next step question — never replace it
        const originalStepPrompt = session.lastPrompt || "What do you need help with today?";

        let mergedPrompt;
        if (llm.smalltalk_reply) {
          mergedPrompt = `${llm.smalltalk_reply} ${originalStepPrompt}`.trim();
        } else {
          mergedPrompt = originalStepPrompt;
        }

        if (!mergedPrompt || mergedPrompt.trim().length < 3) {
          mergedPrompt = originalStepPrompt;
        }

        session.lastPrompt = mergedPrompt;
        addToHistory(session, "assistant", mergedPrompt);

        // Update intent and fields from LLM if they add new info — but NEVER overwrite existing fields
        if (llm.intent && llm.intent !== "UNKNOWN" && !session.intent) session.intent = llm.intent;
        const f = llm.fields || {};
        if (typeof f.job === "string" && f.job.trim().length >= 2 && !session.job) session.job = f.job.trim();
        if (typeof f.address === "string" && validateAddress(f.address) && !session.address) session.address = f.address.trim();
        if (typeof f.name === "string" && validateName(f.name) && !session.name) session.name = f.name.trim();
        if (typeof f.access === "string" && validateAccess(f.access) && !session.accessNote) session.accessNote = f.access.trim();
        if (typeof f.time_text === "string" && f.time_text.trim().length >= 2 && !session.time) {
          session.time = f.time_text.trim();
          const dtTry = parseRequestedDateTime(session.time, tz);
          if (dtTry) session.bookedStartMs = session.bookedStartMs || dtTry.toMillis();
        }

        // Track emotion
        if (llm.off_script) session.offScriptCount = Number(session.offScriptCount || 0) + 1;
        if (llm.emotion && llm.emotion !== "neutral") session.lastEmotion = llm.emotion;

        const actionUrl2 = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
        ask(twiml, mergedPrompt, actionUrl2);
        return sendVoiceTwiml(res, twiml);
      }
    }

    // Global interrupts (context-aware) — do NOT hijack access step
    const yn = detectYesNoFromDigits(digits) || detectYesNo(speech);
    const corrected = speech ? detectCorrection(speech) : false;
    const changeField = detectChangeFieldFromDigits(digits) || detectChangeFieldFromSpeech(speech);
    const canGlobalInterrupt = !session.conversationState.smartModeActive && !["intent", "confirm", "pickSlot", "access"].includes(session.step);

    if (canGlobalInterrupt && (corrected || changeField)) {
      if (changeField) {
        session.step = changeField;
        session.lastPrompt = `Sure — what’s the correct ${changeField}?`;
      } else {
        session.step = "access";
        session.lastPrompt = "Any access notes like gate code, parking, or pets? Say none if not.";
      }
      addToHistory(session, "assistant", session.lastPrompt);

      const actionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      ask(twiml, session.lastPrompt, actionUrl, { session });
      return sendVoiceTwiml(res, twiml);
    }

    if (canGlobalInterrupt && yn === "NO") {
      session.step = "access";
      session.lastPrompt = "Any access notes like gate code, parking, or pets? Say none if not.";
      addToHistory(session, "assistant", session.lastPrompt);

      const actionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      ask(twiml, session.lastPrompt, actionUrl, { session });
      return sendVoiceTwiml(res, twiml);
    }

    // ------------------------------------------------------------------------
    // MAIN FLOW
    // ------------------------------------------------------------------------
    const actionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");

    ["job", "address", "name", "access", "time"].forEach((stepName) => {
      if (session.step === stepName) {
        maybeSkipCollectedField(session, stepName);
      }
    });

    // STEP: intent
    if (session.step === "intent") {
      // If job was already captured in initial step skip straight to address
      if (session.job) {
        const jeIntent = getJobEmpathy(session.job);
        const affIntent = jeIntent || getAffirmation(session);
        session.step = "address";
        session.lastPrompt = `${affIntent} And whereabouts is the job? What is the address?`;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }
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
          ask(twiml, session.lastPrompt, actionUrl, { session });
          return sendVoiceTwiml(res, twiml);
        }

        // Quote
        if (session.intent === "QUOTE") {
          session.step = "job";
          session.lastPrompt = "Sure. What do you need a quote for?";
          addToHistory(session, "assistant", session.lastPrompt);
          ask(twiml, session.lastPrompt, actionUrl, { session });
          return sendVoiceTwiml(res, twiml);
        }

        // Support/admin/existing or booking defaults to normal booking flow.
        session.step = "job";
        session.lastPrompt = "What job do you need help with?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }

      // First speech treated as job description — move straight to address
      if (speech) session.job = speech;
      session.intent = "NEW_BOOKING";
      const jobEmpathy = getJobEmpathy(session.job);
      const affirmJob = jobEmpathy || getAffirmation(session);
      session.step = "address";
      session.lastPrompt = `${affirmJob} And whereabouts is the job? What is the address?`;
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech" });
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: job
    if (session.step === "job") {
      if (speech) session.job = speech;

      if (shouldReject("job", session.job, confidence)) {
        session.lastPrompt = "Sorry — what job do you need help with?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }

      const jeJob = getJobEmpathy(session.job);
      const affJob = jeJob || getAffirmation(session);
      session.step = "address";
      session.lastPrompt = `${affJob} And whereabouts is the job? What is the full address?`;
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { session });
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: address
    if (session.step === "address") {
      if (speech) session.address = speech;

      // Low confidence but not empty — read it back to confirm
      if (
        session.address &&
        session.address.trim().length > 3 &&
        typeof confidence === "number" &&
        confidence > 0 &&
        confidence < 0.5 &&
        !session.addressReadBack
      ) {
        session.addressReadBack = true;
        session.lastPrompt = `Sorry — just to confirm, I heard "${session.address}". Is that right?`;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }

      if (shouldReject("address", session.address, confidence)) {
        const suburbHint = session.suburb ? ` Is it in ${session.suburb}?` : "";
        session.lastPrompt = `Sorry — what is the full street address?${suburbHint}`;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }

      // Read back address if it contains a high number to confirm
      const addrNumberMatch = (session.address || "").match(/\b(\d{3,})\b/);
      if (addrNumberMatch && !session.addressConfirmed) {
        session.addressConfirmed = true;
        const readBack = `Just to confirm — I have got ${session.address}. Is that right?`;
        session.lastPrompt = readBack;
        session.pendingStep = "name";
        session.step = "address_confirm";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }
      session.addressConfirmed = true;

      // address history
      if (tradie.googleRefreshToken && tradie.id && tradie.calendarId) {
        try {
          const { calendar } = await getCalendarClient(tradie.id);
          session.lastAtAddress = await getLastBookingAtAddress(calendar, tradie.calendarId, tz, session.address);
        } catch (error) { console.error("CALENDAR_ADDRESS_HISTORY_ERROR", error); }
      }

      const affirmAddr = getAffirmation(session);
      session.step = "name";
      session.lastPrompt = `${affirmAddr} And what name should I put the booking under?`;
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { session });
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: address_confirm
    if (session.step === "address_confirm") {
      const yn = detectYesNo(speech);
      if (yn === "YES") {
        const affirmAddrConfirm = getAffirmation(session);
        session.step = "name";
        session.lastPrompt = `${affirmAddrConfirm} And what name should I put the booking under?`;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      } else {
        session.address = "";
        session.addressConfirmed = false;
        session.step = "address";
        session.lastPrompt = "No worries — what is the correct address?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }
    }

    // STEP: name
    if (session.step === "name") {
      if (speech) session.name = speech;

      // For names, only reject if completely empty — short names like
      // "Tim" or "Jo" often get low confidence scores from Twilio
      const nameIsEmpty = !session.name || session.name.trim().length < 2;
      if (nameIsEmpty) {
        session.lastPrompt = "Sorry — what name should I put the booking under?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }

      const affName = getAffirmation(session);
      const firstName = session.name ? session.name.split(" ")[0] : "";
      session.step = "access";
      session.lastPrompt = `${affName}${firstName ? " " + firstName + " —" : ""} any access notes like a gate code, parking, or pets? Just say none if not.`;
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { session });
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

      const affAccess = getAffirmation(session);
      session.step = "time";
      session.lastPrompt = `${affAccess} ${micro_commitment_question("day")} ${micro_commitment_question("time")}`;
      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { session });
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: time
    if (session.step === "time") {
      session.time = speech || session.time || "";

      let dt = null;
      if (session.bookedStartMs) dt = DateTime.fromMillis(session.bookedStartMs, { zone: tz });
      else {
        if (!looksLikeAsap(session.time)) dt = parseRequestedDateTime(session.time, tz);
        if (!dt && looksLikeAsap(session.time)) {
          dt = nextBusinessOpenSlot(tradie);
          const asapSlotText = formatForVoice(dt);
          const recallLine = behaviouralEngine.memoryRecallResponse(session.conversation_state, `we have ${asapSlotText} available`);
          session.lastPrompt = `${recallLine ? recallLine + ". " : ""}The next available slot is ${asapSlotText}. Does that work for you?`;
          session.bookedStartMs = dt.toMillis();
          addToHistory(session, "assistant", session.lastPrompt);
          ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 7, speechTimeout: "auto" });
          return sendVoiceTwiml(res, twiml);
        }
        if (!dt && speech) {
          session.lastPrompt = "Sorry, I did not quite catch that time. Please say it again — for example: tomorrow at 2 pm.";
          ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 6, speechTimeout: "auto" });
          return sendVoiceTwiml(res, twiml);
        }
        if (!dt && isAfterHoursNow(tradie)) dt = nextBusinessOpenSlot(tradie);
        if (!dt) dt = DateTime.now().setZone(tz).plus({ minutes: 10 }).startOf("minute");
      }

      if (tradie.googleRefreshToken && tradie.id && tradie.calendarId) {
        session.bookedStartMs = dt.toMillis();
        session.calendarCheck = {
          requestedDtISO: dt.toISO(),
          attempts: 0
        };
        if (!session.calendarCheckAnnounced) {
          session.calendarCheckAnnounced = true;
          keepCallAliveForProcessing(req, twiml, getCalendarWaitMessage());
        } else {
          keepCallAliveForProcessing(req, twiml);
        }
        return sendVoiceTwiml(res, twiml);
      }

      session.bookedStartMs = dt.toMillis();
      session.step = "confirm";

      const whenText = formatForVoice(dt);

      // Duplicate detection (calendar)
      if (tradie.googleRefreshToken && tradie.id && tradie.calendarId) {
        try {
          const { calendar } = await getCalendarClient(tradie.id);
          const dup = await withTimeout(
            findDuplicate(calendar, tradie.calendarId, tz, session.name, session.address, dt),
            CALENDAR_OP_TIMEOUT_MS,
            "calendar duplicate check"
          );
          session.duplicateEvent = dup;
        } catch (error) {
          console.error("CALENDAR_DUPLICATE_CHECK_ERROR", error);
        }
      }

      if (session.duplicateEvent) {
        await sendOwnerSms(tradie, `DUPLICATE BOOKING FLAG ⚠️\nCaller: ${session.from}\nLooks like: ${session.duplicateEvent.summary} at ${session.duplicateEvent.whenText}\nNew request: ${whenText}\n${flowProgress(session)}`).catch(() => {});
      }

      const noteLine = session.customerNote ? `I see a note on your file. ` : "";
      const accessLine = session.accessNote ? `Access notes: ${session.accessNote}. ` : "";

      if (session.confirmPromptSent) {
        ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 7, speechTimeout: "auto" });
        return sendVoiceTwiml(res, twiml);
      }
      session.confirmPromptSent = true;

      const cfFirstName = session.name ? session.name.split(" ")[0] : "";
      const cfPrompt = `Booking for ${session.job} at ${session.address} ${whenText}. Confirm?`;
      session.lastPrompt = cfPrompt;

      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 7, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: pickSlot
    if (session.step === "pickSlot") {
      if (speech && wantsRepeatOptions(speech)) {
        const slots = (session.proposedSlots || []).map((ms) => DateTime.fromMillis(ms, { zone: tz }));
        session.lastPrompt = `No worries. Options are: ${slotsVoiceLine(slots, tz)} Say first, second, or third — or tell me another time.`;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
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
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }

      const slots = (session.proposedSlots || []).map((ms) => DateTime.fromMillis(ms, { zone: tz }));
      if (idx == null || !slots[idx]) {
        session.lastPrompt = "Say first, second, or third. Or press 1, 2, or 3. Or tell me another time.";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }

      const chosen = slots[idx];
      session.bookedStartMs = chosen.toMillis();
      session.step = "confirm";

      const whenText = formatForVoice(chosen);
      const noteLine = session.customerNote ? `I see a note on your file. ` : "";
      const accessLine = session.accessNote ? `Access notes: ${session.accessNote}. ` : "";

      if (session.confirmPromptSent) {
        ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 7, speechTimeout: "auto" });
        return sendVoiceTwiml(res, twiml);
      }
      session.confirmPromptSent = true;

      const cfFirstName = session.name ? session.name.split(" ")[0] : "";
      const cfPrompt = `Booking for ${session.job} at ${session.address} ${whenText}. Confirm?`;
      session.lastPrompt = cfPrompt;

      addToHistory(session, "assistant", session.lastPrompt);
      ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 7, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    // STEP: confirm
    if (session.step === "confirm") {
      const confirmConfidenceTooLow = typeof confidence === "number" && confidence > 0 && confidence < 0.45;
      const yn2 = detectYesNo(speech);

      if (!speech || confirmConfidenceTooLow || !yn2) {
        const interpreted = !speech ? "NO_SPEECH" : (confirmConfidenceTooLow ? "UNCLEAR" : "UNCLEAR");
        console.log(`STEP=confirm speech='${speech}' interpreted='${interpreted}' retryCount=${getRetryCountForStep(session, "confirm") + 1}`);
        const repeatedConfirm = await repeatLastStepPrompt(req, res, twiml, session, "confirm", interpreted, tradie);
        if (repeatedConfirm.shouldReset) resetSession(callSid);
        return sendVoiceTwiml(res, twiml);
      }

      resetRetryCountForStep(session, "confirm");
      // Detect correction attempt — "no no" "wrong" "actually"
      const isCorrecting = /\b(no no|wrong|actually|wait|hang on|sorry that|not right)\b/i.test(speech || "");
      if (isCorrecting) {
        console.log(`AMBIGUOUS_INPUT_RETRY callSid=${callSid} step=confirm reason=correction_phrase`);
        session.lastPrompt = "No worries — tell me exactly what you want changed, and I’ll update it.";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session });
        return sendVoiceTwiml(res, twiml);
      }

      if (yn2 === "NO") {
        console.log(`AMBIGUOUS_INPUT_RETRY callSid=${callSid} step=confirm reason=explicit_no`);
        session.lastPrompt = "No worries — what should I correct: job, time, address, or access notes?";
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 7, speechTimeout: "auto" });
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
      const bookingRef = generateBookingRef();
      session.bookingRef = bookingRef;
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

      function estimateJobValue(job, avgValue) {
        const j = String(job || "").toLowerCase();
        if (/burst pipe|gas leak|emergency|flooding|no power|sparking/.test(j))
          return Math.round(avgValue * 2.5);
        if (/rewire|switchboard|hot water system|bathroom reno|new deck|pergola/.test(j))
          return Math.round(avgValue * 2);
        if (/blocked drain|leaking tap|light fitting|power point|fence panel|painting/.test(j))
          return Math.round(avgValue * 0.8);
        return avgValue;
      }
      const estimatedValue = estimateJobValue(session.job, tradie.avgJobValue || 250);
      const urgencyLine = (session.urgencyScore || 0) >= 7
        ? `\n🚨 Urgency: ${session.urgencyScore}/10 — respond fast` : "";
      const valueLine = `\nEst. job value: $${estimatedValue}`;

      // First time caller detection using customer note absence
      const isFirstTimeCaller = !session.customerNote && !session.isReturningCaller;
      const newCallerLine = isFirstTimeCaller ? "\n🆕 NEW CALLER — first booking from this number" : "";

      await sendOwnerSms(tradie,
`NEW BOOKING ✅
Name: ${session.name}
Phone: ${session.from}
Address: ${session.address}
Job: ${session.job}
Time: ${formatForVoice(startDt)}
Confirm: customer will reply Y/N
${historyLine}${memoryLine}${accessLine2}${valueLine || ""}${urgencyLine || ""}${newCallerLine}`.trim()).catch(() => {});

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
      if (!eventResult.ok) {
        session.conversationState.calendarEventPending = true;
        session.step = "confirm";
        session.confirmPromptSent = false;
        session.lastPrompt = `I am still working on locking this into the calendar.${customerCalendarNotice} Just to confirm again — should I keep trying this booking?`;
        addToHistory(session, "assistant", session.lastPrompt);
        ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 7, speechTimeout: "auto" });
        return sendVoiceTwiml(res, twiml);
      }
      session.conversationState.calendarEventPending = false;
      ensureCallFlow(session);
      session.callFlow.calendarEventCreated = true;
      session.callFlow.hangupAllowed = canHangUp(session);
      console.log("FLOW STATUS:", session.callFlow);

      // Customer SMS: confirm Y/N
      if (session.from) {
        await sendCustomerSms(
          tradie,
          session.from,
          `Booked: ${formatForVoice(startDt)} at ${session.address} for ${session.job}. Ref: ${session.bookingRef || ""}. Reply Y to confirm or N to reschedule.`
        ).catch(() => {});
      }

      session.bookingConfirmed = true;
      ensureCallFlow(session);
      session.callFlow.bookingConfirmed = true;
      session.callFlow.hangupAllowed = canHangUp(session);
      console.log("FLOW STATUS:", session.callFlow);

      // Rating SMS 1 hour after booking end — fire and forget
      const ratingDelayMs = endDt
        ? Math.max((endDt.toMillis() + 60 * 60 * 1000) - Date.now(), 5 * 60 * 1000)
        : 60 * 60 * 1000;
      const ratingFrom = session.from || "";
      const ratingName = session.name || "";
      const ratingNumber = tradie.twilioNumber || tradie.smsFrom || "";
      const ratingTradieKey = tradie.key || "";
      if (ratingFrom && ratingNumber) {
        setTimeout(async () => {
          try {
            await sendSms({
              from: ratingNumber,
              to: ratingFrom,
              body: `Hi ${ratingName ? ratingName + " — " : ""}how did we go today? Reply with a number 1 to 5 to rate your experience. 5 = excellent. Your feedback helps us improve.`
            });
            console.log(`RATING_SMS_SENT to=${ratingFrom} tradie=${ratingTradieKey}`);
          } catch (rErr) {
            console.error("RATING_SMS_ERROR", rErr?.message || rErr);
          }
        }, ratingDelayMs);
      }

      // Pre-appointment checklist SMS 2 hours before start
      const checklistDelayMs = startDt
        ? Math.max((startDt.toMillis() - 2 * 60 * 60 * 1000) - Date.now(), 10 * 60 * 1000)
        : null;
      const checklistFrom = session.from || "";
      const checklistName = session.name || "";
      const checklistJob = session.job || "your job";
      const checklistAccess = session.accessNote || "";
      const checklistNumber = tradie.twilioNumber || tradie.smsFrom || "";
      if (checklistFrom && checklistNumber && checklistDelayMs) {
        setTimeout(async () => {
          try {
            const accessLine = checklistAccess && !/^none$/i.test(checklistAccess.trim())
              ? `\n• Access note: ${checklistAccess}` : "";
            await sendSms({
              from: checklistNumber,
              to: checklistFrom,
              body: `Hi ${checklistName ? checklistName + " — " : ""}your tradie is on the way soon for: ${checklistJob}.\n\nQuick checklist:\n• Ensure access is clear\n• Pets secured if needed${accessLine}\n• Someone home to let them in\n\nReply CANCEL to cancel.`
            });
            console.log(`CHECKLIST_SMS_SENT to=${checklistFrom} tradie=${tradie.key}`);
          } catch (chkErr) {
            console.error("CHECKLIST_SMS_ERROR", chkErr?.message || chkErr);
          }
        }, checklistDelayMs);
      }

      // Generate one line AI summary and SMS to owner
      if (llmReady()) {
        (async () => {
          try {
            const sd = await safeLLMCall({
              model: "gpt-4o",
              max_tokens: 60,
              temperature: 0.3,
              messages: [
                {
                  role: "system",
                  content: "Summarise this trades booking in exactly one sentence under 20 words. Include job, address and time only. Be concise."
                },
                {
                  role: "user",
                  content: `Job: ${session.job}. Address: ${session.address}. Name: ${session.name}. Time: ${session.time}. Urgency: ${session.urgencyScore || 1}/10.`
                }
              ]
            });
            const summaryLine = sd?.choices?.[0]?.message?.content?.trim() || "";
            if (summaryLine) {
              await sendOwnerSms(tradie, `📋 CALL SUMMARY: ${summaryLine}`).catch(() => {});
            }
          } catch {}
        })();
      }

      const summaryJob = session.job || "your job";
      const summaryName = session.name || "you";
      const summaryAddress = session.address || "your address";
      const summaryTime = session.time || "your requested time";
      const summaryText = `Booked: ${summaryJob} at ${summaryAddress} ${summaryTime}. Ref ${session.bookingRef || "confirmed"}.`;
      const afterHoursNote = req._isAfterHours
        ? " We are closed right now but the tradie will confirm your booking first thing tomorrow morning."
        : "";
      const isSameDay = startDt && startDt.hasSame(DateTime.now().setZone(tz), "day");
      const sameDayNote = isSameDay
        ? " I will make sure the tradie sees this straight away."
        : "";
      const finalSummary = summaryText + afterHoursNote + sameDayNote;
      twiml.say(finalSummary, { voice: "Polly.Amy", language: "en-AU" });
      setLockedFlowStep(session, "close", "calendar_success");
      session.step = "close";
      session.lastPrompt = "Before we finish, is there anything else you need? You can say yes that's all, thanks, goodbye, or all done.";
      ensureCallFlow(session);
      session.callFlow.hangupAllowed = canHangUp(session);
      console.log("FLOW STATUS:", session.callFlow);
      ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech", timeout: 7, speechTimeout: "auto" });
      return sendVoiceTwiml(res, twiml);
    }

    if (session.step === "close") {
      const finalAck = /\b(yes\s+that'?s\s+all|that'?s\s+all|thanks|thank\s+you|goodbye|all\s+done|no\s+that'?s\s+it|no\s+that'?s\s+all)\b/i.test(speech || "");
      if (finalAck) {
        ensureCallFlow(session);
        session.callFlow.userFinalAcknowledgement = true;
        session.callFlow.hangupAllowed = canHangUp(session);
        console.log("FLOW STATUS:", session.callFlow);
      }

      if (canHangUp(session)) {
        twiml.say("Perfect. Your booking is fully confirmed. Goodbye.", { voice: "Polly.Amy", language: "en-AU" });
        twiml.hangup();
        return sendVoiceTwiml(res, twiml);
      }

      console.log("HANGUP BLOCKED — booking flow incomplete");
      twiml.say("Before we finish, I just need to confirm a few final details.", { voice: "Polly.Amy", language: "en-AU" });
      twiml.gather({
        input: "speech",
        action: "/process",
        method: "POST",
        speechTimeout: "auto"
      }).say("Let’s continue.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, twiml);
    }

    // Fallback for missing/unknown step
    console.log(`FLOW_GUARD_BLOCK callSid=${callSid} reason=unknown_step step=${session.step}`);
    session.lastPrompt = buildObjectivePrompt(session.conversationState.currentStep, session.conversationState.collectedFields);
    addToHistory(session, "assistant", session.lastPrompt);
    ask(twiml, session.lastPrompt, actionUrl, { session, input: "speech" });
    return sendVoiceTwiml(res, twiml);
    } finally {
      session.processing = false;
    }
  } catch (err) {
    console.error("VOICE ERROR:", err);
    trackError("VOICE /process");
    try {
      const recoveryTwiml = new VoiceResponse();
      const recoveryActionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
      recoveryTwiml.say("Sorry about that — still here with you. Let me get your details.", { voice: "Polly.Amy", language: "en-AU" });
      const recoverGather = recoveryTwiml.gather({
        input: "speech",
        timeout: 8,
        speechTimeout: "auto",
        action: recoveryActionUrl,
        method: "POST",
        language: "en-AU"
      });
      recoverGather.say("What do you need help with today?", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, recoveryTwiml);
    } catch (fatalErr) {
      console.error("FATAL VOICE RECOVERY ERROR:", fatalErr);
      const lastResort = new VoiceResponse();
      lastResort.say("Sorry — please call back and we will get you sorted.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, lastResort);
    }
  }
});

// ----------------------------------------------------------------------------
// SMS ROUTE (customer replies Y/N + QUOTE photos)
// ----------------------------------------------------------------------------
app.post("/sms", async (req, res) => {
  try {
    if (!validateTwilioSignature(req)) {
      const deniedTwiml = new MessagingResponse();
      deniedTwiml.message("Sorry, we are experiencing a temporary issue. Let's continue.");
      return res.type("text/xml").send(deniedTwiml.toString());
    }

    const tradie = await getTradieConfig(req);
    if (tradie.status && tradie.status !== "ACTIVE") {
      const twiml = new MessagingResponse();
      twiml.message("Service unavailable.");
      return res.type("text/xml").send(twiml.toString());
    }

    const from = (req.body.From || "").trim();
    const body = (req.body.Body || "").trim();
    const bodyLower = body.toLowerCase();

    if (from && rateLimit(from)) {
      const twimlRate = new MessagingResponse();
      twimlRate.message("Thanks for your patience — we're receiving a lot of messages right now. We'll keep helping you here.");
      return res.type("text/xml").send(twimlRate.toString());
    }

    if (supabase) {
      await safeSupabaseWrite(supabase.from("messages").insert([{
        from_number: from || null,
        to_number: String(req.body.To || "").trim() || null,
        message: body || null,
        message_sid: String(req.body.MessageSid || "").trim() || null,
        created_at: new Date().toISOString()
      }]), "messages.insert");
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

        // Star rating reply — customer replies 1 to 5
        const ratingMatch = (body || "").trim().match(/^([1-5])$/);
        if (ratingMatch) {
          const rating = Number(ratingMatch[1]);
          const ratingEmoji = rating >= 4 ? "🌟" : rating === 3 ? "👍" : "📝";
          await sendOwnerSms(tradie,
            `${ratingEmoji} RATING RECEIVED\nFrom: ${from}\nRating: ${rating}/5${rating <= 2 ? "\n⚠️ Low rating — consider following up." : ""}`
          ).catch(() => {});
          const replyMsg = rating >= 4
            ? "Thanks so much for the great rating! We really appreciate it. 😊"
            : rating === 3
            ? "Thanks for your feedback — we will use it to keep improving."
            : "Thanks for letting us know. We are sorry it did not meet expectations — someone will be in touch.";
          await sendSms({ from: tradie.smsFrom, to: from, body: replyMsg }).catch(() => {});
          console.log(`RATING_RECEIVED tradie=${tradie.key} from=${from} rating=${rating}`);
          return res.sendStatus(200);
        }

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
    // BLOCK command — owner SMSes "BLOCK +61412345678"
    if (from === tradie.ownerSmsTo && /^BLOCK\s+\+?\d+/i.test(body || "")) {
      const numberToBlock = (body.match(/\+?\d[\d\s\-]{7,}/)?.[0] || "").replace(/\s/g, "").trim();
      if (numberToBlock) {
        const blKey = `${tradie.key}::${numberToBlock}`;
        callerBlacklist.set(blKey, true);
        await sendSms({
          from: tradie.smsFrom,
          to: from,
          body: `✅ Number ${numberToBlock} has been blocked. They can no longer book.`
        }).catch(() => {});
        console.log(`BLACKLIST_ADDED tradie=${tradie.key} number=${numberToBlock}`);
        return res.sendStatus(200);
      }
    }

    // UNBLOCK command — owner SMSes "UNBLOCK +61412345678"
    if (from === tradie.ownerSmsTo && /^UNBLOCK\s+\+?\d+/i.test(body || "")) {
      const numberToUnblock = (body.match(/\+?\d[\d\s\-]{7,}/)?.[0] || "").replace(/\s/g, "").trim();
      if (numberToUnblock) {
        const ubKey = `${tradie.key}::${numberToUnblock}`;
        callerBlacklist.delete(ubKey);
        await sendSms({
          from: tradie.smsFrom,
          to: from,
          body: `✅ Number ${numberToUnblock} has been unblocked.`
        }).catch(() => {});
        console.log(`BLACKLIST_REMOVED tradie=${tradie.key} number=${numberToUnblock}`);
        return res.sendStatus(200);
      }
    }

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
    if (!validateTwilioSignature(req)) {
      const deniedTwiml = new VoiceResponse();
      deniedTwiml.say("Sorry, we are experiencing a temporary issue. Let’s continue.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, deniedTwiml);
    }
    const tradie = await getTradieConfig(req);
    req._tradieConfig = tradie;
    return handleVoiceEntry(req, res);
  } catch (e) {
    console.error("/twilio/voice error", e);
    trackError("VOICE /twilio/voice");
    const twiml = new VoiceResponse();
    const gather = twiml.gather({
      input: "speech",
      timeout: 8,
      speechTimeout: "auto",
      action: "/process",
      method: "POST",
      language: "en-AU"
    });
    gather.say("Hi, sorry about that — still here. What do you need help with today?", { voice: "Polly.Amy", language: "en-AU" });
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
    if (!validateTwilioSignature(req)) {
      const deniedTwiml = new VoiceResponse();
      deniedTwiml.say("Sorry, we are experiencing a temporary issue. Let’s continue.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, deniedTwiml);
    }
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
    if (!validateTwilioSignature(req)) {
      const deniedTwiml = new VoiceResponse();
      deniedTwiml.say("Sorry, we are experiencing a temporary issue. Let’s continue.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, deniedTwiml);
    }
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
    if (!validateTwilioSignature(req)) {
      const deniedTwiml = new VoiceResponse();
      deniedTwiml.say("Sorry, we are experiencing a temporary issue. Let’s continue.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, deniedTwiml);
    }
    const twiml = new VoiceResponse();
    const actionUrl = voiceActionUrl(req);
    twiml.redirect({ method: "POST" }, actionUrl);
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
    if (!validateTwilioSignature(req)) {
      const deniedTwiml = new VoiceResponse();
      deniedTwiml.say("Sorry, we are experiencing a temporary issue. Let’s continue.", { voice: "Polly.Amy", language: "en-AU" });
      return sendVoiceTwiml(res, deniedTwiml);
    }

    const tradie = await getTradieConfig(req);
    const tz = tradie.timezone;
    const callSid = resolveCallSid(req);
    const fromNumber = (req.body.From || "").trim();
    const session = getSession(callSid, fromNumber);
    if (!(tradie.googleRefreshToken && tradie.id)) {
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
      twiml.redirect({ method: "POST" }, "/confirm" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return sendVoiceTwiml(res, twiml);
    }

    if (session.calendarCheck.attempts === 1) {
      twiml.say("I’m having trouble checking the calendar right now. Let me try that again.", { voice: "Polly.Amy", language: "en-AU" });
      twiml.redirect({ method: "POST" }, "/check-availability" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : ""));
      return sendVoiceTwiml(res, twiml);
    }

    session.calendarCheck = null;
    twiml.say("I had trouble checking the calendar just then — no worries though, I have saved your details and we will confirm by SMS shortly.", { voice: "Polly.Amy", language: "en-AU" });
    const fallbackActionUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
    session.step = "confirm";
    twiml.redirect({ method: "POST" }, fallbackActionUrl);
    return sendVoiceTwiml(res, twiml);
  } catch (e) {
    console.error("/check-availability error", e);
    trackError("VOICE /check-availability");
    const recoveryUrl = "/process" + (req.query.tid ? `?tid=${encodeURIComponent(req.query.tid)}` : "");
    twiml.say("I had a little trouble checking the calendar just then — no worries, I have saved your details and we will confirm the time by SMS shortly.", { voice: "Polly.Amy", language: "en-AU" });
    twiml.redirect({ method: "POST" }, recoveryUrl);
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

// 
// Error handler
// 
app.use((err, req, res, next) => {
  console.error("UNHANDLED_ROUTE_ERROR", err);
  if (res.headersSent) return next(err);
  return res.status(500).json({ error: "Internal Server Error" });
});

app.use((req, res) => {
  console.log("404", req.method, req.originalUrl);
  res.status(404).send("Not Found");
});

// 
// Listen
// 
const PORT = Number(process.env.PORT || 10000);
if (!PORT || Number.isNaN(PORT)) throw new Error("PORT missing/invalid");

// Weekly revenue forecast — runs every hour, fires only Monday ~8am AEST (22:00 UTC Sunday)
setInterval(async () => {
  try {
    const now = new Date();
    if (now.getUTCDay() !== 1 || now.getUTCHours() !== 22) return;

    if (!supabase) return;
    const { data: allTradies } = await supabase
      .from(SUPABASE_TRADIES_TABLE)
      .select("*")
      .eq("status", "ACTIVE");

    if (!allTradies?.length) return;

    for (const row of allTradies) {
      try {
        const t = normalizeTradieConfig(row);
        if (!t.ownerSmsTo) continue;

        const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)
          .toISOString().split("T")[0];

        const { data: metrics } = await supabase
          .from(SUPABASE_METRICS_TABLE)
          .select("bookings_created,est_revenue,calls_total")
          .eq("tradie_key", t.key)
          .gte("day", weekAgo);

        if (!metrics?.length) continue;

        const totalBookings = metrics.reduce((s, m) => s + Number(m.bookings_created || 0), 0);
        const totalRevenue = metrics.reduce((s, m) => s + Number(m.est_revenue || 0), 0);
        const totalCalls = metrics.reduce((s, m) => s + Number(m.calls_total || 0), 0);
        const projectedMonthly = Math.round(totalRevenue * 4.3);

        await sendSms({
          from: t.smsFrom || process.env.TWILIO_SMS_FROM || "",
          to: t.ownerSmsTo,
          body: `📊 WEEKLY SUMMARY\nCalls: ${totalCalls}\nBookings: ${totalBookings}\nEst. revenue: $${Math.round(totalRevenue)}\nProjected monthly: $${projectedMonthly}\n\nHave a great week! 💪`
        });
        console.log(`WEEKLY_FORECAST_SENT tradie=${t.key}`);
      } catch (tErr) {
        console.error("WEEKLY_FORECAST_TRADIE_ERROR", tErr?.message || tErr);
      }
    }
  } catch (schedErr) {
    console.error("WEEKLY_FORECAST_ERROR", schedErr?.message || schedErr);
  }
}, 60 * 60 * 1000);

// Render keep-alive ping every 14 minutes to prevent cold starts mid-call
const SELF_URL = (process.env.BASE_URL || "http://localhost:" + (process.env.PORT || 10000)).replace(/\/+$/, "");
setInterval(async () => {
  try {
    const http = require("https");
    const url = new URL(SELF_URL + "/health");
    const req = (url.protocol === "https:" ? require("https") : require("http")).request(url, { method: "GET" }, (res) => {
      res.resume();
    });
    req.on("error", () => {});
    req.end();
  } catch {}
}, 14 * 60 * 1000);

// Daily health check — runs every hour, fires once at 8am AEST (22:00 UTC)
setInterval(async () => {
  try {
    const now = new Date();
    if (now.getUTCHours() !== 22) return;

    if (!supabase) return;
    const { data: tradieRows } = await supabase
      .from(SUPABASE_TRADIES_TABLE)
      .select("*")
      .eq("status", "ACTIVE");

    if (!tradieRows?.length) return;

    for (const row of tradieRows) {
      try {
        const t = normalizeTradieConfig(row);
        if (!t.ownerSmsTo) continue;
        if (!t.googleRefreshToken) {
          await sendSms({
            from: t.smsFrom || process.env.TWILIO_SMS_FROM || "",
            to: t.ownerSmsTo,
            body: `⚠️ HEALTH CHECK: Google Calendar not connected for your AI booking bot. Callers cannot book until you reconnect. Visit: ${process.env.BASE_URL || ""}/api/google/connect?tradieId=${row.id}`
          }).catch(() => {});
          console.log(`HEALTH_CHECK_NO_CALENDAR tradie=${t.key}`);
        }
      } catch (hErr) {
        console.error("HEALTH_CHECK_TRADIE_ERROR", hErr?.message || hErr);
      }
    }
  } catch (schedErr) {
    console.error("HEALTH_CHECK_ERROR", schedErr?.message || schedErr);
  }
}, 60 * 60 * 1000);

if (require.main === module) {
  app.listen(PORT, () => console.log("Server listening on", PORT));
}

module.exports = app;
