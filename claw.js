require('dotenv').config();

const OpenAI = require('openai');
const { createClient } = require('@supabase/supabase-js');
const config = require('./claw.config');

const CLAW_INTERVAL_MS = Number(config.intervalMs) || 60000;

process.on('uncaughtException', (err) => {
  console.error('Claw uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('Claw unhandled rejection:', err);
});

const openai = process.env.OPENAI_API_KEY
  ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
  : null;

const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
const supabase = (process.env.SUPABASE_URL && supabaseServiceKey)
  ? createClient(process.env.SUPABASE_URL, supabaseServiceKey)
  : null;

async function scanMessages() {
try {
console.log("Claw scanning database...");

if (!supabase) {
console.warn("Supabase not configured.");
return;
}

if (!openai) {
console.warn("OpenAI not configured.");
return;
}

const { data: messages, error } = await supabase
.from("messages")
.select("*")
.eq("processed", false)
.order("created_at", { ascending: false })
.limit(1);

if (error) {
console.error("Supabase fetch error:", error);
return;
}

if (!messages || messages.length === 0) {
console.log("No new messages.");
return;
}

const latest = messages[0];

console.log("Processing message:", latest.id);

const completion = await openai.chat.completions.create({
model: "gpt-4o-mini",
messages: [
{ role: "system", content: "You are a professional AI receptionist." },
{ role: "user", content: latest.message }
]
});

const reply = completion.choices[0].message.content;

// Insert response
const { error: insertError } = await supabase
.from("responses")
.insert({
message_id: latest.id,
response: reply
});

if (insertError) {
console.error("Failed to insert response:", insertError);
return;
}

// Mark message as processed
const { error: updateError } = await supabase
.from("messages")
.update({ processed: true })
.eq("id", latest.id);

if (updateError) {
console.error("Failed to update message:", updateError);
return;
}

console.log("Message processed successfully.");

} catch (err) {
console.error("Claw fatal cycle error:", err);
}
}
async function improvementCycle() {
console.log("Claw improvement cycle running...");

// 1. Pull latest logs from Supabase
// 2. Analyze errors
// 3. Suggest patch
// 4. Commit to claw-dev branch only

setTimeout(improvementCycle, 300000); // every 5 mins
}

improvementCycle();

setInterval(async () => {
  try {
    await scanMessages();
    console.log('Claw heartbeat:', new Date().toISOString());
  } catch (err) {
    console.error('Claw loop survived error:', err);
  }
}, 60000);
