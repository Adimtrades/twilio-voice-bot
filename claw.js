process.on('uncaughtException', (err) => {
console.error('Claw uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
console.error('Claw unhandled rejection:', err);
});

require('dotenv').config();
const OpenAI = require("openai");
const { createClient } = require('@supabase/supabase-js');
const config = require('./claw.config');

const openai = new OpenAI({
apiKey: process.env.OPENAI_API_KEY
});

const supabase = createClient(
process.env.SUPABASE_URL,
process.env.SUPABASE_SERVICE_ROLE_KEY
);

async function runClaw() {
console.log("Claw scanning database...");

const { data: messages } = await supabase
.from('messages')
.select('*')
.order('timestamp', { ascending: false })
.limit(1);

if (!messages || messages.length === 0) return;

const latest = messages[0];

const completion = await openai.chat.completions.create({
model: "gpt-4o-mini",
messages: [
{ role: "system", content: "You are Claw. Analyze and decide action." },
{ role: "user", content: latest.message }
]
});

console.log("Claw decision:", completion.choices[0].message.content);
}

setInterval(runClaw, 30000); // every 30 seconds

{
async function clawCycle() {
// TODO: place continuous testing/improvement logic here.
// For now, just log a heartbeat so we can confirm it runs 24/7.
console.log("Claw heartbeat:", new Date().toISOString());

if (process.env.RUN_TESTS === "true") {
console.log("Running test hook...");
// Future test runner logic here
}
}

async function runClaw() {
console.log("Claw cycle started at", new Date().toISOString());

try {
await clawCycle();
} catch (err) {
console.error("Claw cycle error:", err);
}

setTimeout(runClaw, config.intervalMs);
}

runClaw();
}
