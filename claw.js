require('dotenv').config();

const OpenAI = require('openai');
const { createClient } = require('@supabase/supabase-js');

console.log('Claw booting...');

const CLAW_INTERVAL_MS = 60000;

// ---------- ENV SETUP ----------

const openai = process.env.OPENAI_API_KEY
? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
: null;

const supabaseServiceKey =
process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

const supabase =
process.env.SUPABASE_URL && supabaseServiceKey
? createClient(process.env.SUPABASE_URL, supabaseServiceKey)
: null;

// ---------- SAFETY HANDLERS ----------

process.on('uncaughtException', (err) => {
console.error('Claw uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
console.error('Claw unhandled rejection:', err);
});

// ---------- CORE LOOP ----------

async function scanMessages() {
try {
console.log('Claw scanning database...');

if (!supabase) {
console.warn('Supabase not configured.');
return;
}

// Always use created_at (timestamp column is unreliable)
const { data, error } = await supabase
.from('messages')
.select('*')
.order('created_at', { ascending: false })
.limit(20);

if (error) {
console.error('Claw query error:', error);
return;
}

console.log(`Claw fetched ${data.length} messages.`);

if (!openai) {
console.warn('OpenAI not configured. Skipping AI processing.');
return;
}

if (!data || data.length === 0) return;

const latest = data[0];

console.log('Processing message:', latest.message);

const completion = await openai.chat.completions.create({
model: 'gpt-4o-mini',
messages: [
{ role: 'system', content: 'You are improving the Twilio voice bot.' },
{ role: 'user', content: latest.message || 'No message content.' }
]
});

console.log('AI suggestion:', completion.choices[0].message.content);

} catch (err) {
console.error('Claw cycle error:', err);
}
}

// ---------- IMPROVEMENT CYCLE ----------

async function improvementCycle() {
try {
console.log('Claw improvement cycle running...');
await scanMessages();
} catch (err) {
console.error('Claw improvement loop error:', err);
}
}

// ---------- START ----------

console.log('Claw started.');

setInterval(async () => {
try {
await improvementCycle();
console.log('Claw heartbeat:', new Date().toISOString());
} catch (err) {
console.error('Claw loop survived error:', err);
}
}, CLAW_INTERVAL_MS);
