require('dotenv').config();

const OpenAI = require('openai');
const { createClient } = require('@supabase/supabase-js');

console.log('Claw booting...');

const CLAW_INTERVAL_MS = 60000; // 1 minute

// ---------------- ENV SETUP ----------------

const openai = process.env.OPENAI_API_KEY
? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
: null;

const supabaseServiceKey =
process.env.SUPABASE_SERVICE_ROLE_KEY ||
process.env.SUPABASE_SERVICE_KEY;

const supabase =
process.env.SUPABASE_URL && supabaseServiceKey
? createClient(process.env.SUPABASE_URL, supabaseServiceKey)
: null;

// ---------------- SAFETY HANDLERS ----------------

process.on('uncaughtException', (err) => {
console.error('Claw uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
console.error('Claw unhandled rejection:', err);
});

// ---------------- CORE SCAN ----------------

async function scanMessages() {
try {
if (!supabase) {
console.warn('Supabase not configured.');
return;
}

if (!openai) {
console.warn('OpenAI not configured.');
return;
}

console.log('Claw scanning database...');

const { data, error } = await supabase
.from('messages')
.select('*')
.order('created_at', { ascending: false })
.limit(1);

if (error) {
console.error('Claw query error:', error);
return;
}

if (!data || data.length === 0) {
console.log('No messages found.');
return;
}

const latest = data[0];

console.log('Processing message:', latest.message);

const completion = await openai.chat.completions.create({
model: 'gpt-4o-mini',
messages: [
{ role: 'system', content: 'You are an AI assistant.' },
{ role: 'user', content: latest.message || 'No message content.' }
],
});

const reply = completion.choices[0].message.content;

console.log('AI suggestion:', reply);

// OPTIONAL: insert into responses table (only if it exists)
/*
await supabase.from('responses').insert({
message_id: latest.id,
response: reply,
});
*/

} catch (err) {
console.error('Claw cycle error:', err);
}
}

// ---------------- MAIN LOOP ----------------

async function mainLoop() {
try {
await scanMessages();
console.log('Claw heartbeat:', new Date().toISOString());
} catch (err) {
console.error('Claw loop survived error:', err);
}
}

console.log('Claw started.');
setInterval(mainLoop, CLAW_INTERVAL_MS);
