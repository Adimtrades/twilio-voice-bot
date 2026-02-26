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

async function clawCycle() {
  console.log('Claw heartbeat:', new Date().toISOString());

  if (!supabase) {
    console.warn('Claw skipped: missing Supabase env vars (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_SERVICE_KEY)).');
    return;
  }

  if (!openai) {
    console.warn('Claw skipped: missing OPENAI_API_KEY.');
    return;
  }

  console.log('Claw scanning database...');
  const { data: messages, error } = await supabase
    .from('messages')
    .select('*')
    .order('timestamp', { ascending: false })
    .limit(1);

  if (error) throw error;
  if (!messages || messages.length === 0) return;

  const latest = messages[0];
  const completion = await openai.chat.completions.create({
    model: 'gpt-4o-mini',
    messages: [
      { role: 'system', content: 'You are Claw. Analyze and decide action.' },
      { role: 'user', content: latest.message || '' }
    ]
  });

  console.log('Claw decision:', completion.choices?.[0]?.message?.content || '(no response)');
}

async function runClawForever() {
  console.log(`Claw loop started. Interval: ${CLAW_INTERVAL_MS}ms`);

  while (true) {
    try {
      await clawCycle();
    } catch (err) {
      console.error('Claw cycle error:', err);
    }

    await new Promise((resolve) => setTimeout(resolve, CLAW_INTERVAL_MS));
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

runClawForever();
