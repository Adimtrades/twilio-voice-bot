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
    console.log('Claw scanning database...');

    // Try ordering by timestamp first
    let { data, error } = await supabase
      .from('messages')
      .select('*')
      .order('timestamp', { ascending: false })
      .limit(20);

    // If timestamp column fails, fallback to created_at
    if (error && error.message.includes('timestamp')) {
      console.warn('Timestamp column missing. Falling back to created_at.');

      const fallback = await supabase
        .from('messages')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(20);

      data = fallback.data;
      error = fallback.error;
    }

    if (error) {
      console.error('Claw query error:', error);
      return;
    }

    console.log(`Claw fetched ${data.length} messages.`);

    // Continue processing safely
    return data;
  } catch (err) {
    console.error('Claw fatal cycle error:', err);
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
