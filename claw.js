require('dotenv').config();
const OpenAI = require("openai");
const { createClient } = require('@supabase/supabase-js');

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
