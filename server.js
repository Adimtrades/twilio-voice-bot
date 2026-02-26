require('dotenv').config();

const express = require('express');
const twilio = require('twilio');
const { createClient } = require('@supabase/supabase-js');
const OpenAI = require('openai');

const app = express();
const port = Number(process.env.PORT) || 10000;

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Missing Supabase environment variables: SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY');
}

if (!process.env.OPENAI_API_KEY) {
  console.error('Missing OpenAI environment variable: OPENAI_API_KEY');
}

const supabase = createClient(
  process.env.SUPABASE_URL || '',
  process.env.SUPABASE_SERVICE_ROLE_KEY || ''
);

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

app.post('/voice', async (req, res) => {
  try {
    const from = req.body.From || 'unknown';
    console.log('Incoming voice call from:', from);

    const { error } = await supabase.from('calls').insert({
      phone: from,
      timestamp: new Date().toISOString(),
    });

    if (error) {
      console.error('Supabase insert error (calls):', error.message);
    } else {
      console.log('Call logged to Supabase calls table');
    }

    const twiml = new twilio.twiml.VoiceResponse();
    twiml.say('Hello, thank you for calling. Please tell me how I can help you after the beep.');

    twiml.gather({
      input: 'speech',
      action: '/process',
      method: 'POST',
      speechTimeout: 'auto',
    });

    twiml.say("Sorry, I didn't catch that. Please call again.");

    res.type('text/xml');
    res.send(twiml.toString());
  } catch (err) {
    console.error('Error in /voice route:', err);

    const twiml = new twilio.twiml.VoiceResponse();
    twiml.say('Sorry, something went wrong. Please try again later.');

    res.type('text/xml');
    res.status(500).send(twiml.toString());
  }
});

app.post('/process', async (req, res) => {
  try {
    const phone = req.body.From || 'unknown';
    const speechResult = (req.body.SpeechResult || '').trim();

    console.log('Processing speech input:', { phone, speechResult });

    const { error: bookingError } = await supabase.from('bookings').insert({
      phone,
      details: speechResult,
      timestamp: new Date().toISOString(),
    });

    if (bookingError) {
      console.error('Supabase insert error (bookings):', bookingError.message);
    } else {
      console.log('Booking details saved to Supabase bookings table');
    }

    let aiResponseText = 'Thanks for your request. We will get back to you shortly.';

    if (speechResult) {
      const completion = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [
          {
            role: 'system',
            content:
              'You are a polite and concise AI receptionist. Summarize and acknowledge caller requests clearly.',
          },
          {
            role: 'user',
            content: `Caller said: ${speechResult}`,
          },
        ],
      });

      aiResponseText =
        completion.choices?.[0]?.message?.content?.trim() || aiResponseText;
    }

    console.log('AI voice response generated');

    const twiml = new twilio.twiml.VoiceResponse();
    twiml.say(aiResponseText);

    res.type('text/xml');
    res.send(twiml.toString());
  } catch (err) {
    console.error('Error in /process route:', err);

    const twiml = new twilio.twiml.VoiceResponse();
    twiml.say('Sorry, there was an error processing your request.');

    res.type('text/xml');
    res.status(500).send(twiml.toString());
  }
});

app.post('/sms', async (req, res) => {
  try {
    const phone = req.body.From || 'unknown';
    const smsBody = (req.body.Body || '').trim();

    console.log('Incoming SMS:', { phone, smsBody });

    const { error: messageError } = await supabase.from('messages').insert({
      phone,
      message: smsBody,
      timestamp: new Date().toISOString(),
    });

    if (messageError) {
      console.error('Supabase insert error (messages):', messageError.message);
    } else {
      console.log('SMS saved to Supabase messages table');
    }

    let aiReply = 'Thank you for your message. We will reply soon.';

    if (smsBody) {
      const completion = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [
          {
            role: 'system',
            content: 'You are a friendly AI receptionist replying to customer text messages.',
          },
          {
            role: 'user',
            content: smsBody,
          },
        ],
      });

      aiReply = completion.choices?.[0]?.message?.content?.trim() || aiReply;
    }

    console.log('AI SMS reply generated');

    const twiml = new twilio.twiml.MessagingResponse();
    twiml.message(aiReply);

    res.type('text/xml');
    res.send(twiml.toString());
  } catch (err) {
    console.error('Error in /sms route:', err);

    const twiml = new twilio.twiml.MessagingResponse();
    twiml.message('Sorry, there was an error handling your message. Please try again later.');

    res.type('text/xml');
    res.status(500).send(twiml.toString());
  }
});

app.listen(port, () => {
  console.log(`Twilio AI receptionist server running on port ${port}`);
});
