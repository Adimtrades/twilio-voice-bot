# twilio-voice-bot

## Google service account configuration

`server.js` loads Google service account credentials using the following precedence:

1. `GOOGLE_SERVICE_JSON`
   - If it starts with `{`, it is parsed as inline JSON.
   - Otherwise, it is treated as a file path to a JSON file.
2. `GOOGLE_SERVICE_JSON_FILE`
   - Treated as a file path relative to the project root.
3. `GOOGLE_APPLICATION_CREDENTIALS`
   - Treated as a file path.

If none of the above are set, the app throws: `Missing GOOGLE_SERVICE_JSON env/config`.
