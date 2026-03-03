-- Keep tradie_accounts as a lightweight phone_number -> tradie_id mapping table.
alter table if exists public.tradie_accounts
  drop column if exists google_connected,
  drop column if exists google_refresh_token,
  drop column if exists google_access_token,
  drop column if exists google_token_expiry,
  drop column if exists google_expiry_date,
  drop column if exists calendar_id,
  drop column if exists calendar_email,
  drop column if exists tradie_key,
  drop column if exists timezone,
  drop column if exists twilio_number;

drop index if exists idx_tradie_accounts_google_connected;
