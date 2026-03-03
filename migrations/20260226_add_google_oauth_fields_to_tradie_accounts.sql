alter table if exists public.tradies
add column if not exists google_refresh_token text,
add column if not exists google_access_token text,
add column if not exists google_expiry_date timestamptz;

create index if not exists idx_tradies_google_refresh_token
on public.tradies (google_refresh_token);
