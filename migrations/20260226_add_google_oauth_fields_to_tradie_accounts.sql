alter table if exists public.tradie_accounts
add column if not exists google_connected boolean default false,
add column if not exists google_refresh_token text,
add column if not exists google_access_token text,
add column if not exists google_token_expiry timestamptz;

-- Optional: index for faster lookups
create index if not exists idx_tradie_accounts_google_connected
on public.tradie_accounts (google_connected);

-- Optional: safety check constraint (connected implies refresh token exists)
-- (Leave commented if you want to allow connected without refresh token)
-- alter table public.tradie_accounts
-- add constraint google_connected_requires_refresh
-- check (google_connected = false or google_refresh_token is not null);
