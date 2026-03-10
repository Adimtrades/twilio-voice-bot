create extension if not exists "pgcrypto";

create table if not exists public.onboarding_leads (
  id uuid primary key default gen_random_uuid(),
  email text not null,
  phone text,
  onboarding_token_hash text,
  token_expires_at timestamptz,
  created_at timestamptz not null default now(),
  completed boolean not null default false,
  onboarding_email_sent_at timestamptz,
  completed_at timestamptz,
  tradie_id uuid references public.tradies(id)
);

alter table public.onboarding_leads
  add column if not exists email text,
  add column if not exists phone text,
  add column if not exists onboarding_token_hash text,
  add column if not exists token_expires_at timestamptz,
  add column if not exists created_at timestamptz default now(),
  add column if not exists completed boolean default false,
  add column if not exists onboarding_email_sent_at timestamptz,
  add column if not exists completed_at timestamptz,
  add column if not exists tradie_id uuid references public.tradies(id);

create index if not exists onboarding_leads_email_idx on public.onboarding_leads (lower(email));
create index if not exists onboarding_leads_token_hash_idx on public.onboarding_leads (onboarding_token_hash);
create index if not exists onboarding_leads_tradie_id_idx on public.onboarding_leads (tradie_id);
