-- Scope active-player context by tenant and product. This migration is
-- intentionally forward-only because 003_user_context_lookup.sql may already
-- be applied in a persistent local PostgreSQL volume.

BEGIN;

ALTER TABLE public.poker_user_context
    ADD COLUMN IF NOT EXISTS tenant_id TEXT,
    ADD COLUMN IF NOT EXISTS product_id TEXT;

-- Existing POC rows were generated with the historical defaults used by the
-- hand-history simulator.
UPDATE public.poker_user_context
SET tenant_id = 'demo'
WHERE tenant_id IS NULL;

UPDATE public.poker_user_context
SET product_id = 'poker'
WHERE product_id IS NULL;

ALTER TABLE public.poker_user_context
    ALTER COLUMN tenant_id SET NOT NULL,
    ALTER COLUMN product_id SET NOT NULL;

ALTER TABLE public.poker_user_context
    DROP CONSTRAINT IF EXISTS poker_user_context_pkey,
    DROP CONSTRAINT IF EXISTS poker_user_context_user_id_effective_at_key,
    DROP CONSTRAINT IF EXISTS poker_user_context_scope_effective_uk;

ALTER TABLE public.poker_user_context
    ADD CONSTRAINT poker_user_context_pkey
        PRIMARY KEY (tenant_id, product_id, user_id, context_version),
    ADD CONSTRAINT poker_user_context_scope_effective_uk
        UNIQUE (tenant_id, product_id, user_id, effective_at);

DROP INDEX IF EXISTS public.poker_user_context_effective_lookup;

CREATE INDEX poker_user_context_effective_lookup
ON public.poker_user_context (
    tenant_id,
    product_id,
    user_id,
    effective_at DESC,
    context_version DESC
);

COMMENT ON COLUMN public.poker_user_context.tenant_id IS
    'Tenant boundary copied from the authoritative hand/account scope';
COMMENT ON COLUMN public.poker_user_context.product_id IS
    'Product boundary copied from the authoritative hand/account scope';

COMMIT;
