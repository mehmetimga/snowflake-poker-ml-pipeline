-- Idempotent forward migration for local volumes created before fault suites.
-- Fresh databases already receive this column and constraint from 001.

ALTER TABLE public.hand_history
    ADD COLUMN IF NOT EXISTS simulation_scenario TEXT;

UPDATE public.hand_history
SET simulation_scenario = 'acceptance'
WHERE simulation_scenario IS NULL;

ALTER TABLE public.hand_history
    ALTER COLUMN simulation_scenario SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.hand_history'::regclass
          AND conname = 'hand_history_simulation_scenario_check'
    ) THEN
        ALTER TABLE public.hand_history
            ADD CONSTRAINT hand_history_simulation_scenario_check
            CHECK (simulation_scenario ~ '^[a-z0-9_]+$');
    END IF;
END;
$$;
