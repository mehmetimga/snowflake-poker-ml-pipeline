package com.aicampions.poker.context.app;

import com.aicampions.poker.context.ContextEnrichmentJob;

/** Canonical hands-only entrypoint with lazy internal Snowflake context resolution. */
public final class ActiveContextEnrichmentJob {
    private ActiveContextEnrichmentJob() {}

    public static void main(String[] arguments) throws Exception {
        ContextEnrichmentJob.runActive(arguments);
    }
}
