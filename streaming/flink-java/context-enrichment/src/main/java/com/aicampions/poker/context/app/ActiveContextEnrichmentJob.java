package com.aicampions.poker.context.app;

import com.aicampions.poker.context.ContextEnrichmentJob;

/** Canonical hands-only entrypoint with lazy PostgreSQL context resolution. */
public final class ActiveContextEnrichmentJob {
    private ActiveContextEnrichmentJob() {}

    public static void main(String[] arguments) throws Exception {
        ContextEnrichmentJob.runActive(arguments);
    }
}
