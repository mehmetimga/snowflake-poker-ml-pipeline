package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/risk"
)

func main() {
	modelDir := flag.String("model-dir", "../../models/pair-catboost-full-v2", "model artifact directory")
	ruleRolloutPath := flag.String("rule-rollout", "../../schemas/rules/rule-rollout-v1.json", "governed Rules v2 enablement and rollback JSON")
	tritonURL := flag.String("triton-url", "http://127.0.0.1:8000", "Triton V2 HTTP base URL")
	listenAddress := flag.String("listen", ":8080", "HTTP listen address")
	assemblyTTL := flag.Duration("assembly-ttl", 30*time.Minute, "complete-hand correction cache TTL")
	requestTimeout := flag.Duration("request-timeout", 10*time.Second, "Triton request timeout")
	allowedTenants := flag.String("allowed-tenants", "", "comma-separated tenant allowlist; empty allows all for development")
	pprofAddress := flag.String("pprof-listen", "", "optional loopback-only pprof listen address, for example 127.0.0.1:6060")
	buildVersion := flag.String("build-version", "dev", "immutable service image or source build version")
	flag.Parse()

	bundle, err := risk.LoadArtifactBundle(*modelDir)
	if err != nil {
		log.Fatalf("load model artifacts: %v", err)
	}
	client := &http.Client{Timeout: *requestTimeout}
	backend, err := risk.NewTritonBackend(*tritonURL, bundle.Contract.Batching.TritonModel, client)
	if err != nil {
		log.Fatalf("configure Triton backend: %v", err)
	}
	scorer, err := risk.NewScorerWithBuildVersion(bundle, backend, nil, *buildVersion)
	if err != nil {
		log.Fatalf("configure scorer: %v", err)
	}
	ruleRollout, err := risk.LoadRuleRollout(*ruleRolloutPath)
	if err != nil {
		log.Fatalf("load rule rollout: %v", err)
	}
	if err := scorer.SetRuleRollout(ruleRollout); err != nil {
		log.Fatalf("configure rule rollout: %v", err)
	}
	assembler, err := risk.NewHandAssembler(bundle.Contract.Batching.ExpectedPairsPerSixPlayerHand, *assemblyTTL)
	if err != nil {
		log.Fatalf("configure hand assembler: %v", err)
	}
	service, err := risk.NewHTTPService(scorer, assembler, 2*time.Second)
	if err != nil {
		log.Fatalf("configure HTTP service: %v", err)
	}
	if *allowedTenants != "" {
		if err := service.SetAllowedTenants(strings.Split(*allowedTenants, ",")); err != nil {
			log.Fatalf("configure tenant allowlist: %v", err)
		}
	}

	server := &http.Server{
		Addr:              *listenAddress,
		Handler:           service.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var profilingServer *http.Server
	if *pprofAddress != "" {
		if err := risk.ValidateProfilingAddress(*pprofAddress); err != nil {
			log.Fatalf("configure profiling: %v", err)
		}
		profilingServer = &http.Server{
			Addr: *pprofAddress, Handler: risk.ProfilingHandler(),
			ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 35 * time.Second,
		}
		go func() {
			if err := profilingServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("profiling server: %v", err)
			}
		}()
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown: %v", err)
		}
		if profilingServer != nil {
			if err := profilingServer.Shutdown(shutdownCtx); err != nil {
				log.Printf("profiling shutdown: %v", err)
			}
		}
	}()
	log.Printf("risk-scorer model=%s run=%s rule_rollout=%s listen=%s triton=%s", bundle.Contract.ModelName, bundle.Contract.RunID, ruleRollout.RolloutID, *listenAddress, *tritonURL)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve HTTP: %v", err)
	}
}
