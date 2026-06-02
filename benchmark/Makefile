# pg2iceberg AWS benchmark — one-command UX.
#
#   make up      provision infra (OpenTofu) + build & push images
#   make seed    create schema, load fake data, start pg2iceberg
#   make bench   run k6 locally for DURATION + poll lag (incl. drain)
#   make results render report.md + run correctness verify
#   make logs    tail pg2iceberg logs
#   make down    destroy everything
#
# Prereqs: tofu, docker, aws (configured), k6, jq, curl, bc.
# Override any knob: make bench DURATION=30m RATE=400 VUS=200

TF      := tofu -chdir=terraform
TFDIR   := terraform

# Bench knobs
DURATION ?= 15m
RATE     ?= 200
VUS      ?= 100
DRAIN    ?= 300          # seconds to keep polling lag after traffic stops

# Seed knobs
RIDERS   ?= 50000
DRIVERS  ?= 5000
RIDES    ?= 200000

# Local image tags (retagged to ECR before push)
BASE_IMG := pg2iceberg-bench-base:latest
# ARM64 (Graviton) Fargate — matches Apple Silicon, so images build
# natively with no emulation/cross-compile. Must match the task defs'
# runtime_platform.cpu_architecture.
PLATFORM := linux/arm64

.PHONY: help init up images deploy seed bench results verify report logs down fmt validate

help:
	@grep -E '^#   make' Makefile | sed 's/^#   //'

init:
	$(TF) init -input=false

up: init
	$(TF) apply -auto-approve
	$(MAKE) images
	$(MAKE) deploy

# Build arm64 images (Graviton Fargate) and push to ECR. Reads repo
# coordinates from tofu outputs, so it must run after `up`/apply.
images:
	@set -e; \
	REGION=$$($(TF) output -raw region); \
	ACCOUNT=$$(aws sts get-caller-identity --query Account --output text); \
	PG=$$($(TF) output -raw ecr_pg2iceberg); \
	SHIM=$$($(TF) output -raw ecr_shim); \
	echo ">> building pg2iceberg base (repo root, --features prod)"; \
	docker build --platform $(PLATFORM) -t $(BASE_IMG) -f ../Dockerfile ..; \
	echo ">> building pg2iceberg benchmark image"; \
	docker build --platform $(PLATFORM) --build-arg BASE=$(BASE_IMG) -t $$PG:latest pg2iceberg; \
	echo ">> building shim image"; \
	docker build --platform $(PLATFORM) -t $$SHIM:latest shim; \
	echo ">> pushing to ECR"; \
	aws ecr get-login-password --region $$REGION | docker login --username AWS --password-stdin $$ACCOUNT.dkr.ecr.$$REGION.amazonaws.com; \
	docker push $$PG:latest; \
	docker push $$SHIM:latest

# Force the shim service to pull the freshly pushed image (on first apply
# the task could not pull yet). pg2iceberg stays at desired 0 until seed.
deploy:
	@REGION=$$($(TF) output -raw region); CLUSTER=$$($(TF) output -raw cluster); \
	aws ecs update-service --region $$REGION --cluster $$CLUSTER --service shim --force-new-deployment >/dev/null; \
	echo "shim redeploying; run 'make seed' once it is healthy."

seed:
	@set -e; \
	TARGET=$$($(TF) output -raw alb_url); \
	echo ">> waiting for shim health at $$TARGET/healthz"; \
	for i in $$(seq 1 90); do curl -fsS "$$TARGET/healthz" >/dev/null 2>&1 && break; sleep 5; done; \
	curl -fsS "$$TARGET/healthz" >/dev/null || { echo "shim never became healthy"; exit 1; }; \
	echo ">> migrating schema"; curl -fsS -X POST "$$TARGET/admin/migrate"; echo; \
	echo ">> seeding $(RIDERS) riders / $(DRIVERS) drivers / $(RIDES) rides"; \
	curl -fsS -X POST "$$TARGET/admin/seed" -H 'content-type: application/json' \
	  -d '{"riders":$(RIDERS),"drivers":$(DRIVERS),"rides":$(RIDES)}'; echo; \
	REGION=$$($(TF) output -raw region); CLUSTER=$$($(TF) output -raw cluster); SVC=$$($(TF) output -raw pg2iceberg_service); \
	echo ">> scaling pg2iceberg to 1 (initial snapshot, then streaming)"; \
	aws ecs update-service --region $$REGION --cluster $$CLUSTER --service $$SVC --desired-count 1 >/dev/null; \
	echo "pg2iceberg starting. Tail it with 'make logs' until snapshot completes, then 'make bench'."

bench:
	@set -e; \
	TARGET=$$($(TF) output -raw alb_url); \
	mkdir -p loadtest/out; \
	dsec=$$(echo "$(DURATION)" | sed -E 's/h/*3600/; s/m/*60/; s/s//' | bc); \
	total=$$(( dsec + $(DRAIN) )); \
	echo ">> polling lag for $${total}s (k6 $(DURATION) + drain $(DRAIN)s) against $$TARGET"; \
	bash scripts/poll_lag.sh "$$TARGET" "$$total" loadtest/out/lag.csv 5 & pollpid=$$!; \
	( cd loadtest && TARGET="$$TARGET" BENCH_DURATION=$(DURATION) BENCH_RATE=$(RATE) BENCH_VUS=$(VUS) k6 run rideshare.js ); \
	echo ">> traffic done; draining $(DRAIN)s to measure catch-up..."; \
	wait $$pollpid; \
	echo ">> bench complete. Run 'make results'."

results: report verify

report:
	bash scripts/report.sh loadtest/out

verify:
	-bash scripts/verify.sh

logs:
	@REGION=$$($(TF) output -raw region); CLUSTER=$$($(TF) output -raw cluster); \
	aws logs tail "/ecs/$$CLUSTER/pg2iceberg" --follow --region $$REGION

down:
	-@REGION=$$($(TF) output -raw region 2>/dev/null); \
	NS=$$($(TF) output -raw namespace 2>/dev/null); \
	if [ -n "$$NS" ]; then \
	  echo ">> deleting Glue tables pg2iceberg created (so the database can be destroyed)"; \
	  for t in $$(aws glue get-tables --region $$REGION --database-name $$NS --query 'TableList[].Name' --output text 2>/dev/null); do \
	    echo "   delete table $$t"; \
	    aws glue delete-table --region $$REGION --database-name $$NS --name $$t || true; \
	  done; \
	fi
	$(TF) destroy -auto-approve

fmt:
	$(TF) fmt -recursive

validate: init
	$(TF) validate
