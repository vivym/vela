# Agent Knowledge Index

This directory contains repository-specific research guides. Treat each guide as
evidence-bounded context, not as a replacement for inspecting current code and
runtime state.

## Guides

| Topic | Guide | Sources | Updated | Retrieval hints |
| --- | --- | ---: | --- | --- |
| Dynamo, llm-d, and inference orchestration patterns applicable to Vela | [dynamo-llm-d-inference-orchestration-for-vela.md](dynamo-llm-d-inference-orchestration-for-vela.md) | 63 | 2026-09-03 | Dynamo Router, Planner, llm-d EPP, Flow Control, disaggregated H3 stages, multi-member gangs, exact cache, Vela Job/Attempt/Lease, PostgreSQL authority |

## Retrieval Notes

- Read the guide's evidence boundary before reusing a recommendation.
- Resolve source IDs such as `D17`, `R05`, or `V03` through
  `resources/dynamo-llm-d-inference-orchestration-for-vela-sources.json`.
- The upstream commits, original Vela research baseline, and post-research Vela
  implementation alignment are pinned. Reinspect current revisions before new
  implementation because routing, planner, and llm-d plugin surfaces are fast
  moving.
- Keep confirmed facts, derived interpretations, Vela recommendations, and
  unknowns separate.
- Do not infer production readiness from implemented code. The guide records the
  Vela baseline as `0/9 PASS` production gates.
