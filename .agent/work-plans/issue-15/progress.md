---
issue: 15
---

# Issue #15 — Log the dish's full status and history for analysis, and add a connection state that works on the Mini

## Local Review (Pre-Push)
**Status**: complete
**When**: 2026-10-09 14:09 -04:00
**By**: Claude Code Agent (Claude Opus 5.5)
**Verdict**: approved

**Branch**: feature/issue-15 at `67bacae`
**Mode**: pre-push
**Depth**: Deep (reason: 1047 changed lines, 10 files)
**Must-fix**: 2 (both fixed on the branch) | **Suggestions**: 11 (8 fixed, 3 not changed)
**Round**: 1 | **Ship**: recommended — every must-fix fixed and tested; a Light re-review of the fixes found no must-fix

Specialists: static analysis (ament flake8/pep257/copyright/xmllint) clean; governance; Claude Lens A + Lens B; cross-model Codex (complete) and Gemini (failed: 420 s print timeout); Light re-review of the fix commits. No work plan, so no plan-drift check.

### Findings
- [x] (must-fix) Well-known imports copied without their own imports (type.proto) — `starlink_stats/starlink_stats/grpc_reflection.py` (Codex; fixed 0190caa, 67bacae)
- [x] (must-fix) PR must say Part of #15, not Closes: live-dish validation and BizzyBoat config still open — PR body (governance)
- [x] (suggestion) History fetched while the dish is unreachable, holding the worker — `starlink_diagnostics_node.py` (Lens A + Lens B; fixed c1bfb1c)
- [x] (suggestion) A telemetry publish failure skipped the firmware handling — `starlink_diagnostics_node.py` (Lens B; fixed c1bfb1c)
- [x] (suggestion) Stale power_in_w shown beside a current state — `starlink_diagnostics_node.py` (Lens A; fixed c1bfb1c)
- [x] (suggestion) Descriptors saved under firmware '?' — `starlink_diagnostics_node.py` (Lens A; fixed c1bfb1c)
- [x] (suggestion) A NaN newest history sample blanked the power value — `diagnostics_logic.py` (Lens A; fixed 762e9d4)
- [x] (suggestion) Inhibited dish GPS WARNed as no fix — `diagnostics_logic.py` (Lens A; fixed 762e9d4)
- [x] (suggestion) Test gaps: history period 0, retry, outage, deterministic well-known imports — tests (Lens A; fixed c1bfb1c, 0190caa)
- [x] (suggestion) Served and copied well-known files added out of order — `grpc_reflection.py` (Light re-review; fixed 67bacae)
- [ ] (suggestion) A done-callback can run on the rclpy thread if its future completes before the callback is attached; pre-existing, narrow — `starlink_diagnostics_node.py` (Lens B; not changed)
- [ ] (suggestion) History shares grpc_timeout_sec with status; documented, check on the boat — README (Lens B; documented only)
- [ ] (suggestion) Follow-up in unh_echoboats_project11: enable publish_telemetry and add the JSON topics to the boat bag list (governance; out of this repo)
