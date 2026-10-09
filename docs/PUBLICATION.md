# Publication checklist

Preparing a candidate does not change repository visibility or grant a license.
The owner must approve the exact publication action after these gates are resolved.

## Blocking decisions and review

- [ ] Owner chooses a license and confirms ownership/contribution rights. No license
  file or metadata grant has been added by this preparation.
- [ ] Audit every reachable Git branch/tag/history, releases, workflow artifacts,
  attachments and previous generated profiles for secrets/private material. The
  current 230-file baseline snapshot is not a whole-history audit.
- [ ] Resolve any finding with the owner. Never print a secret in a report. Rotate
  exposed credentials through an authorized process; do not silently rewrite history.
- [ ] Review dependency/resource licenses and any notices required for distribution.
- [ ] Review the exact candidate diff and sanitized evidence. Record its remote SHA
  after checkpointing; keep source and package hashes distinct.
- [ ] Run the agreed test matrix, including separately provisioned infrastructure
  gates if full-stack acceptance is required. Explain every skip and unexecuted gate.
- [ ] Confirm README/repository description say A2A rather than ACP and do not claim
  customer adoption, certification, external exactly-once effects or unmeasured speed.
- [ ] Owner approves changing visibility or creating a separate clean public repository.

## Prepared content

README, compatibility matrix, contributor/security guidance, bounded failure demo,
measured-evidence format, dependency constraints and publication checklist are reviewable
within this candidate. No repository settings, protection rules, CI availability,
release, tag, merge, license choice or public visibility is changed by these files.

## Public package allowlist

Publish source, tests, maintained docs and explicitly reviewed sanitized evidence.
Exclude virtual environments, caches, downloads, native binary archives, generated
local profiles, private logs, raw database/backup files and `.local-validation`.
Historical engineering records should be visibly historical and reviewed for private
paths/hosts before publication. A file being tracked is not proof it is safe to publish.
