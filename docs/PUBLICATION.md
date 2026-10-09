# Publication checklist

Preparing a candidate does not change repository visibility or grant a license.
The owner must approve the exact publication action after these gates are resolved.
The candidate is privately checkpointed at
`dfebe8d0f6ef7cbb96b7489440e67a5c5c64ff40`; no license grant has been added.

## Completed scoped review

- [x] Inspect ordinary reachable history and enumerated remote metadata for the
  audited baseline `2f57ad386c8dfba94c1910cbcc639c809af28f34`: 22 refs, 461 commits,
  458 trees and 1,100 unique blobs, all content hashes verified; releases, PR/comment
  collections and all 450 Actions runs' artifact collections inspected.
- [x] Separately screen the 248-file candidate and match every file blob to the
  privately checkpointed candidate above.

No confirmed live credential or personal-data leak was found in those scopes.
**Public clearance remains open:** a corrupt embedded bundle has 285 unrecovered
objects out of 304 declared; separate surfaces remain uninspected. See the
[dated exposure audit](publication-exposure-20261009.md) for findings, identities,
coverage limits and safe options. These completed checks are not complete-history
certification or authorization to publish.

## Blocking decisions and review

- [ ] Owner chooses a license and confirms ownership/contribution rights. No license
  file or metadata grant has been added by this preparation.
- [ ] Resolve the corrupt embedded-bundle coverage gap and the intended exposure of
  five staging-bearing branch tips. Keep old branches/history unchanged unless the
  owner explicitly approves a different action.
- [ ] Inspect or verify the exclusion of wiki/project content, Actions caches,
  external registries/services and any undisclosed attachments or raw profiles.
  Recheck refs, releases and artifacts for drift before publication.
- [ ] Resolve any finding with the owner. Never print a secret in a report. Rotate
  exposed credentials through an authorized process; do not silently rewrite history.
- [ ] Review dependency/resource licenses and any notices required for distribution.
- [ ] Review the exact candidate diff and sanitized evidence. Use the exact checkpointed
  SHA and identify any later documentation/packaging refresh separately; keep source
  and package hashes distinct.
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
