# Repository publication preparation

This existing repository is an experimental CV/portfolio project. The bounded
Local-lite experiment is complete; its guarantees and test limits remain in
[STATUS](STATUS.md) and [VALIDATION](VALIDATION.md). No license grant is supplied.
License selection, a replacement repository and additional production certification
are not requirements of this preparation.

## Current tracked-tree review, 2026-10-11

The reviewed main baseline is `15e3d04997a7b1f7f20b765f4d2f1971bf3042a2`:
257 files, 2,730,028 bytes, with every Git blob hash verified. All are regular UTF-8
files. No confirmed live credential, tracked runtime profile, database, raw log,
binary archive, symlink, submodule, LFS pointer or file larger than 1 MB was found.
Credential-pattern matches were synthetic fixtures or credential-generation code.
All 148 relative Markdown file/directory links and both local heading anchors
resolved. External URLs were not checked.

The largest file is the retained `core.junit.xml` (678,975 bytes). The three retained
JUnit files contain no failure, error, system-out or system-err bodies. Historical
evidence and its checksums are preserved; cleanup does not hide failing results or
convert skipped infrastructure into PASS. No runtime code or package version changed
in the documentation cleanup.

## Repository hygiene, 2026-10-11

- `main` is the sole remaining branch and unchanged default branch.
- `v0.1.0-rc1` and `v0.1.0-rc2` retain their original annotated tags. Both tagged
  commits are ancestors of the reviewed main baseline.
- Retired 16 unnecessary branch names: five already integrated into main, three
  duplicate/ancestor references, and eight obsolete staging/automation branches.
- Closed [PR #1](https://github.com/rodhayl/A2A_MQ/pull/1) without merging its obsolete
  snapshot-import workflow; preserved its original text and history. PRs #2 and #3
  remain merged. No open PR remains.
- Corrected the repository description from `ACP Message Queue` to `A2A Message Queue`.

Unique historical commits were preserved before their final remote refs were
removed. The private recovery archive `a2a-mq-obsolete-branches-recovery-20261011.zip`
contains a standalone Git bundle, all 11 divergent original branch refs, exact SHA
mapping, restore instructions and verification records. It contains 59 commits
(39 divergent and 20 shared ancestors), 146 trees and 105 blobs. Bundle verification,
restoration into empty bare repositories and full strict Git integrity checks passed.
A download of the persisted private archive matched its exact hash:

- Recovery ZIP SHA-256: `0bb407b662d22a25654e70bfe99a2e8cbb329481b9d4fe7280dfea02aa25cba7`
- Git bundle SHA-256: `5c8687f0d1214ff7d435682118e573399fa06f5b5a3454b8dec4addfaac9bc71`

The archive stays outside this repository. Historical direct changes consist only
of staging markers, encapsulated snapshots/bundles/patches, recovery/publication
workflows and `FINAL_HANDOFF.md`; no direct product-source or test changes awaited
integration. Preserving those payloads does not assert their semantic equivalence
to current main. Never execute or republish historical workflows merely to restore
an old branch.

## Exposure review and its limits

The [2026-10-09 audit](publication-exposure-20261009.md) remains the dated record of
ordinary reachable history: 22 refs, 461 commits, 458 trees and 1,100 unique blobs,
all content hashes checked. The four later main commits and the current tracked
tree retain distinct identities. No confirmed live credential or personal-data leak
was found within the recorded scopes; this is not exhaustive secret certification.

Fresh repository-surface checks found:

- Wiki has no pages; linked Projects has zero open and zero closed projects.
- Actions has zero caches. Its 450 run records have no update after the earlier
  audit; that audit inspected every artifact collection and found zero artifacts.
  The latest run artifact collection was checked again and is empty.
- No releases or GitHub packages are published. Pages and Discussions are disabled.

Retained evidence includes generic execution/worktree paths, an internal issuer
hostname and a disposable execution hostname. These are engineering metadata,
not confirmed credentials. Git commit/author metadata and historical PR/run records
remain part of the repository's history.

Deleting branch refs is ordinary repository cleanup, not a history rewrite or
secure erasure. The historical corrupt embedded bundle remains corrupt in the
private recovery copy; its 285 unrecovered nested objects are not newly certified.
Old commit URLs, PR refs, Actions records or unadvertised server objects may remain
accessible according to GitHub retention. External registries/services and
undisclosed attachments are outside this repository review.

## Visibility and distribution boundary

The repository remained private through this preparation. The exact visibility
change is a separate owner-approved action; these records do not claim it happened.
Repository visibility does not add an open-source license grant.

Publish maintained source, tests, documentation and reviewed sanitized evidence.
Keep virtual environments, caches, native downloads, generated profiles, signing
keys/tokens, raw logs/databases/backups and `.local-validation` outside commits.
Bundled redistribution of third-party dependencies or native tools requires its
own exact-artifact notices and review. Optional infrastructure certification remains
outside the completed CV/portfolio scope.
