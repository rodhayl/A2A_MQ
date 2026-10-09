# Publication exposure audit 2026-10-09

**Ordinary reachable-history review is complete within the scope below. Public
publication clearance remains open.** No confirmed live credential or personal-data
leak was found in the screened material. This bounded inspection is not a guarantee
that every possible secret format or undisclosed object has been found.

## Identities and timing

- Historical audit: 2026-10-09, 13:53–14:07 UTC, with main at
  `2f57ad386c8dfba94c1910cbcc639c809af28f34`. All 22 listed refs remained unchanged
  during that audit.
- Privately checkpointed candidate:
  `dfebe8d0f6ef7cbb96b7489440e67a5c5c64ff40`, tree
  `b01618a612366763278d3cad82334aee2b35adb5`, whose parent is the historical baseline.
  Its 248 file blobs were separately matched exactly to the candidate inventory
  screened at 14:06 UTC.
- The repository was still private when the candidate identity was checked.
  This documentation update does not change visibility, choose a license or certify
  later commits, rebuilt packages or newly created remote artifacts.

The ordinary-history totals below describe the earlier listed-ref snapshot, not a
new traversal counted through the candidate or this documentation update.

## Completed inspection

| Surface | Observed scope and result |
| --- | --- |
| Listed Git refs | 16 branches, 2 annotated tags, 4 PR refs; pagination exhausted |
| Ordinary reachable ancestry | 461 unique commits; every referenced parent accounted for |
| Trees and file content | 458 distinct untruncated trees; 1,100 unique blobs; 285 paths; 12,846,842 bytes |
| Content integrity | All 1,100 blobs matched their declared size and exact Git object SHA |
| Git LFS and submodules | No LFS pointers or gitlink/submodule entries in those trees |
| Issues and PRs | 3 PR bodies; no standalone issues, issue/inline comments or PR reviews; no attachment links in the inspected bodies |
| Commit comments | Empty for all 461 inspected commits |
| Releases | No releases or release assets in the exhausted collection |
| Actions | 450 runs, including all job attempts: 1,234 jobs; every run's artifact collection checked; zero artifacts |
| Execution/log boundary | All jobs had zero recorded steps and runner ID 0; three representative job logs returned 404/BlobNotFound |
| Candidate files | 248 files screened separately; exact match to the checkpointed candidate; no symlinks or unscanned binary files |

Content screening covered provider-token/private-key/JWT patterns, credential
assignments and URLs, personal contacts/private links, licensing/provenance markers
and LFS pointers, followed by review of matching contexts. Private-key/credential
matches were synthetic test fixtures or workflow variables. Candidate JUnit matches
were negative-test names, without captured system-out/system-err or failure bodies.
Commit metadata in the ordinary history used GitHub noreply/service addresses.

No source LICENSE/NOTICE/COPYING file or vendored-source directory was identified.
This inventory does not establish ownership or dependency redistribution rights.
The Apache-2.0 decision remains pending.

The existing documentation-inclusive sdist member inventory was also checked for
runtime credentials, databases, logs and staging payload paths, with none found.
A later rebuild needs its own archive identity and content check.

## Corrupt embedded history remains unresolved

Two branch tips retain an encoded Git bundle under `.bundle/`:
`automation/import-mvp-bundle-20260916` and `handoff/mvp-final-20260916`.

The stored `.bundle/part-01`, blob
`f347f90d3b4db2c80ae61d718b69c41af3dc9be8`, is 9,420 bytes and literally ends in an
ellipsis. Its Git blob hash was verified, so this is stored corruption rather than
a truncated audit response. Concatenated parts are invalid base64. The recoverable
prefix declares 304 packed objects, but only 19 recovered: 10 commits, 6 blobs and
3 trees. **The remaining 285 declared objects were not recovered or certified.**

The embedded head `30deeb55b448cc33020a28665670552b9ecae0de` returned 404 from the
GitHub Git-commit API. Recovered text contained local CI/test material and bot
metadata, with no confirmed live credential; that does not clear the unreadable
remainder. This embedded archive is separate from the fully enumerated ordinary
ref ancestry.

Three other branch tips retain staging material:
`automation/recovery-diagnostic-20260916` and
`automation/replay-mvp-20260916-v3` each expose nine `.publish` patches;
`automation/publish-final-bundle-20260916` exposes six `.stage` markers.
Historical import/replay/finalize workflows include write-capable operations.
The candidate itself contains only `.github/workflows/ci.yml`.

A clean main tree does not remove these older branch/history bytes. No branch,
history or artifact was deleted, rewritten or made public by this audit.

## Other exposure boundaries

- Historical acceptance records retain generic machine/worktree paths, workload
  names, image digests and an internal issuer hostname. These are engineering
  metadata, not confirmed credentials; retention requires an explicit publication
  choice. Preserve checksum-bound historical evidence rather than silently editing it.
- Wiki and Projects are enabled, but their content was not inventoried. Actions
  caches also remain uninspected; the available read route did not support them.
- External package/container registries, deployment services and object stores were
  not inventoried. Do not assume they contain published artifacts.
- Deleted/unadvertised server objects and undisclosed attachments are not enumerated
  by the listed-ref traversal.
- Repository metadata reported Pages and Discussions disabled. This is not an
  account-wide website audit. Hosted job metadata is not evidence of executed tests.

## Safe next steps

1. Keep the repository private while the license/rights decision and unresolved
   exposure decisions remain open.
2. Obtain the authentic complete source of the embedded bundle for inspection, or
   agree with the owner on a different publication surface. A separately approved
   clean source export can avoid publishing old repository history, but must be
   audited in its own right. No such repository creation or sharing is authorized
   merely by this report.
3. Decide with the owner how to handle the five staging-bearing branch tips and
   historical engineering metadata. Deleting files in a new commit does not erase
   older objects; branch/history deletion or rewriting requires separate approval.
4. Inspect relevant wiki/project/cache and registry surfaces through an authorized
   read route, or document a verified exclusion from the intended publication.
5. Add only the owner-approved license/notices, review final package contents and
   keep raw profiles, signing keys, generated tokens and local logs excluded.
6. Recheck refs, releases and artifacts for drift and obtain approval for the exact
   public visibility action. Preserve the separate test limits in
   [portfolio validation](validation/portfolio-20261009.md).

See the [publication checklist](PUBLICATION.md) for the remaining gates.
