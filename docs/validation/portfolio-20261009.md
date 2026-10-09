# Portfolio candidate validation — 2026-10-09

Status: prepared candidate, with remaining infrastructure/publication gates explicitly open.
The exact baseline is
`2f57ad386c8dfba94c1910cbcc639c809af28f34`, all 230 files verified against Git blob
identities. This is a source snapshot, not a clone with complete Git history.

The package remains 0.1.0. The [tested-artifact manifest](evidence/20261009/tested-artifacts.json)
records the exact baseline, runtime content identity and verified package hashes;
[sanitized evidence checksums](evidence/20261009/checksums.json) bind the retained results.
No old result is relabeled as current acceptance.

## Tested identity and documentation-only refresh

- Runtime/demo/pyproject SHA-256:
  `95cb5a3d081ba143a921865f81181da3ae35fb866216e12d23206897fffb1fee`.
  The manifest describes the exact path/byte framing used by `source_digest` in
  `scripts/portfolio_demo.py`; this is not a whole-repository or Git commit digest.
- Tested wheel, `a2a_mq-0.1.0-py3-none-any.whl`, 186,625 bytes, SHA-256:
  `db8574312619f9f72b053e0638f94e36eaf497f7e7717dadb44d39230f0025b3`.
- Independently reviewed source distribution, `a2a_mq-0.1.0.tar.gz`, 560,020 bytes,
  SHA-256: `57b248f46a119c33031145d0c6171cb5e86e755df4469abc6a0906f144420a6e`.

This final report/manifest refresh is documentation-only and postdates that sdist.
The tested wheel and runtime are unchanged. A rebuilt, documentation-inclusive sdist
has a separate delivery-side package manifest with its own archive SHA-256; an archive
cannot contain its own byte hash. Do not confuse that packaging refresh with a new
runtime test campaign or silently reuse the earlier sdist hash.

## Defects reproduced during preparation

1. Binding inventory used v0.3-style labels while official A2A 1.0 SDK routes rejected
   them. A descriptor-based regression failed before updating both manifest copies
   and constants to the canonical eleven method names. Actual wire routes did not change.
2. An older native-adapter test expected subscription to an already-completed task,
   contradicting [A2A 1.0 §3.1.6](https://a2a-protocol.org/v1.0.0/specification/#316-subscribe-to-task),
   the current bridge and another existing focused test. The obsolete expectation was
   corrected to typed `UnsupportedOperationError`; runtime terminal guards were retained.
3. SDK 1.1.2 REST query parsing uses a deprecated protobuf descriptor property. Under
   the existing warnings-as-errors policy it caused a real REST ListTasks failure.
   Downgrading protobuf is incompatible with the Dapr generated-code requirements.
   Official [SDK 1.1.5](https://github.com/a2aproject/a2a-python/releases/tag/v1.1.5)
   includes the upstream fix (PR #1158). The candidate pins that patch and removes
   the old local SDK-validation warning suppression. No new warning suppression was added.
4. In a restricted host, Dapr could not enumerate network interfaces. The single-host
   launcher now explicitly advertises loopback using the documented `DAPR_HOST_IP`
   setting, consistent with its existing loopback listeners. No OS/network permission
   or security setting was changed. See the [Dapr environment reference](https://docs.dapr.io/reference/environment/).
5. Mypy found two existing narrowing/inference errors. Explicit route-list typing and
   the existing JWT integer-expiry guard expressed as an explicit branch resolve them
   without weakening validation.
6. Strict full-suite teardown exposed an event loop allocated by pytest-asyncio
   1.3.0's temporary event-loop-policy fixture. A creation-stack diagnostic retained
   the source of the unclosed loop. The official
   [pytest-asyncio 1.4.0 changelog](https://pytest-asyncio.readthedocs.io/en/stable/reference/changelog.html)
   documents the fix. The dev dependency is pinned to 1.4.0 and the obsolete resource
   warning exclusions are removed; the candidate keeps `filterwarnings=["error"]`.

## Evidence scopes

Broker-independent tests include official SDK routes and MCP fixture tests. They do
not establish PostgreSQL/Vault/distributed broker behavior. The failure demo separately
uses real pinned native Dapr/JetStream and a separate official SDK producer process.
The small sequential latency sample is labeled with polling, payload, concurrency
and profile limitations; it is not a load/capacity claim.

No whole-history secret audit, public visibility change, license choice, remote CI,
release/tag/merge, new cloud infrastructure, GUI/video campaign or external business
integration is implied. Historical source/image-specific acceptance remains indexed
in [VALIDATION](../VALIDATION.md).


## Candidate results

| Check | Observed result | Evidence / limit |
| --- | --- | --- |
| Baseline materialization | PASS, 230/230 Git blobs | Commit identified above; history unavailable |
| Strict broker-independent suite | PASS, 1,205 tests, zero failures/errors/skips; 6 infrastructure cases deselected; process exit 0 | [JUnit](evidence/20261009/core.junit.xml); Python 3.12.14, SDK 1.1.5, pytest-asyncio 1.4.0 |
| Real PostgreSQL | PASS, 4/4, process exit 0 | [JUnit](evidence/20261009/postgres.junit.xml); PostgreSQL 17.11, UTF-8, verify-full TLS |
| Native provenance | PASS_NATIVE_VERSION | [Dapr/NATS version + hashes](evidence/20261009/native-versions.json) |
| Real Local-lite failure demo | PASS, process exit 0 | [Sanitized JSON](evidence/20261009/demo.json); 2 physical deliveries, 1 terminal record, duplicate/conflict, pause/cancel, real DLQ archive + replay publication, SDK producer |
| Static checks | PASS | Repository checker, Ruff, mypy across 55 source files, pip check |
| Build | PASS | sdist + wheel; package 0.1.0, [tested-artifact identities](evidence/20261009/tested-artifacts.json); final documentation-only archive identity is delivered separately |
| Optional Vault TLS test | BLOCKED | No preprovisioned TLS Transit fixture, versions 1+2 and scoped test token/config |
| Distributed offline-worker Docker test | BLOCKED | Docker/Compose fixture unavailable; Local-lite demo does not replace independent workers/observers |
| Full Git history and remote artifacts | NOT_RUN | Current source tree reviewed only; publication remains blocked |
| Hosted CI / deployment / GUI | NOT_RUN | No new Actions run, image deployment, browser recording or native-platform claim |

The unfiltered run was also executed: 1,205 tests passed, four PostgreSQL tests and
one Vault test failed on missing fixture configuration, and the Docker test skipped.
PostgreSQL was then supplied and all four gates passed separately. The strict core
command was rerun after fixing the test-runner teardown leak. This is not a claim
that every optional/deployment gate is green.

The fresh PostgreSQL cluster used only owned paths/processes and a unique loopback
port; it was stopped after testing. An initial SQL_ASCII disposable database was
replaced by a separate UTF-8 database for the text-based driver contract. No database
code was weakened to accommodate the unsuitable fixture.

The baseline full-core RED had 1,197 passing assertions and two failures before the
SDK/terminal-subscription corrections. Additional diagnostic REDs, exact commands,
patch/file manifests and package hashes are retained in the preparation evidence.
No failed measurement has been relabeled as successful.

## Cleanup and remaining work

- Current README/STATUS were consolidated; the dated 2026-09-29 report was moved to
  the existing archive and linked from the documentation map. Historical ADR/evidence
  versions remain intact.
- The operations semantic review read complete modules and their entry points; all
  three operations modules remain used. No source deletion was justified. Their
  profile-specific contracts are now documented. Focused operations review: 66 passed.
- Obsolete SDK and test-runner warning workarounds were removed after upstream fixes.
  There are no resource-warning exclusions in the current pytest/acceptance policy.
- Generated profiles, native archives, environments, raw logs and build intermediates
  are outside the publication candidate. Only reviewed result/JUnit metadata is retained
  here with [checksums](evidence/20261009/checksums.json).
- Publication still requires owner licensing/rights decisions, full-history/remote
  artifact review, remaining agreed test gates and explicit visibility approval.

## Fresh wheel installation

A separate runtime virtual environment installed only the built wheel with its
`local` extra and the recorded constraints. Isolated Python verified imports came
from that environment, not the source checkout. The same native failure demo then
passed against the installed package; see
[installed-wheel demo](evidence/20261009/installed-wheel-demo.json).

The measured 20-task sequential installed-wheel sample had median 232.57 ms and
nearest-rank p95 250.74 ms. These include HTTP and 50 ms polling on the synthetic
33-byte text workload, concurrency 1, warm profile. Effective container CPU/memory
limits could not be read because cgroups were not mounted. These observations
are not capacity or production-latency promises.

## Replay observation limit from independent review

The replay step verifies DLQ archival, authorized replay publication and an unchanged
previously terminal task snapshot. It does not prove consumer receipt or execution of
that replay, or execution of its terminal guard. The retained initial run contained
no additional delivery after its replay publication checkpoint. Original reports
remain in preparation evidence; current reports explicitly mark consumer replay
evidence `NOT_VERIFIED`. Broker message-ID deduplication can make these distinctions
important. A separate consumer-observed replay scenario remains future acceptance work.

The packaging review also ensured the sdist includes `.github/workflows/ci.yml`,
which its included CI-contract tests read. No workflow was modified or triggered.

## Independent change review

A separate read-only reviewer inspected all changed executable/test modules and
adjacent contracts. It independently passed **132 focused existing tests**, **6 new
contract probes**, and **18 tests from the extracted reviewed sdist** (CI contract,
binding and demo harness), plus the repository checker. These are separate scopes,
not a sum of unique tests or an independent rerun of the 1,205-test core/real-PG/demo
campaigns. It also verified owned-process-group SIGTERM/SIGINT cleanup, all 57 wheel
runtime Python/JSON files against source, the 247-file pre-refresh inventory, and
public evidence checksums.

Two findings were resolved: the replay claim was narrowed to publication and an
unchanged snapshot; the sdist gained the CI workflow fixture read by its tests.
The review verdict is readiness for the **bounded local candidate handoff**, with
Vault, Docker, consumer-observed replay, full-history/remote-artifact review,
licensing/rights and explicit visibility approval still open. It is not full
acceptance, production certification, publication authorization or user acceptance.
The repository remains private at preparation time. No license grant was added.

Reproduction entry points are [CONTRIBUTING](../../CONTRIBUTING.md) for strict
core/static checks and [the demo guide](../PORTFOLIO_DEMO.md) for real native
transport. To rebuild the packages, install the dev requirements and run
`python -m build`; verify archive SHA-256 values against the delivery manifest.
PostgreSQL/Vault live checks require their documented disposable fixtures;
never reuse production credentials or relabel missing infrastructure as PASS.
