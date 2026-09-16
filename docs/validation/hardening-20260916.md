# Hardening execution record - 2026-09-16

## Executed scopes

All counts below refer to distinct commands, not a cumulative total. Repeated or
overlapping tests must not be added together. No historical 462/571-test MVP claim
is used as evidence for this branch.

| Scope | Actual result | Meaning |
| --- | --- | --- |
| Reference replay regression, before its fix | 2 failed, 2 passed | Identical event IDs carried different timestamp bytes on repeat and across subprocesses. |
| Reference replay regression, after its fix | 4 passed | Stable bytes/digests and tenant/consumer namespaces. Change committed at `9b9be20bdb7ab1c45cd7395b561f6bfdda73c5c1`. |
| Transport/CLI/reference replay selection after lifecycle fixes | 74 passed | Warnings treated as errors. Includes HTTP mocks, ASGI callbacks and real loopback CLI/subprocess tests; not real Dapr. Change committed at `c4449c94bf1bad5b9ea1ca2f6f4538999156d7bc`. |
| Separate repeat of transport lifecycle tests | 37 passed | Subset of the preceding 74, not additional unique acceptance tests. |
| Independent-review harness | 25 passed | Disposable Git repos validate SHA, clean-tree and evidence checks; not an MVP simulation pass. Change committed at `32a0463be3b08446f18d051e5817873e6b8b1f70`. |
| Final review-tooling plus static CI selection | 30 passed; failures=0, errors=0, skipped=0 | 25 reviewer tests plus 5 workflow contract tests, with warnings as errors. Code corresponds to `0911cfb2a84e46b7b983676ed4de438bfa489dc7`. |

Environment for those executions: Python 3.13.5, pytest 9.0.2. The final tooling
selection used the installed PyYAML 6.0.3. Earlier focused checks do not establish
that every test in the final expanded branch ran together. The final full application
suite, both-Python matrix and external infrastructure were not rerun successfully.

The original JUnit and JSON fixes are preserved in `871c62b43f60681e2c4f0517da1005e4b926c419`
and `29d677c72e0a8c4f543280da33ea035be3662f89`. Their commit messages contain prior
focused results; they are not silently added to the final tooling count above.

## Reproduce the final tooling selection

After installing the development dependencies, choose a fresh evidence directory
outside the source. This command is narrower than the independent local review:

```sh
PYTHONDONTWRITEBYTECODE=1 python -m pytest \
  tests/test_independent_review.py tests/test_ci_contract.py \
  -q -W error -p no:cacheprovider --junitxml=/tmp/a2amq-tooling-review.junit.xml
python scripts/assert_junit_complete.py /tmp/a2amq-tooling-review.junit.xml
```

For a complete declared local review and separate blocking full verdict, use
[TEST_MVP.md](../../TEST_MVP.md). Retain exact SHA and fresh outputs. There is no
assertion that fresh execution must reproduce a historic timing or test count after
future changes.

## Publication and infrastructure

Commits were created with full source blobs and fast-forward branch updates through
the GitHub connector; `main` and recovery branches were preserved. Source components
used for earlier application tests were exported and checked against Git blob hashes,
not obtained through a successful network clone. The final tooling tests used their
own disposable Git fixtures.

GitHub Actions was triggered by pushes. Run `35153620064` at `c4449c9` failed before
step evidence was available. The revised workflow is a configured gate with static
tests, not a verified hosted pass. No Docker or real external infrastructure pass
is asserted. See [STATUS.md](../STATUS.md) for recovery and integration blockers.
