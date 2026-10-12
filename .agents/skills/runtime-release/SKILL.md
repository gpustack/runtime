---
name: runtime-release
description: Cut a gpustack-runtime package release end to end (select and push the tag, monitor the release workflow, curate the GitHub Release notes).
---

# Runtime release

One maintainer duty: cut a package release. Push a version tag; `release.yml` builds and publishes
it. Then write the GitHub Release notes.

## Cut a package release

The git tag is the single source of truth: hatch-vcs derives the package version from it at build
time, and there is no version file to bump. Pushing a `v*.*.*` tag runs
`.github/workflows/release.yml`: build the distributions, create the GitHub Release with the
wheel/sdist assets, publish to PyPI, then verify an isolated install. Branch pushes to `main` or
`v*-dev` run the same job against TestPyPI and create no release.

Two workflow facts shape the procedure:

- The GitHub Release is a pre-release only when the tag contains `rc`. A final tag goes public and
  becomes `latest` immediately. There is no promotion gate.
- The workflow generates no baseline notes. The release body starts empty, so curating the notes is
  not polish; it is the entire text the reader gets.

PyPI is the irreversible step: a version once published there cannot be re-uploaded or edited.

### Hard rules

- Tag shapes: `vX.Y.Z` final, `vX.Y.ZrcN` pre-release, `vX.Y.Z.postN` post-release. PEP 440
  canonical spellings only; history contains the non-dotted `v0.2.5postN` — do not repeat it.
- A tag whose PyPI publish succeeded is burned. NEVER delete or move it; fix forward with a new
  version. Delete-and-recreate is limited to a tag that never reached PyPI and is interactive-only.
- An `rc` tag stays a pre-release; never mark it latest.
- Release from `origin/main` HEAD by default. A maintenance line (`v*-dev` branch) targets that
  branch's HEAD; flag the choice in the summary.
- Interactive mode confirms every mutating step (tag push, note replacement). Auto mode runs
  unattended through note curation and reports the release URL.

### 1. Preflight (read-only)

```sh
gh auth status
git fetch origin --tags --prune
git status --porcelain                            # must be empty
git tag -l 'v*' --sort=-v:refname | head -5
git rev-parse origin/main
```

### 2. Version and commit (confirm)

- Read the range since the previous tag: `git log --no-merges --oneline <last>..origin/main`.
- Propose the bump from the Conventional Commits in range: any `feat` → minor; fixes, docs, and
  chores → patch. Use `.postN` only to re-issue the same version line (packaging metadata or a
  docs-only rebuild of unchanged content).
- Confirm the (version, commit) pair before tagging. A final tag publishes immediately, so this
  confirmation is the only gate.

### 3. Draft notes

Group the range by type: Features / Fixes / Docs / Other; omit empty sections. Keep highlights and
fold noisy `chore`/`ci`/`test` entries. Link PRs from `(#NNN)` subjects. Do not open the body with
a heading that repeats the tag. End with the Full Changelog compare URL
(`https://github.com/gpustack/runtime/compare/<last>...<VER>`).

### 4. Tag and monitor (confirm, then read-only)

```sh
git tag -a "$VER" "$SHA" -m "Release $VER"
git push origin "$VER"
gh run list --branch "$VER" --limit 5     # the release.yml run for the tag
gh run watch <run-id> --exit-status       # background it; wait for the completion notification
```

The job's own verify step installs the package from PyPI in an isolated env, but it is unpinned and
only prints the installed version. A green run proves the upload succeeded, not that PyPI already
serves the new version; read the verify log's `version =` line and confirm it equals `$VER`. On
failure read `gh run view <run-id> --log-failed`; retry the same run on transient errors. If the
failure is real and PyPI already holds the version, cut the next version instead — never re-tag.

### 5. Curate notes (confirm)

```sh
gh release view "$VER" --json body,isPrerelease     # the body starts empty
gh release edit "$VER" --notes-file notes.md
```

A maintenance-line final keeps `--latest=false` when a newer stable tag exists. Verify the badge
with `gh api repos/gpustack/runtime/releases/latest --jq .tag_name`. An `rc` release stays a
pre-release and needs no further action.

## Scope

Package releases and their notes run here. No other repository document changes with a release:
the version comes from the tag, and no README or docs table tracks it. Container image builds are
`make package` (`pack/`) and deployment manifests live in `deploy/`; both are outside this skill.
Docs-only changes never trigger a release: the release workflow ignores docs paths on branch
pushes, so docs updates need no tag.
