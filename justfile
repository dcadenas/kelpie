# Kelpie tasks. Run `just` to list them.
#
# The release recipe exists because a breaking schema change shipped twice
# under an unchanged version string. Everything here is safe to run repeatedly.

set shell := ["bash", "-uc"]

_default:
    @just --list --unsorted

# Fast type check while editing.
check:
    cargo check --all-targets

# Format in place.
fmt:
    cargo fmt

# Everything CI enforces, plus the consistency checks nothing else catches.
gates: verify
    cargo fmt --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-targets

# The version in Cargo.toml, which is the single source of truth.
version:
    @grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/'

# Consistency checks that no compiler or test catches.
verify: _verify-license _verify-version-cli _verify-protocol _verify-schema _verify-version-unreleased _verify-changelog
    @echo "verify: ok"

# Every released version needs an entry someone can read to decide whether to
# upgrade and what it costs them. A version with no section is a release nobody
# outside this machine can act on.
_verify-changelog:
    #!/usr/bin/env bash
    set -euo pipefail
    v=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
    if ! grep -q "^## ${v}\$" CHANGELOG.md; then
        echo "verify: CHANGELOG.md has no '## ${v}' section" >&2
        exit 1
    fi

# --version is env!("CARGO_PKG_VERSION"). A hardcoded string would let
# Cargo.toml and the binaries disagree with no compile error.
_verify-version-cli:
    #!/usr/bin/env bash
    set -euo pipefail
    for bin in kelpie kelpied; do
        if ! rg -q 'env!\("CARGO_PKG_VERSION"\)' "src/bin/${bin}.rs"; then
            echo "verify: src/bin/${bin}.rs does not print CARGO_PKG_VERSION" >&2
            exit 1
        fi
    done

# SPEC is the contract. A protocol constant that moved in code but not in
# SPEC.md is a release that documents the old Herdr and speaks the new one.
_verify-protocol:
    #!/usr/bin/env bash
    set -euo pipefail
    code=$(sed -n 's/^pub const SUPPORTED_PROTOCOL: u32 = \([0-9]*\);/\1/p' src/herdr.rs)
    if [[ -z "${code}" ]]; then
        echo "verify: could not read SUPPORTED_PROTOCOL from src/herdr.rs" >&2
        exit 1
    fi
    if ! rg -q "exactly protocol ${code}" SPEC.md; then
        echo "verify: SPEC.md does not say 'exactly protocol ${code}'" >&2
        exit 1
    fi
    if ! rg -q "protocol ${code}" README.md; then
        echo "verify: README.md does not mention protocol ${code}" >&2
        exit 1
    fi

# The highest numbered migration, SCHEMA_VERSION, and that file's
# PRAGMA user_version must be the same number. A schema move since the
# current version's tag is a bump the unreleased check would also catch;
# this names it.
_verify-schema:
    #!/usr/bin/env bash
    set -euo pipefail
    schema=$(sed -n 's/^const SCHEMA_VERSION: i64 = \([0-9]*\);/\1/p' src/store.rs)
    if [[ -z "${schema}" ]]; then
        echo "verify: could not read SCHEMA_VERSION from src/store.rs" >&2
        exit 1
    fi
    highest=$(find migrations -name '*.sql' -printf '%f\n' | sed 's/^\([0-9][0-9][0-9]\)_.*/\1/' | sort -n | tail -1)
    if [[ $((10#${highest})) -ne ${schema} ]]; then
        echo "verify: SCHEMA_VERSION is ${schema} but the highest migration is ${highest}" >&2
        exit 1
    fi
    mig=$(echo migrations/"${highest}"_*.sql)
    if ! rg -q "^PRAGMA user_version = ${schema};" "${mig}"; then
        echo "verify: ${mig} does not set PRAGMA user_version = ${schema}" >&2
        exit 1
    fi
    v=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
    tag="v${v}"
    if ! git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
        exit 0
    fi
    tagged=$(git show "${tag}:src/store.rs" | sed -n 's/^const SCHEMA_VERSION: i64 = \([0-9]*\);/\1/p')
    if [[ -n "${tagged}" && "${tagged}" != "${schema}" ]]; then
        echo "verify: schema moved ${tagged} -> ${schema} since ${tag}, but version is still ${v}" >&2
        echo "        Cut the next one with: just release <next-version>" >&2
        exit 1
    fi

# A released version must identify exactly one build. Once `vX` is tagged,
# further commits carrying X make `--version` a lie: two different binaries
# answer the same. This happened twice, and `--version` was the feature it hid.
_verify-version-unreleased:
    #!/usr/bin/env bash
    set -euo pipefail
    v=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
    tag="v${v}"
    if ! git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
        exit 0
    fi
    tagged=$(git rev-list -n1 "${tag}")
    head=$(git rev-parse HEAD)
    if [[ "${tagged}" == "${head}" ]]; then
        exit 0
    fi
    # Only what ships counts. A justfile or docs change since the tag leaves
    # every build reporting ${v} identical, so it needs no bump.
    # migrations/*.sql and skills/kelpie/SKILL.md are include_str!'d into the
    # binary; skills/kelpie/references/ ship as the version-matched skill
    # package even though they are not compiled in.
    changed=$(git diff --name-only "${tag}..HEAD" -- \
        src migrations skills/kelpie Cargo.toml Cargo.lock)
    if [[ -n "${changed}" ]]; then
        echo "verify: ${tag} is already released at ${tagged:0:7}, but these changed since:" >&2
        echo "${changed}" | sed 's/^/          /' >&2
        echo "        Two builds would report ${v}. Cut the next one with:" >&2
        echo "          just release <next-version>" >&2
        exit 1
    fi

# The manifest's license must match the LICENSE file.
_verify-license:
    #!/usr/bin/env bash
    set -euo pipefail
    declared=$(grep -m1 '^license = ' Cargo.toml | sed 's/license = "\(.*\)"/\1/')
    if ! head -1 LICENSE | grep -qi "${declared}"; then
        echo "verify: Cargo.toml says license=${declared}, LICENSE says: $(head -1 LICENSE)" >&2
        exit 1
    fi

# Install this tree's release binaries onto the running user kelpied and restart it.
# Discovers the unit's database, socket, and install path rather than assuming them.
# `just release-push` runs this after the tag is on GitHub.
deploy:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! systemctl --user cat kelpied >/dev/null 2>&1; then
        echo "deploy: no user unit kelpied" >&2
        exit 1
    fi
    unit=$(systemctl --user cat kelpied)
    database=$(printf '%s\n' "${unit}" | awk '/--database/{print $2; exit}')
    socket=$(printf '%s\n' "${unit}" | awk '/--socket/{print $2; exit}')
    execstart=$(printf '%s\n' "${unit}" | awk '/^ExecStart=/{sub(/^ExecStart=/,""); print $1; exit}')
    if [[ -z "${database}" || -z "${socket}" || -z "${execstart}" ]]; then
        echo "deploy: could not read ExecStart/--database/--socket from kelpied unit" >&2
        exit 1
    fi
    schema=$(sed -n 's/^const SCHEMA_VERSION: i64 = \([0-9]*\);/\1/p' src/store.rs)
    version=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
    before_version=$(kelpie --version 2>/dev/null || true)
    before_schema=$(sqlite3 -readonly "${database}" "PRAGMA user_version;")
    before_report=$(kelpie report --active 2>/dev/null | wc -l)
    stamp=$(date +%Y%m%d-%H%M%S)
    backup="${database}.bak-schema${before_schema}-${stamp}"
    echo "deploy: ${before_version:-unknown} schema ${before_schema} -> ${version} schema ${schema}"
    echo "deploy: backup ${backup}"
    sqlite3 "${database}" ".backup '${backup}'"
    export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${TMPDIR:-/tmp}/kelpie-target}"
    cargo build --release
    src_kelpie="${CARGO_TARGET_DIR}/release/kelpie"
    src_kelpied="${CARGO_TARGET_DIR}/release/kelpied"
    dirs=()
    dirs+=("$(dirname "${execstart}")")
    while IFS= read -r path; do
        dirs+=("$(dirname "${path}")")
    done < <(which -a kelpie kelpied 2>/dev/null || true)
    unique_dirs=$(printf '%s\n' "${dirs[@]}" | awk 'NF && !seen[$0]++')
    while IFS= read -r dir; do
        echo "deploy: install ${dir}"
        install -m755 "${src_kelpie}" "${dir}/kelpie"
        install -m755 "${src_kelpied}" "${dir}/kelpied"
    done <<<"${unique_dirs}"
    systemctl --user restart kelpied
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        if kelpie who >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    after_schema=$(sqlite3 -readonly "${database}" "PRAGMA user_version;")
    after_version=$(kelpie --version)
    after_report=$(kelpie report --active | wc -l)
    active=$(systemctl --user is-active kelpied)
    if [[ "${active}" != "active" ]]; then
        echo "deploy: kelpied is ${active}" >&2
        systemctl --user status kelpied --no-pager >&2 || true
        exit 1
    fi
    if [[ "${after_schema}" != "${schema}" ]]; then
        echo "deploy: schema is ${after_schema}, expected ${schema}" >&2
        exit 1
    fi
    if [[ "${after_version}" != "kelpie ${version}" ]]; then
        echo "deploy: ${after_version} is not kelpie ${version}" >&2
        exit 1
    fi
    echo "deploy: ${after_version} schema ${after_schema} active"
    echo "deploy: report --active lines ${before_report} -> ${after_report} (backup ${backup})"
    if [[ "${after_report}" != "${before_report}" ]]; then
        echo "deploy: warning: active report line count changed; inspect kelpie report --active" >&2
    fi

# Cut a release: `just release 0.2.0-alpha.5`
#
# Refuses a dirty tree or a wrong branch rather than producing a half-release.
# Tags, because a manifest version with no tag leaves nothing to check out.
release new_version:
    #!/usr/bin/env bash
    set -euo pipefail
    branch=$(git rev-parse --abbrev-ref HEAD)
    if [[ "${branch}" != "main" ]]; then
        echo "release: on ${branch}, expected main" >&2
        exit 1
    fi
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "release: working tree is dirty; commit or stash first" >&2
        git status --short >&2
        exit 1
    fi
    if git rev-parse -q --verify "refs/tags/v{{new_version}}" >/dev/null; then
        echo "release: tag v{{new_version}} already exists" >&2
        exit 1
    fi
    # Checked before the bump so a missing entry leaves the tree untouched
    # rather than half-released.
    if ! grep -q "^## {{new_version}}\$" CHANGELOG.md; then
        echo "release: add a '## {{new_version}}' section to CHANGELOG.md first." >&2
        echo "         Say what an operator has to do, not what changed in git." >&2
        exit 1
    fi
    sed -i '0,/^version = ".*"/s//version = "{{new_version}}"/' Cargo.toml
    cargo update --workspace --quiet
    just gates
    git add Cargo.toml Cargo.lock
    git commit -m "Release {{new_version}}"
    git tag -a "v{{new_version}}" -m "{{new_version}}"
    # Plain echo: `@` is just's line-suppression syntax and is not valid inside
    # a shebang recipe body, where the whole recipe is one shell script.
    echo
    echo "Committed and tagged v{{new_version}}. Nothing is pushed."
    echo "Push with: just release-push {{new_version}}"

# Push a release commit and its tag. Separate so the tag is reviewable first.
# Requires the crate to already be live on crates.io, so a GitHub tag cannot
# exist without the matching published version (and the argument cannot
# disagree with the manifest).
release-push version:
    #!/usr/bin/env bash
    set -euo pipefail
    manifest=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
    if [[ "{{version}}" != "${manifest}" ]]; then
        echo "release-push: asked to push {{version}}, but Cargo.toml is ${manifest}" >&2
        exit 1
    fi
    if ! git rev-parse -q --verify "refs/tags/v{{version}}" >/dev/null; then
        echo "release-push: local tag v{{version}} is missing; cut it with just release first" >&2
        exit 1
    fi
    yanked=$(curl -sS -A "kelpie-release-push" \
        "https://crates.io/api/v1/crates/kelpie-herdr/{{version}}" \
        | python3 -c 'import json,sys; v=json.load(sys.stdin).get("version"); print("missing" if not v else v.get("yanked"))')
    if [[ "${yanked}" != "False" ]]; then
        echo "release-push: crates.io does not have an unyanked kelpie-herdr {{version}} (got ${yanked})" >&2
        echo "        Publish first, then push, so the tag and the crate cannot drift." >&2
        exit 1
    fi
    git push origin main
    git push origin "v{{version}}"
    echo
    echo "Pushed v{{version}}. Installing onto this machine's kelpied and restarting."
    just deploy
