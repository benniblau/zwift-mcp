#!/usr/bin/env bash
#
# Update a deployed zwift-mcp: pull, install any new dependencies, restart,
# and confirm the server came back.
#
# Run it from the install directory:
#     ./deploy/update.sh
#
# .env, the database and fits/ are gitignored, so nothing here touches them.
# A local edit to a tracked file will stop the pull rather than be discarded.

set -euo pipefail

cd "$(dirname "$0")/.."
INSTALL_DIR="$PWD"
SERVICE="${ZWIFT_MCP_SERVICE:-zwift-mcp}"

if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "Tracked files have local changes. Commit or discard them first:"
    git status --short --untracked-files=no
    exit 1
fi

before="$(git rev-parse --short HEAD)"
git pull --ff-only
after="$(git rev-parse --short HEAD)"

if [[ "$before" == "$after" ]]; then
    echo "Already up to date at $after."
    exit 0
fi

echo "Updated $before → $after:"
git --no-pager log --oneline "$before..$after"

.venv/bin/pip install -q -r requirements.txt

# The schema has no migrations, so a column added upstream will not appear in
# an existing database and the next sync fails on it. Say so rather than let
# it surface at 9am from cron.
if git diff --name-only "$before" "$after" | grep -q '^schema/'; then
    echo
    echo "NOTE: the schema changed. There are no migrations — if a sync fails"
    echo "      with 'no such column', delete the database and re-sync. The"
    echo "      cached FITs in fits/ mean nothing is re-downloaded."
fi

sudo systemctl restart "$SERVICE"
sleep 2

port="$(grep -E '^ZWIFT_MCP_HTTP_PORT=' .env 2>/dev/null | cut -d= -f2 || true)"
port="${port:-8081}"
if curl -fsS --max-time 10 "http://127.0.0.1:${port}/api/v1/health"; then
    echo
    echo "$SERVICE is up on port $port."
else
    echo "$SERVICE did not answer on port $port — check: journalctl -u $SERVICE -n 40"
    exit 1
fi
