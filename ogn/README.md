# OGN Wings

Minimal native startup-admission fork of Calagopus Wings. Upstream licensing and
attribution are retained. The upstream release and commit are recorded in
`ogn/upstream.json`; OGN releases add build metadata, initially `1.2.8+ogn.1`.
The stable version requirement remains compatible with Panel 1.2.8, while update
checks and system information distinguish OGN revisions.

## Why a native queue

An egg wrapper can delay Java, but Wings has already started its container, begun
uptime/session accounting, and considered startup boost. A five-wrapper probe on
stock Wings confirmed all five containers Starting and positive uptime while the
fifth wrapper remained unadmitted. Such a gate is not native node-wide admission.
Panel-only interception also misses daemon-internal schedule and crash-recovery
paths. OGN queues at the shared native Start path instead.

```yaml
limits:
  server_concurrent_starts: 4
```

The node-wide FIFO limit covers preparation and Starting until Running or Offline.
Queued servers remain offline and consume no container/session uptime. Duplicate
requests do not create extra tickets. Stop cancels a queued start; cancellation of
an already admitted request returns a conflict instead of unexpectedly stopping it.
An over-five-minute start retains its slot and requests attention. No automatic
force-kill or retry is introduced. External Docker-engine actions remain outside
Wings' control.

The authenticated per-server `/startup-queue` endpoint supplies queue position,
wait/start timestamps and slot counts. The OGN panel extension proxies it without
exposing node credentials. Queued requests/history are currently memory-local and
are not replayed after a daemon restart. Recovery behavior must pass the live dev
checks before release; standalone policy tests alone are not deployment evidence.

## Build and release

`cargo test --manifest-path ogn/checks/Cargo.toml` compiles the actual queue policy
and adapter against a narrow test server interface. Full validation also compiles
the real Wings application. `ogn/verify_release.py` checks base/version provenance.

The OGN validation workflow checks pull requests. The OGN build workflow produces
an x86_64 GNU Linux binary, checksums and provenance; an OGN release tag creates
only a **draft** release. Publishing is a separate review action after live queue,
restart/recovery, cancellation, update and rollback checks. No node is deployed
by these workflows. Other architectures need their own build and live validation.

The compiled updater defaults to `mike9918/ogn-wings`, never the stock release
channel. Existing upstream nightly/release jobs are gated to the upstream repository
so syncing this fork cannot accidentally publish stock Wings as an OGN build.
Do not override the update repository back to upstream on an OGN deployment.

## Keeping current

Keep fork `main` available for upstream history. The initial OGN patch is reviewed
against the released tag on `codex/release-1.2.8`, through `codex/ogn-queue`. Do not
force-reset the OGN release branch with GitHub's Sync Fork command.

The upstream-compatibility workflow fetches the chosen upstream ref, attempts a
merge in a disposable runner, then runs queue tests and a full native compile.
It has read-only repository permission: it neither merges, publishes nor deploys.
Review conflicts and upstream changes in a PR, update provenance/version, then
repeat dev acceptance before making the next release. Scheduled Actions run only
from the repository's default branch; the schedule becomes active after this
workflow is present on the chosen OGN default/release branch. Until then invoke
the workflow manually. Enable Actions for the new fork if GitHub requires it.

For AIO development, use the Panel's supported build-time `WINGS_BINARY_PATH` and
`WINGS_RELEASE` inputs and force a managed rebuild. Retain the stock embedded Wings
and previous Panel binary for rollback. A Panel extension rebuild by itself does
not install this fork. Production deployment remains a separate step.
