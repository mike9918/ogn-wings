# OGN Wings

Minimal native startup-admission fork of Calagopus Wings. Upstream licensing and
attribution are retained. The upstream release and commit are recorded in
`ogn/upstream.json`; OGN releases add build metadata, currently `1.2.8+ogn.4`.
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

The node-wide admission limit covers preparation and Starting until Running or Offline.
Queued servers remain offline and consume no container/session uptime. Duplicate
requests do not create extra tickets. Stop cancels a queued start; cancellation of
an already admitted request returns a conflict instead of unexpectedly stopping it.
An over-five-minute start retains its slot and requests attention. No automatic
force-kill or retry is introduced. External Docker-engine actions remain outside
Wings' control.

The authenticated per-server `/startup-queue` endpoint supplies queue position,
wait/start timestamps and slot counts. The OGN panel extension proxies it without
exposing node credentials. Waiting requests are atomically journaled before they appear in the queue,
preserving captured priority, FIFO order and waiting age across daemon restarts.
Recent completed timing history remains memory-local. Durable per-server admission markers
restore unfinished containers as Starting before attachment, retaining their slots.
The current container log tail (10,000 lines, at most 4 MiB) is replayed to detect
readiness emitted during the outage; if readiness is absent from that bound, the
server conservatively retains its slot until a new readiness line or Stop. Marker
files are outside server-controlled storage. Live recovery checks are required
before release; policy tests alone are not deployment evidence.

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

## Plan priorities and admin monitoring (OGN revision 3)

Before admission Wings reads `/api/remote/servers/{uuid}/ogn-startup-policy`
using its existing node authentication. The hosting extension resolves the
owner's plan to priority 0–10; larger numbers start first. Unassigned accounts
and older extensions without this endpoint use priority zero. Lookup errors
reject Start visibly instead of silently discarding a paid entitlement.
Priority is captured for that request; changing a plan affects future requests.

Each five full minutes waiting adds one effective priority level, without a cap,
so a continuous stream of new high-priority requests cannot starve an older one.
Ties remain FIFO; active starts are never preempted. The authenticated node
`/api/system/startup-queue` snapshot supplies active, waiting and recent outcomes,
including recovered starts whose earlier timing is unavailable. Per-server
cancellation accepts an optional queued_at match checked under the queue mutex.

## Durable waiting requests (OGN revision 4)

The daemon-private `.ogn-startup-queue.json` stores pending intent using atomic
replacement, file sync and directory sync. Admission and cancellation commit their
removal before changing the visible ticket. Runtime shutdown does not cancel
accepted waiting requests. Boot attaches containers before resuming matching
generations. Removed or locked servers are reconciled instead of blindly replayed.
The existing power API remains asynchronous; a visible Queued ticket is durable.
Interrupted admitted preparation follows native autostart semantics; this journal
covers waiting requests rather than replaying power calls.

Corrupt/unsupported journals remain untouched and block admission until repaired.
I/O failure pauses admission and fails cancellation visibly; failed writes never
acknowledge cancellation. Snapshots expose durable_waiting and a recovery error.
Empty the queue before rolling back to Wings that cannot read the journal.
