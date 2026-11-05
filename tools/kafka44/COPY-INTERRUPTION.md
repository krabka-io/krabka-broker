# Inside-copy payload-close interruption

Use only task-owned isolated RF1 fixtures. This lane parks a real copy after the
payload `.log` has been copied/closed, before its indexes and the copy callback
complete. It is not a partial-byte, fsync, power-loss or RF3 guarantee.

The released `RecordingStorage` has an opt-in filesystem gate: create
`ROOT/gate-enabled` before seeding; `ROOT/gate-entered` records the copied log
path. Without that file the plugin's existing behavior is unchanged. Remove the
gate-enabled file before reopening the same container/data.

For native, `CopyCloseGate.c` is a test-only LD_PRELOAD interposer, not a broker
feature or production storage backend. Compile with:

```
cc -Wall -Wextra -Werror -shared -fPIC -o gate.so tools/kafka44/CopyCloseGate.c -ldl
```

Launch the exact retained broker executable with `LD_PRELOAD=/absolute/gate.so`,
`COPY_GATE_ROOT=/absolute/owned/archive/` (trailing slash required) and
`COPY_GATE_MARKER=/absolute/owned/gate-entered`. It delegates actual libc close,
then parks only the first closed archive `.log`. No record, filesystem copy or
metadata response is synthesized. Other descriptors and directories are not
gated. Recovery omits all three gate variables.

Use `RemoteRecoveryProbe seed` against fresh endpoints: it writes 48 exact64KiB
records and delays copying30s, allowing the seed to complete. Wait for the gate
marker, record payload size/hash and local segment presence, decode the real
`__remote_log_metadata` log with released `DumpLogSegments
--remote-log-metadata-decoder`, and require COPY_SEGMENT_STARTED without
COPY_SEGMENT_FINISHED for the gated segment. Only then SIGKILL the owned process
or container. Reopen exactly the same binary/config/data without the gate; wait
for natural local offset-zero eviction, then use `RemoteRecoveryProbe read` to
require all48 original offset/key/value bytes from remote storage. Never delete
local segments to fabricate a remote-read witness.

The native broker embedding SPI currently selects Local/S3/Gcs through an enum;
it does not expose a custom RemoteStorageManager override. The interposer keeps
this execution on the unchanged production LocalTieredStorage binary rather
than introducing a production injection seam for a qualification fixture.

`InterruptedPayload GATED_LOG` decodes the actual copied prefix with pinned
released codecs, requires valid batch CRCs and all15 offset/key/value records
(with64KiB repeated-offset payloads). It complements, rather than replaces, the
post-eviction48-record consumer check. Decode STARTED metadata before killing;
merely seeing a file or a gate marker does not establish the metadata state.

Measured receipt: `qualification/kafka44-copy-interruption/receipt.json`. Both
released4.4 and retained native8eab7ff8 passed gated payload CRC/record checks,
STARTED-only metadata before SIGKILL, same-data recovery, natural local eviction
and exact48 consumer records. Initial native readiness warnings cleared on the
successful attempt; the old7c4e3aa7 baseline was stopped early and is incomplete,
not reported as a persistent failure. Native identities are exact retained
binary hashes, not an asserted build of the latest documentation commit.
