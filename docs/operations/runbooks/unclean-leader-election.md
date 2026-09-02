# Unclean leader election

**Alert:** `KrabkaUncleanLeaderElection`, `rate(krabka_broker_unclean_leader_elections_total[5m]) > 0`.

## What it means

The controller elected a leader from outside the ISR because the ISR was
empty and the topic had `unclean.leader.election.enable=true`, or an operator
ran an unclean election. Records that only the lost ISR members held are gone.
The partition is available again, and consumers may see offsets move
backwards.

A KIP-966 recovery from a surviving eligible leader replica is not counted
here, because it loses nothing.

## Confirm

The counter names the controller that drove the election. The audit topic
`__krabka_audit` carries the election with the partition and the principal.
`kafka-topics --describe` shows the new leader, which is not in the old ISR.

## Diagnose

1. Find out which partition it was. Read the audit topic, or the controller's
   log at the moment the counter moved.
2. Find out why the ISR was empty. Follow
   [offline partitions](offline-partitions.md) for the chain of events.
3. Decide whether the election was expected. An operator's
   `kafka-leader-election --election-type unclean` shows the operator as the
   principal. An automatic one shows the controller.

## Fix

- Tell the topic's owner which partition lost data and the offset range the
  new leader starts from. Consumers with committed offsets past the new log
  end reset according to their `auto.offset.reset`.
- If the election was automatic and unwanted, set
  `unclean.leader.election.enable=false` on the topic. The partition then stays
  offline until an ISR member returns, which is the safer failure.
- If `[break_glass]` is configured and the election ran without an approved
  proposal, follow [break-glass bypassed](break-glass-bypassed.md).

## Escalate

Keep the old leader's log directory if the broker comes back. It holds the
records that the new leader does not. A recovery from it is manual and needs
the topic owner's decision.
