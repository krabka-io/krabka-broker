# Break-glass bypassed

**Alert:** `KrabkaBreakGlassBypassed`, `rate(krabka_broker_break_glass_bypassed_total[5m]) > 0`.

## What it means

A privileged transition ran without an approved break-glass proposal. The
`action` label names it. The one path that can bump this counter is the
background unclean recovery with `background_unclean_recovery = "audit-only"`
in `[break_glass]`: that path has no caller to refuse, so the broker lets the
recovery run and counts it. The cluster took a data-losing step past the
two-person rule.

Refusals are a different series. `krabka_broker_break_glass_refusals_total`
is the expected answer when an operator runs a tool before the approval
lands, and a steady rate there is normal.

## Confirm

The audit topic `__krabka_audit` carries the transition with the partition
and the reason. `krabka_broker_unclean_leader_elections_total` moves at the
same time when the action is `unclean_recovery`.

## Diagnose

1. Read the audit record. It names the partition and the replica the broker
   promoted.
2. Follow [unclean leader election](unclean-leader-election.md) to find out
   why the ISR was empty.
3. Check `krabka_broker_break_glass_proposals{state="pending"}`. A pending
   proposal beside a bypass means an operator was in the middle of the
   two-person flow and the background path ran first.

## Fix

- Tell the topic's owner which partition lost data.
- If the cluster should never recover unclean on its own, set
  `background_unclean_recovery` to the refusing mode. Partitions with an empty
  ISR then stay offline until two people approve a recovery through
  `krabka-guard`.

## Escalate

Every bypass is a finding for the security owner. Record the audit entry, the
partition and the time.
