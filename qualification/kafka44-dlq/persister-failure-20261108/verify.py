#!/usr/bin/env python3
"""Verify retained live observations; no broker launch or mutation."""
import argparse, hashlib, json, pathlib
p=argparse.ArgumentParser();p.add_argument('directory',type=pathlib.Path);a=p.parse_args()
r=a.directory
results=json.loads((r/'results.json').read_text())
assert results=={'native':'PASS','oracle':'PASS'}, results
summary={}
for peer in ('native','oracle'):
 t=json.loads((r/f'{peer}-timeline.json').read_text())
 assert t['state3_observed_ns'] <= t['state_quorum_stopped_ns'] < t['persister_failure_observed_ns'] < t['kill_completed_ns']
 count_before=r/f'{peer}-count-before.txt'
 if not count_before.exists():count_before=r/f'{peer}-count.txt'
 assert 'COUNT dlq_records=1' in count_before.read_text()
 expected=2 if peer=='native' else 1
 assert f'FINAL dlq_records={expected} start=0 lag=0' in (r/f'{peer}-verify.txt').read_text()
 failure=(r/f'{peer}-source-failure.txt').read_text()
 assert ('share-partition state persist failed' in failure) if peer=='native' else ('Persister broker=1' in failure and 'Cancelled in-flight WRITE_SHARE_GROUP_STATE request' in failure)
 before=[json.loads(x.split(' payload: ',1)[1])['data'] for x in (r/f'{peer}-state-before.txt').read_text().splitlines() if 'group-rf3persist08' in x and ' payload: ' in x]
 assert before[-1]['stateBatches'][0]['deliveryState']==3
 summary[peer]={}
 for node in (1,2,3):
  rows=[json.loads(x.split(' payload: ',1)[1])['data'] for x in (r/f'{peer}{node}-state-after.txt').read_text().splitlines() if 'group-rf3persist08' in x and ' payload: ' in x]
  last=rows[-1]
  assert last['startOffset']==0 and last['deliveryCompleteCount']==1,last
  # State update batches are deltas; an empty later update preserves offset0.
  # This fixture has exactly one offset, never a tombstone or later snapshot.
  nonempty=[row['stateBatches'] for row in rows if row['stateBatches']]
  assert nonempty[-1]==[{'firstOffset':0,'lastOffset':0,'deliveryState':4,'deliveryCount':1}],rows
  summary[peer][str(node)]={**last,'effectiveStateBatches':nonempty[-1]}
print(json.dumps(summary,indent=2))
print('PASS: paired real failure timelines and six final ARCHIVED replicas')
