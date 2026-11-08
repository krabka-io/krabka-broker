#!/usr/bin/env python3
"""Fresh private paired RF3 phase-two persister transport failure; loopback only."""
import argparse,base64,hashlib,json,os,pathlib,signal,socket,subprocess,time,uuid
p=argparse.ArgumentParser();p.add_argument('--run',required=True);p.add_argument('--libs',required=True);p.add_argument('--broker',required=True);p.add_argument('--formatter',required=True);a=p.parse_args()
r=pathlib.Path(a.run).resolve();r.mkdir(mode=0o700,parents=True,exist_ok=False)
repo=pathlib.Path(__file__).resolve().parents[3]; q=repo/'qualification/kafka44-dlq'; cp=f'{r}/classes:{a.libs}/*';image='apache/kafka@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f'
(r/'classes').mkdir(); fixture=(q/'ShareFault.java').read_text().replace('public class ShareFault','public class ShareFaultRf3').replace('new NewTopic(SOURCE,Map.of(0,List.of(1)))','new NewTopic(SOURCE,Map.of(0,List.of(1,2,3))).configs(Map.of("min.insync.replicas","2"))').replace('new NewTopic(DEST,Map.of(0,List.of(2,3)))','new NewTopic(DEST,Map.of(0,List.of(2,3,1)))').replace('"min.insync.replicas","1"','"min.insync.replicas","2"')
# Pin the state leader off the killed source, while requiring a replica quorum.
fixture=fixture.replace('admin.createTopics(List.of(new NewTopic(SOURCE', 'admin.createTopics(List.of(new NewTopic("__share_group_state",Map.of(0,List.of(3,1,2))).configs(Map.of("min.insync.replicas","2")))).all().get();\n        admin.createTopics(List.of(new NewTopic(SOURCE')
# Count must scan the full window, not stop after the first nonempty fetch.
fixture=fixture.replace('if(args[1].equals("count") && count>0)break;', '')
(r/'ShareFaultRf3.java').write_text(fixture)
def call(cmd,log,timeout=120):
 with (r/log).open('w') as f:subprocess.run(list(map(str,cmd)),stdout=f,stderr=subprocess.STDOUT,check=True,timeout=timeout)
def java(peer,mode,extra=(),props=()):call(['java',*props,'-Ddlq.run=rf3persist08','-cp',cp,'ShareFaultRf3',f'127.0.0.1:{ports[3]}',mode,*extra],f'{peer}-{mode}.txt')
def wait(pred,seconds=90):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  if pred():return
  time.sleep(.05)
 raise RuntimeError('timed out waiting for condition')
call(['javac','-cp',f'{a.libs}/*','-d',r/'classes',q/'ShareBoundary.java',q/'WireResponseGate.java',r/'ShareFaultRf3.java'],'javac.txt')
results={}
for peer,base in [('native',64800),('oracle',64900)]:
 ports={i:base+i for i in (1,2,3)};ctrl={i:base+10+i for i in (1,2,3)};gateport=base+20
 for port in [*ports.values(),*ctrl.values(),gateport]:
  with socket.socket() as s:s.bind(('127.0.0.1',port))
 processes=[];containers=[];nodes={};gate=r/f'{peer}-gate';gate.mkdir();timeline={}
 def launch_native(i):
  f=(r/f'{peer}{i}-broker-{time.time_ns()}.txt').open('w'); proc=subprocess.Popen([a.broker,'--config-file',str(r/f'{peer}{i}/broker.toml'),'--controller-listen-addr',f'127.0.0.1:{ctrl[i]}','--metrics-listen-addr','none','--health-listen-addr','none'],stdout=f,stderr=subprocess.STDOUT);processes.append(proc);nodes[i]=proc
 try:
  gate_log=(r/f'{peer}-gate.txt').open('w');g=subprocess.Popen(['java','-cp',cp,'WireResponseGate',str(gateport),str(ports[2]),str(gate)],stdout=gate_log,stderr=subprocess.STDOUT);processes.append(g)
  ids={i:base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=') for i in ports}; voters=','.join(f'{i}@127.0.0.1:{ctrl[i]}' for i in ports)
  for i in ports:
   d=r/f'{peer}{i}';d.mkdir(mode=0o777);os.chmod(d,0o777);advert=gateport if i==2 else ports[i]
   if peer=='native':
    cfg=f'controller_quorum_voters = [{",".join(json.dumps(v) for v in voters.split(","))}]\nbroker_id = {i}\nlog_dir = "{d}/data"\n[[listeners]]\nname = "PLAINTEXT"\nbind_addr = "127.0.0.1:{ports[i]}"\nadvertised = "127.0.0.1:{advert}"\nprotocol = "Plaintext"\n[runtime]\noffsets_topic_replication_factor = 3\ntransaction_state_replication_factor = 3\ntransaction_state_min_isr = 2\nshare_state_replication_factor = 3\nshare_state_min_isr = 2\nshare_state_num_partitions = 1\n'
    (d/'broker.toml').write_text(cfg); initial=','.join(f'{j}@127.0.0.1:{ctrl[j]}:{ids[j]}' for j in ports)
    call([a.formatter,'--log-dir',d/'data','--node-id',i,'--cluster-id','REZDUkFTSDA4Q0xVU1RFUg','--release-version','4.4-IV2','--directory-id',ids[i],'--initial-controllers',initial],f'{peer}{i}-format.txt')
   else:
    cfg=f'node.id={i}\nprocess.roles=broker,controller\nlisteners=PLAINTEXT://127.0.0.1:{ports[i]},CONTROLLER://127.0.0.1:{ctrl[i]}\nadvertised.listeners=PLAINTEXT://127.0.0.1:{advert}\ncontroller.listener.names=CONTROLLER\ncontroller.quorum.voters={voters}\ninter.broker.listener.name=PLAINTEXT\nlistener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\nlog.dirs=/evidence/data\noffsets.topic.replication.factor=3\ntransaction.state.log.replication.factor=3\ntransaction.state.log.min.isr=2\nshare.coordinator.state.topic.replication.factor=3\nshare.coordinator.state.topic.min.isr=2\nshare.coordinator.state.topic.num.partitions=1\n'
    (d/'broker.properties').write_text(cfg)
    call(['docker','run','--rm','--network','host','--user','0','-v',f'{d}:/evidence','--entrypoint','/opt/kafka/bin/kafka-storage.sh',image,'format','--cluster-id','REZDUkFTSDA4Q0xVU1RFUg','--release-version','4.4-IV2','--config','/evidence/broker.properties'],f'{peer}{i}-format.txt')
  for i in ports:
   if peer=='native':launch_native(i)
   else:
    name=f'dlq-rf3-persist08-final-{i}';containers.append(name);call(['docker','run','-d','--name',name,'--network','host','--user','0','-e','KAFKA_HEAP_OPTS=-Xms128m -Xmx384m','-e','LOG_DIR=/evidence/logs','-v',f'{r}/{peer}{i}:/evidence','--entrypoint','/opt/kafka/bin/kafka-server-start.sh',image,'/evidence/broker.properties'],f'{peer}{i}-start.txt')
  def listening():
   try:
    for port in ports.values():
     with socket.create_connection(('127.0.0.1',port),timeout=.5):pass
    return True
   except OSError:return False
  wait(listening);time.sleep(5);java(peer,'setup')
  ack=r/f'{peer}-ack'; rejectlog=r/f'{peer}-reject.txt';f=rejectlog.open('w');reject=subprocess.Popen(['java','-Ddlq.run=rf3persist08','-cp',cp,'ShareFaultRf3',f'127.0.0.1:{ports[3]}','reject',str(ack)],stdout=f,stderr=subprocess.STDOUT);processes.append(reject)
  wait(lambda:'SOURCE_ACQUIRED' in rejectlog.read_text());ack.touch();wait(lambda:(gate/'held.txt').exists());java(peer,'count');assert 'COUNT dlq_records=1' in (r/f'{peer}-count.txt').read_text();(r/f'{peer}-count-before.txt').write_text((r/f'{peer}-count.txt').read_text())
  state=r/f'{peer}3/data/__share_group_state-0/00000000000000000000.log'
  call(['java','-cp',cp,'org.apache.kafka.tools.DumpLogSegments','--files',state,'--share-group-state-decoder'],f'{peer}-state-before.txt')
  text=(r/f'{peer}-state-before.txt').read_text();rows=[json.loads(x.split(' payload: ',1)[1])['data'] for x in text.splitlines() if 'group-rf3persist08' in x and ' payload: ' in x];assert rows and any(x['deliveryState']==3 for x in rows[-1]['stateBatches']),rows
  timeline={'held':(gate/'held.txt').read_text(),'state3_observed_ns':time.time_ns()}
  # Withhold the state topic's quorum before returning the successful DLQ append.
  if peer=='native':
   for i in (2,3):nodes[i].send_signal(signal.SIGSTOP)
  else:
   for name in containers[1:]:call(['docker','pause',name],f'{peer}-{name}-pause.txt')
  timeline['state_quorum_stopped_ns']=time.time_ns();(gate/'release').touch()
  # Require a real phase-two failure in the running source broker, not elapsed time.
  def failure_text():
   if peer=='native':return '\n'.join(x.read_text() for x in r.glob('native1-broker-*.txt'))
   return subprocess.run(['docker','logs',containers[0]],capture_output=True,text=True).stdout
  def failed():
   text=failure_text()
   return ('share-partition state persist failed' in text) if peer=='native' else ('Persister broker=1' in text and 'Cancelled in-flight WRITE_SHARE_GROUP_STATE request' in text)
  try:wait(failed,180)
  finally:(r/f'{peer}-source-failure.txt').write_text(failure_text())
  timeline['persister_failure_observed_ns']=time.time_ns()
  if peer=='native':
   nodes[1].kill();nodes[1].wait();timeline['kill_completed_ns']=time.time_ns()
   for i in (2,3):nodes[i].send_signal(signal.SIGCONT)
   launch_native(1)
  else:
   call(['docker','kill','--signal','KILL',containers[0]],f'{peer}-kill.txt');timeline['kill_completed_ns']=time.time_ns()
   for name in containers[1:]:call(['docker','unpause',name],f'{peer}-{name}-unpause.txt')
   call(['docker','start',containers[0]],f'{peer}-restart.txt')
  (r/f'{peer}-timeline.json').write_text(json.dumps(timeline,indent=2));wait(listening);java(peer,'resume');java(peer,'count');(r/f'{peer}-recovery-count.txt').write_text((r/f'{peer}-count.txt').read_text())
  count=int((r/f'{peer}-count.txt').read_text().split('COUNT dlq_records=')[-1].split()[0]);assert count in ((2,) if peer=='native' else (1,2)),count
  java(peer,'verify',props=[f'-Ddlq.expected.count={count}'])
  for i in ports:call(['java','-cp',cp,'org.apache.kafka.tools.DumpLogSegments','--files',r/f'{peer}{i}/data/__share_group_state-0/00000000000000000000.log','--share-group-state-decoder'],f'{peer}{i}-state-after.txt')
  results[peer]='PASS';(r/'results.json').write_text(json.dumps(results,indent=2));print(peer,'PASS',flush=True)
 finally:
  for name in containers[1:]:subprocess.run(['docker','unpause',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
  for proc in processes:
   if proc.poll() is None:proc.send_signal(signal.SIGCONT)
  for name in containers:subprocess.run(['docker','stop','-t','15',name],stdout=subprocess.DEVNULL)
  for proc in processes:
   if proc.poll() is None:proc.terminate()
  for proc in processes:
   try:proc.wait(timeout=20)
   except subprocess.TimeoutExpired:proc.kill();proc.wait()
