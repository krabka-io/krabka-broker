import pathlib,subprocess,os,json
root=pathlib.Path(__file__).parent;classes='/home/matt/.openclaw/workspace/coordinator/.private/assignor-invalid-20261108/classes'; binary='/home/matt/.openclaw/workspace/coordinator/.private/streams-delete-legacy-target/debug/examples/streams_fault_broker'
(root/'oracle').mkdir(exist_ok=True);(root/'oracle').chmod(0o777)
record={}
for node in range(1,4):
 port=65000+(node-1)*2
 conf=f'''node.id={node}
process.roles=broker,controller
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{port+1}
advertised.listeners=PLAINTEXT://127.0.0.1:{port}
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.quorum.voters=1@127.0.0.1:65001,2@127.0.0.1:65003,3@127.0.0.1:65005
log.dirs=/evidence/data{node}
offsets.topic.replication.factor=3
offsets.topic.num.partitions=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
group.streams.session.timeout.ms=180000
group.streams.assignors=AssignorPlugin,AssignorPlugin$Last,AssignorPlugin$Failure,sticky
'''
 (root/'oracle'/f'broker{node}.properties').write_text(conf)
 cmd=['docker','run','-d','--name',f'assignor-movement-20261108-{node}','--network','host','-v',f'{root}/oracle:/evidence','-v',f'{classes}:/classes:ro','-e','KAFKA_HEAP_OPTS=-Xmx384m -Xms128m','--entrypoint','/bin/bash','apache/kafka:4.4.0','-c',f'export CLASSPATH=/classes; /opt/kafka/bin/kafka-storage.sh format --cluster-id MkU3OEVBNTcwNTJENDM2Qk --release-version 4.4-IV2 --feature streams.version=1 --config /evidence/broker{node}.properties && exec /opt/kafka/bin/kafka-server-start.sh /evidence/broker{node}.properties']
 record[f'oracle{node}']=subprocess.check_output(cmd,text=True).strip()
 env=os.environ.copy();env.update(STREAMS_FIXTURE_ASSIGNORS='1',STREAMS_FIXTURE_NODE=str(node),STREAMS_FIXTURE_VOTERS='1@127.0.0.1:65021,2@127.0.0.1:65023,3@127.0.0.1:65025')
 log=(root/f'native{node}.log').open('w');p=subprocess.Popen([binary,str(root/f'native{node}'),str(root/'flags'),f'127.0.0.1:{65020+(node-1)*2}'],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);record[f'native{node}']=p.pid
(root/'processes.json').write_text(json.dumps(record,indent=2))
