# Native Streams assignor qualification

Baseline: released Apache Kafka 4.4.0. The native embedding API is intentionally
not a JVM class loader. Released-JVM custom-plugin interoperability remains a
separate qualification lane; this artifact alone does not close #1971.

## Released contract

- [Registry order and validation](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorConfig.java): first implementation is the default, names identify registered implementations and ambiguous registration is rejected.
- [Group selection and fallback](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java): `streamsGroupTaskAssignor` chooses a registered override and falls back to the broker default when a persisted override is unavailable.
- KIP-1357 is describe-name reporting, not proof of custom implementation execution.

## Executed checks

- `cargo test -p krabka-broker --lib task_assignor_registry`: **7 passed**, including distinct assignment placement, selection, cooperative reassignment and retaining the previous target on failure.
- `cargo test -p krabka-broker --lib native_registry_stateright`: **passed**, all **36 unique states** (3 implementations including one failure, 4 member sets, 3 task universes). Every transition runs the production registry, override validation and output validator. It requires exact selected placement, or rejection for unknown members/failing implementations.
- `cargo test -p krabka-verified --lib streams_assignment`: **1 passed**, including exact replica-limit and i32-boundary cases.
- `cargo clippy -p krabka-broker --all-targets -- -D warnings`: **passed**.
- `aspect check-cpd`: **passed**, zero clones.

The finite registry model is exhaustive within its configuration domain; it is
not a distributed coordinator or replicated transaction model. Existing actor
regressions supply the cooperative handoff/previous-target retention evidence.

## Formal boundary

The production output validator calls
`krabka_verified::streams_assignment::next_standby_count`. Its precondition-free
contract admits exactly the next count when `0 <= current < maximum`, rejects
negative or full counters, and guarantees exact increment without i32 overflow.
Identity, task-universe and coordinator ownership observations are host inputs,
not conclusions of this arithmetic proof. Annotation presence alone is not
proof discharge: the repository requires matching Creusot 0.14/Why3 tooling.


Negative controls used isolated copies of the actual production kernels with
unchanged contracts: returning NONE (-1) instead of UNCERTAIN (-2) after a
failed store, and admitting a standby when `current == maximum`. Matching
Creusot 0.14 generated both faulty bodies and Why3find rejected **both mutated
verification conditions** (exit failure: 2 unproved files); the three unmodified
production conditions prove. These controls demonstrate kernel-contract
sensitivity, not completeness of the bounded actor model or Jepsen coverage.


Formal discharge: **1/1 VC proved**, `vc_next_standby_count`, Alt-Ergo0.007s,
using `ghcr.io/krabka-io/creusot:v0.14.0-4b4c83aaef59d968` (digest
`sha256:992bd1a7a51c58e188dc2b31a6eef36be106217dcf24450f9a4a52cfdeee561b`).
Artifacts: `verif/krabka_verified_rlib/streams_assignment/`. Generation used
`cargo creusot --package krabka-verified --output-dir target/streams-verif
--dry-run-why3find -- --ignore-rust-version`, then direct Why3find `prove -j 2
--summary --no-autodetect-provers` on the generated `.coma`, with
`DUNE_DIR_LOCATIONS=why3find:lib:/root/.local/share/creusot/share/why3find` and
`WHY3CONFIG=/root/.local/share/creusot/creusot_why3.conf`, exactly as the Cargo
Creusot dry-run specifies. No empty-selection or annotation-only run is counted.


The negative control changes only the production body comparison from
`current >= maximum` to `current > maximum`. With the original contract,
Why3find reports `Goal Coma.vc_next_standby_count: ✘ (2/4)` and exits nonzero;
the original production body proves in one VC. The companion failed-store
control reports `Goal Coma.vc_finish_store: ✘ (3/5)`. These are source-body
mutations, not removed preconditions or deliberately weakened specifications.


## Released executable component probe

`streams-assignor-released-component.json` records the real released JAR probe
from the pinned Apache Kafka 4.4.0 image (version read from AppInfoParser).
The probe constructs `GroupCoordinatorConfig` with the real `CONFIG_DEF`,
loads two custom implementations through Kafka's real registry and invokes each
actual `TaskAssignor.assign` callback. It measures default sticky selection,
ordered/default first selection, reversed order, distinct a/b placement for the
same two tasks, duplicate/built-in-name errors, and missing/wrong-class rejection.
The native profile retains sticky, so its matching reference registration is
`first,second,sticky`, recorded separately from a custom-only Java list.

This executable probe exposed an actual native restriction: Java accepts custom
assignors named `auto`, `highly_available` and `highly-available`, while the native
registry had incorrectly reserved its legacy aliases. The native guard now
reserves only Kafka's real built-in `sticky`; registered custom names take
precedence over legacy alias parsing, with a dedicated behavioral regression.
The probe also showed that a blank plugin-provided name is accepted. Native
registration now preserves that name and its actual assignment behavior, rather
than applying an unsupported stricter name policy. Duplicate names and sticky
are still rejected. Registration preserves names byte-for-byte; group override
values follow Kafka STRING trimming. This is not arbitrary JVM-plugin parity.

The prototype source and full raw output are retained by the worker handoff,
with source SHA256 in the snapshot. This is component-level registration and
callback evidence, not broker wire/group-override/reassignment qualification.
The coordinator's reusable differential lane still owns that broader result.


The actual released `GroupConfig` probe also distinguishes Java STRING trimming
from Rust Unicode trimming: ASCII space/tab around `auto` is removed, while
nonbreaking spaces around a custom name are retained. The native configuration
application and persisted-name availability check now use the same Java trimming
helper as validation. A real persisted-image regression checks that a registered
Unicode name is selected instead of silently falling back to the first assignor,
while unrelated group overrides remain intact. Blank and ASCII-padded plugin
names were also executed as released broker defaults (all tasks on member b).


After the released-component corrections, the focused registry suite passes
**9/9**, including byte-preserved names, legacy-alias precedence and persisted
Unicode selection. Full workspace all-targets Clippy passes again (3m03s).
The 36-state model and the proved standby kernel are unchanged by these
name-selection corrections. The broader wire/interoperability gate remains open.


## Callback error atomicity follow-up

Released `GroupMetadataManager.maybeUpdateStreamsTargetAssignment` wraps
`TaskAssignorException` in `UnknownServerException` before its coordinator
operation can append records. The previous native implementation swallowed the
callback error and could acknowledge a new member with a dirty assignment.
The native path now returns UNKNOWN_SERVER_ERROR with Kafka's epoch/message
shape, restores the pre-request actor state and writes no metadata records.
It keeps the actor alive so the same member can retry after backend repair.
Delayed assignment failure similarly restores prior state without converting a
plugin error into an offsets-log failure.

The real-actor regression checks failed new membership, unchanged durable cache
and append count, unchanged live membership, then successful repair/retry. A
second Stateright model enumerates all 2,801 operation histories up to depth4
with callback failure, repair, join, heartbeat, leave, coordinator reconstruction
and delayed delivery to retired handles. Its healthy operations must succeed;
errors must leave live and persisted membership unchanged. This is bounded
implementation-driven coverage, not a claim of replicated-log or unbounded
liveness verification. Final executed results are recorded below.


## Executed reference component source

The following exact source produced the recorded JSON; its SHA256 is retained
in the snapshot. This is a preserved component experiment, not a newly
registered repository task or a substitute for the differential lane. It was
compiled with `javac -cp "libs/*" AssignorProbe.java` and run with
`java -cp "libs/*:." AssignorProbe`, using the pinned image's `/opt/kafka/libs`.

```java
import java.util.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.api.streams.assignor.*;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AssignorProbe {
  public static class First implements TaskAssignor {
    public String name() { return "first"; }
    public GroupAssignment assign(GroupSpec spec, TopologyDescriber topology) {
      return new GroupAssignment(Map.of("a", new MemberAssignment(Map.of("sub", Set.of(0,1)), Map.of())));
    }
  }
  public static class Second implements TaskAssignor {
    public String name() { return "second"; }
    public GroupAssignment assign(GroupSpec spec, TopologyDescriber topology) {
      return new GroupAssignment(Map.of("b", new MemberAssignment(Map.of("sub", Set.of(0,1)), Map.of())));
    }
  }
  public static class Duplicate extends First { public String name() { return "first"; } }
  public static class Reserved extends First { public String name() { return "sticky"; } }
  public static class Blank extends Second { public String name() { return ""; } }
  public static class Padded extends Second { public String name() { return " padded "; } }
  public static class LegacyAlias extends First { public String name() { return "auto"; } }
  public static class LegacyAvailable extends First { public String name() { return "highly_available"; } }
  public static class LegacyHyphen extends First { public String name() { return "highly-available"; } }
  static GroupCoordinatorConfig config(List<String> classes) {
    var props = new HashMap<String,Object>();
    if (classes != null) props.put(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNORS_CONFIG, classes);
    return new GroupCoordinatorConfig(new AbstractConfig(GroupCoordinatorConfig.CONFIG_DEF, props, false));
  }
  static Map<String,Object> registration(List<String> classes) {
    var result = new LinkedHashMap<String,Object>();
    try {
      var configured = config(classes).streamsGroupAssignors();
      result.put("accepted", true);
      result.put("ordered_names", configured.stream().map(TaskAssignor::name).toList());
    } catch (ConfigException e) {
      result.put("accepted", false);
      result.put("error", e.getClass().getSimpleName());
      result.put("message", e.getMessage());
    }
    return result;
  }
  static void require(boolean condition, String scenario) {
    if (!condition) throw new IllegalStateException("Failed: " + scenario);
  }
  public static void main(String[] args) throws Exception {
    var rows = new LinkedHashMap<String,Object>();
    rows.put("image", "apache/kafka@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f");
    rows.put("kafka_version", org.apache.kafka.common.utils.internals.AppInfoParser.getVersion());
    rows.put("java_runtime", System.getProperty("java.runtime.version"));
    rows.put("default", registration(null));
    String unicodeName = "\u00a0custom\u00a0";
    String ascii = new GroupConfig(Map.of("streams.assignor.name", " \tauto\t ")).streamsAssignorName().orElseThrow();
    String unicode = new GroupConfig(Map.of("streams.assignor.name", unicodeName)).streamsAssignorName().orElseThrow();
    require(ascii.equals("auto"), "ASCII trim");
    require(unicode.equals(unicodeName), "Unicode name preserved");
    rows.put("group_override_ascii", ascii);
    rows.put("group_override_unicode", unicode);
    rows.put("group_override_blank", new GroupConfig(Map.of("streams.assignor.name", "")).streamsAssignorName().orElse("<absent>"));
    var ordered = List.of(First.class.getName(), Second.class.getName());
    var registry = config(ordered).streamsGroupAssignors();
    require(registry.stream().map(TaskAssignor::name).toList().equals(List.of("first", "second")), "order");
    var first = registry.get(0).assign(null, null);
    var second = registry.get(1).assign(null, null);
    require(first.members().keySet().equals(Set.of("a")), "first placement");
    require(second.members().keySet().equals(Set.of("b")), "second placement");
    require(first.members().get("a").activeTasks().equals(Map.of("sub",Set.of(0,1))), "first tasks");
    require(second.members().get("b").activeTasks().equals(Map.of("sub",Set.of(0,1))), "second tasks");
    rows.put("ordered_registry", registration(ordered));
    rows.put("native_profile_registry", registration(List.of(First.class.getName(), Second.class.getName(), "sticky")));
    rows.put("first_default_member", first.members().keySet());
    rows.put("selected_second_member", second.members().keySet());
    rows.put("reverse_order", registration(List.of(Second.class.getName(), First.class.getName())));
    rows.put("duplicate_name", registration(List.of(First.class.getName(), Duplicate.class.getName())));
    rows.put("reserved_builtin_name", registration(List.of(Reserved.class.getName())));
    rows.put("unknown_class", registration(List.of("not.present.Assignor")));
    rows.put("wrong_class_type", registration(List.of(String.class.getName())));
    rows.put("empty_registration_list", registration(List.of()));
    rows.put("blank_plugin_name", registration(List.of(Blank.class.getName())));
    rows.put("blank_default_member", config(List.of(Blank.class.getName())).streamsGroupAssignors().get(0).assign(null,null).members().keySet());
    rows.put("padded_plugin_name", registration(List.of(Padded.class.getName())));
    rows.put("padded_default_member", config(List.of(Padded.class.getName())).streamsGroupAssignors().get(0).assign(null,null).members().keySet());
    rows.put("legacy_alias_plugin_name", registration(List.of(LegacyAlias.class.getName())));
    rows.put("legacy_available_plugin_name", registration(List.of(LegacyAvailable.class.getName())));
    rows.put("legacy_hyphen_plugin_name", registration(List.of(LegacyHyphen.class.getName())));
    for (String key : List.of("duplicate_name","reserved_builtin_name","unknown_class","wrong_class_type")) {
      require(Boolean.FALSE.equals(((Map<?,?>) rows.get(key)).get("accepted")), key);
    }
    rows.put("qualification_scope", "released component registration/execution; not broker wire/coordinator/plugin JVM parity");
    System.out.println(new ObjectMapper().writeValueAsString(rows));
  }
}

```


Latest executed callback-error verification: focused `task_assignor_registry`
suite **11/11 passed** (1.91s), including the **2,801-history** real-actor
Stateright model. The newly built broker unit-test executable's `streams`
filter passes **387/387** (4.49s), including the pre-existing reconciliation
model and wire/transaction handler regressions. Nightly workspace rustfmt
check, zero-clone CPD and diff whitespace checks pass. The 36-state registry
model and proved standby kernel are unchanged. Source-body negative controls
were executed for the formal kernels; no actor-model source-mutation run is
claimed.

Final corrected-model workspace gate: `cargo clippy --workspace --all-targets -- -D warnings` **passed** (4m05s, including the preceding test-build lock wait). No Clippy suppressions were added.
