//! A schema closure compiled once and reused for every record that names it.
//!
//! Turning schema text into something that can check a body — parsing Avro
//! JSON, resolving JSON Schema references and building a `jsonschema`
//! validator, or parsing and linking `.proto` files into a descriptor pool —
//! costs far more than the check itself. A cached
//! [`SchemaBody`][super::cache::SchemaBody] is immutable for the life of its
//! cache entry, because a registry never changes what a schema id names, so
//! the compiled form is built on the first `Full` check of that entry and
//! shared by every later one. It goes when the entry does: a TTL expiry or an
//! LRU eviction drops the body and its compiled form together.
//!
//! A compile failure is cached as well, as the message it produced, and is
//! reported at the same point of the check, with the same text, as when the
//! schema was compiled on every record.

use {
    apache_avro::{
        Schema as AvroSchema,
        reader::datum::{GenericDatumReader, GenericDatumReaderBuilder},
    },
    jsonschema::uri,
    krabka_schema_serde::{error::SchemaSerdeError, subject::SchemaKind},
    prost_reflect::{
        DescriptorPool, DynamicMessage, FileDescriptor, MessageDescriptor,
        prost_types::FileDescriptorSet,
    },
    std::{collections::HashMap, error::Error},
};

/// The name the root `.proto` file is parsed under, as
/// `krabka_schema_serde` names it.
const ROOT_PROTO: &str = "schema.proto";

/// One schema closure in the form a body check consumes.
#[derive(derive_more::Debug)]
pub(super) enum CompiledSchema {
    /// The parsed Avro writer schema, or the message its parse failed with.
    Avro(Result<AvroSchemata, String>),
    /// The built JSON Schema validator, or the stage that failed to build it.
    Json(#[debug(skip)] CompiledJson),
    /// The linked Protobuf descriptor pool, or the message its parse or link
    /// failed with.
    Protobuf(Result<ProtobufFiles, String>),
}

/// A linked Protobuf descriptor pool and the root file in it.
#[derive(Debug)]
pub(super) struct ProtobufFiles {
    /// The file a message-index walks. `None` when the pool holds no such
    /// file, which is reported per record, after the link, as before.
    root: Option<FileDescriptor>,
    /// Whether references were linked in. It selects which of
    /// `krabka_schema_serde`'s two messages a missing root file reports.
    with_references: bool,
}

/// A parsed Avro writer schema and the schemata its references name.
#[derive(Debug)]
pub(super) struct AvroSchemata {
    writer: AvroSchema,
    /// `None` when the schema has no references. The reader then resolves the
    /// writer schema alone, as `krabka_schema_serde`'s `validate_avro` does.
    imported: Option<Vec<AvroSchema>>,
}

/// A JSON Schema closure, split by where a failure surfaces in the check.
///
/// The schema text is parsed before the body and the validator is built
/// after it, so a body that is not JSON reports that ahead of a reference
/// that does not resolve. Keeping the two failures apart keeps that order.
pub(super) enum CompiledJson {
    /// The schema text is not JSON. Reported before the body is read.
    UnparsedSchema(String),
    /// The schema parsed, but its base URI, a reference or the validator
    /// could not be built. Reported after the body parses.
    Unbuildable(String),
    Ready(Box<jsonschema::Validator>),
}

impl CompiledSchema {
    /// Compile `schema` and its references for `kind`.
    pub(super) fn compile(
        kind: SchemaKind,
        schema: &str,
        references: &HashMap<String, String>,
        reference_order: &[String],
    ) -> Self {
        match kind {
            SchemaKind::Avro => Self::Avro(compile_avro(schema, references, reference_order)),
            SchemaKind::Json => Self::Json(compile_json(schema, references)),
            SchemaKind::Protobuf => Self::Protobuf(compile_protobuf(schema, references)),
        }
    }

    /// Check `body` against this closure. Only Protobuf reads
    /// `message_index`.
    pub(super) fn validate(
        &self,
        message_index: &[i32],
        body: &[u8],
    ) -> Result<(), SchemaSerdeError> {
        match self {
            Self::Avro(compiled) => {
                let compiled = compiled
                    .as_ref()
                    .map_err(|message| SchemaSerdeError::Schema(message.clone()))?;
                validate_avro(compiled, body)
            }
            Self::Json(compiled) => validate_json(compiled, body),
            Self::Protobuf(compiled) => {
                let compiled = compiled
                    .as_ref()
                    .map_err(|message| SchemaSerdeError::Schema(message.clone()))?;
                validate_protobuf(compiled, message_index, body)
            }
        }
    }
}

fn compile_avro(
    schema: &str,
    references: &HashMap<String, String>,
    reference_order: &[String],
) -> Result<AvroSchemata, String> {
    if references.is_empty() {
        let writer = AvroSchema::parse_str(schema).map_err(|error| error.to_string())?;
        return Ok(AvroSchemata {
            writer,
            imported: None,
        });
    }
    let sources: Vec<_> = reference_order
        .iter()
        .filter_map(|name| references.get(name).map(String::as_str))
        .collect();
    let (writer, imported) =
        AvroSchema::parse_str_with_list(schema, sources).map_err(|error| error.to_string())?;
    Ok(AvroSchemata {
        writer,
        imported: Some(imported),
    })
}

fn validate_avro(compiled: &AvroSchemata, body: &[u8]) -> Result<(), SchemaSerdeError> {
    let mut cursor = body;
    // The reader borrows the schemata, so it is built per record; resolving
    // the names of an already parsed schema is cheap next to parsing it.
    let builder = GenericDatumReader::builder(&compiled.writer);
    let reader = match &compiled.imported {
        Some(imported) => builder
            .writer_schemata(imported.iter().collect())
            .and_then(GenericDatumReaderBuilder::build),
        None => builder.build(),
    };
    reader
        .and_then(|reader| reader.read_value(&mut cursor))
        .map_err(|error| SchemaSerdeError::Deserialize(format!("avro body: {error}")))?;
    // `read_value` reads one datum and stops. Bytes after it are not part of
    // any datum this schema describes, so the body is not an instance of it.
    if cursor.is_empty() {
        Ok(())
    } else {
        Err(SchemaSerdeError::Deserialize(format!(
            "avro body: {} trailing byte(s) after the datum",
            cursor.len()
        )))
    }
}

/// Parse and link the root file and its references.
///
/// The steps, their order and their messages are `krabka_schema_serde`'s:
/// every reference is parsed, in the map's iteration order, before the root,
/// and the first failure is the one reported. The map is the cached entry's
/// own, so that order is the one every record of the entry used to see.
fn compile_protobuf(
    schema: &str,
    references: &HashMap<String, String>,
) -> Result<ProtobufFiles, String> {
    let parse = |name: &str, source: &str| {
        protox_parse::parse(name, source).map_err(|error| format!("protobuf schema: {error}"))
    };
    let mut files = Vec::with_capacity(references.len() + 1);
    for (name, source) in references {
        files.push(parse(name, source)?);
    }
    files.push(parse(ROOT_PROTO, schema)?);
    let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: files })
        .map_err(|error| format!("protobuf link: {error}"))?;
    let with_references = !references.is_empty();
    let root = if with_references {
        pool.get_file_by_name(ROOT_PROTO)
    } else {
        // One file went into the pool, so its messages are the ones to walk.
        pool.files().next()
    };
    Ok(ProtobufFiles {
        root,
        with_references,
    })
}

fn validate_protobuf(
    compiled: &ProtobufFiles,
    message_index: &[i32],
    body: &[u8],
) -> Result<(), SchemaSerdeError> {
    // An empty path is Confluent's implicit `[0]`.
    let path = if message_index.is_empty() {
        &[0][..]
    } else {
        message_index
    };
    let missing =
        || SchemaSerdeError::Schema(format!("protobuf message-index {path:?} names no message"));
    let root = match &compiled.root {
        Some(root) => root,
        None if compiled.with_references => {
            return Err(SchemaSerdeError::Schema(
                "protobuf root file missing".into(),
            ));
        }
        None => return Err(missing()),
    };
    let descriptor = message_at(root, path).ok_or_else(missing)?;
    DynamicMessage::decode(descriptor, body)
        .map_err(|error| SchemaSerdeError::Deserialize(format!("protobuf body: {error}")))?;
    Ok(())
}

/// Walk a Confluent message-index path to the message it names.
///
/// The first index is into the root file's top-level messages, and each later
/// one into the nested messages of the one before.
fn message_at(root: &FileDescriptor, path: &[i32]) -> Option<MessageDescriptor> {
    let (&first, rest) = path.split_first()?;
    let mut current = root.messages().nth(usize::try_from(first).ok()?)?;
    for &step in rest {
        let child = current.child_messages().nth(usize::try_from(step).ok()?)?;
        current = child;
    }
    Some(current)
}

fn compile_json(schema: &str, references: &HashMap<String, String>) -> CompiledJson {
    let writer: serde_json::Value = match serde_json::from_str(schema) {
        Ok(writer) => writer,
        Err(error) => return CompiledJson::UnparsedSchema(error.to_string()),
    };
    match build_json_validator(&writer, references) {
        Ok(validator) => CompiledJson::Ready(Box::new(validator)),
        Err(message) => CompiledJson::Unbuildable(message),
    }
}

fn build_json_validator(
    writer: &serde_json::Value,
    references: &HashMap<String, String>,
) -> Result<jsonschema::Validator, String> {
    let base_uri = uri::from_str(
        writer
            .get("$id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(""),
    )
    .map_err(|error| error.to_string())?;
    let references = references
        .iter()
        .map(|(name, source)| {
            let schema = serde_json::from_str(source).map_err(|error| error.to_string())?;
            let uri = uri::resolve_against(&base_uri.borrow(), name)
                .map_err(|error| error.to_string())?;
            Ok((uri.to_string(), schema))
        })
        .collect::<Result<HashMap<_, _>, String>>()?;
    jsonschema::options()
        .with_retriever(CachedJsonSchemas(references))
        .build(writer)
        .map_err(|error| error.to_string())
}

fn validate_json(compiled: &CompiledJson, body: &[u8]) -> Result<(), SchemaSerdeError> {
    if let CompiledJson::UnparsedSchema(message) = compiled {
        return Err(SchemaSerdeError::Schema(message.clone()));
    }
    let instance: serde_json::Value = serde_json::from_slice(body)
        .map_err(|error| SchemaSerdeError::Deserialize(error.to_string()))?;
    match compiled {
        CompiledJson::Ready(validator) => validator.validate(&instance).map_err(|error| {
            SchemaSerdeError::Deserialize(format!("json schema validation: {error}"))
        }),
        CompiledJson::UnparsedSchema(message) | CompiledJson::Unbuildable(message) => {
            Err(SchemaSerdeError::Schema(message.clone()))
        }
    }
}

/// The registry-cached sources a JSON Schema's references resolve to. A
/// reference outside them fails the build rather than reaching the network.
#[derive(Debug)]
struct CachedJsonSchemas(HashMap<String, serde_json::Value>);

impl jsonschema::Retrieve for CachedJsonSchemas {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
        self.0.get(uri.as_str()).cloned().ok_or_else(|| {
            format!("JSON Schema reference {uri} is not present in the registry cache").into()
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_schema_serde::format::validate::{validate_body, validate_protobuf_with_references},
    };

    const ORDER_AVRO: &str =
        r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;
    const ORDER_JSON: &str =
        r#"{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}"#;
    /// Parses as JSON, but its reference is not in the registry cache, so no
    /// validator can be built from it.
    const DANGLING_JSON: &str = r#"{"$ref":"missing.json"}"#;

    /// Which way a check went, without the message.
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Pass,
        Schema,
        Deserialize,
        /// Any other error kind, which no check here should return.
        Other,
    }

    fn outcome(result: &Result<(), SchemaSerdeError>) -> Outcome {
        match result {
            Ok(()) => Outcome::Pass,
            Err(SchemaSerdeError::Schema(_)) => Outcome::Schema,
            Err(SchemaSerdeError::Deserialize(_)) => Outcome::Deserialize,
            Err(_) => Outcome::Other,
        }
    }

    /// Validate `body` twice against one compiled closure, and require the two
    /// answers to agree word for word: a cached compile result must not
    /// change the second record's verdict or message.
    fn validate_twice(kind: SchemaKind, schema: &str, body: &[u8]) -> Result<(), SchemaSerdeError> {
        let references = HashMap::new();
        let compiled = CompiledSchema::compile(kind, schema, &references, &[]);
        let first = compiled.validate(&[], body);
        let second = compiled.validate(&[], body);
        check!(
            format!("{first:?}") == format!("{second:?}"),
            "{kind:?} {schema}"
        );
        first
    }

    #[yare::parameterized(
        avro_datum = { ORDER_AVRO, &[0x02, b'a'] },
        avro_trailing_bytes = { ORDER_AVRO, &[0x02, b'a', 0xFF] },
        avro_truncated = { ORDER_AVRO, &[0x02] },
        avro_unparseable_schema = { "{not avro}", &[0x02, b'a'] },
    )]
    fn avro_without_references_answers_as_the_uncached_check_does(schema: &str, body: &[u8]) {
        let cached = validate_twice(SchemaKind::Avro, schema, body);
        let uncached = validate_body(SchemaKind::Avro, schema, &[], body);
        check!(format!("{cached:?}") == format!("{uncached:?}"));
    }

    #[yare::parameterized(
        instance = { ORDER_JSON, br#"{"id":"a"}"#, Outcome::Pass },
        not_an_instance = { ORDER_JSON, br#"{"id":1}"#, Outcome::Deserialize },
        body_not_json = { ORDER_JSON, b"plain text", Outcome::Deserialize },
        // The schema text is parsed before the body, so its failure wins.
        unparsed_schema_and_body = { "{not json", b"plain text", Outcome::Schema },
        // The validator is built after the body parses, so a body that is not
        // JSON is what this reports.
        unbuildable_schema_and_bad_body = { DANGLING_JSON, b"plain text", Outcome::Deserialize },
        unbuildable_schema = { DANGLING_JSON, b"{}", Outcome::Schema },
    )]
    fn json_failures_surface_in_the_order_the_check_reads_them(
        schema: &str,
        body: &[u8],
        want: Outcome,
    ) {
        let result = validate_twice(SchemaKind::Json, schema, body);
        check!(outcome(&result) == want, "{result:?}");
    }

    #[test]
    fn a_json_mismatch_names_the_schema_validation() {
        let result = validate_twice(SchemaKind::Json, ORDER_JSON, br#"{"id":1}"#);
        let message = result.map_err(|error| error.to_string()).unwrap_err();
        check!(
            message.starts_with("deserialize error: json schema validation: "),
            "{message}"
        );
    }

    #[test]
    fn avro_references_resolve_from_the_cached_sources() {
        let references = HashMap::from([(
            "Base".to_owned(),
            r#"{"type":"record","name":"Base","fields":[{"name":"id","type":"string"}]}"#
                .to_owned(),
        )]);
        let order = ["Base".to_owned()];
        let schema = r#"{"type":"array","items":"Base"}"#;
        let compiled = CompiledSchema::compile(SchemaKind::Avro, schema, &references, &order);

        let ok = compiled.validate(&[], &[0x02, 0x02, b'a', 0x00]);
        check!(outcome(&ok) == Outcome::Pass, "{ok:?}");
        let trailing = compiled.validate(&[], &[0x00, 0x00]);
        check!(outcome(&trailing) == Outcome::Deserialize, "{trailing:?}");
    }

    const ORDER_PROTO: &str = r#"
        syntax = "proto3";
        message Order {
          string id = 1;
          message Line { string sku = 1; }
        }
        message Other { int32 n = 1; }
    "#;
    const MONEY_PROTO: &str =
        "syntax = \"proto3\"; package money; message Money { int64 cents = 1; }";
    const PRICED_PROTO: &str =
        "syntax = \"proto3\"; import \"money.proto\"; message Order { money.Money total = 1; }";

    /// `Order { id: "a" }`, and equally `Order.Line { sku: "a" }`.
    const ID_A: &[u8] = &[0x0A, 0x01, b'a'];
    /// `Order { total: Money { cents: 19 } }` of `PRICED_PROTO`.
    const PRICED: &[u8] = &[0x0A, 0x02, 0x08, 0x13];

    /// Check one Protobuf body twice against one compiled closure, and
    /// require both answers to equal, word for word, what
    /// `krabka_schema_serde` answers when it compiles the schema per call.
    fn check_protobuf_parity(
        schema: &str,
        references: &[(&str, &str)],
        message_index: &[i32],
        body: &[u8],
    ) -> Result<(), SchemaSerdeError> {
        let references: HashMap<String, String> = references
            .iter()
            .map(|&(name, source)| (name.to_owned(), source.to_owned()))
            .collect();
        let compiled = CompiledSchema::compile(SchemaKind::Protobuf, schema, &references, &[]);
        let first = compiled.validate(message_index, body);
        let second = compiled.validate(message_index, body);
        let uncached = if references.is_empty() {
            validate_body(SchemaKind::Protobuf, schema, message_index, body)
        } else {
            validate_protobuf_with_references(schema, &references, message_index, body)
        };
        check!(format!("{first:?}") == format!("{second:?}"));
        check!(format!("{first:?}") == format!("{uncached:?}"));
        first
    }

    #[yare::parameterized(
        first_message = { &[0], ID_A, Outcome::Pass },
        implicit_first_message = { &[], ID_A, Outcome::Pass },
        nested_message = { &[0, 0], ID_A, Outcome::Pass },
        wrong_wire_type = { &[1], ID_A, Outcome::Deserialize },
        not_a_message = { &[0], &[0x00, 0x01], Outcome::Deserialize },
        no_such_message = { &[9], ID_A, Outcome::Schema },
        no_such_nested_message = { &[0, 9], ID_A, Outcome::Schema },
        negative_index = { &[-1], ID_A, Outcome::Schema },
    )]
    fn protobuf_without_references_answers_as_the_uncached_check_does(
        message_index: &[i32],
        body: &[u8],
        want: Outcome,
    ) {
        let result = check_protobuf_parity(ORDER_PROTO, &[], message_index, body);
        check!(outcome(&result) == want, "{result:?}");
    }

    #[yare::parameterized(
        unparseable = { "syntax = ", &[], &[0], Outcome::Schema },
        unlinkable = {
            "syntax = \"proto3\"; message A { Missing m = 1; }", &[], &[0], Outcome::Schema
        },
        linked_reference = { PRICED_PROTO, &[("money.proto", MONEY_PROTO)], &[0], Outcome::Pass },
        referenced_no_such_message = {
            PRICED_PROTO, &[("money.proto", MONEY_PROTO)], &[5], Outcome::Schema
        },
        unparseable_reference = { PRICED_PROTO, &[("money.proto", "syntax = ")], &[0], Outcome::Schema },
        unresolved_import = { PRICED_PROTO, &[("other.proto", MONEY_PROTO)], &[0], Outcome::Schema },
    )]
    fn protobuf_schema_failures_answer_as_the_uncached_check_does(
        schema: &str,
        references: &[(&str, &str)],
        message_index: &[i32],
        want: Outcome,
    ) {
        let result = check_protobuf_parity(schema, references, message_index, PRICED);
        check!(outcome(&result) == want, "{result:?}");
    }

    #[test]
    fn a_referenced_protobuf_body_that_does_not_decode_is_a_body_error() {
        let result = check_protobuf_parity(PRICED_PROTO, &[("money.proto", MONEY_PROTO)], &[0], &[
            0x00, 0x01,
        ]);
        check!(outcome(&result) == Outcome::Deserialize, "{result:?}");
    }
}
