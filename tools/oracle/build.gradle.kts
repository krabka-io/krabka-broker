plugins {
    java
    application
}

repositories { mavenCentral() }

dependencies {
    // Keep this version equal to the Kafka release the repository tests
    // against. That release is `apache/kafka:4.3.1`, pinned in
    // `MODULE.bazel` and `bazel/images/BUILD.bazel`, and named as the
    // oracle in `docs/KIP_MATRIX.md`.
    implementation("org.apache.kafka:kafka-clients:4.3.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.2")
    // Compression codec libraries. The `compress` and `decompress` ops need
    // them at compile time.
    implementation("org.xerial.snappy:snappy-java:1.1.10.8")
    implementation("com.github.luben:zstd-jni:1.5.7-16")
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

application { mainClass.set("com.krabka.oracle.Oracle") }
