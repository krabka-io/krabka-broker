"""The locally loaded krabka broker image, shared by its two consumers.

//packaging assembles the image and loads it under `KRABKA_IMAGE_TAG`;
//bazel/defs.bzl hands the same tarball and the same tag to a container-driven
Rust suite that runs brokers as real processes out of it. Neither can load a
constant from the other's `BUILD.bazel`, so the pair lives here.
"""

# What `//packaging:image_load` tags the image with locally, and the name a
# suite passes to `docker run`. A push supplies its own tags.
KRABKA_IMAGE_TAG = "docker.io/krabka-io/krabka-broker:dev"

# The `docker load`-able tarball that tag comes out of, in the shape
# //bazel:docker_test.sh already consumes for the Kafka images.
KRABKA_IMAGE_TAR = "//packaging:image_tar"
