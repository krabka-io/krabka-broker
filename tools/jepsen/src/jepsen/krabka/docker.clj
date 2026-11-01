(ns jepsen.krabka.docker
  "A Jepsen remote that reaches each DB node through `docker exec` and
  `docker cp`, with the container named after the node.

  `aspect jepsen` starts one container per node, `n1` to `n5`, with the node
  name as both the container name and the hostname. Jepsen's own
  `jepsen.control.docker` finds a container by a published port instead, which
  these containers do not have, so this remote names it directly. Its
  `docker exec` also passes `-i` when a command has input: Jepsen writes files
  by piping them to `cat`, and without `-i` Docker drops the pipe. A download
  goes through a temporary directory: Jepsen's store paths hold the test's
  start time, `2026-10-31T23:30:29Z`, and `docker cp` reads a local path with a
  colon as another container."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [jepsen.control.core :as core]
            [jepsen.control.docker :as docker])
  (:import (java.nio.file CopyOption Files StandardCopyOption)
           (java.nio.file.attribute FileAttribute)))

(defn exec
  "Runs a shell command in a container, with the action's :in as its stdin."
  [container {:keys [cmd in]}]
  (if in
    (sh "docker" "exec" "-i" container "sh" "-c" cmd :in in)
    (sh "docker" "exec" container "sh" "-c" cmd)))

(defn download!
  "Copies files from a container to a local path, which may name a file or
  an existing directory."
  [container remote-paths local-path]
  (doseq [remote-path (flatten [remote-paths])]
    (let [tmp  (Files/createTempDirectory "jepsen-krabka-cp"
                                          (make-array FileAttribute 0))
          name (.getName (io/file remote-path))
          dest (let [f (io/file local-path)]
                 (if (.isDirectory f) (io/file f name) f))]
      (try
        (docker/cp-from container remote-path (str tmp))
        (io/make-parents dest)
        (Files/move (.resolve tmp ^String name) (.toPath dest)
                    (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))
        (finally
          (doseq [f (reverse (file-seq (.toFile tmp)))]
            (io/delete-file f true)))))))

(defrecord NamedContainerRemote [container]
  core/Remote
  (connect [this conn-spec]
    (assoc this :container (:host conn-spec)))

  (disconnect! [this]
    (dissoc this :container))

  (execute! [_ _ctx action]
    (exec container action))

  (upload! [_ _ctx local-paths remote-path _opts]
    (docker/cp-to container local-paths remote-path))

  (download! [_ _ctx remote-paths local-path _opts]
    (download! container remote-paths local-path)))

(def remote
  "Runs control commands in the container named after each node."
  (->NamedContainerRemote nil))
