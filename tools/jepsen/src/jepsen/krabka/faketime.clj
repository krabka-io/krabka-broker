(ns jepsen.krabka.faketime
  "Clock faults for nodes that share one kernel clock, such as Docker
  containers.

  Jepsen's own clock nemesis steps the system clock with `settimeofday`. Every
  container on a host shares that clock, so it would skew every node, and the
  host, at once. Instead, each broker runs with libfaketime preloaded and reads
  its wall-clock offset from a file on its node. This nemesis rewrites that
  file. It takes the operations of Jepsen's clock nemesis, so Jepsen's clock
  generator, final reset and clock plot work unchanged:

    {:f :reset,  :value [node ...]}
    {:f :bump,   :value {node delta-ms, ...}}
    {:f :strobe, :value {node {:delta ms, :period ms, :duration s}, ...}}
    {:f :check-offsets}

  Only the wall clock moves. `FAKETIME_DONT_FAKE_MONOTONIC` leaves the
  monotonic clock alone, as stepping the system clock does. libfaketime
  re-reads the file at most once a second (`FAKETIME_CACHE_DURATION`), so a
  strobe faster than that shows the broker fewer flips than it makes."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [nemesis :as n]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.combined :as nc])
  (:import (java.util Locale)))

(def offset-file
  "Where a node keeps its broker's wall-clock offset, in libfaketime's
  relative format."
  "/opt/krabka/clock-offset")

(def library
  "libfaketime's thread-safe build, as Debian installs it."
  "/usr/lib/x86_64-linux-gnu/faketime/libfaketimeMT.so.1")

(def env
  "The environment a broker starts with to follow the offset file."
  {:LD_PRELOAD                   library
   :FAKETIME_TIMESTAMP_FILE      offset-file
   :FAKETIME_DONT_FAKE_MONOTONIC 1
   :FAKETIME_CACHE_DURATION      1})

(defn offset-string
  "An offset in milliseconds as libfaketime reads it: signed seconds with
  millisecond precision, `+0.250` or `-12.000`."
  [ms]
  (String/format Locale/ROOT "%+.3f" (object-array [(/ (double ms) 1000.0)])))

(defn parse-offset
  "The offset in a file's text, in milliseconds. An empty file is no offset."
  [text]
  (let [t (str/trim (str text))]
    (if (str/blank? t)
      0
      (Math/round (* 1000.0 (Double/parseDouble t))))))

(defn current-offset!
  "This node's offset in milliseconds."
  []
  (parse-offset (c/exec :cat offset-file)))

(defn set-offset!
  "Sets this node's offset in milliseconds. Returns the offset in seconds, the
  unit Jepsen's clock plot reads from :clock-offsets."
  [ms]
  (c/su (cu/write-file! (offset-string ms) offset-file))
  (/ (double ms) 1000.0))

(defn strobe!
  "Flips this node's offset between `delta` past its current value and back,
  every `period` ms for `duration` seconds, then restores it."
  [{:keys [delta period duration]}]
  (let [base     (current-offset!)
        deadline (+ (System/nanoTime) (* duration 1000000000))]
    (loop [high? true]
      (when (< (System/nanoTime) deadline)
        (set-offset! (if high? (+ base delta) base))
        (Thread/sleep (long period))
        (recur (not high?))))
    (set-offset! base)))

(defn nemesis
  "Moves each broker's wall clock through its offset file."
  []
  (reify n/Nemesis
    (setup! [this test]
      (c/with-test-nodes test (set-offset! 0))
      this)

    (invoke! [_ test op]
      (assoc op :clock-offsets
             (case (:f op)
               :reset         (c/on-nodes test (:value op)
                                          (fn [_ _] (set-offset! 0)))
               :check-offsets (c/on-nodes test
                                          (fn [_ _] (/ (current-offset!) 1000.0)))
               :bump          (let [m (:value op)]
                                (c/on-nodes test (keys m)
                                            (fn [_ node]
                                              (set-offset! (+ (current-offset!)
                                                              (get m node))))))
               :strobe        (let [m (:value op)]
                                (c/on-nodes test (keys m)
                                            (fn [_ node] (strobe! (get m node))))))))

    (teardown! [_ test]
      (c/with-test-nodes test (set-offset! 0)))

    n/Reflection
    (fs [_] #{:reset :bump :strobe :check-offsets})))

(defn package
  "Jepsen's clock package with this nemesis in place of the one that steps the
  system clock."
  [opts]
  (info "Clock faults move each broker's clock through libfaketime")
  (assoc (nc/clock-package opts)
         :nemesis (n/compose {{:reset-clock         :reset
                               :check-clock-offsets :check-offsets
                               :strobe-clock        :strobe
                               :bump-clock          :bump}
                              (nemesis)})))
