(ns manifold.stream.random-access
  {:no-doc true}
  (:require
    [clj-commons.primitive-math :as p]
    [manifold.deferred :as d]
    [manifold.stream.core :as s])
  (:import
    [java.util
     List]
    [java.util.concurrent.atomic
     AtomicLong]))

(set! *unchecked-math* true)

(s/def-source RandomAccessSource
  [^List list
   ^AtomicLong idx
   ^long size]

  (isSynchronous [_]
    true)

  (close [_]
    (.set idx size))

  (description [this]
    {:type     "random-access-list"
     :drained? (s/drained? this)})

  (take [this default-val blocking?]

    (let [idx' (.getAndIncrement idx)]
      (if (p/< idx' size)
        (let [val (.get list idx')]
          (if blocking?
            val
            (d/success-deferred val)))
        (do
          (.markDrained this)
          (if blocking?
            default-val
            (d/success-deferred default-val))))))

  (take [this default-val blocking? timeout timeout-val]
    (.take this default-val blocking?)))

(extend-protocol s/Sourceable

  java.util.RandomAccess
  (to-source [list]
    (->RandomAccessSource
      list
      (AtomicLong. 0)
      (.size ^List list))))
