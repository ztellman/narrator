(ns narrator.operators.streaming
  (:use
    [narrator.core])
  (:require
    [clojure.core.reducers :as r]
    [narrator.utils
     [math :as m]])
  (:import
    [com.clearspring.analytics.stream.membership
     BloomFilter]
    [com.clearspring.analytics.stream.cardinality
     HyperLogLogPlus]
    [com.clearspring.analytics.stream.quantile
     QDigest]))

(defn ^QDigest digest-union
  ([a]
     a)
  ([^QDigest a ^QDigest b]
     (QDigest/unionOf a b)))

(defn-operator quantiles
  ([]
     (quantiles nil))
  ([{:keys [quantiles clear-on-reset? compression-factor]
     :or {quantiles [0.5 0.9 0.95 0.99 0.999]
          compression-factor 1e4
          clear-on-reset? true}}]
     (stream-aggregator-generator
       :ordered? true
       :emit (fn [^QDigest digest]
               (try
                 (zipmap quantiles (map #(.getQuantile digest %) quantiles))
                 (catch IndexOutOfBoundsException _
                   {})))
       :combine #(reduce digest-union %)
       :create (fn []
                 (let [digest (atom (QDigest. compression-factor))]
                   (stream-aggregator
                     :process (fn [ns]
                                (let [^QDigest digest @digest]
                                  (doseq [n ns]
                                    (.offer digest (long n)))))
                     :deref (fn []
                              @digest)
                     :reset (fn []
                              (when clear-on-reset?
                                (reset! digest (QDigest. compression-factor))))))))))

(defn hll-precision ^long [^double error]
  (long
    (Math/ceil
      (m/log2
        (/ 1.08
          (* error error))))))

(defn-operator cardinality-by
  ([facet]
     (cardinality-by facet nil))
  ([facet
    {:keys [clear-on-reset? error]
     :or {clear-on-reset? true
          error 0.01}}]
     (stream-aggregator-generator
       :ordered? true
       :emit (fn [^HyperLogLogPlus hll]
               (.cardinality hll))
       :combine (fn [s]
                  (if (= 1 (count s))
                    (first s)
                    (.merge ^HyperLogLogPlus (first s) (into-array (rest s)))))
       :create (fn []
                 (let [hll (atom (HyperLogLogPlus.
                                   (hll-precision error)
                                   (hll-precision (/ error 2))))]
                   (stream-aggregator
                     :process (fn [msgs]
                                (doseq [x msgs]
                                  (.offer ^HyperLogLogPlus @hll (facet x))))
                     :deref (fn []
                              @hll)
                     :reset (fn []
                              (when clear-on-reset?
                                (reset! hll (HyperLogLogPlus.
                                              (hll-precision error)
                                              (hll-precision (/ error 2))))))))))))

(defn-operator distinct-by
  "Filters out duplicate messages, based on the value returned by `(facet msg)`, which must
   be a keyword or string.

   This is an approximate filtering, using Bloom filters.  This means that some elements
   (by default ~1%) will be incorrectly filtered out.  Using these appropriately means
   setting the `error`, which is the proportion of false positives from 0 to 1, and
   `cardinality`, which is the maximum expected unique facets.

   If `clear-on-reset?` is true, messages will ony be distinct within a given period.  If
   not, they're distinct over the lifetime of the stream."
  ([facet]
     (distinct-by facet nil))
  ([facet
    {:keys [clear-on-reset? error cardinality]
     :or {clear-on-reset? true
          error 0.01
          cardinality 1e5}}]
     (stream-processor-generator
       :ordered? true
       :create (fn []
                 (let [bloom (atom (BloomFilter. (int cardinality) (double error)))]
                   (stream-processor
                     :reset #(when clear-on-reset?
                               (reset! bloom (BloomFilter. (int cardinality) (double error))))
                     :reducer (r/filter
                                (fn [msg]
                                  (let [^BloomFilter b @bloom
                                        f (name (facet msg))]
                                    (if (.isPresent b f)
                                      false
                                      (do
                                        (.add b f)
                                        true)))))))))))
