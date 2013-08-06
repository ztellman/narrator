(ns narrator.operators.sampling
  (:use
    [narrator.core])
  (:require
    [narrator.utils.math :as m])
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
       :ordered? false
       :emitter (fn [^QDigest digest]
                  (try
                    (zipmap quantiles (map #(.getQuantile digest %) quantiles))
                    (catch IndexOutOfBoundsException _
                      {})))
       :combiner #(reduce digest-union %)
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
       :emitter (fn [^HyperLogLogPlus hll]
                  (.cardinality hll))
       :combiner (fn [s]
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
