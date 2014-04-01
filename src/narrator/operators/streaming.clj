(ns narrator.operators.streaming
  (:use
    [narrator.core])
  (:require
    [primitive-math :as p]
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
  "Gives the statistical distribution of values passed in, which must be numerical.  The values
   emitted are a map of the quantile onto the value at the point in the statistical distribution.
   For instance, a map representing a median value of 50.0 would look like:

     {0.5 50.0}

   By default, `quantiles` will emit the 50th, 90th, 95th, 99th, and 99.9th percentiles.

   Since values are represented internally as integers, `quantiles` treats these values as fixed
   point numbers, with six decimal places of precision.  If more is required, this can be set
   via the `precision` key.

   By default, only values received in the last period will be represented.  If you wish to
   include all values seen since the beginning of the stream, set `clear-on-reset?` to `false`."
  ([]
     (quantiles nil))
  ([{:keys [quantiles clear-on-reset? compression-factor precision]
     :or {quantiles [0.5 0.9 0.95 0.99 0.999]
          compression-factor 1e4
          precision 6
          clear-on-reset? true}}]
     (let [scaling-factor (Math/pow 10 precision)]
       (stream-aggregator-generator
         :ordered? true
         :emit (fn [^QDigest digest]
                 (try
                   (zipmap
                     quantiles
                     (map #(p/div (double (.getQuantile digest %)) scaling-factor) quantiles))
                   (catch IndexOutOfBoundsException _
                     {})))
         :combine #(reduce digest-union %)
         :create (fn [options]
                   (let [digest (atom (QDigest. compression-factor))]
                     (stream-aggregator
                       :process (fn [ns]
                                  (let [^QDigest digest @digest]
                                    (doseq [n ns]
                                      (.offer digest (long (p/* scaling-factor (double n)))))))
                       :deref (fn []
                                @digest)
                       :reset (fn []
                                (when clear-on-reset?
                                  (reset! digest (QDigest. compression-factor)))))))))))

(defn hll-precision ^long [^double error]
  (long
    (Math/ceil
      (m/log2
        (/ 1.08
          (* error error))))))

(defn-operator quasi-cardinality
  "Gives the approximate cardinality of unique values, which must be strings, keywords, bytes,
   or numbers.  The memory required for tracking this value is O(log log n), with a constant
   factor controlled by the desired level of error, which defaults to 1%.

   If `clear-on-reset?` is true, the set of tracked values will be emptied, and the cardinality
   will be reset to 0."
  ([]
     (quasi-cardinality nil))
  ([{:keys [clear-on-reset? error]
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
       :create (fn [options]
                 (let [hll (atom (HyperLogLogPlus.
                                   (hll-precision error)
                                   (hll-precision (/ error 2))))]
                   (stream-aggregator
                     :process (fn [msgs]
                                (doseq [x msgs]
                                  (.offer ^HyperLogLogPlus @hll
                                    (if (keyword? x)
                                      (name x)
                                      x))))
                     :deref (fn []
                              @hll)
                     :reset (fn []
                              (when clear-on-reset?
                                (reset! hll (HyperLogLogPlus.
                                              (hll-precision error)
                                              (hll-precision (/ error 2))))))))))))

(defn-operator quasi-distinct-by
  "Filters out duplicate messages, based on the value returned by `(facet msg)`, which must
   be a keyword or string.

   This is an approximate filtering, using Bloom filters.  This means that some elements
   (by default ~1%) will be incorrectly filtered out.  Using these appropriately means
   setting the `error`, which is the proportion of false positives from 0 to 1, and
   `cardinality`, which is the maximum expected unique facets.

   If `clear-on-reset?` is true, messages will ony be distinct within a given period.  If
   not, they're distinct over the lifetime of the stream."
  ([facet]
     (quasi-distinct-by facet nil))
  ([facet
    {:keys [clear-on-reset? error cardinality]
     :or {clear-on-reset? true
          error 0.01
          cardinality 1e6}}]
     (stream-processor-generator
       :ordered? true
       :create (fn [options]
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
