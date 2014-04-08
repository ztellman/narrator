(ns narrator.operators.streaming
  (:use
    [narrator.core])
  (:require
    [byte-transforms :as bt]
    [byte-streams :as bs]
    [primitive-math :as p]
    [clojure.core.reducers :as r]
    [narrator.utils
     [math :as m]])
  (:import
    [com.clearspring.analytics.stream.membership
     BloomFilter]
    [com.clearspring.analytics.stream.cardinality
     HyperLogLogPlus
     HyperLogLogPlus$Builder]
    [com.clearspring.analytics.stream.frequency
     CountMinSketch]
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
          compression-factor 1e3
          precision 6
          clear-on-reset? true}}]
     (let [scaling-factor (Math/pow 10 precision)]
       (stream-aggregator-generator
         :concurrent? false
         :emit (fn [^QDigest digest]
                 (if digest
                   (try
                     (zipmap
                       quantiles
                       (map #(p/div (double (.getQuantile digest %)) scaling-factor) quantiles))
                     (catch IndexOutOfBoundsException _
                       {}))
                   {}))
         :combine #(reduce digest-union %)
         :create (fn [options]
                   (let [digest (atom (QDigest. compression-factor))]
                     (stream-aggregator
                       :serialize (fn [digest]
                                    (-> digest
                                      QDigest/serialize
                                      (bt/compress :bzip2)
                                      (bt/encode :base64)
                                      bs/to-string))
                       :deserialize (fn [x]
                                      (-> x
                                        (bt/decode :base64)
                                        (bt/decompress :bzip2)
                                        bs/to-byte-array
                                        QDigest/deserialize))
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
   factor controlled by the desired level of error, which defaults to 0.01, or 1%.

   If `clear-on-reset?` is true, the set of tracked values will be emptied, and the cardinality
   will be reset to 0."
  ([]
     (quasi-cardinality nil))
  ([{:keys [clear-on-reset? error]
     :or {clear-on-reset? true
          error 0.01}}]
     (stream-aggregator-generator
       :concurrent? false
       :emit (fn [^HyperLogLogPlus hll]
               (.cardinality hll))
       :combine (fn [s]
                  (if (= 1 (count s))
                    (first s)
                    (.merge
                      (-> ^HyperLogLogPlus (first s) .getBytes HyperLogLogPlus$Builder/build)
                      (into-array (rest s)))))
       :create (fn [options]
                 (let [hll (atom (HyperLogLogPlus.
                                   (hll-precision error)
                                   (hll-precision (/ error 2))))]
                   (stream-aggregator
                     :serialize (fn [^HyperLogLogPlus hll]
                                  (-> hll
                                    .getBytes
                                    (bt/compress :bzip2)
                                    (bt/encode :base64)
                                    bs/to-string))
                     :deserialize (fn [x]
                                    (-> x
                                      (bt/decode :base64)
                                      (bt/decompress :bzip2)
                                      bs/to-byte-array
                                      HyperLogLogPlus$Builder/build))
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

(defn-operator quasi-frequency-by
  "Returns a function which, given a string or keyword returned by `(facet msg)`, returns the
   approximate number of times that value was seen in the stream.

   If `clear-on-reset?` is true, the frequency counts will be zeroed out at the end of each period."
  ([facet]
     (quasi-frequency-by facet nil))
  ([facet
    {:keys [clear-on-reset? epsilon confidence]
     :or {clear-on-reset? true
          epsilon 0.005
          confidence 0.99}}]
     (stream-aggregator-generator
       :concurrent? false
       :emit (fn [^CountMinSketch cms]
               (fn [s]
                 (.estimateCount cms (name s))))
       :combine (fn [s]
                  (if (= 1 (count s))
                    (first s)
                    (CountMinSketch/merge (into-array s))))
       :create (fn [options]
                 (let [cms (atom (CountMinSketch.
                                   (double epsilon)
                                   (double confidence)
                                   (p/int (System/nanoTime))))]
                   (stream-aggregator
                     :serialize (fn [cms]
                                  (-> cms
                                    CountMinSketch/serialize
                                    (bt/compress :bzip2)
                                    (bt/encode :base64)
                                    bs/to-string))
                     :deserialize (fn [x]
                                    (-> x
                                      (bt/decode :base64)
                                      (bt/decompress :bzip2)
                                      bs/to-byte-array
                                      CountMinSketch/deserialize))
                     :process (fn [msgs]
                                (let [facet->count (->> msgs (map facet) frequencies)
                                      ^CountMinSketch cms @cms]
                                  (doseq [[k v] facet->count]
                                    (.add cms ^String (name k) (long v)))))
                     :deref (fn []
                              @cms)
                     :reset (fn []
                              (when clear-on-reset?
                                (reset! cms
                                  (CountMinSketch.
                                    (double epsilon)
                                    (double confidence)
                                    (p/int (System/nanoTime))))))))))))

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
       :concurrent? false
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
