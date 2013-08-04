(ns narrator.utils.bloom-filter
  (:refer-clojure :exclude [contains?])
  (:use
    [potemkin])
  (:require
    [narrator.utils
     [locks :as l]]
    [clojure.core.reducers :as r]
    [primitive-math :as p]
    [byte-streams :as bs]
    [byte-transforms :as bt])
  (:import
    [java.util
     BitSet]
    [java.nio
     ByteBuffer]))

(p/use-primitive-operators)

(definterface+ IBloomFilter
  (^int num-hashes [_] "Returns the number of hash functions used by the Bloom filter.")
  (^:private contains?- [_ hashes])
  (add-all! [_ s] "Adds the elements in `s` to the set."))

(defn hashes
  "Returns an array of at least `num-hashes` 32-bit hashes based on `x`."
  [x ^long num-hashes]
  (let [x (bs/to-byte-buffer x)
        n (p/>> (+ num-hashes 3) 2)
        buf (ByteBuffer/allocate (p/<< n 4))]
    (dotimes [i n]
      (.put buf ^bytes (bt/hash x :murmur128 {:seed i})))
    (->> buf .array bt/hash->ints)))

(defn contains? [bloom x]
  (contains?- bloom (hashes x (num-hashes bloom))))

(defn bloom-filter-parameters [^long cardinality ^double error]
  (let [num-bits (long
                   (/ (* (double cardinality)
                        (Math/log (/ 1.0 error)))
                     (* (Math/log 2) (Math/log 2))))
        num-hashes (long
                     (/ (double (* (double num-bits) (Math/log 2)))
                       (double cardinality)))]
    [num-bits num-hashes]))

(definline ^:private truncated-hash [hash num-bits]
  `(Math/abs (rem (long ~hash) ~num-bits)))

(defn bloom-filter [^long cardinality ^double error]
  (let [[num-bits num-hashes] (bloom-filter-parameters cardinality error)
        num-hashes (long num-hashes)
        num-bits (long num-bits)
        lock (l/asymmetric-lock)
        bitset (BitSet. num-bits)]
    (reify
      clojure.lang.Counted

      ;; an approximate value only
      ;; http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter
      (count [_]
        (int
          (/ (* (- (double num-bits))
               (Math/log
                 (- 1.0
                   (/ (double (.cardinality bitset))
                     (double num-bits)))))
            (double num-hashes))))
      
      IBloomFilter
      (num-hashes [_]
        num-hashes)
      (contains?- [_ hashes]
        (l/with-lock lock
          (loop [idx 0]
            (if (>= idx num-hashes)
              true
              (if (.get bitset (truncated-hash (aget ^ints hashes idx) num-bits))
                (recur (inc idx))
                false)))))
      (add-all! [_ s]
        (let [hashes (doall (map #(hashes % num-hashes) s))]
          (l/with-exclusive-lock lock
            (doseq [ary hashes]
              (dotimes [idx num-hashes]
                (.set bitset
                  (truncated-hash (aget ^ints ary idx) num-bits)
                  true)))))))))
