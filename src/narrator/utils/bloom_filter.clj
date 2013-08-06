(ns narrator.utils.bloom-filter
  (:refer-clojure :exclude [contains?])
  (:use
    [potemkin])
  (:require
    [narrator.utils
     [locks :as l]]
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
  (add! [_ s] "Adds the element `x` to the Bloom filter."))

(defn hashes
  "Returns an array of at least `num-hashes` 32-bit hashes based on `x`."
  [x ^long num-hashes]
  (let [x (bs/to-byte-buffer x)
        n (p/>> (+ num-hashes 3) 2)
        buf (ByteBuffer/allocate (p/<< n 4))]
    (dotimes [i n]
      (.put buf ^bytes (bt/hash x :murmur128 {:seed i})))
    (->> buf .array bt/hash->ints)))

;; this bit of indirection is so we can test multiple filter, but only hash once
(defn contains?
  "Returns true if the Bloom filter contains the element `x`."
  [bloom x]
  (contains?- bloom (hashes x (num-hashes bloom))))

(defn bloom-filter-parameters
  "Given a max cardinality and target error, returns a tuple containing the optimal number
   of bits and number of hash functions for the Bloom filter."
  [^long cardinality ^double error]
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

;; http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter
(defn approximate-cardinality
  ^long [^long num-bits ^long num-hashes ^long cardinality]
  (long
    (/ (* (- (double num-bits))
         (Math/log
           (- 1.0
             (/ (double cardinality)
               (double num-bits)))))
      (double num-hashes))))

(defn- bitset-contains? [^BitSet bitset ^ints hashes ^long num-hashes ^long num-bits]
  (loop [idx 0]
    (if (>= idx num-hashes)
      true
      (if (.get bitset (truncated-hash (aget ^ints hashes idx) num-bits))
        (recur (inc idx))
        false))))

(defn- add-to-bitset! [^BitSet bitset ^ints hashes ^long num-hashes ^long num-bits]
  (dotimes [idx num-hashes]
    (.set bitset
      (truncated-hash (aget ^ints hashes idx) num-bits)
      true)))

(defn bloom-filter
  "Returns a Bloom filter which can be interacted with via `contains?` and `add!`."
  ([^long cardinality ^double error]
     (bloom-filter true cardinality error))
  ([thread-safe? ^long cardinality ^double error]
     (let [[num-bits num-hashes] (bloom-filter-parameters cardinality error)
           num-hashes (long num-hashes)
           num-bits (long num-bits)
           bitset (BitSet. num-bits)]

       (if thread-safe?

         (let [lock (l/asymmetric-lock)]
           (reify
             clojure.lang.Counted
          
             (count [_]
               (approximate-cardinality num-bits num-hashes (.cardinality bitset)))
          
             IBloomFilter
             (num-hashes [_]
               num-hashes)
             (contains?- [_ hashes]
               (l/with-lock lock
                 (bitset-contains? bitset hashes num-hashes num-bits)))
             (add! [_ x]
               (let [hs (hashes x num-hashes)]
                 (l/with-exclusive-lock lock
                   (add-to-bitset! bitset hs num-hashes num-bits))))))

         (reify
           clojure.lang.Counted
           
           (count [_]
             (approximate-cardinality num-bits num-hashes (.cardinality bitset)))
           
           IBloomFilter
           (num-hashes [_]
             num-hashes)
           (contains?- [_ hashes]
             (bitset-contains? bitset hashes num-hashes num-bits))
           (add! [_ x]
             (add-to-bitset! bitset (hashes x num-hashes) num-hashes num-bits)))))))
