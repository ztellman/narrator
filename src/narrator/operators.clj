(ns narrator.operators
  (:refer-clojure :exclude [group-by concat filter])
  (:use
    [potemkin]
    [narrator core])
  (:require
    [clojure.edn :as edn]
    [clojure.set :as set]
    [clojure.core.reducers :as r]
    [narrator.utils.rand :as rand]
    [primitive-math :as p]
    [narrator.operators
     sampling
     streaming])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]
    [java.util.concurrent.atomic
     AtomicLong
     AtomicReference]))

;;;

(defn-operator rate
  "Yields the number of messages seen since it has been reset."
  []
  (stream-aggregator-generator
    :combine #(apply + %)
    :concurrent? true
    :create (fn [options]
              (let [cnt (AtomicLong. 0)]
                (stream-aggregator
                  :process #(.addAndGet cnt (count %))
                  :deref #(.get cnt)
                  :reset #(.set cnt 0))))))

(defn-operator sum
  "Yields the sum of all messages.  If `clear-on-reset?` is true, it will be reset to 0
   at the beginning of every period."
  ([]
     (sum nil))
  ([{:keys [clear-on-reset?]
     :or {clear-on-reset? true}}]
     (stream-aggregator-generator
       :combine #(apply + %)
       :concurrent? true
       :create (fn [options]
                 (let [cnt (AtomicLong. 0)]
                   (stream-aggregator
                     :process (fn [msgs]
                                (let [sum (double (reduce + msgs))]
                                  (loop []
                                    (let [current (.get cnt)
                                          val     (Double/longBitsToDouble current)]
                                      (when-not (.compareAndSet cnt current
                                                  (Double/doubleToRawLongBits
                                                    (p/+ val sum)))
                                        (recur))))))
                     :deref #(Double/longBitsToDouble (.get cnt))
                     :reset #(when clear-on-reset?
                               (.set cnt (Double/doubleToRawLongBits 0)))))))))

(defn-operator delta
  "Emits the difference between the current and previous values.  The first value will
   be emitted as-is."
  ([]
     (delta nil))
  ([{:keys [clear-on-reset?]
     :or {clear-on-reset? true}}]
     (stream-processor-generator
       :concurrent? false
       :create (fn [options]
                 (let [ref (AtomicReference. nil)]
                   (stream-processor
                     :reducer (r/map
                                (fn [x]
                                  (let [x' (.getAndSet ref x)]
                                    (if (nil? x')
                                      x
                                      (- x x')))))
                     :reset (when clear-on-reset?
                              #(.set ref nil))))))))

(defn-operator transitions
  "Emits only values which differ from the previous value.  The first value is always emitted."
  ([]
     (transitions nil))
  ([{:keys [clear-on-reset?]
     :or {clear-on-reset? false}}]
     (stream-processor-generator
       :concurrent? true
       :create (fn [options]
                 (let [ref (AtomicReference. ::none)]
                   (stream-processor
                     :reducer (r/filter
                                (fn [x]
                                  (not= x (.getAndSet ref x))))
                     :reset (when clear-on-reset?
                              #(.set ref ::none))))))))

(defn-operator latest
  "Emits the latest value seen within the period, or within the entire sequence if no period is defined."
  []
  (stream-aggregator-generator
    :concurrent? true
    :combine last
    :create (fn [options]
              (let [ref (atom nil)]
                (stream-aggregator
                  :process (fn [msgs]
                             (reset! ref (last msgs)))
                  :deref #(deref ref))))))


(defn-operator mean
  "Yields the mean value of all messages.  If `clear-on-reset?` is true, it will be reset at
   the beginning of every period.  Emits `NaN` if no messages are received."
  ([]
     (mean nil))
  ([{:keys [clear-on-reset?]
     :or {clear-on-reset? true}}]
     (monoid-aggregator
       :clear-on-reset? clear-on-reset?
       :initial (constantly [0.0 0])
       :emit (fn [[sum cnt]]
               (if (zero? cnt)
                 Double/NaN
                 (/ sum cnt)))
       :pre-process (fn [n] [n 1])
       :combine (fn [[s-a c-a] [s-b c-b]]
                  [(+ s-a s-b) (+ c-a c-b)]))))

(defn-operator group-by
  "Splits the stream by `facet`, and applies `ops` to the substreams in parallel."
  ([facet]
     (group-by facet nil accumulator))
  ([facet ops]
     (group-by facet nil ops))
  ([facet
    {:keys [expiration clear-on-reset?]
     :or {clear-on-reset? true}
     :as options}
    ops]
     (let [generator (compile-operators ops)
           de-nil #(if (nil? %) ::nil %)
           re-nil #(if (identical? ::nil %) nil %)]
       (stream-aggregator-generator
         :recur-to #(recur-to! generator %)
         :serialize (fn [m]
                      (zipmap (keys m)
                        (map (serializer generator) (vals m))))
         :deserialize (fn [m]
                        (zipmap (keys m)
                          (map (deserializer generator) (vals m))))
         :descriptor (list 'group-by facet options ops)
         :concurrent? (concurrent? generator)
         :combine (when-let [combiner (combiner generator)]
                    (fn [ms]
                      (let [ks (->> ms (mapcat keys) distinct)]
                        (zipmap
                          ks
                          (map
                            (fn [k]
                              (->> ms
                                (map #(get % k ::none))
                                (remove #(identical? ::none %))
                                combiner))
                            ks)))))
         :emit (fn [m]
                 (when-not (empty? m)
                   (zipmap
                     (keys m)
                     (map (emitter generator) (vals m)))))
         :create (fn [options]
                   (let [m (ConcurrentHashMap.)]
                     (stream-aggregator
                       :process (fn [msgs]
                                  (doseq [msg msgs]
                                    (let [k (de-nil (facet msg))]
                                      (if-let [op (.get m k)]
                                        (process! op msg)
                                        (let [op (create generator
                                                   (assoc options :execution-affinity
                                                     (when-not (concurrent? generator)
                                                       (rand/rand-int))))
                                              op (or (.putIfAbsent m k op) op)]
                                          (process! op msg))))))
                       :flush #(doseq [x (vals m)]
                                 (flush-operator x))
                       :deref #(zipmap
                                 (map re-nil (keys m))
                                 (map deref (vals m)))
                       :reset #(if clear-on-reset?
                                 (.clear m)
                                 (doseq [x (vals m)]
                                   (reset-operator! x))))))))))

(defn-operator recur
  "Passes the stream back through the top-level stream operator, allowing for analysis of
   nested data-structures."
  []
  (let [gen (atom nil)]
    (stream-aggregator-generator
      :recur-to #(reset! gen %)
      :descriptor 'recur
      :concurrent? true ;; we can assume this, and it doesn't change anything if we're wrong
      :combine #(if (every? nil? %)
                  nil
                  ((combiner @gen) %))
      :emit #(when %
               ((emitter @gen) %))
      :serialize #((serializer @gen) %)
      :deserialize #((deserializer @gen) %)
      :create (fn [options]
                (when-not @gen
                  (throw (IllegalArgumentException. "'recur' requires matching 'recur-to'")))
                (let [op (delay (create @gen options))]
                  (stream-aggregator
                    :process (fn [msgs]
                               (when-not (empty? msgs)
                                 (process-all! @op msgs)))
                    :flush #(when (realized? op)
                              (flush-operator @op))
                    :deref #(when (realized? op)
                              @@op)
                    :reset #(when (realized? op)
                              (reset-operator! op))))))))

(defn recur-to
  "Defines the point all contained `recur` operators will return to."
  [query-descriptor]
  (let [generator (compile-operators query-descriptor)]
    (recur-to! generator generator)
    (stream-aggregator-generator
      :recur-to (fn [_])
      :serialize (serializer generator)
      :deserialize (deserializer generator)
      :combine (combiner generator)
      :emit (emitter generator)
      :create #(create generator %))))

(defn-operator filter
  [predicate]
  (reducer-op (r/filter predicate)))

(defn-operator concat
  "Takes a stream of sequences, and emits a stream of the elements within the seqs."
  []
  (mapcat-op seq))

(import-vars
  [narrator.operators.sampling
   sample]
  [narrator.operators.streaming
   quantiles
   quasi-distinct-by
   quasi-cardinality
   quasi-frequency-by])

;;;

(defn moving
  "Emulates a moving window over the operator, of width `interval`.  Since smooth windowing
   requires specialized algorithms, this instead keeps n-many operators in memory, spanning
   the given `interval`.  This means that memory usage will be increased, and if `interval`
   is not an even multiple of the period, the value will be approximate.

   The given operator must be combinable, which is automatically true of any composition of
   the built-in operators, and any operator defined via `narrator.core/monoid`."
  [interval query-descriptor]
  (let [generator (compile-operators query-descriptor)]
    (assert (combiner generator) "Any `moving` operator must be combinable.")
    (stream-aggregator-generator
      :recur-to #(recur-to! generator %)
      :serialize (serializer generator)
      :deserialize (deserializer generator)
      :descriptor (list 'moving interval query-descriptor)
      :concurrent? (concurrent? generator)
      :combine (combiner generator)
      :emit (emitter generator)
      :create (fn [{:keys [now]
                    :as options}]
                (assert now "Moving operators require that :timestamp be defined.")
                (let [windowed-values (atom (sorted-map))
                      trimmed-values (fn [m]
                                       (let [t (now)
                                             cutoff (- t interval)]
                                         (->> m
                                           (drop-while #(<= (key %) cutoff))
                                           (into (sorted-map)))))
                      op (create generator options)]
                  (stream-aggregator
                    :process #(process-all! op %)
                    :flush #(flush-operator op)
                    :deref (fn []
                             (swap! windowed-values
                               #(assoc (trimmed-values %) (now) @op))
                             ((combiner generator) (vals @windowed-values)))
                    :reset #(reset-operator! op)))))))
