(ns narrator.operators
  (:refer-clojure :exclude [group-by concat filter])
  (:use
    [potemkin]
    [narrator core])
  (:require
    [clojure.set :as set]
    [clojure.core.reducers :as r]
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
    :ordered? false
    :create (fn []
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
       :ordered? false
       :create (fn []
                 (let [cnt (AtomicLong. 0)]
                   (stream-aggregator
                     :process #(.addAndGet cnt (reduce + %))
                     :deref #(.get cnt)
                     :reset #(when clear-on-reset? (.set cnt 0))))))))

(defn-operator delta
  "Emits the difference between the current and previous values.  The first value will
   be emitted as-is."
  ([]
     (delta nil))
  ([{:keys [clear-on-reset?]
     :or {clear-on-reset? true}}]
     (stream-processor-generator
       :ordered? true
       :combine #(apply + %)
       :create (fn []
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
       :ordered? false
       :create (fn []
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
    :ordered? false
    :combine last
    :create (fn []
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
     (let [generator (compile-operators ops false)
           de-nil #(if (nil? %) ::nil %)
           re-nil #(if (identical? ::nil %) nil %)]
       (stream-aggregator-generator
         :descriptor (list 'group-by facet options ops)
         :ordered? (ordered? generator)
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
         :emit #(zipmap
                  (keys %)
                  (map (emitter generator) (vals %)))
         :create (fn []
                   (let [m (ConcurrentHashMap.)
                         context (capture-context)]
                     (stream-aggregator
                       :process (fn [msgs]
                                  (doseq [msg msgs]
                                    (let [k (de-nil (facet msg))]
                                      (if-let [op (.get m k)]
                                        (process! op msg)
                                        (with-bindings context
                                          (binding [*execution-affinity* (when ordered? (hash k))]
                                            (let [op (create generator)
                                                  op (or (.putIfAbsent m k op) op)]
                                              (process! op msg))))))))
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
  (let [combiner (promise)]
    (stream-aggregator-generator
      :ordered? false ;; we can assume this, and it doesn't change anything if we're wrong
      :combine (fn [s] (@combiner s))
      :create (fn []
                (let [context (capture-context)
                      op (delay
                           (let [op (with-bindings context
                                      (create @*top-level-generator*))]
                             (deliver combiner @*top-level-generator*)
                             op))]
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
   quasi-cardinality])

;;;

(defn moving
  "Emulates a moving window over the operator, of width `interval`.  Since smooth windowing
   requires specialized algorithms, this instead keeps n-many operators in memory, spanning
   the given `interval`.  This means that memory usage will be increased, and if `interval`
   is not an even multiple of the period, the value will be approximate.

   The given operator must be combinable, which is automatically true of any composition of
   the built-in operators, and any operator defined via `narrator.core/monoid`."
  [interval operator]
  (let [operator (compile-operators operator)
        windowed-values (atom (sorted-map))
        combine-fn (combiner operator)
        op (create operator)]
    (assert combine-fn "Any `moving` operator must be combinable.")
    (stream-aggregator-generator
      :ordered? (ordered? operator)
      :emit (emitter operator)
      :create (fn []
                (let [now *now-fn*
                      trimmed-values (fn [m]
                                       (let [t (now)
                                             cutoff (- t interval)]
                                         (->> m
                                           (drop-while #(<= (key %) cutoff))
                                           (into (sorted-map)))))]
                  (assert *now-fn* "No global clock defined.")
                  (stream-aggregator
                    :process #(process-all! op %)
                    :flush #(flush-operator op)
                    :deref (fn []
                             (swap! windowed-values
                               #(assoc (trimmed-values %) (now) @op))
                             (->> windowed-values
                               deref
                               vals
                               combine-fn))
                    :reset #(reset-operator! op)))))))
