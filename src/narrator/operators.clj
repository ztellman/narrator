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

(defn-operator mean
  "Yields the mean value of all messages.  If `clear-on-reset?` is true, it will be reset at
   the beginning of every period.  Emits `NaN` if no messages are received."
  ([]
     (mean nil))
  ([{:keys [clear-on-reset?]
     :or {clear-on-reset? true}}]
     (stream-aggregator-generator
       :combine #(map + %&)
       :emit (fn [[sum cnt]]
               (if (zero? cnt)
                 Double/NaN
                 (/ sum cnt)))
       :ordered? false
       :create (fn []
                 (let [v (atom [0.0 0])]
                   (stream-aggregator
                     :process (fn [ns] (swap! v (fn [[sum cnt]] [(+ sum (reduce + ns)) (+ cnt (count ns))])))
                     :deref #(deref v)
                     :reset #(when clear-on-reset? (reset! v [0.0 0]))))))))

(defn-operator group-by
  "Splits the stream by `facet`, and applies `ops` to the substreams in parallel."
  ([facet]
     (group-by facet nil nil))
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
  (stream-aggregator-generator
    :ordered? false ;; we can assume this, and it doesn't change anything if we're wrong
    :create (fn []
              (let [context (capture-context)
                    op (delay
                         (with-bindings context
                           (create @*top-level-generator*)))]
                (stream-aggregator
                  :process (fn [msgs]
                             (when-not (empty? msgs)
                               (process-all! @op msgs)))
                  :flush #(when (realized? op)
                            (flush-operator @op))
                  :deref #(when (realized? op)
                            @@op)
                  :reset #(when (realized? op)
                            (reset-operator! op)))))))

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
        op (AtomicReference. (create operator))
        combine-fn (combiner operator)]
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
                                           (drop-while #(<= (- (key %) 0.001) cutoff))
                                           (into (sorted-map)))))]
                  (assert *now-fn* "No global clock defined.")
                  (stream-aggregator
                    :process #(process-all! (.get op) %)
                    :flush #(flush-operator (.get op))
                    :deref #(combine-fn
                              (clojure.core/concat
                                (map deref (vals (trimmed-values @windowed-values)))
                                [@(.get op)]))
                    :reset (fn []
                             (let [op' (create operator)
                                   op (.getAndSet op op')
                                   t (now)
                                   cutoff (- (now) interval)]
                               (swap! windowed-values
                                 #(assoc (trimmed-values %) t op'))))))))))
