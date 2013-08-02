(ns narrator.operators
  (:refer-clojure :exclude [group-by concat filter])
  (:use
    [potemkin]
    [narrator core])
  (:require
    [clojure.set :as set]
    [clojure.core.reducers :as r]
    [narrator.utils
     sampling])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]
    [java.util.concurrent.atomic
     AtomicLong]))

;;;

(defn-operator rate
  "Yields the number of messages seen since it has been reset."
  []
  (stream-aggregator-generator
    :combiner #(apply + %)
    :ordered? false
    :create (fn []
              (let [cnt (AtomicLong. 0)]
                (stream-aggregator
                  :process #(.addAndGet cnt (count %))
                  :deref #(.get cnt)
                  :reset #(.set cnt 0))))))

(defn-operator sum
  "Yields the sum of all messages seen since it has been reset."
  ([]
     (sum nil))
  ([options]
     (stream-aggregator-generator
       :combiner #(apply + %)
       :ordered? false
       :create (fn []
                 (let [cnt (AtomicLong. 0)]
                   (stream-aggregator
                     :process #(.addAndGet cnt (reduce + %))
                     :deref #(.get cnt)
                     :reset #(.set cnt 0)))))))

(defn-operator group-by
  ""
  ([facet]
     (group-by facet nil nil))
  ([facet ops]
     (group-by facet nil ops))
  ([facet
    {:keys [expiration clear-on-reset?]
     :or {clear-on-reset? true}}
    ops]
     (let [generator (compile-operators ops)
           de-nil #(if (nil? %) ::nil %)
           re-nil #(if (identical? ::nil %) nil %)]
       (stream-aggregator-generator
         :ordered? (ordered? generator)
         :create (fn []
                   (let [m (ConcurrentHashMap.)
                         wrapper *compiled-operator-wrapper*
                         now-fn *now-fn*
                         top-level-generator *top-level-generator*]
                     (stream-aggregator
                       :ordered? ordered?
                       :process (fn [msgs]
                                  (doseq [msg msgs]
                                    (let [k (de-nil (facet msg))]
                                      (if-let [op (.get m k)]
                                        (process! op msg)
                                        (binding [*compiled-operator-wrapper* wrapper
                                                  *now-fn* now-fn
                                                  *top-level-generator* top-level-generator
                                                  *execution-affinity* (when ordered? (hash k))]
                                          (let [op (create generator)
                                                op (or (.putIfAbsent m k op) op)]
                                   (process! op msg)))))))
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
              (create *top-level-generator*))))

(defn-operator filter
  [predicate]
  (reducer-op (r/filter predicate)))

(defn-operator concat
  []
  (mapcat-op seq))

(import-vars
  [narrator.utils.sampling
   sample
   moving-sample
   quantiles
   moving-quantiles])
