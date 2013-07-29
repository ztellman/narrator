(ns narrator.operators
  (:use
    [potemkin]
    [narrator core])
  (:require
    [clojure.set :as set])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]
    [java.util.concurrent.atomic
     AtomicLong]))

;;;

(defn-operator rate
  "Yields the number of messages seen since it has been reset."
  []
  (let [cnt (AtomicLong. 0)]
    (stream-aggregator
      :process #(.addAndGet cnt (count %))
      :deref #(.get cnt)
      :reset #(.set cnt 0))))

(defn-operator sum
  "Yields the sum of all messages seen since it has been reset."
  ([]
     (sum nil))
  ([options]
     (let [cnt (AtomicLong. 0)]
       (stream-aggregator
         :process #(.addAndGet cnt (reduce + %))
         :deref #(.get cnt)
         :reset #(.set cnt 0)))))

(defn-operator by
  ""
  ([facet]
     (by facet nil nil))
  ([facet ops]
     (by facet nil ops))
  ([facet
    {:keys [expiration clear-on-reset?]
     :or {clear-on-reset? true}}
    ops]
     (let [m (ConcurrentHashMap.)
           de-nil #(if (nil? %) ::nil %)
           re-nil #(if (identical? ::nil %) nil %)
           generator (compile-operators->generator ops)
           ordered? (ordered? (generator))

           wrapper *operator-wrapper*
           top-level-generator *top-level-generator*
           now-fn *now-fn*]
       (stream-aggregator
         :ordered? ordered?
         :process (fn [msgs]
                    (doseq [msg msgs]
                      (let [k (de-nil (facet msg))]
                        (if-let [op (.get m k)]
                          (process! op msg)
                          (binding [*operator-wrapper* wrapper
                                    *top-level-generator* top-level-generator
                                    *now-fn* now-fn]
                            (let [op (generator (when ordered? (hash k)))
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
                     (reset-operator! x)))))))

(defn-operator recur
  "Passes the stream back through the top-level stream operator, allowing for analysis of
   nested data-structures."
  []
  (*top-level-generator*))

(import-vars
  [narrator.utils.sampling
   sample
   moving-sample])
