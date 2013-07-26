(ns narrator.query
  (:use
    [potemkin])
  (:require
    [clojure.core.reducers :as r]
    [narrator.operators :as op]
    [narrator.executor :as ex]))

;;;

(defn- query-seq-
  [op
   ^long start-time
   {:keys [period timestamp value]
    :or {value identity
         timestamp (constantly 0)
         period 1}
    :as options}
   s]
  (when-not (empty? s)
    (lazy-seq
      (let [end (long (+ start-time (long period)))
            s (loop [s s]
                (when-not (empty? s)
                  (let [x (first s)
                        t (long (timestamp x))]
                    (if (< t end)
                      (do
                        (op/offer! op x)
                        (recur (rest s)))
                      s))))]
        (op/flush op)
        (cons
          {:timestamp end
           :value (let [x @op]
                    (op/reset! op)
                    x)}
          (query-seq- op end options s))))))

(defn query-seq
  ([query-descriptor s]
     (-> (query-seq query-descriptor nil s)
       first
       :value))
  ([query-descriptor
    {:keys [start-time period timestamp value block-size]
     :or {value identity
          timestamp (constantly 0)
          period 1
          block-size 1024}
     :as options}
    s]
     (let [generator (op/operators->generator query-descriptor)
           ordered? (op/ordered? (generator))
           semaphore (ex/semaphore)]
       (binding [op/*operator-wrapper* (fn [op hash]
                                         (ex/buffered-aggregator
                                           :semaphore semaphore
                                           :operator op
                                           :hash hash
                                           :capacity block-size))]
         (query-seq-
           (generator (when ordered? (rand-int Integer/MAX_VALUE)))
           (timestamp (first s))
           options
           s)))))

    
    
