(ns narrator.query
  (:use
    [potemkin])
  (:require
    [clojure.core.reducers :as r]
    [narrator.core :as c]
    [narrator.executor :as ex]))

;;;

(defn- query-seq-
  [op
   current-time
   start-time
   {:keys [period timestamp value]
    :or {value identity
         timestamp (constantly 0)
         period 1}
    :as options}
   s]
  (when-not (empty? s)
    (lazy-seq
      (reset! current-time start-time)
      (let [end (long (+ start-time period))
            s (loop [s s]
                (when-not (empty? s)
                  (let [x (first s)
                        t (long (timestamp x))]
                    (if (< t end)
                      (do
                        (c/process! op x)
                        (recur (rest s)))
                      s))))]
        (c/flush-operator op)
        (cons
          {:timestamp end
           :value (let [x @op]
                    (c/reset-operator! op)
                    x)}
          (query-seq- op current-time end options s))))))

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
     (let [op (c/compile-operators query-descriptor)
           ordered? (c/ordered? op)
           semaphore (ex/semaphore)
           start-time (or start-time (timestamp (first s)))
           current-time (atom start-time)]
       (binding [c/*now-fn* #(deref current-time)
                 c/*operator-wrapper* (fn [op hash]
                                         (ex/buffered-aggregator
                                           :semaphore semaphore
                                           :operator op
                                           :hash hash
                                           :capacity block-size))]
         (query-seq-
           op
           current-time
           (timestamp (first s))
           options
           s)))))

    
    
