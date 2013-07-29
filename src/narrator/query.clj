(ns narrator.query
  (:use
    [potemkin])
  (:require
    [primitive-math :as p]
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
      (let [end (long (+ start-time period))
            s (loop [s s]
                (when-not (empty? s)
                  
                  (if (chunked-seq? s)

                    ;; chunked seq
                    (let [c (chunk-first s)
                          cnt (count c)
                          [recur? s] (loop [idx 0]
                                       (if (p/< idx cnt)
                                         (let [x (.nth c idx)
                                               t (long (timestamp x))]
                                           (if (< t end)
                                             (do
                                               (c/process! op x)
                                               (recur (p/inc idx)))
                                             
                                             ;; stopping mid-chunk, cons the remainder back on
                                             (let [c' (chunk-buffer (p/- cnt idx))]
                                               (dotimes [idx' (count c')]
                                                 (chunk-append c' (.nth c (p/+ idx idx'))))
                                               [false (chunk-cons c' (chunk-rest s))])))
                                         [true (chunk-rest s)]))]
                      (if recur?
                        (recur s)
                        s))

                    ;; non-chunked seq
                    (let [x (first s)
                          t (long (timestamp x))]
                      (if (< t end)
                        (do
                          (c/process! op x)
                          (recur (rest s)))
                        s)))))]
        (c/flush-operator op)
        (reset! current-time end)
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
     (let [start-time (or start-time (timestamp (first s)))
           current-time (atom start-time)
           semaphore (ex/semaphore)]
       (binding [c/*now-fn* #(deref current-time)
                 c/*operator-wrapper* (fn [op hash]
                                         (ex/buffered-aggregator
                                           :semaphore semaphore
                                           :operator op
                                           :hash hash
                                           :capacity block-size))]
         (let [op (c/compile-operators* query-descriptor)
               ordered? (c/ordered? op)]
           (query-seq-
             op
             current-time
             (timestamp (first s))
             options
             s))))))

    
    
