(ns narrator.query
  (:use
    [potemkin])
  (:require
    [narrator.utils
     [rand :as r]
     [time :as t]]
    [primitive-math :as p]
    [narrator.core :as c]
    [narrator.executor :as ex]
    [clojure.core.async :as a]))

;;;

(defn- create-operator
  [query-descriptor
   {:keys [now buffer? block-size]
    :or {buffer? true
         block-size 1024}}]
  (let [semaphore (ex/semaphore)]
    (binding [c/*now-fn* now
              c/*compiled-operator-wrapper* (if buffer?
                                              (fn [op]
                                                (ex/buffered-aggregator
                                                  :semaphore semaphore
                                                  :operator op
                                                  :capacity block-size))
                                              identity)
              c/*aggregator-generator-wrapper* (fn [gen]
                                                 (if (and (c/ordered? gen) (c/combiner gen))
                                                   (ex/thread-local-aggregator gen)
                                                   gen))]
      (let [gen (c/compile-operators query-descriptor)]
        (binding [c/*execution-affinity* (when (c/ordered? gen) (r/rand-int Integer/MAX_VALUE))]
          (c/create gen))))))

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
                                           (if (p/< t end)
                                             (do
                                               (c/process! op x)
                                               (recur (p/inc idx)))
                                             
                                             ;; stopping mid-chunk, cons the remainder back on
                                             (let [remaining (p/- cnt idx)
                                                   b (chunk-buffer remaining)]
                                               (dotimes [idx' remaining]
                                                 (chunk-append b (.nth c (p/+ idx idx'))))
                                               [false (chunk-cons (chunk b) (chunk-rest s))])))
                                         [true (chunk-rest s)]))]
                      (if recur?
                        (recur s)
                        s))

                    ;; non-chunked seq
                    (let [x (first s)
                          t (long (timestamp x))]
                      (if (p/< t end)
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
    {:keys [start-time period timestamp value buffer? block-size]
     :or {value identity
          timestamp (constantly 0)
          period 1}
     :as options}
    s]
     (let [start-time (or start-time (timestamp (first s)))
           current-time (atom start-time)]
       (query-seq-
         (create-operator query-descriptor (assoc options :now #(deref current-time)))
         current-time
         (timestamp (first s))
         options
         s))))

;;;

(defn seq->channel [s]
  (let [out (a/chan)]
    (a/go
      (loop [s s]
        (when-not (empty? s)
          (a/>! out (first s))
          (recur (rest s))))
      (a/close! out))
    out))

(defn channel->seq [ch]
  (lazy-seq
    (let [msg (a/<!! ch)]
      (when-not (nil? msg)
        (cons msg (channel->seq ch))))))

(defn query-channel
  [query-descriptor
   {:keys [period timestamp value buffer? block-size]
    :or {value identity
         period Long/MAX_VALUE}
    :as options}
   ch]
  (let [out (a/chan)
        current-time (atom (when-not timestamp (System/currentTimeMillis)))
        op (create-operator query-descriptor (assoc options :now #(deref current-time)))]
    (if-not timestamp
      (let [stop (a/chan)
            flush (a/chan)]

        ;; set up periodic flushing
        (a/go
          (loop []
            (let [v (a/alt!
                     (a/timeout period) true
                     stop false)]
              (when v
                (a/>! flush true)
                (recur)))))

        ;; handle incoming messages
        (a/go
          (loop []
            (let [msg (a/alt!
                        flush ::flush
                        ch ([msg _] msg))]
              (if (or (nil? msg) (identical? ::flush msg))
                (do
                  (c/flush-operator op)
                  (reset! current-time (System/currentTimeMillis))
                  (let [x @op]
                    (c/reset-operator! op)
                    (a/>! out x)
                    (when-not (nil? msg)
                      (recur))))
                (when-not (nil? msg)
                  (c/process! op msg)
                  (recur)))))
          (a/>! stop true))))
    out))
    
