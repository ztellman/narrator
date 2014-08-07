(ns narrator.query
  (:use
    [potemkin])
  (:require
    [manifold
     [stream :as s]
     [deferred :as d]]
    [narrator.utils
     [rand :as r]
     [time :as t]
     [locks :as lock]]
    [primitive-math :as p]
    [narrator.core :as c]
    [narrator.executor :as ex]))

;;;

(defn- create-operator
  [gen
   {:keys [now buffer? block-size]
    :or {buffer? true
         block-size 1024}}]
  (let [semaphore (ex/semaphore)]
    (c/create gen
      {:execution-affinity (when-not (c/concurrent? gen)
                             (r/rand-int Integer/MAX_VALUE))

       :now now


       :compiled-operator-wrapper
       (if buffer?
         (fn [op {:keys [execution-affinity]}]
           (ex/buffered-aggregator
             {:semaphore semaphore
              :operator op
              :capacity block-size
              :execution-affinity execution-affinity}))
         (fn [op _]
           op))

       :aggregator-generator-wrapper
       (fn [gen]
         (if (and (not (c/concurrent? gen)) (c/combiner gen))
           (ex/thread-local-aggregator gen)
           gen))})))

;;;

(defn- deref-fn [mode generator serialize]
  (apply comp
    serialize
    (case mode
      :full [c/deref']
      :partial [(c/serializer generator) deref])))

(defn- query-seq-
  [generator
   op
   current-time
   start-time
   {:keys [period
           timestamp
           value
           mode
           serialize]
    :or {value identity
         timestamp (constantly 0)
         period Long/MAX_VALUE
         mode :full
         serialize identity}
    :as options}
   input-seq]
  (lazy-seq
    (let [deref' (deref-fn mode generator serialize)
          end (long (+ start-time period))
          s' (loop [s input-seq]
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
                                              (c/process! op (value x))
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
         :value (let [x (deref' op)]
                  (c/reset-operator! op)
                  x)}
        (when-not (empty? s')
          (query-seq- generator op current-time end options s'))))))

(defn query-seq
  "Applies the `query-descriptor` to the sequence of messages.  If `:timestamp` and `:period` are specified, then returns
   a sequence of maps containing `:timestamp` and `:value` entries, representing the output of the query between that
   timestamp and the previous one.  If they are not specified, then a single result is returned, representing the consumption
   of the entire sequence.

       (query-seq rate (range 10)) => 10

       (query-seq rate {:period 5, :timestamp identity} (range 10)) => ({:timestamp 5, :value 5} {:timestamp 10, :value 5})

   This behaves lazily; elements of the input seq will only be consumed when elements from the output seq are consumed.

   Optional arguments:

   `start-time` - the beginning of time, defaults to the timestamp of the first element
   `value` - the actual payload of the incoming messages that should be queried, defaults to `identity`
   `buffer?` - if true, messages are not immediately processed, and may be processed in parallel, defaults to true
   `block-size` - the size of the messages buffers, defaults to 1024"
  ([query-descriptor s]
     (query-seq query-descriptor nil s))
  ([query-descriptor
    {:keys [start-time period timestamp value buffer? block-size]
     :or {value identity}
     :as options}
    s]
     (let [start-time (or start-time (if timestamp (timestamp (first s)) 0))
           current-time (atom start-time)
           transform (if (and period timestamp)
                       identity
                       #(first (map :value %)))
           generator (c/compile-operators query-descriptor)
           operator (create-operator
                      generator
                      (assoc options :now (when timestamp #(deref current-time))))]
       (transform
         (query-seq-
           generator
           operator
           current-time
           start-time
           options
           s)))))

(defn combiner
  "Returns a function that combines the output of :partial queries."
  ([query-descriptor]
     (combiner query-descriptor nil))
  ([query-descriptor
    {:keys [deserialize]
     :or {deserialize identity}}]
     (let [gen (c/compile-operators query-descriptor)]
       (comp
         (c/emitter gen)
         (c/combiner gen)
         (partial map (comp (c/deserializer gen) deserialize))))))

;;;

(defn query-stream
  "Behaves like `query-seq`, except that the input is assumed to be a Manifold source, or
   something that can be coerced to it (which includes a seq, a core.async stream, and a
   BlockingQueue).  Returns a Manifold source.

   A `:period` must be provided.  If no `:timestamp` is given, then the analysis will occur in
   realtime, emitting query results  periodically without any timestamp.  If `:timestamp` is
   given, then it will emit maps with `:timestamp` and `:value` entries whenever a period
   elapses in the input stream."
  [query-descriptor
   {:keys [period timestamp value start-time buffer? block-size mode serialize]
    :or {value identity
         mode :full
         serialize identity
         period Long/MAX_VALUE}
    :as options}
   s]
  (assert period "A :period must be specified.")
  (let [s (s/->source s)
        out (s/stream)
        now #(System/currentTimeMillis)
        period (long period)
        current-time (atom (when-not timestamp (now)))
        generator (c/compile-operators query-descriptor)
        op (create-operator
             generator
             (assoc options :now #(deref current-time)))
        deref' (deref-fn mode generator serialize)]

    (if-not timestamp

      ;; process everything in realtime
      (d/loop
        [next-flush (+ @current-time period)]
        (let [timeout (- next-flush (now))]
          (d/chain (if (neg? timeout)
                     ::none
                     (s/try-take! s ::none timeout ::none))
            (fn [x]
              (if (identical? ::none x)

                ;; flush
                (do
                  (c/flush-operator op)
                  (reset! current-time (now))
                  (let [x (deref' op)]
                    (c/reset-operator! op)
                    (d/chain (s/put! out x)
                      (fn [x]
                        (if (and x (not (s/drained? s)))
                          (d/recur (+ next-flush period))
                          (s/close! out))))))

                ;; process
                (do
                  (c/process! op (value x))
                  (d/recur next-flush)))))))

      ;; go by timestamps of messages
      (d/loop
        [next-flush (when start-time (+ start-time period))]
        (d/chain (s/take! s ::none)
          (fn [x]
            (let [next-flush (or next-flush
                               (when-not (identical? ::none x)
                                 (+ (timestamp x) period)))]
              (if (or (identical? ::none x)
                    (>= (timestamp x) next-flush))

                ;; flush
                (do
                  (c/flush-operator op)
                  (reset! current-time next-flush)
                  (let [val (deref' op)]
                    (c/reset-operator! op)
                    (d/chain (s/put! out {:timestamp (or next-flush 0) :value val})
                      (fn [res]
                        (if (and res (not (identical? ::none x)))
                          (do
                            (c/process! op (value x))
                            (d/recur (+ next-flush period)))
                          (s/close! out))))))

                ;; process
                (do
                  (c/process! op (value x))
                  (d/recur next-flush))))))))

    out))
