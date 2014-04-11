(ns narrator.query
  (:use
    [potemkin])
  (:require
    [narrator.utils
     [rand :as r]
     [time :as t]
     [locks :as lock]]
    [primitive-math :as p]
    [narrator.core :as c]
    [narrator.executor :as ex]))

;;;

(defn- create-gen+operator
  [query-descriptor
   {:keys [now buffer? block-size]
    :or {buffer? true
         block-size 1024}}]
  (let [semaphore (ex/semaphore)]
    (let [options {:now now

                   :top-level? true

                   :compiled-operator-wrapper
                   (if buffer?
                     (fn [op]
                       (ex/buffered-aggregator
                         :semaphore semaphore
                         :operator op
                         :capacity block-size))
                     identity)

                   :aggregator-generator-wrapper
                   (fn [gen]
                     (if (and (not (c/concurrent? gen)) (c/combiner gen))
                       (ex/thread-local-aggregator gen)
                       gen))}

          gen (c/compile-operators query-descriptor options)]

      [gen
       (c/create gen
         (assoc options
           :execution-affinity
           (when-not (c/concurrent? gen)
             (r/rand-int Integer/MAX_VALUE))))])))

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
           [gen operator] (create-gen+operator
                            query-descriptor
                            (assoc options :now (when timestamp #(deref current-time))))]
       (transform
         (query-seq-
           gen
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
     (let [gen (c/compile-operators query-descriptor nil)]
       (comp
         (c/emitter gen)
         (c/combiner gen)
         (partial map (comp (c/deserializer gen) deserialize))))))

;;;

(defmacro ^:private when-core-async [& body]
  (when (try
          (require '[clojure.core.async :as a])
          true
          (catch Exception _
            false))
    `(do ~@body)))

(when-core-async

  (defn query-channel
    "Behaves like `query-seq`, except that the input is assumed to be a core.async channel, and the return value is also
     a core.async channel.  A `:period` must be provided.  If no `:timestamp` is given, then the analysis will occur in
     realtime, emitting query results  periodically without any timestamp.  If `:timestamp` is given, then it will emit
     maps with `:timestamp` and `:value` entries whenever a period elapses in the input stream."
    [query-descriptor
     {:keys [period timestamp value start-time buffer? block-size mode serialize]
      :or {value identity
           mode :full
           serialize identity
           period Long/MAX_VALUE}
      :as options}
     ch]
    (assert period "A :period must be specified.")
    (let [out (a/chan)
          now #(System/currentTimeMillis)
          period (long period)
          current-time (atom (when-not timestamp (now)))
          [gen op] (create-gen+operator
                     query-descriptor
                     (assoc options :now #(deref current-time)))
          deref' (deref-fn mode gen serialize)]
      (if-not timestamp

        ;; process everything in realtime
        (a/go
          (loop
            [next-flush (+ @current-time period)
             flush-signal (a/timeout period)]
            (let [msg (a/alt!
                        flush-signal ::flush
                        ch ([msg _] msg))]
              (if (or (nil? msg) (identical? ::flush msg))

                ;; flush
                (do
                  (c/flush-operator op)
                  (reset! current-time (now))
                  (let [x (deref' op)]
                    (c/reset-operator! op)
                    (a/>! out x)
                    (when-not (nil? msg)
                      (recur
                        (+ next-flush period)
                        (a/timeout (- period (- (now) next-flush)))))))

                ;; process
                (do
                  (c/process! op (value msg))
                  (recur next-flush flush-signal))))))

        ;; go on timestamps of messages
        (a/go
          (loop
            [next-flush (when start-time (+ start-time period))]
            (let [msg (a/<! ch)]
              (let [next-flush (or next-flush
                                 (when-not (nil? msg)
                                   (+ (timestamp msg) period)))]
                (if (or (nil? msg) (>= (timestamp msg) next-flush))

                  ;; flush
                  (do
                    (c/flush-operator op)
                    (reset! current-time next-flush)
                    (let [x (deref' op)]
                      (c/reset-operator! op)
                      (a/>! out {:timestamp (or next-flush 0) :value x})
                      (when-not (nil? msg)
                        (c/process! op (value msg))
                        (recur (+ next-flush period)))))

                  ;; process
                  (do
                    (c/process! op (value msg))
                    (recur next-flush))))))))
      out)))

(defmacro ^:private when-lamina [& body]
  (when (try
          (require '[lamina.core :as l])
          (require '[lamina.api :as la])
          true
          (catch Exception _
            false))
    `(do ~@body)))

(when-lamina
  (defn query-lamina-channel
    "Behaves like `query-seq`, except that the input is assumed to be a Lamina channel, and the return value is also
     a Lamina channel.  A `:period` must be provided.  If no `:timestamp` is given, then the analysis will occur in
     realtime, emitting query results  periodically without any timestamp.  If `:timestamp` is given, then it will emit
     maps with `:timestamp` and `:value` entries whenever a period elapses in the input stream."
    [query-descriptor
     {:keys [period timestamp value start-time buffer? block-size mode serialize]
      :or {value identity
           period Long/MAX_VALUE
           mode :full
           serialize identity}
      :as options}
     ch]
    (let [now #(System/currentTimeMillis)
          period (long period)
          current-time (atom (when-not timestamp (now)))
          [gen op] (create-gen+operator
                     query-descriptor
                     (assoc options :now #(deref current-time)))
          deref' (deref-fn mode gen serialize)
          out (l/channel)
          lock (lock/asymmetric-lock)]
      (if-not timestamp

        ;; do realtime processing
        (do

          (l/join
            (l/periodically
              {:period period}
              (fn []
                (lock/with-exclusive-lock lock
                  (c/flush-operator op)
                  (let [x (deref' op)]
                    (c/reset-operator! op)
                    x))))
            out)

          (la/bridge-siphon ch out "query-lamina-channel"
            (fn [msg]
              (lock/with-lock lock
                (swap! current-time + period)
                (c/process! op (value msg)))))

          (l/on-drained ch
            (fn []
              (lock/with-exclusive-lock lock
                (c/flush-operator op)
                (let [x (deref' op)]
                  (l/enqueue out x)
                  (l/close out))))))

        ;; go on timestamps of messages
        (let [next-flush (atom (when start-time (+ start-time period)))]

          (la/bridge-siphon ch out "query-lamina-channel"
            (fn [msg]
              (let [t (timestamp msg)]
                (when-not @next-flush
                  (reset! current-time t)
                  (reset! next-flush (+ t period)))
                (when (< @next-flush t)
                  (lock/with-exclusive-lock lock
                    (c/flush-operator op)
                    (let [x (deref' op)]
                      (c/reset-operator! op)
                      (l/enqueue out {:timestamp @next-flush :value x})
                      (reset! current-time @next-flush)
                      (swap! next-flush + period))))
                (lock/with-lock lock
                  (c/process! op (value msg))))))

          (l/on-drained ch
            (fn []
              (lock/with-exclusive-lock lock
                (c/flush-operator op)
                (let [x (deref' op)]
                  (l/enqueue out {:timestamp @next-flush :value x})
                  (l/close out)))))))
      out)))
