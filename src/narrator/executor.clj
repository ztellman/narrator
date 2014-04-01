(ns narrator.executor
  (:use
    [potemkin])
  (:require
    [primitive-math :as p]
    [narrator.utils.rand :as r]
    [narrator.core :as c])
  (:import
    [java.util
     ArrayList]
    [narrator.core
     IBufferedAggregator
     StreamOperator]
    [java.util.concurrent
     ConcurrentHashMap
     Semaphore
     Executors
     ThreadFactory
     ExecutorService
     ArrayBlockingQueue]
    [java.util.concurrent.atomic
     AtomicLong]))

;;;

(def ^:private num-cores (.availableProcessors (Runtime/getRuntime)))

(def ^:private max-permits (* 2 num-cores))

;;;

(defn acquire [^Semaphore semaphore]
  (.acquire semaphore))

(defn acquire-all [^Semaphore semaphore]
  (.acquire semaphore max-permits))

(defn release
  ([semaphore]
     (release semaphore 1))
  ([^Semaphore semaphore ^long permits]
     (.release semaphore permits)))

(def ^:private ^:dynamic *has-exclusive-lock* false)

(defmacro with-exclusive-lock [semaphore & body]
  `(let [semaphore# ~semaphore
         has-lock?# *has-exclusive-lock*]
     (when-not has-lock?#
       (acquire-all semaphore#))
     (try
       (binding [*has-exclusive-lock* true]
         ~@body)
       (finally
         (when-not has-lock?#
           (release semaphore# max-permits))))))

(defn ^Semaphore semaphore []
  (Semaphore. max-permits false))

;;;

(let [cnt (atom 0)]
  (def ^:private thread-factory
    (reify ThreadFactory
      (newThread [_ runnable]
        (let [name (str "narrator-query-executor-" (swap! cnt inc))]
          (doto
            (Thread.
              (fn []
                (try
                  (.run ^Runnable runnable)
                  (catch Throwable _
                    ))))
            (.setName name)
            (.setDaemon true)))))))

(def ^:private executors
  (object-array
    (repeatedly num-cores
      #(Executors/newSingleThreadExecutor thread-factory))))

(def ^:private ^:dynamic *task-id* nil)

(let [leases (ConcurrentHashMap.)
      task-counter (AtomicLong. 0)

      inc-task (fn inc-task [semaphore]
                 (if-let [task-id *task-id*]
                   ;; task-id is already defined, update lease counts
                   (do
                     (if-not (.contains leases task-id)
                       (let [cnt (AtomicLong. 1)
                             cnt (or (.putIfAbsent leases task-id cnt) cnt)]
                         (.incrementAndGet ^AtomicLong cnt))
                       (.incrementAndGet ^AtomicLong (.get leases task-id)))
                     task-id)

                   ;; no task-id, acquire semaphore and create task-id
                   (do
                     (acquire semaphore)
                     (.getAndIncrement task-counter))))

      dec-task (fn dec-task [task-id semaphore]
                 (if-let [^AtomicLong cnt (.get leases task-id)]

                   ;; if we're back to zero,
                   (when (zero? (.decrementAndGet cnt))
                     (.remove leases task-id)
                     (release semaphore))

                   ;; we never created a lease in the first place
                   (release semaphore)))]
  (defn submit
    [f ^Semaphore semaphore hash]
    (let [task-id (inc-task semaphore)

          ^Runnable r
          (fn []
            (binding [*task-id* task-id]
              (try
                (f)
                (finally
                  (dec-task task-id semaphore)))))]
      (try
        (.submit ^ExecutorService
          (aget ^objects executors (Math/abs (p/rem hash num-cores)))
          r)
        (catch Exception e
          (dec-task task-id semaphore))))))

;;;

(definterface+ IAccumulator
  (^boolean add! [_ x]))

(deftype+ Accumulator
  [^objects ary
   ^long capacity
   ^AtomicLong idx]
  IAccumulator
  (add! [_ x]
    (let [idx (.getAndIncrement idx)]
      (if (< idx capacity)
        (do
          (aset ary idx x)
          true)
        false)))
  clojure.lang.Seqable
  (seq [_]
    (let [idx (.get idx)]
      (if (<= capacity idx)
        (seq ary)
        (let [ary' (object-array idx)]
          (System/arraycopy ary 0 ary' 0 idx)
          (seq ary'))))))

(defn accumulator [^long capacity]
  (Accumulator.
    (object-array capacity)
    capacity
    (AtomicLong. 0)))

(defn buffered-aggregator
  [& {:keys [operator capacity semaphore execution-affinity]
      :or {capacity 1024
           semaphore (semaphore)}}]
  (let [hash execution-affinity
        ^Semaphore semaphore semaphore
        acc-ref (atom (accumulator capacity))
        flush (fn flush [acc sync?]
                (when (compare-and-set! acc-ref acc (accumulator capacity))
                  (if sync?
                    (c/process-all! operator (seq acc))
                    (submit
                      #(c/process-all! operator (seq acc))
                      semaphore
                      (or hash (r/rand-int num-cores))))))]
    (reify
      StreamOperator
      (reset-operator! [_]
        (c/reset-operator! operator))
      (process-all! [this msgs]
        (doseq [msg msgs]
          (c/process! this msg)))

      IBufferedAggregator
      (flush-operator [_]
        (with-exclusive-lock semaphore
          (flush @acc-ref true)
          (c/flush-operator operator)))
      (process! [this msg]
        (loop []
          (let [acc @acc-ref]
            (if (add! acc msg)
              nil
              (do
                (flush acc *has-exclusive-lock*)
                (recur))))))

      clojure.lang.IDeref
      (deref [_]
        @operator))))

;;;

(defn thread-local-aggregator
  [generator]
  (c/stream-aggregator-generator
    :ordered? false
    :create (fn [options]
              (let [m (ConcurrentHashMap.)]
                (c/stream-aggregator
                  :reset (fn []
                           (doseq [op (vals m)]
                             (c/reset-operator! op)))
                  :process #(let [id (.getId (Thread/currentThread))]
                              (if-let [op (.get m id)]
                                (c/process-all! op %)
                                (let [op (c/create generator options)]
                                  (.putIfAbsent m id op)
                                  (c/process-all! op %))))
                  :deref #(let [combiner (c/combiner generator)
                                emitter (c/emitter generator)]
                            (->> m vals (map deref) combiner emitter)))))))
