(ns narrator.executor
  (:use
    [potemkin])
  (:require
    [clojure.tools.logging :as log]
    [narrator.utils.locks :as l]
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
     ExecutorService]
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
  (Semaphore. max-permits true))

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
                     (if-let [cnt (.get leases task-id)]
                       (.incrementAndGet ^AtomicLong cnt)
                       (let [cnt (AtomicLong. 1)
                             cnt (or (.putIfAbsent leases task-id cnt) cnt)]
                         (.incrementAndGet ^AtomicLong cnt)))
                     task-id)

                   ;; no task-id, acquire semaphore and create task-id
                   (do
                     (acquire semaphore)
                     (.incrementAndGet task-counter))))

      dec-task (fn dec-task [task-id semaphore]
                 (if-let [^AtomicLong cnt (.get leases task-id)]

                   ;; if we're back to zero,
                   (when (zero? (.decrementAndGet cnt))
                     (.remove leases task-id)
                     (release semaphore))

                   ;; we never created a lease in the first place
                   (release semaphore)))]
  (defn submit
    [f ^Semaphore semaphore affinity]
    (let [task-id (inc-task semaphore)

          ^Runnable r
          (fn []
            (binding [*task-id* task-id]
              (try
                (f)
                (catch Throwable e
                  (log/error e "Error in Narrator query"))
                (finally
                  (dec-task task-id semaphore)))))]
      (try
        (.submit ^ExecutorService
          (aget ^objects executors (Math/abs (p/rem affinity num-cores)))
          r)
        (catch Throwable e
          (dec-task task-id semaphore))))))

;;;

(definterface+ IAccumulator
  (^boolean add! [_ x])
  (flush! [_ token]))

(deftype+ Accumulator
  [^objects ^:volatile-mutable ary
   ^:volatile-mutable token
   ^long capacity
   ^AtomicLong idx
   lock]
  IAccumulator
  (add! [_ x]
    (l/with-lock lock
      (let [idx (.getAndIncrement idx)]
        (if (< idx capacity)
          (do
            (aset ary idx x)
            nil)
          token))))
  (flush! [_ t]
    (l/with-exclusive-lock lock
      (when (or (nil? t) (identical? t token))
        (let [s (if (<= capacity idx)
                  (seq ary)
                  (let [ary' (object-array idx)]
                    (System/arraycopy ary 0 ary' 0 idx)
                    (seq ary')))]
          (.set idx 0)
          (set! ary (object-array capacity))
          (set! token (Object.))
          s)))))

(defn accumulator [^long capacity]
  (Accumulator.
    (object-array capacity)
    (Object.)
    capacity
    (AtomicLong. 0)
    (l/asymmetric-lock)))

(defn buffered-aggregator
  [{:keys [operator capacity semaphore execution-affinity]
    :or {capacity 1024
         semaphore (semaphore)}}]
  (let [^Semaphore semaphore semaphore
        acc (accumulator capacity)
        flush (fn flush [acc token sync?]
                (when-let [s (flush! acc token)]
                  (if sync?
                    (c/process-all! operator s)
                    (submit
                      #(c/process-all! operator s)
                      semaphore
                      (or execution-affinity (r/rand-int num-cores))))))]
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
          (flush acc nil true)
          (c/flush-operator operator)))
      (process! [this msg]
        (loop []
          (if-let [token (add! acc msg)]
            (do
              (flush acc token *has-exclusive-lock*)
              (recur))
            nil)))

      clojure.lang.IDeref
      (deref [_]
        @operator))))

;;;

(defn thread-local-aggregator
  [generator]
  (c/stream-aggregator-generator
    :concurrent? true
    :emit (c/emitter generator)
    :serialize (c/serializer generator)
    :deserialize (c/deserializer generator)
    :create (fn [options]
              (let [m (ConcurrentHashMap.)
                    options' (dissoc options :aggregator-generator-wrapper)]
                (c/stream-aggregator
                  :reset (fn []
                           (doseq [op (vals m)]
                             (c/reset-operator! op)))
                  :process (fn [msgs]
                             (let [id (.getId (Thread/currentThread))]
                               (if-let [op (.get m id)]
                                 (c/process-all! op msgs)
                                 (let [op (c/create generator options')]
                                   (.putIfAbsent m id op)
                                   (c/process-all! op msgs)))))
                  :flush (fn []
                           (doseq [op (vals m)]
                             (c/flush-operator op)))
                  :deref #(let [combiner (c/combiner generator)]
                            (when-not (.isEmpty m)
                              (->> m vals (map deref) doall combiner))))))))
