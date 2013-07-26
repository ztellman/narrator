(ns narrator.executor
  (:use
    [potemkin])
  (:require
    [narrator.operators :as op])
  (:import
    [java.util
     ArrayList]
    [narrator.operators
     IBufferedAggregator]
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

(def ^:private ^:const num-cores (.availableProcessors (Runtime/getRuntime)))

(def ^:private ^:const max-permits 1 #_(* 2 num-cores))

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

(def ^:private ^:dynamic *exclusive-lock* true)

(defmacro with-exclusive-lock [semaphore & body]
  `(let [semaphore# ~semaphore
         lock?# *exclusive-lock*]
     (when lock?#
       (acquire-all semaphore#))
     (try
       (binding [*exclusive-lock* false]
         ~@body)
       (finally
         (when lock?#
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
    [f ^Semaphore semaphore idx]
    (let [task-id (inc-task semaphore)
          ^Runnable r
          (fn []
            (binding [*task-id* task-id]
              (try
                (f)
                (finally
                  (dec-task task-id semaphore)))))]
      (try
        (.submit ^ExecutorService (aget ^objects executors idx) r)
        (catch Exception _
          (dec-task task-id semaphore))))))

;;;

(defn buffered-aggregator
  [& {:keys [operator capacity semaphore hash]
      :or {capacity 1024
           semaphore (semaphore)}}]
  (let [^Semaphore semaphore semaphore
        q-ref (atom (ArrayBlockingQueue. capacity))
        flush (fn flush [^ArrayBlockingQueue q sync?]
                (when (compare-and-set! q-ref q (ArrayBlockingQueue. capacity))
                  (if sync?
                    (op/process! operator q)
                    (submit
                      #(op/process! operator q)
                      semaphore
                      (if hash
                        (rem hash num-cores)
                        (rand-int num-cores))))))]
    (reify
      op/StreamOperator
      (aggregator? [_] true)
      (ordered? [_] (op/ordered? operator))
      (reset! [_] (op/reset! operator))
      (process! [this msgs]
        (doseq [msg msgs]
          (op/offer! this msg)))
      
      IBufferedAggregator
      (flush [_]
        (with-exclusive-lock semaphore
          (flush @q-ref true)
          (op/flush operator)))
      (offer! [this msg]
        (loop []
          (let [q @q-ref]
            (when-not (.offer ^ArrayBlockingQueue q msg)
              (flush q (not *exclusive-lock*))
              (recur)))))

      clojure.lang.IDeref
      (deref [_]
        @operator))))


