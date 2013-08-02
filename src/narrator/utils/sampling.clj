(ns narrator.utils.sampling
  (:use
    [potemkin]
    [narrator core])
  (:require
    [primitive-math :as p]
    [narrator.utils
     [math :as m]
     [rand :as r]
     [time :as t]
     [locks :as l]])
  (:import
    [java.util.concurrent
     ConcurrentSkipListMap]
    [java.util.concurrent.atomic
     AtomicLong
     AtomicReferenceArray]))

;; implementations strongly based on those in Code Hale's metrics-core library
;; which are in turn based on:
;; http://www.research.att.com/people/Cormode_Graham/library/publications/CormodeShkapenyukSrivastavaXu09.pdf

(p/use-primitive-operators)

(defn priority ^double [^double alpha ^long elapsed]
  (/
    (Math/exp
      (* alpha (double elapsed)))
    (r/rand)))

(def rescale-interval (t/hours 1))

(defn-operator moving-sample
  "Emits a sampling of messages, weighted toward those in the last `window` milliseconds."
  ([]
     (moving-sample nil))
  ([{:keys [window sample-size]
     :or {sample-size 1024
          window (t/hours 1)}}]
     (stream-aggregator-generator
       :ordered? false
       :create
       (fn []
         (let [lock (l/asymmetric-lock)
               samples (ConcurrentSkipListMap.)
               counter (AtomicLong. 0)
               start-time (AtomicLong. (now))
               next-rescale (AtomicLong. (+ (now) (long rescale-interval)))
               alpha (double (/ 8e-2 window))
               sample-size (long sample-size)]
           (stream-aggregator
             :process
             (fn [msgs]
               (let [elapsed (- (now) (.get start-time))]
                 (l/with-lock lock
                   (doseq [msg msgs]
                     (let [pr (priority alpha elapsed)]
                       (if (<= (.incrementAndGet counter) sample-size)
                         
                         ;; we don't have our full sample size, add everything
                         (.put samples pr msg)
                         
                         ;; check to see if we should displace an existing sample
                         (let [frst (double (.firstKey samples))]
                           (when (< frst pr)
                             (when-not (.putIfAbsent samples pr msg)
                               (loop [frst frst]
                                 (when-not (.remove samples frst)
                                   (recur (double (.firstKey samples))))))))))))))
             
             :deref
             (fn []
               (l/with-exclusive-lock lock
                 
                 ;; do we need to rescale?
                 (let [now (now)]
                   (when (>= now (.get next-rescale))
                     (.set next-rescale (+ (long (now)) (long rescale-interval)))
                     (let [prev-start-time (.get start-time)
                           _ (.set start-time now)
                           scale-factor (Math/exp
                                          (*
                                            (- alpha)
                                            (double (- now prev-start-time))))]
                       
                       ;; rescale each key
                       (doseq [k (keys samples)]
                         (let [val (.remove samples k)]
                           (.put samples (* scale-factor (double k)) val)))
                       
                       ;; make sure counter reflects size of collection
                       (.set counter (count samples)))))

                 (->> samples vals doall)))))))))

(defn-operator sample
  "Emits a uniform sampling of messages.  If `clear-on-reset?` is true, the sample only
   represents messages in the last period, otherwise the sample is over the entire lifetime
   of the stream."
  ([]
     (sample nil))
  ([{:keys [clear-on-reset? sample-size]
     :or {clear-on-reset? true
          sample-size 1024}}]
     (stream-aggregator-generator
       :ordered? false
       :create
       (fn []
         (let [sample-size (long sample-size)
               samples (AtomicReferenceArray. sample-size)
               counter (AtomicLong. 0)]
           (stream-aggregator
             :process
             (fn [msgs]
               (doseq [msg msgs]
                 (let [cnt (.incrementAndGet counter)]
                   (if (<= cnt sample-size)
                     
                     ;; we don't have our full sample size, add everything
                     (.set samples (dec cnt) msg)
                     
                     ;; check to see if we should displace an existing sample
                     (let [idx (r/rand-int cnt)]
                       (when (< idx sample-size)
                         (.set samples idx msg)))))))

             :deref
             (fn []
               (let [cnt (min (.get counter) sample-size)
                     ^objects ary (object-array cnt)]
                 (dotimes [i cnt]
                   (aset ary i (.get samples i)))

                 (seq ary)))))))))

(defn-operator quantiles
  "Emits the statistical distribution of messages.  If `clear-on-reset?` is true, this
   only represents the distribution of messages in the last period, otherwise it
   represents the distribution over the entire lifetime of the stream."
  ([]
     (quantiles nil))
  ([{:keys [quantiles clear-on-reset? sample-size]
     :or {quantiles [0.5 0.9 0.95 0.99 0.999]}
     :as options}]
     (compile-operators
       [(sample options) #(zipmap quantiles (m/quantiles % quantiles))])))

(defn-operator moving-quantiles
  "Emits the statistical distribution of messages, weighted towards those in the last
   `window` milliseconds."
  ([]
     (quantiles nil))
  ([{:keys [quantiles window sample-size]
     :or {quantiles [0.5 0.9 0.95 0.99 0.999]}
     :as options}]
     (compile-operators
       [(moving-sample options) #(zipmap quantiles (m/quantiles % quantiles))])))
