(ns narrator.utils.sampling
  (:use
    [potemkin]
    [narrator core])
  (:require
    [primitive-math :as p]
    [narrator.utils
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

(defn priority ^double [^double alpha ^long elapsed]
  (p/div
    (Math/exp
      (p/* alpha (double elapsed)))
    (r/rand)))

(def rescale-interval (t/hours 1))

(defn-operator moving-sample
  ([]
     (moving-sample nil))
  ([{:keys [window sample-size]
     :or {sample-size 1024
          window (t/hours 1)}}]
     (stream-aggregator-generator
       :ordered? false
       :create
       (fn [_]
         (let [lock (l/asymmetric-lock)
               samples (ConcurrentSkipListMap.)
               counter (AtomicLong. 0)
               start-time (AtomicLong. (now))
               next-rescale (AtomicLong. (p/+ (now) (long rescale-interval)))
               alpha (double (/ 8e-2 window))
               sample-size (long sample-size)]
           (stream-aggregator
             :process
             (fn [msgs]
               (let [elapsed (p/- (now) (.get start-time))]
                 (l/with-lock lock
                   (doseq [msg msgs]
                     (let [pr (priority alpha elapsed)]
                       (if (p/<= (.incrementAndGet counter) sample-size)
                         
                         ;; we don't have our full sample size, add everything
                         (.put samples pr msg)
                         
                         ;; check to see if we should displace an existing sample
                         (let [frst (double (.firstKey samples))]
                           (when (p/< frst pr)
                             (when-not (.putIfAbsent samples pr msg)
                               (loop [frst frst]
                                 (when-not (.remove samples frst)
                                   (recur (double (.firstKey samples))))))))))))))
             
             :deref
             (fn []
               (l/with-exclusive-lock lock
                 
                 ;; do we need to rescale?
                 (let [now (now)]
                   (when (p/>= now (.get next-rescale))
                     (.set next-rescale (p/+ (long (now)) (long rescale-interval)))
                     (let [prev-start-time (.get start-time)
                           _ (.set start-time now)
                           scale-factor (Math/exp
                                          (p/*
                                            (p/- alpha)
                                            (double (p/- now prev-start-time))))]
                       
                       ;; rescale each key
                       (doseq [k (keys samples)]
                         (let [val (.remove samples k)]
                           (.put samples (p/* scale-factor (double k)) val)))
                       
                       ;; make sure counter reflects size of collection
                       (.set counter (count samples)))))
                 
                 (doall (vals samples))))))))))

(defn-operator sample
  ([]
     (sample nil))
  ([{:keys [clear-on-reset? sample-size]
     :or {clear-on-reset? true
          sample-size 1024}}]
     (stream-aggregator-generator
       :ordered? false
       :create
       (fn [_]
         (let [sample-size (long sample-size)
               samples (AtomicReferenceArray. sample-size)
               counter (AtomicLong. 0)]
           (stream-aggregator
             :process
             (fn [msgs]
               (doseq [msg msgs]
                 (let [cnt (.incrementAndGet counter)]
                   (if (p/<= cnt sample-size)
                     
                     ;; we don't have our full sample size, add everything
                     (.set samples (p/dec cnt) msg)
                     
                     ;; check to see if we should displace an existing sample
                     (let [idx (r/rand-int cnt)]
                       (when (p/< idx sample-size)
                         (.set samples idx msg)))))))

             :deref
             (fn []
               (let [cnt (min (.get counter) sample-size)
                     ^objects ary (object-array cnt)]
                 (dotimes [i cnt]
                   (aset ary i (.get samples i)))
                 (seq ary)))))))))
