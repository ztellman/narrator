(ns narrator.operators.sampling
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

                 ary))))))))

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
       [(sample options)
        #(zipmap quantiles (m/quantiles % quantiles))])))

