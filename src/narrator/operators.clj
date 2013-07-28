(ns narrator.operators
  (:use
    [potemkin])
  (:require
    [clojure.set :as set]
    [clojure.core.reducers :as r])
  (:import
    [java.util
     ArrayList]
    [java.util.concurrent
     ConcurrentHashMap]
    [java.util.concurrent.atomic
     AtomicLong]))

;;;

(definterface+ IBufferedAggregator
  (process! [_ msg])
  (flush-operator [_]))

(defprotocol+ StreamOperator
  (aggregator? [_])
  (ordered? [_])
  (reducer [_])
  (reset-operator! [_])
  (process-all! [_ msgs]))

;;;

(defn stream-reducer
  [& {:keys [reset ordered? reducer]
      :or {ordered? false}}]
  (assert reducer)
  (reify StreamOperator
    (aggregator? [_] false)
    (ordered? [_] ordered?)
    (reset-operator! [_] (when reset (reset)))
    (reducer [_] reducer)))

(defn stream-aggregator
  [& {:keys [name reset process flush deref ordered?]
      :or {ordered? false}
      :as args}]
  (assert (and process deref))
  (reify
    StreamOperator
    (aggregator? [_] true)
    (ordered? [_] ordered?)
    (reset-operator! [_] (when reset (reset)))
    (process-all! [_ msgs] (process msgs))

    IBufferedAggregator
    (process! [_ msg] (process [msg]))
    (flush-operator [_] (when flush (flush)))
    
    clojure.lang.IDeref
    (deref [_] (deref))))

(defmacro generator [& body]
  `(with-meta (fn [] ~@body) {::stream-generator true}))

(defn generator? [x]
  (-> x meta ::stream-generator))

(defn map-op [f]
  (generator
    (stream-reducer
      :reducer (r/map f)
      :ordered? false)))

(defn mapcat-op [f]
  (generator
    (stream-reducer
      :reducer (r/mapcat f)
      :ordered? false)))

(defonce stream-operators (atom {}))

(defmacro defn-operator [name & rest]
  (let [f (macroexpand
            `(defn ~name ~@rest))]
    `(do
       (~@(take-while (complement seq?) f)
        ~(let [fn-form (last f)]
           `(~@(take-while (complement seq?) fn-form)
             ~@(map
                 (fn [[args & body]]
                   `(~args (generator ~@body)))
                 (drop-while (complement seq?) fn-form)))))
       (alter-var-root (var ~name)
         (fn [f#]
           (with-meta f#{::stream-operator true})))
       (alter-meta! (var ~name) assoc ::stream-operator true)
       (swap! stream-operators assoc ~(str name) (var ~name))
       (var ~name))))

;;;

(def ^:dynamic *operator-wrapper* (fn [x _] x))

(defn- ->stream-reducer [x]
  (if (aggregator? x)
    (r/map #(do (process! x %) (flush-operator x) @x))
    (reducer x)))

(defn- unroll-generators [x]
  (->> x
    (iterate #(%))
    (take-while generator?)
    last))

(declare accumulator)

(defn operators->generator
  [ops]
  (let [generators (map
                     #(unroll-generators
                        (if (-> % meta ::stream-operator)
                          (%)
                          %))
                     ops)
        gen+instances (map #(list % (%)) generators)
        ordered? (->> gen+instances
                   (map second)
                   (some ordered?)
                   boolean)
        [pre [aggr & post]] [(->> gen+instances
                               (take-while (comp (complement aggregator?) second))
                               (map first))
                             (->> gen+instances
                               (drop-while (comp (complement aggregator?) second))
                               (map first))]]
    (if-not aggr
      (operators->generator
        (concat ops [(accumulator)]))
      (fn this
        ([]
           (this nil))
        ([hash]
           (let [pre (map #(%) pre)
                 aggr (aggr)
                 post (map #(%) post)
                 ops (concat pre [aggr] post)
                 pre (when (seq pre)
                       (->> pre
                         (map ->stream-reducer)
                         (apply comp)))
                 post (when (seq post)
                        (->> post
                          (map ->stream-reducer)
                          (apply comp)))
                 deref-fn (if post
                            #(first (into [] (post [@aggr])))
                            #(deref aggr))
                 process-fn (if pre
                              (if ordered?
                                #(process-all! aggr (into [] (pre %)))
                                #(process-all! aggr (r/foldcat (pre %))))
                              #(process-all! aggr %))
                 flush-ops (filter #(instance? IBufferedAggregator %) ops)]
             (*operator-wrapper*
               (stream-aggregator
                 :ordered? ordered?
                 :reset #(doseq [r ops] (reset-operator! r))
                 :flush #(doseq [r flush-ops] (flush-operator r))
                 :deref deref-fn
                 :process process-fn)
               (or hash (when ordered? (rand-int Integer/MAX_VALUE))))))))))

(defn compose-operators
  [ops]
  ((operators->generator ops)))

;;;

(defn-operator accumulator
  []
  (let [acc (atom (ArrayList.))]
    (stream-aggregator
      :process #(locking acc (.addAll ^ArrayList @acc %))
      :deref #(deref acc)
      :reset #(clojure.core/reset! acc (ArrayList.)))))

(defn-operator rate
  [& _]
  (let [cnt (AtomicLong. 0)]
    (stream-aggregator
      :process #(.addAndGet cnt (count %))
      :deref #(.get cnt)
      :reset #(.set cnt 0))))

(defn-operator sum
  [& _]
  (let [cnt (AtomicLong. 0)]
    (stream-aggregator
      :process #(.addAndGet cnt (reduce + %))
      :deref #(.get cnt)
      :reset #(.set cnt 0))))

(defn-operator by
  ([facet]
     (by facet nil nil))
  ([facet ops]
     (by facet nil ops))
  ([facet
    {:keys [expiration clear-on-reset?]
     :or {clear-on-reset? true}}
    ops]
     (let [m (ConcurrentHashMap.)
           de-nil #(if (nil? %) ::nil %)
           re-nil #(if (identical? ::nil %) nil %)
           generator (operators->generator ops)
           wrapper *operator-wrapper*
           ordered? (ordered? (generator))]
       (stream-aggregator
         :ordered? ordered?
         :process (fn [msgs]
                    (doseq [msg msgs]
                      (let [k (de-nil (facet msg))]
                        (if-let [op (.get m k)]
                          (process! op msg)
                          (binding [*operator-wrapper* wrapper]
                            (let [op (generator (when ordered? (hash k)))
                                  op (or (.putIfAbsent m k op) op)]
                              (process! op msg)))))))
         :flush #(doseq [x (vals m)]
                   (flush-operator x))
         :deref #(zipmap
                   (map re-nil (keys m))
                   (map deref (vals m)))
         :reset #(if clear-on-reset?
                   (.clear m)
                   (doseq [op (vals m)]
                     (reset-operator! op)))))))
