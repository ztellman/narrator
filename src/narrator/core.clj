(ns narrator.core
  (:use
    [potemkin])
  (:require
    [clojure.core.reducers :as r]
    [narrator.utils.rand :as rand])
  (:import
    [java.util ArrayList]))

;;;

(definterface+ IBufferedAggregator
  (process! [_ msg])
  (flush-operator [_]))

(definterface+ StreamOperatorGenerator
  (aggregator? [_])
  (combiner [_])
  (ordered? [_])
  (create [_])
  (descriptor [_]))

(definterface+ StreamOperator
  (reducer [_])
  (reset-operator! [_])
  (process-all! [_ msgs]))

;;;

(defn stream-reducer
  "A stream operator which for each message emits zero or more messages.  It must be given
   a `reducer` parameter, which describes the reducer function that processes the messages.

   Optional parameters are:

   `reset` - a no-arg function which can be used to reset internal state, if it exists"
  [& {:keys [reset reducer]}]
  (assert reducer)
  (reify StreamOperator
    (reset-operator! [_] (when reset (reset)))
    (reducer [_] reducer)))

(defn stream-reducer-generator
  [& {:keys [ordered? create descriptor]}]
  (assert create)
  (reify StreamOperatorGenerator
    (combiner [_] nil)
    (aggregator? [_] false)
    (ordered? [_] ordered?)
    (create [_] (create))
    (descriptor [_] descriptor)))

(defn stream-aggregator
  "A stream operator which accepts messages, and can be dereferenced to get a description of
   all messages it has seen.  It must be given a `process` parameter, which is a single-arg
   function that takes a sequence of messages, and a `deref` parameter which is a no-arg
   function that returns a description of all messages it has seen.

   Optional parameters are:

   `reset` - a no-arg function which resets internal state
   `flush` - a no-arg function which flushes any messages which are currently buffered
   "
  [& {:keys [reset process flush deref ordered?]
      :or {ordered? false}
      :as args}]
  (assert (and process deref))
  (reify
    StreamOperator
    (reducer [_] nil)
    (reset-operator! [_] (when reset (reset)))
    (process-all! [_ msgs] (process msgs))

    IBufferedAggregator
    (process! [_ msg] (process [msg]))
    (flush-operator [_] (when flush (flush)))
    
    clojure.lang.IDeref
    (deref [_] (deref))))

(defn stream-aggregator-generator
  [& {:keys [ordered? create descriptor combiner]}]
  (assert create)
  (reify StreamOperatorGenerator
    (combiner [_] combiner)
    (aggregator? [_] true)
    (ordered? [_] ordered?)
    (create [_] (create))
    (descriptor [_] descriptor)))

(defn map-op
  "Returns an unordered stream operator that maps `f` over every message."
  [f]
  (stream-reducer-generator
    :ordered? false
    :create (constantly
              (stream-reducer
                :reducer (r/map f)))))

(defn mapcat-op
  "Returns an unordered stream operator that mapcats `f` over every message."
  [f]
  (stream-reducer-generator
    :ordered? false
    :create (constantly
              (stream-reducer
                :reducer (r/mapcat f)))))

(defn reducer-op
  "Returns an unordered stream operator that applies the reducer `f` over the message stream."
  [f]
  (stream-reducer-generator
    :ordered? false
    :create (constantly
              (stream-reducer
                :reducer f))))

;;;

(def ^:dynamic *now-fn* nil)

(defn ^long now
  "Returns a lower bound on the timestamp of messages flowing through the stream operators."
  ^long []
  (if *now-fn*
    (*now-fn*)
    (throw (IllegalStateException. "No global clock defined."))))

;;;

(defmacro defn-operator [name & rest]
  `(do
     (defn ~name ~@rest)
     (alter-meta! (var ~name) assoc ::generator-generator true)
     (alter-var-root (var ~name) (fn [f#] (with-meta f# {::generator-generator true})))
     (var ~name)))

(def ^:dynamic *compiled-operator-wrapper* identity)
(def ^:dynamic *top-level-generator* nil)
(def ^:dynamic *aggregator-generator-wrapper* identity)
(def ^:dynamic *execution-affinity* nil)

(defn top-level-generator []
  (when *top-level-generator*
    @*top-level-generator*))

(defn- create-stream-reducer [gen]
  (if (aggregator? gen)
    (let [reducer-fn (or (reducer gen) first)
          op (create gen)]
      (stream-reducer
        :reducer (r/map #(do (process! op %) (flush-operator op) (reducer-fn [@op])))))
    (create gen)))

(declare accumulator split)

(defn- ->operator-generator [x]
  (cond
    (instance? StreamOperatorGenerator x) x
    (-> x meta ::generator-generator) (x)
    (map? x) (split x)
    (ifn? x) (map-op x)
    :else (throw (IllegalArgumentException. (str "Don't know how to handle " (pr-str x))))))

(defn compile-operators
  "Takes a descriptor of stream operations, and returns a function that generates a single
   stream operator that is the composition of all described operators."
  [op-descriptor]
  (if-not (sequential? op-descriptor)
    (compile-operators [op-descriptor])
    (let [generators (map ->operator-generator op-descriptor)
          [pre [aggr & post]] [(take-while (complement aggregator?) generators)
                               (drop-while (complement aggregator?) generators)]
          aggr (*aggregator-generator-wrapper* aggr)
          ordered? (or
                     (some ordered? pre)
                     (ordered? aggr))
          agg-combiner (or (combiner aggr) first)]
      (if-not aggr
        (compile-operators
          (concat op-descriptor [(accumulator)]))
        (let [generator (promise)]
          (deliver generator
            (stream-aggregator-generator
              :descriptor op-descriptor
              :ordered? ordered?
              :create (fn []
                        (binding [*top-level-generator* (or *top-level-generator* @generator)]
                          (let [aggr (create aggr)
                                pre (map create-stream-reducer pre)
                                post (map create-stream-reducer post)
                                ops (concat pre [aggr] post)
                                pre (when (seq pre)
                                      (->> pre (map reducer) reverse (apply comp)))
                                post (when (seq post)
                                       (->> post (map reducer) reverse (apply comp)))
                                deref-fn (if post
                                           #(first (into [] (post [(agg-combiner [@aggr])])))
                                           #(agg-combiner [(deref aggr)]))
                                process-fn (if pre
                                             (if ordered?
                                               #(process-all! aggr (into [] (pre %)))
                                               #(process-all! aggr (r/foldcat (pre %))))
                                             #(process-all! aggr %))
                                flush-ops (filter #(instance? IBufferedAggregator %) ops)]
                            (*compiled-operator-wrapper*
                              (stream-aggregator
                                :ordered? ordered?
                                :reset #(doseq [r ops] (reset-operator! r))
                                :flush #(doseq [r flush-ops] (flush-operator r))
                                :deref deref-fn
                                :process process-fn)))))))
          @generator)))))

(defn compile-operators*
  "Given a descriptor of stream operations, returns an instance of an operator that is the
   composition of all operations."
  [op-descriptor]
  (create (compile-operators op-descriptor)))

;;;

(defn-operator accumulator
  "Yields a list of all messages seen since it has been reset."
  []
  (stream-aggregator-generator
    :ordered? true
    :create (fn []
              (let [acc (atom (ArrayList.))]
                (stream-aggregator
                  :process #(locking acc (.addAll ^ArrayList @acc %))
                  :deref #(deref acc)
                  :reset #(clojure.core/reset! acc (ArrayList.)))))))

(defn-operator split
  ""
  [name->ops]
  (let [ks (keys name->ops)
        generators (map compile-operators (vals name->ops))
        ordered? (boolean (some ordered? generators))]
    (stream-aggregator-generator
      :descriptor name->ops
      :ordered? ordered?
      :create (fn []
                (let [ops (doall (map create generators))]
                  (stream-aggregator
                    :process (fn [msgs]
                               (doseq [op ops]
                                 (process-all! op msgs)))
                    :flush #(doseq [x ops]
                              (flush-operator x))
                    :deref #(zipmap ks (map deref ops))
                    :reset #(doseq [x ops]
                              (reset-operator! x))))))))
