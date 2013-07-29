(ns narrator.core
  (:use
    [potemkin])
  (:require
    [clojure.core.reducers :as r])
  (:import
    [java.util ArrayList]))

;;;

(definterface+ IBufferedAggregator
  (process! [_ msg])
  (flush-operator [_]))

(defprotocol+ StreamOperator
  (aggregator? [_])
  (ordered? [_])
  (reducer [_])
  (combiner [_])
  (reset-operator! [_])
  (process-all! [_ msgs]))

;;;

(defn stream-reducer
  "A stream operator which for each message emits zero or more messages.  It must be given
   a `reducer` parameter, which describes the reducer function that processes the messages.

   Optional parameters are:

   `ordered?` - if true, messages will be given in order on a single thread, defaults to false
   `reset` - a no-arg function which can be used to reset internal state, if it exists"
  [& {:keys [reset ordered? reducer]
      :or {ordered? false}}]
  (assert reducer)
  (reify StreamOperator
    (aggregator? [_] false)
    (combiner [_] nil)
    (ordered? [_] ordered?)
    (reset-operator! [_] (when reset (reset)))
    (reducer [_] reducer)))

(defn stream-aggregator
  "A stream operator which accepts messages, and can be dereferenced to get a description of
   all messages it has seen.  It must be given a `process` parameter, which is a single-arg
   function that takes a sequence of messages, and a `deref` parameter which is a no-arg
   function that returns a description of all messages it has seen.

   Optional parameters are:

   `ordered?` - if true, messagse will be given in order on a single thread, defaults to false
   `reset` - a no-arg function which resets internal state
   `flush` - a no-arg function which flushes any messages which are currently buffered
   `combiner` - a single-arg function which takes a sequence of dereferenced values and returns a single value.  This is only valid if `ordered?` is false.
   "
  [& {:keys [reset process flush deref ordered? combiner]
      :or {ordered? false}
      :as args}]
  (assert (and process deref))
  (assert (or (not ordered?) (not combiner)))
  (reify
    StreamOperator
    (aggregator? [_] true)
    (ordered? [_] ordered?)
    (combiner [_] combiner)
    (reset-operator! [_] (when reset (reset)))
    (process-all! [_ msgs] (process msgs))

    IBufferedAggregator
    (process! [_ msg] (process [msg]))
    (flush-operator [_] (when flush (flush)))
    
    clojure.lang.IDeref
    (deref [_] (if combiner
                 (combiner [(deref)])
                 (deref)))))

(defmacro generator [& body]
  `(with-meta (fn [] ~@body) {::stream-generator true}))

(defn generator? [x]
  (-> x meta ::stream-generator))

(defn map-op
  "Returns an unordered stream operator that maps `f` over every message."
  [f]
  (generator
    (stream-reducer
      :reducer (r/map f)
      :ordered? false)))

(defn mapcat-op
  "Returns an unordered stream operator that mapcats `f` over every message."
  [f]
  (generator
    (stream-reducer
      :reducer (r/mapcat f)
      :ordered? false)))

(defn reducer-op
  "Returns an unordered stream operator that applies the reducer `f` over the message stream."
  [f]
  (generator
    (stream-reducer
      :reducer f
      :ordered? false)))

(defonce stream-operators (atom {}))

(defmacro defn-operator
  "A variant of `defn` which is used to define a funciton which, given arguments,
  returns a stream operator."
  [name & rest]
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

(def ^:dynamic *now-fn* nil)

(defn ^long now
  "Returns a lower bound on the timestamp of messages flowing through the stream operators."
  ^long []
  (if *now-fn*
    (*now-fn*)
    (throw (IllegalStateException. "No global clock defined."))))

;;;

(def ^:dynamic *operator-wrapper* (fn [x _] x))

(def ^:dynamic *top-level-generator*)

(defn- ->stream-reducer [x]
  (if (aggregator? x)
    (r/map #(do (process! x %) (flush-operator x) @x))
    (reducer x)))

(defn- unroll-generators [x]
  (->> x
    (iterate #(%))
    (take-while generator?)
    last))

(declare accumulator split)

(defn- ->operator-generator [x]
  (unroll-generators
    (cond
      (satisfies? StreamOperator x) x
      (-> x meta ::stream-generator) x
      (-> x meta ::stream-operator) (x)
      (map? x) (split x)
      (ifn? x) (map-op x)
      :else (throw (IllegalArgumentException. (str "Don't know how to handle " (pr-str x)))))))

(defn compile-operators
  "Takes a descriptor of stream operations, and returns a function that generates a single
   stream operator that is the composition of all described operators."
  [op-descriptor]
  (if-not (sequential? op-descriptor)
    (compile-operators [op-descriptor])
    (let [generators (map ->operator-generator op-descriptor)
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
        (compile-operators
          (concat op-descriptor [(accumulator)]))
        (fn this
          ([]
             (this nil))
          ([hash]
             (binding [*top-level-generator* (or *top-level-generator* this)]
               (let [pre (map #(%) pre)
                     aggr (aggr)
                     post (map #(%) post)
                     ops (concat pre [aggr] post)
                     pre (when (seq pre)
                           (->> pre
                             (map ->stream-reducer)
                             reverse
                             (apply comp)))
                     post (when (seq post)
                            (->> post
                              (map ->stream-reducer)
                              reverse
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
                   (or hash (when ordered? (rand-int Integer/MAX_VALUE))))))))))))

(defn compile-operators*
  "Given a descriptor of stream operations, returns an instance of an operator that is the
   composition of all operations."
  [op-descriptor]
  ((compile-operators op-descriptor)))

;;;

(defn-operator accumulator
  "Yields a list of all messages seen since it has been reset."
  []
  (let [acc (atom (ArrayList.))]
    (stream-aggregator
      :process #(locking acc (.addAll ^ArrayList @acc %))
      :deref #(deref acc)
      :reset #(clojure.core/reset! acc (ArrayList.))
      :combiner #(apply concat %))))

(defn-operator split
  ""
  [name->ops]
  (let [ks (keys name->ops)
        ops (map compile-operators* (vals name->ops))]
    (stream-aggregator
      :ordered? (boolean (some ordered? ops))
      :process (fn [msgs]
                 (doseq [op ops]
                   (process-all! op msgs)))
      :flush #(doseq [x ops]
                (flush-operator x))
      :deref #(zipmap ks (map deref ops))
      :reset #(doseq [x ops]
                (reset-operator! x)))))
