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
  (emitter- [_])
  (combiner [_])
  (ordered? [_])
  (create [_])
  (descriptor [_]))

(defn emitter [x]
  (or
    (when (instance? StreamOperatorGenerator x)
      (emitter- x))
    (-> x meta ::emitter)
    identity))

(defn deref' [x]
  ((emitter x) @x))

(definterface+ StreamOperator
  (reducer [_])
  (reset-operator! [_])
  (process-all! [_ msgs]))

;;;

(defn stream-processor
  "A stream operator which for each message emits zero or more messages.  It must be given
   a `reducer` parameter, which describes the reducer function that processes the messages.

   Optional parameters are:

   `reset` - a no-arg function which can be used to reset internal state, if it exists"
  [& {:keys [reset reducer]}]
  (assert reducer)
  (reify StreamOperator
    (reset-operator! [_] (when reset (reset)))
    (reducer [_] reducer)))

(defn stream-processor-generator
  [& {:keys [ordered? create descriptor]}]
  (assert create)
  (reify StreamOperatorGenerator
    (emitter- [_] identity)
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
  [& {:keys [reset process flush deref]
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
  [& {:keys [ordered? create descriptor combine emit]
      :or {emit identity}}]
  (assert create)
  (reify StreamOperatorGenerator
    (emitter- [_] emit)
    (combiner [_] combine)
    (aggregator? [_] true)
    (ordered? [_] ordered?)
    (create [_] (with-meta (create) {::emitter emit}))
    (descriptor [_] descriptor)))

(defn reducer-op
  "Returns an unordered stream operator that applies the reducer `f` over the message stream."
  [f]
  (stream-processor-generator
    :ordered? false
    :create (constantly
              (stream-processor
                :reducer f))))

(defn map-op
  "Returns an unordered stream operator that maps `f` over every message."
  [f]
  (reducer-op (r/map f)))

(defn mapcat-op
  "Returns an unordered stream operator that mapcats `f` over every message."
  [f]
  (reducer-op (r/mapcat f)))

(defn monoid-aggregator
  "Returns an unordered stream aggregator that combines messages via the two-arity `combine`
   function, starting with an initial value from the zero-arity `initial`. If the combined
   value needs to be processed before emitting, a custom `emit` function may be defined."
  [& {:keys [initial combine pre-process emit clear-on-reset?]
      :or {emit identity
           clear-on-reset? true}}]
  (stream-aggregator-generator
    :ordered? false
    :combine combine
    :emit emit
    :create (fn []
              (let [val (atom (initial))]
                (stream-aggregator
                  :reset (when clear-on-reset? #(reset! val (initial)))
                  :deref #(deref val)
                  :process (fn [msgs]
                             (let [msgs (if pre-process
                                          (map pre-process msgs)
                                          msgs)
                                   val' (reduce combine (initial) msgs)]
                               (swap! val combine val'))))))))

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

(defn capture-context []
  (let [vars [#'*compiled-operator-wrapper*
              #'*top-level-generator*
              #'*aggregator-generator-wrapper*]]
    (zipmap vars (map deref vars))))

(def ^:dynamic *execution-affinity* nil)

(defn top-level-generator []
  (when *top-level-generator*
    @*top-level-generator*))

(defn create-stream-processor [gen]
  (if (aggregator? gen)
    (let [combiner-fn (or (combiner gen) first)
          emitter-fn (emitter gen)
          op (create gen)]
      (stream-processor
        :reducer (r/map #(do (process! op %) (flush-operator op) (->> [(deref' op)] combiner-fn emitter-fn)))))
    (create gen)))

(declare accumulator split)

(defn ->operator-generator [x]
  (cond
    (instance? StreamOperatorGenerator x) x
    (-> x meta ::generator-generator) (x)
    (map? x) (split x)
    (ifn? x) (map-op x)
    :else (throw (IllegalArgumentException. (str "Don't know how to handle " (pr-str x))))))

(defn- combine-processors [fs]
  (when-let [fs (->> fs
                  (map reducer)
                  reverse
                  seq)]
    (apply comp fs)))

(defn compile-operators
  "Takes a descriptor of stream operations, and returns a function that generates a single
   stream operator that is the composition of all described operators."
  ([op-descriptor]
     (compile-operators op-descriptor true))
  ([op-descriptor top-level?]
     (cond
       (-> op-descriptor meta ::compiled)
       op-descriptor

       (not (sequential? op-descriptor))
       (compile-operators [op-descriptor] top-level?)

       :else
       (let [generators (map ->operator-generator op-descriptor)
             [pre [aggr & post]] [(take-while (complement aggregator?) generators)
                                  (drop-while (complement aggregator?) generators)]]
         (if-not aggr
           (compile-operators
             (concat op-descriptor [(accumulator)])
             top-level?)
           (let [generator (promise)
                 ordered? (or
                            (some ordered? pre)
                            (and (ordered? aggr) ;; assumes that we can do thread-local aggregation
                              (not (combiner aggr))))
                 pre (map create-stream-processor pre)
                 post (map create-stream-processor post)
                 ops (concat pre post)
                 pre (combine-processors pre)
                 post (combine-processors post)]
             (deliver generator
               (with-meta
                 (stream-aggregator-generator
                   :descriptor op-descriptor
                   :ordered? ordered?
                   :combine (combiner aggr)
                   :emit (let [aggr-emitter (emitter aggr)]
                           (if post
                             #(first (into [] (post [(aggr-emitter %)])))
                             aggr-emitter))
                   :create (fn []
                             (with-bindings (if top-level?
                                              {#'*top-level-generator* generator}
                                              {})
                               (let [aggr-generator (*aggregator-generator-wrapper* aggr)
                                     aggr (create aggr-generator)
                                     ops (conj ops aggr)
                                     process-fn (if pre
                                                  (if ordered?
                                                    #(process-all! aggr (into [] (pre %)))
                                                    #(process-all! aggr (r/foldcat (pre %))))
                                                  #(process-all! aggr %))
                                     flush-ops (filter #(instance? IBufferedAggregator %) ops)]
                                 ((if top-level?
                                    *compiled-operator-wrapper*
                                    identity)
                                   (stream-aggregator
                                     :ordered? ordered?
                                     :reset #(doseq [r ops] (reset-operator! r))
                                     :flush #(doseq [r flush-ops] (flush-operator r))
                                     :deref #(deref aggr)
                                     :process process-fn))))))
                 {::compiled true}))
             @generator))))))

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
    :combine #(apply concat %)
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
        generators (map #(compile-operators % false) (vals name->ops))
        ordered? (boolean (some ordered? generators))]
    (stream-aggregator-generator
      :descriptor name->ops
      :ordered? ordered?
      :combine (let [cs (map combiner generators)]
                 (when (every? (complement nil?) cs)
                   (fn [xs]
                     (zipmap
                       ks
                       (map
                         (fn [f k]
                           (->> xs
                             (map #(get % k ::none))
                             (remove #(identical? ::none %))
                             (f)))
                         cs
                         ks)))))
      :emit (let [emitters (map emitter generators)]
              (fn [m]
                (zipmap
                  ks
                  (map #(%1 (get m %2)) emitters ks))))
      :create (fn []
                (let [ops (doall
                            (map create generators))]
                  (stream-aggregator
                    :process (fn [msgs]
                               (doseq [op ops]
                                 (process-all! op msgs)))
                    :flush #(doseq [x ops]
                              (flush-operator x))
                    :deref #(zipmap ks (map deref ops))
                    :reset #(doseq [x ops]
                              (reset-operator! x))))))))
