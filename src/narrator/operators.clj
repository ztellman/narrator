(ns narrator.operators
  (:refer-clojure :exclude [reset! group-by flush])
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
     AtomicLong]
    [org.cliffc.high_scale_lib
     Counter]))

;;;

(definterface+ IBufferedAggregator
  (offer! [_ msg])
  (flush [_]))

(defprotocol+ StreamOperator
  (aggregator? [_])
  (ordered? [_])
  (reducer [_])
  (reset! [_])
  (process! [_ msgs]))

;;;

(defn stream-reducer
  [& {:keys [reset ordered? reducer]
      :or {ordered? false}}]
  (assert reducer)
  (reify StreamOperator
    (aggregator? [_] false)
    (ordered? [_] ordered?)
    (reset! [_] (when reset (reset)))
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
    (reset! [_] (when reset (reset)))
    (process! [_ msgs] (process msgs))

    IBufferedAggregator
    (offer! [_ msg] (process [msg]))
    (flush [_] (when flush (flush)))
    
    clojure.lang.IDeref
    (deref [_] (deref))))

(defn unordered-map [f]
  (stream-reducer
    :reducer (r/map f)
    :ordered? false))

;;;

(defn- ->reducer [x]
  (if (aggregator? x)
    (r/map #(do (offer! x %) (flush x) @x))
    (reducer x)))

(declare accumulator)

(defn compose-operators
  [ops]
  (let [ordered? (boolean (some ordered? ops))
        [pre [aggr & post]] [(take-while (complement aggregator?) ops)
                             (drop-while (complement aggregator?) ops)]]
    (if aggr
      (let [pre (when (seq pre)
                  (->> pre
                    (map ->reducer)
                    (apply comp)))
            post (when (seq post)
                   (->> post
                     (map ->reducer)
                     (apply comp)))
            deref-fn (if post
                       #(first (into [] (post [@aggr])))
                       #(deref aggr))
            process-fn (if pre
                         (if ordered?
                           #(process! aggr (into [] (pre %)))
                           #(process! aggr (r/foldcat (pre %))))
                         #(process! aggr %))
            flush-ops (filter #(instance? IBufferedAggregator %) ops)]
        (stream-aggregator
          :ordered? ordered?
          :reset #(doseq [r ops] (reset! r))
          :flush #(doseq [r flush-ops] (flush r))
          :deref deref-fn
          :process process-fn))
      (compose-operators
        (concat ops [(accumulator)])))))

;;;

(def ^:dynamic *operator-wrapper* (fn [x _] x))

(defonce stream-operators (atom {}))

(defmacro defn-stream [name & rest]
  `(let [var# (defn ~name ~@rest)]
     (swap! stream-operators update-in [~(str name)]
       (fn [v#]
         (when (and v# (not= (-> v# meta :ns str) ~(str *ns*)))
           (println
             (str "WARNING: stream operator '" ~(str name) "' in "
               (-> v# meta :ns str)
               " is being shadowed by " ~(str *ns*))))
         var#))
     (alter-meta! var# assoc ::stream-operator true)
     var#))

(defn resolve-operator [x]
  (or
    (and
      (resolve x)
      (-> x meta ::stream-operator))
    (let [n (name x)]
      (get @stream-operators n))))

(defn operators->generator [ops]
  (let [op-generators
        (map
          (fn [x]
            (cond
              (symbol? x) (if-let [v (resolve-operator x)]
                            #(v)
                            (throw (IllegalArgumentException.
                                     (str "'" x "' is not a valid stream operator"))))
              (seq? x) (if-let [v (resolve-operator (first x))]
                         (let [args (map
                                      #(if-not (vector? %)
                                         (eval %)
                                         %)
                                      (rest x))]
                           #(apply v args))
                         ((throw (IllegalArgumentException.
                                   (str "'" (first x) "' is not a valid stream operator")))))
              (ifn? x) #(unordered-map x)
              :else (throw (IllegalArgumentException.
                             (str "'" (pr-str x) "' is not a valid stream operator")))))
          ops)]
    (fn this
      ([]
         (this nil))
      ([hash]
         (*operator-wrapper*
           (compose-operators
             (map #(%) op-generators))
           hash)))))

;;;

(defn-stream accumulator
  [& _]
  (let [acc (atom (ArrayList.))]
    (stream-aggregator
      :process #(locking acc (.addAll ^ArrayList @acc %))
      :deref #(deref acc)
      :reset #(clojure.core/reset! acc (ArrayList.)))))

(defn-stream rate
  [& _]
  (let [cnt (AtomicLong. 0)]
    (stream-aggregator
      :process #(.addAndGet cnt (count %))
      :deref #(.get cnt)
      :reset #(.set cnt 0))))

(defn-stream sum
  [& _]
  (let [cnt (AtomicLong. 0)]
    (stream-aggregator
      :process #(.addAndGet cnt (reduce + %))
      :deref #(.get cnt)
      :reset #(.set cnt 0))))

(defn-stream group-by
  ([facet]
     (group-by facet nil nil))
  ([facet ops]
     (group-by facet nil ops))
  ([facet {:keys [expiration]} ops]
     (let [m (ConcurrentHashMap.)
           de-nil #(if (nil? %) ::nil %)
           re-nil #(if (identical? ::nil %) nil %)
           generator (operators->generator ops)
           wrapper *operator-wrapper*
           ordered? (ordered? (generator nil))]
       (stream-aggregator
         :ordered? ordered?
         :process (fn [msgs]
                    (doseq [msg msgs]
                      (let [k (de-nil (facet msg))]
                        (if-let [op (.get m k)]
                          (offer! op msg)
                          (binding [*operator-wrapper* wrapper]
                            (let [op (generator (when ordered? (hash k)))
                                  op (or (.putIfAbsent m k op) op)]
                              (offer! op msg)))))))
         :flush #(doseq [x (vals m)]
                   (flush x))
         :deref #(zipmap
                   (map re-nil (keys m))
                   (map deref (vals m)))
         :reset #(doseq [op (vals m)]
                   (reset! op))))))
