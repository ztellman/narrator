(ns narrator.utils.math
  (:require
    [primitive-math :as p])
  (:import
    [java.util Arrays]
    [java.lang.reflect Array]))

(p/use-primitive-operators)

(defn ->sorted-double-array [s]
  (let [ary (double-array s)]
    (Arrays/sort ^doubles ary)
    ary))

(defn lerp ^double [^double lower ^double upper ^double t]
  (+ lower (* t (- upper lower))))

(defn lerp-array ^double [^doubles ary ^double t]
  (let [len (Array/getLength ary)]
    (cond

      (== 0 len)
      0.0

      (== 1 len)
      (aget ary 0)

      :else
      (if (== 1.0 t)
        (aget ary (dec len))
        (let [cnt (dec len)
              idx (* (double cnt) t)
              idx-floor (double (int idx))
              sub-t (- idx idx-floor)]
          (lerp
            (aget ary idx-floor)
            (aget ary (inc idx-floor))
            sub-t))))))

(defn quantiles [s quantiles]
  (let [ary (->sorted-double-array s)]
    (map #(lerp-array ary %) quantiles)))
