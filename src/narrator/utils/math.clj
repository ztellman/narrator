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

(def ^:const ln2 (Math/log 2))

(defn log2 [x]
  (/ (Math/log x) ln2))

(defn lerp ^double [^double lower ^double upper ^double t]
  (+ lower (* t (- upper lower))))

(defn lerp-array ^double [^doubles ary ^double t]
  (let [len (Array/getLength ary)]
    (cond
      (== len 0) 0.0
      (== len 1) (aget ary 0)

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
