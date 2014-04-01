(ns narrator.streaming-test
  (:use
    [clojure test]
    [narrator core])
  (:require
    [narrator.operators :as n]
    [criterium.core :as c])
  (:import
    [java.util
     UUID]))

(defn random-elements [n]
  (->> #(UUID/randomUUID)
    (repeatedly n)
    (map str)))

(defn false-positive-rate [op pool]
  (process-all! op (random-elements pool))
  (flush-operator op)
  (/ (- pool (count @op)) pool))

(deftest test-false-positives
  (dorun
    (for [cardinality [1e4 1e5]
          error [0.001 0.01 0.02 0.05 0.1]]
      (is
        (< (false-positive-rate
             (compile-operators* (n/quasi-distinct-by identity {:cardinality cardinality, :error error}))
             cardinality)
          (+ error (/ error 3)))))))
