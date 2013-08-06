(ns narrator.bloom-filter-test
  (:use
    [clojure test])
  (:require
    [narrator.utils.bloom-filter :as b]
    [criterium.core :as c])
  (:import
    [java.util
     UUID]))

(defn random-elements [n]
  (->> #(UUID/randomUUID)
    (repeatedly n)
    (map str)))

(defn false-positive-rate [s samples pool]
  (let [es (random-elements pool)]
    (doseq [x (take samples es)]
      (b/add! s x))
    (double
      (/ (- (->> es (filter #(b/contains? s %)) count) samples)
        pool))))

(deftest test-false-positives
  (dorun
    (for [cardinality [1e3 2e3 5e3 1e4]
          error [0.001 0.01 0.02 0.05 0.1]]
      (is
        (< (false-positive-rate (b/bloom-filter cardinality error) 1e3 1e5)
          (+ error (/ error 5)))))))

(deftest ^:benchmark benchmark-bloom-filter
  (let [x (first (random-elements 1))
        b (b/bloom-filter false 1e3 0.01)]
    (println "add element")
    (c/quick-bench
      (b/add! b x))
    (println "check element")
    (c/quick-bench
      (b/contains? b x))))
