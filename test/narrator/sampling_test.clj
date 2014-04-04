(ns narrator.sampling-test
  (:use
    [clojure test]
    [narrator query]
    [narrator.operators
     sampling])
  (:require
    [narrator.core :as nc]
    [criterium.core :as c]))

(def num-sanity-checks 1)

(deftest sanity-check-samples
  (testing "sample"
    (dotimes [_ num-sanity-checks]
      (let [s (nc/compile-operators* sample)
            _ (nc/process-all! s (range 1e6))
            val @s
            avg (double (/ (reduce + val) (count val)))]
        (is (< 4e5 avg 6e5))))))

(deftest ^:benchmark benchmark-samplers
  (println "sample")
  (let [s (nc/compile-operators* sample)]
    (c/quick-bench
      (nc/process-all! s (range 1e6)))))
