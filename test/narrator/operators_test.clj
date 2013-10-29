(ns narrator.operators-test
  (:use
    [clojure test]
    [narrator core])
  (:require
    [narrator.operators :as n]
    [criterium.core :as c]))

(deftest ^:benchmark benchmark-operators
  (println "rate")
  (let [op (compile-operators* n/rate)]
    (c/quick-bench
      (dotimes [_ 1000]
        (process! op nil))))

  (println "sum")
  (let [op (compile-operators* n/sum)]
    (c/quick-bench
      (process-all! op (range 1e3))))

  (println "mean")
  (let [op (compile-operators* n/mean)]
    (c/quick-bench
      (process-all! op (range 1e3)))))
