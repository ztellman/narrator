(ns narrator.operators-test
  (:use
    [clojure test]
    [narrator core operators])
  (:require
    [criterium.core :as c]))

(deftest ^:benchmark benchmark-operators
  (println "rate")
  (let [op (compile-operators* rate)]
    (c/quick-bench
      (dotimes [_ 1000]
        (process! op nil))))

  (println "sum")
  (let [op (compile-operators* sum)]
    (c/quick-bench
      (process-all! op (range 1e3)))))
