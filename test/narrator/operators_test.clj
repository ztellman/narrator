(ns narrator.operators-test
  (:require
    [clojure.test :refer :all]
    [criterium.core :as c]
    [narrator.executor :as ex]
    [narrator.operators :refer :all]))

(deftest ^:benchmark benchmark-operators
  (println "rate")
  (let [op ((rate))]
    (c/quick-bench
      (dotimes [_ 1000]
        (process! op nil))))

  (println "sum")
  (let [op ((sum))]
    (c/quick-bench
      (process-all! op (range 1e3)))))
