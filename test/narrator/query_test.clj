(ns narrator.query-test
  (:use
    [narrator core operators query]
    [clojure test])
  (:require
    [criterium.core :as c]))

(def data
  (map #(hash-map :one 1 :n %) (range 1e3)))

(deftest test-basic-parsing
  (are [expected descriptor]
    (= expected (query-seq descriptor data))

    1000 rate
    1000 [:one sum]
    3000 [:one inc inc sum]
    3002 [:one inc inc sum inc inc]))

(deftest test-group-by
  (is (= {true 50000, false 50000}
        (query-seq
          (by even? rate)
          (range 1e5))))
  (is (= {true {true 50000} false {false 50000}}
        (query-seq
          (by even? (by even? rate))
          (range 1e5))))
  (is (= {0 25000, 1 25000, 2 25000, 3 25000}
        (query-seq
          (by #(rem % 4) rate)
          (range 1e5)))))

(deftest test-group-by
  (are [expected descriptor]
    (= expected (query-seq descriptor (range 1e5)))

    {true 50000, false 50000}
    (by even? rate)

    {true {true 50000}, false {false 50000}}
    (by even? (by even? rate))

    {0 25000, 1 25000, 2 25000, 3 25000}
    (by #(rem % 4) rate)))

(deftest test-split
  (is (= {:a 1000
          :b 1001
          :c 1002}
        (query-seq
          {:a [rate]
           :b [rate inc]
           :c [rate inc inc]}
          (range 1e3)))))

(deftest ^:benchmark benchmark-query-seq
  (c/quick-bench
    (query-seq
      rate
      (range 1e6)))
  (c/quick-bench
    (query-seq
      (by even? rate)
      (range 1e6)))
  (c/quick-bench
    (query-seq
      (by #(rem % 4) rate)
      (range 1e6))))

(deftest ^:stress stress-query-seq
  (apply =
    (repeatedly
      1e5
      (fn []
        (query-seq
          (by #(rem % 4)
            (by #(rem % 8)
              (by #(rem % 16)
                rate)))
          (range 1e6))))))
