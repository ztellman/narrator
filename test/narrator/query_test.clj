(ns narrator.query-test
  (:require
    [clojure.test :refer :all]
    [narrator.query :refer :all]
    [criterium.core :as c]
    [narrator.operators :refer :all]))

(defn-operator incr []
  (map-op inc))

(deftest test-basic-parsing
  (is (= 1000 (query-seq [rate] (range 1e3))))
  (is (= 3000 (query-seq [incr incr sum sum] (repeat 1e3 1))))
  (is (= 3002 (query-seq [incr incr sum sum sum incr incr] (repeat 1e3 1)))))

(deftest test-group-by
  (is (= {true 50000, false 50000}
        (query-seq [(by even? [rate])] (range 1e5))))
  (is (= {true {true 50000} false {false 50000}}
        (query-seq [(by even? [(by even? [rate])])] (range 1e5))))
  (is (= {0 25000, 1 25000, 2 25000, 3 25000}
        (query-seq [(by #(rem % 4) [rate])] (range 1e5)))))

(deftest ^:benchmark benchmark-query-seq
  (c/quick-bench
    (query-seq
      [rate]
      (range 1e6)))
  (c/quick-bench
    (query-seq
      [(by even? [rate])]
      (range 1e6)))
  (c/quick-bench
    (query-seq
      [(by #(rem % 4) [rate])]
      (range 1e6))))

(deftest ^:benchmark stress-query-seq
  (apply =
    (repeatedly
      1e4
      (fn []
        (query-seq
          [(by #(rem % 4) [(by #(rem % 8) [(by #(rem % 16) [rate])])])]
          (range 1e5))))))
