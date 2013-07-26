(ns narrator.query-test
  (:require
    [clojure.test :refer :all]
    [narrator.query :refer :all]
    [criterium.core :as c]
    [narrator.operators :as op]))

(op/defn-stream incr []
  (op/unordered-map inc))

(deftest test-basic-parsing
  (is (= 1000 (query-seq '[rate] (range 1e3))))
  (is (= 3000 (query-seq '[incr incr sum sum] (repeat 1e3 1))))
  (is (= 3002 (query-seq '[incr incr sum sum sum incr incr] (repeat 1e3 1)))))

(deftest test-group-by
  (is (= {true 50000, false 50000}
        (query-seq '[(group-by even? [rate])] (range 1e5))))
  (is (= {true {true 50000} false {false 50000}}
        (query-seq '[(group-by even? [(group-by even? [rate])])] (range 1e5)))))

(deftest ^:benchmark benchmark-query-seq
  (c/quick-bench
    (query-seq
      '[rate]
      (range 1e6)))
  (c/quick-bench
    (query-seq
      '[(group-by even? [rate])]
      (range 1e6)))
  #_(c/quick-bench
    (query-seq
      '[(group-by #(rem % 4) [rate])]
      (range 1e6))))
