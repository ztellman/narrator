(ns narrator.query-test
  (:use
    [narrator core query]
    [clojure test])
  (:require
    [clojure.core.async :as a]
    [narrator.operators :as n]
    [criterium.core :as c]))

(def data
  (map #(hash-map :one 1 :n %) (range 1e3)))

(deftest test-basic-parsing
  (are [expected descriptor]
    (= expected (query-seq descriptor data))

    1000 n/rate
    1000 [:one n/sum]
    3000 [:one inc inc n/sum]
    3002 [:one inc inc n/sum inc inc]))

(deftest test-periodic-basic-parsing
  (are [expected descriptor period]
    (= expected (map :value (query-seq descriptor {:timestamp :n, :period period} data)))

    (range 1e3) [:n n/sum] 1))

(deftest test-group-by
  (is (= {true 50000, false 50000}
        (query-seq
          (n/group-by even? n/rate)
          (range 1e5))))
  (is (= {true {true 50000} false {false 50000}}
        (query-seq
          (n/group-by even? (n/group-by even? n/rate))
          (range 1e5))))
  (is (= {0 25000, 1 25000, 2 25000, 3 25000}
        (query-seq
          (n/group-by #(rem % 4) n/rate)
          (range 1e5)))))

(deftest test-group-by
  (are [expected descriptor]
    (= expected (query-seq descriptor (range 1e5)))

    {true 50000, false 50000}
    (n/group-by even? n/rate)

    {true {true 50000}, false {false 50000}}
    (n/group-by even? (n/group-by even? n/rate))

    {0 25000, 1 25000, 2 25000, 3 25000}
    (n/group-by #(rem % 4) n/rate)))

(deftest test-split
  (is (= {:a 1000
          :b 1001
          :c 1002}
        (query-seq
          {:a [n/rate]
           :b [n/rate inc]
           :c [n/rate inc inc]}
          (range 1e3)))))

(deftest test-recur
  (let [x {:name "foo"
           :children [{:name "bar"
                       :children [{:name "quux"}]}
                      {:name "baz"}
                      {:name "baz"}]}]
    (is (=
          {"foo" {:children {"bar" {:children {"quux" {:children nil, :rate 1}}
                                    :rate 1}
                             "baz" {:children nil
                                    :rate 2}}
                  :rate 1}}
          (query-seq
            (n/group-by :name {:rate n/rate, :children [:children n/concat n/recur]})
            [x])))))

;;;

(defn seq->channel [s]
  (let [out (a/chan)]
    (a/thread
      (loop [s s]
        (when-not (empty? s)
          (a/>!! out (first s))
          (recur (rest s))))
      (a/close! out))
    out))

(defn channel->seq [ch]
  (lazy-seq
    (let [msg (a/<!! ch)]
      (when-not (nil? msg)
        (cons msg (channel->seq ch))))))

(deftest test-query-channel
  (are [expected descriptor]
    (= expected (->> data
                  seq->channel
                  (query-channel descriptor {:period 1e6})
                  channel->seq
                  first))

    1000 n/rate
    1000 [:one n/sum]
    3000 [:one inc inc n/sum]
    3002 [:one inc inc n/sum inc inc]))

;;;

(deftest ^:benchmark benchmark-query-seq
  (c/quick-bench
    (query-seq
      n/rate
      (range 1e6)))
  (c/quick-bench
    (query-seq
      (n/group-by even? n/rate)
      (range 1e6)))
  (c/quick-bench
    (query-seq
      (n/group-by #(rem % 4) n/rate)
      (range 1e6))))

(deftest ^:stress stress-query-seq
  (apply =
    (repeatedly
      1e5
      (fn []
        (query-seq
          (n/group-by #(rem % 4)
            (n/group-by #(rem % 8)
              (n/group-by #(rem % 16)
                n/rate)))
          (range 1e6))))))
