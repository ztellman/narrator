(ns narrator.utils.rand
  (:refer-clojure :exclude [rand rand-int])
  (:import
    [narrator.utils
     ThreadLocalRandom]))

(definline rand-int [n]
  `(.nextLong (ThreadLocalRandom/current) ~n))

(defn rand
  (^double []
     (.nextDouble (ThreadLocalRandom/current) 1))
  (^double [^double n]
     (.nextDouble (ThreadLocalRandom/current) n)))
