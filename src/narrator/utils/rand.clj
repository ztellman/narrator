(ns narrator.utils.rand
  (:refer-clojure :exclude [rand rand-int])
  (:import
    [narrator.utils
     ThreadLocalRandom]))

(definline rand-int
  "An optimized random integer generator."
  [n]
  `(.nextLong (ThreadLocalRandom/current) ~n))

(defn rand
  "An optimized random floating-point generator."
  (^double []
     (.nextDouble (ThreadLocalRandom/current) 1))
  (^double [^double n]
     (.nextDouble (ThreadLocalRandom/current) n)))
