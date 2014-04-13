(ns narrator.utils.rand
  (:refer-clojure :exclude [rand rand-int])
  (:import
    [narrator.utils
     ThreadLocalRandom]))

(defn ^long rand-int
  "An optimized random integer generator."
  (^long []
     (.nextLong (ThreadLocalRandom/current) (long Integer/MAX_VALUE)))
  (^long [n]
     (.nextLong (ThreadLocalRandom/current) (long n))))

(defn rand
  "An optimized random floating-point generator."
  (^double []
     (.nextDouble (ThreadLocalRandom/current) 1))
  (^double [^double n]
     (.nextDouble (ThreadLocalRandom/current) n)))
