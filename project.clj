(defproject narrator "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "MIT License"
            :url ""}
  :dependencies [[potemkin "0.3.1"]
                 [com.github.stephenc.high-scale-lib/high-scale-lib "1.1.4"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]
                                  [criterium "0.4.1"]]}}
  :warn-on-reflection true
  :test-selectors {:default (complement :benchmark)
                   :benchmark :benchmark}
  :jvm-opts ^:replace ["-server"])
