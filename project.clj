(defproject narrator "0.1.0-SNAPSHOT"
  :description "concise, expressive stream analysis"
  :url "http://example.com/FIXME"
  :license {:name "MIT License"
            :url ""}
  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :dependencies [[potemkin "0.3.2"]
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
                 [byte-transforms "0.1.1-SNAPSHOT"]
                 [com.clearspring.analytics/stream "2.4.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]
                                  [criterium "0.4.1"]
                                  [codox-md "0.2.0" :exclusions [org.clojure/clojure]]
                                  [org.clojure/core.async "0.1.0-SNAPSHOT"]]}}
  :global-vars {*warn-on-reflection* true}
  :test-selectors {:default #(not (or (:stress %) (:benchmark %)))
                   :benchmark :benchmark
                   :stress :stress}
  :plugins [[codox "0.6.4"]]
  :codox {:writer codox-md.writer/write-docs}
  :jvm-opts ^:replace ["-server" "-Xmx2g"]
  :java-source-paths ["src"]
  :javac-options ["-target" "1.5" "-source" "1.5"])
