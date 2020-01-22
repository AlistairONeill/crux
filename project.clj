(def modules
  ["crux-core"
   "crux-rdf"
   "crux-rocksdb" "crux-lmdb"
   "crux-jdbc"
   "crux-http-client" "crux-http-server"
   "crux-kafka-embedded" "crux-kafka-connect" "crux-kafka"
   "crux-uberjar"
   "crux-test"])

(defproject juxt/crux-dev "crux-dev-SNAPSHOT"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :middleware [leiningen.project-version/middleware]

  :plugins [[lein-sub "0.3.0"]]
  :sub ~modules

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-lmdb "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-kafka-connect "derived-from-git"]
                 [juxt/crux-kafka-embedded "derived-from-git"]
                 [juxt/crux-jdbc "derived-from-git"]
                 [juxt/crux-http-server "derived-from-git"]
                 [juxt/crux-rdf "derived-from-git"]
                 [juxt/crux-test "derived-from-git"]
                 [org.clojure/tools.namespace "0.2.11"]]

  :profiles {:hakan {:dependencies [[org.ejml/ejml-dsparse "0.38"
                                     :exclusions [com.google.code.findbugs/jsr305]]]}

             :matrix {:dependencies [[org.roaringbitmap/RoaringBitmap "0.8.11"]]}}

  :source-paths ["dev"]

  :test-paths ["crux-core/test"
               "crux-rdf/test"
               "crux-rocksdb/test"
               "crux-lmdb/test"
               "crux-kafka-connect/test"
               "crux-kafka-embedded/test"
               "crux-kafka/test"
               "crux-jdbc/test"
               "crux-http-client/test"
               "crux-http-server/test"
               "crux-uberjar/test"
               "crux-test/test"]

  :jvm-opts ["-Dlogback.configurationFile=logback-dev.xml"]
  :global-vars {*warn-on-reflection* true}

  :aliases {"check" ["sub" "-s" ~(->> modules (remove #{"crux-jdbc"}) (clojure.string/join ":")) "check"]
            "build" ["do" ["sub" "install"] ["sub" "test"]]})
