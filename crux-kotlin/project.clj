(defproject juxt/crux-kotlin "crux-git-version-beta"
  :description "Crux Kotlin"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.jetbrains.kotlin/kotlin-stdlib "1.4.21"]
                 [org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "crux-git-version-beta"]]
  :profiles {:test {:dependencies [[juxt/crux-test "crux-git-version"]]}}
  :middleware [leiningen.project-version/middleware]
  :prep-tasks ["kotlin"]
  :kotlin-compiler-version "1.4.21"
  :plugins [[lein-kotlin "0.0.2"]]
  :kotlin-source-path "src"
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :pedantic? :warn)
