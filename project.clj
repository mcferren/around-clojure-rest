;;; Benjamin McFerren
;;; DePaul Univeristy CDM
;;; CSC 358
;;; 3/19/2014

(defproject helloworld "1.0.0-SNAPSHOT"
  :description "csc358 final project"
  :url "http://helloworld.herokuapp.com"
  :license {:name "FIXME: choose"
            :url "http://example.com/FIXME"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [compojure "1.1.8"]
                 [ring/ring-json "0.3.1"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [cheshire "5.4.0"]
                 [ring/ring-jetty-adapter "1.3.2"]
                 [environ "0.2.1"]
                 [hiccup "1.0.0"]
                 [com.novemberain/monger "2.0.0"]
                 [org.clojure/data.json "0.2.5"]]
                 ; [org.mortbay.thread]]
  :min-lein-version "2.0.0"
  :plugins [[environ/environ.lein "0.2.1"]
            [lein-ring "0.7.3"]]
  :hooks [environ.leiningen.hooks]
  ; :profiles {:production {:env {:production true}}};;
  :profiles {:dev {:dependencies [[ring-mock "0.1.3"]]}}
  :ring {:handler recursiftion.controller/app})
