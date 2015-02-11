;; inject libraries so that we can use the namespace in our views

(ns recursiftion.views
  (:require [hiccup.core :refer (html)]
            [hiccup.form :as f]
            [recursiftion.model :as model]))


;; wrapper method to keep things modular

(defn layout [title & content]
  (html
    [:head [:title title]]
    [:body content]))


;; this view displays the entire dictionary

(defn home-page []
  (let [dict (recursiftion.model/all)]
    (layout "HOME PAGE"
            [:h1 "Home Page"]
            [:a {:href "/addword"} "Add"]
            [:h2 (str dict)])))


;; this view displays the response from GET request

(defn get-page []
  (let [response (recursiftion.model/getrequesting)]
    (layout "GETTING"
            [:h1 "GETTING"]
            [:h2 (str response)])))



;; this view offers a gimpse of the word object

(defn main-page [id]
  (let [varwordobject (recursiftion.model/retrieveword id)]
    (layout "Word"
            [:h1 id]
            [:h2 (str "Edit Post: " varwordobject)])))


;; this view offers reports relative to a specific word / pos tuple

(defn focus-page [id pos]
  (let [varwordobject (recursiftion.model/retrieveword id)
        varsentencereport (recursiftion.model/getsentencereport id pos)
        varwordpositionreport (recursiftion.model/getwordpositionreport id pos)
       ]

    (layout "Word - Part of Speech"
            [:h1 id " - " pos]
            [:hr (str "Sentence Report: " varsentencereport)]
            [:hr (str "Word Position Report: " varwordpositionreport)]
            [:hr]
            [:h2 (str "Word Object: " varwordobject)])))


;; this views offers a number of input elements that POST their
;; data points to the server side

(defn add-word []
  (layout "Recursiftion"
    (list
      [:h2 "Add Word"]
      (f/form-to [:post "/createword"]
        (f/label "word" "Word")
        (f/text-field "word") [:br]
        (f/label "pos" "Part of Speech")
        (f/text-field "pos") [:br]
        (f/label "defins" "Definition")
        (f/text-field {:rows 20} "defins") [:br]
        (f/label "synons" "Synonems")
        (f/text-field {:rows 20} "synons") [:br]
        (f/submit-button "Save")))))
