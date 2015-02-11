;; the top section includes all of the libraries
;; injected so that we can use their namespace

(ns recursiftion.controller
  (:use recursiftion.views
        [ring.adapter.jetty :only [run-jetty]])
  (:require [compojure.core :refer :all]
            [compojure.handler :as handler]
            [compojure.route :as route]
            [clojure.java.io :as io]
            [ring.util.io :refer [string-input-stream]]
            [ring.util.response :as resp]
            [ring.util.response :refer [response]]
            [ring.middleware.json :as middleware]
            [environ.core :refer [env]]
            [cheshire.core :refer :all]
            [recursiftion.model :as model]
            [monger.json]
            [clojure.pprint :refer [pprint]]))



;; here we define the REST API - each path triggers various functions

(defroutes app-routes

  (GET "/addword" []
       {:status 200
        :headers {"Content-Type" "text/html"}
        :body (add-word)})
  (POST "/createword" [word pos defins synons]
    (do (recursiftion.model/acceptword word pos defins synons)
        (recursiftion.model/traversesentence defins)
        (resp/redirect "/")))
  (GET "/burp" []
    (do (recursiftion.model/insertdb "burp" "red" "ranch")
        (resp/redirect "/")))
  (GET "/blue" []
       {:status 200
        :headers {"Content-Type" "text/html"}
        :body (get-page)}) 
  ; (GET "/low" [] (generate-string (recursiftion.model/getrequesting)))
  ; (POST "/ozzy" {body :body} (slurp body))
  ; (POST "/ozzy" body (recursiftion.model/acceptdata body))
  ; (POST "/ozzy" body (recursiftion.model/acceptdata body))
  ; (POST "/ozzy" {body :body}
    ; (POST "/ozzy"  {params :params}  (json-response (address/create params)))
      ; (do 
      ;   {:status 200
      ;     :headers {"Content-Type" "application/json"}
      ;     :body (generate-string (recursiftion.model/getrequesting) {:pretty true})}))
  ;   (json-response (recursiftion.model/getrequesting))
    ; (generate-string body {:pretty true}))




  ; (GET "/low" [] (generate-string (recursiftion.model/getrequesting)))
  ; (POST "/ozzy" {body :body} 
  ;   (generate-string (recursiftion.model/acceptdata body){:pretty true}))
  (POST "/ozzy" {body :body} 
    (generate-string (recursiftion.model/acceptdata body){:pretty true}))
       ; {:status 200
       ;  :headers {"Content-Type" "application/json"}
       ;  :body body})
  ; (POST "/ozzy" {riddle :riddle} (slurp riddle))
       ; {:status 200
       ;  :headers {"Content-Type" "application/json"}
       ;  :body riddle})
  ; (POST "/ozzy" body (recursiftion.model/acceptdata body))
  ; (POST "/ozzy" body (recursiftion.model/acceptdata body))
  ; (POST "/ozzy" {body :body}
    ; (POST "/ozzy" {params :params}  
    ;       {:status 200
    ;       :headers {"Content-Type" "application/json"}
    ;       :body (generate-string params {:pretty true})})
      ; (do 
      ;   {:status 200
      ;     :headers {"Content-Type" "application/json"}
      ;     :body (generate-string (recursiftion.model/getrequesting) {:pretty true})}))
  ;   (json-response (recursiftion.model/getrequesting))
    ; (generate-string body {:pretty true}))
  (POST "/funky" request
      (let [word (or (get-in request [:params :word])
                     (get-in request [:body :word])
                     "ROUTER_ERROR")]
      ; (do (binding [*out* *err*]
      ;       (pprint data)))

              {:status 200
          ; "Content-Type" = "text/html;charset=UTF-8";
         :headers {"Content-Type" "application/json"}
         :body (recursiftion.model/acceptdata word)
         ; :body {
         ;          :name (recursiftion.model/getrequesting)
         ;          :desc (str "The name you sent to me was ")
         ;       }
         ; :body {
         ;          :name likes
         ;          :desc (str "The name you sent to me was ")
         ;       }
        }
  ))
  (POST "/question" request
      (let [question_object (or (get-in request [:params :question_object])
                                (get-in request [:body :question_object])
                                "ROUTER_ERROR")]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (recursiftion.model/acceptQuestionAuthored question_object)
        }
  ))
  ; (POST "/categoryhistory" request
  ;     (let [categories_object (or (get-in request [:params :categories_object])
  ;                                 (get-in request [:body :categories_object])
  ;                             "ROUTER_ERROR")]
  ;       {:status 200
  ;        :headers {"Content-Type" "application/json"}
  ;        :body (recursiftion.model/insertCategory categories_object) ;;
  ;       }
  ; ))
  (POST "/searchcategories" request
      (let [categories_object (or (get-in request [:params :categories_object])
                                  (get-in request [:body :categories_object])
                              "ROUTER_ERROR")]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (recursiftion.model/searchCategories categories_object) ;;
        }
  ))
  (POST "/category" request
      (let [category_object (or (get-in request [:params :category_object])
                                (get-in request [:body :category_object])
                            "ROUTER_ERROR")]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (recursiftion.model/acceptCategory category_object)
        }
  ))
  (POST "/user" request
      (let [user_object (or (get-in request [:params :user_object])
                            (get-in request [:body :user_object])
                            "ROUTER_ERROR")]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (recursiftion.model/acceptUser user_object)
        }
  ))
  (GET "/low" [] 
       {:status 200
        :headers {"Content-Type" "application/json"}
        :body (generate-string (recursiftion.model/getrequesting) {:pretty true})}) 
  (GET "/" []
       {:status 200
        :headers {"Content-Type" "text/html"}
        ;;:body (pr-str ["Hello" :from 'DePaul])})
        :body (home-page)})
  (GET "/:string" [string] (main-page string))
  (GET "/:string/:pos" [string pos] (focus-page string pos))

)


;; error page for when response cannot be obtained

(defn wrap-error-page [handler]
  (fn [req]
    (try (handler req)
         (catch Exception e
           {:status 500
            :headers {"Content-Type" "text/html"}
            :body (slurp (io/resource "500.html"))}))))

; (def app (wrap-params routes)) ;; allows us to use parameters

(def app
 (-> (handler/api app-routes)
      (middleware/wrap-json-body {:keywords? true})
      middleware/wrap-json-response))

; (app {
;       :request-method :put 
;       :uri "/searches/1" 
;       :content-type "application/json" 
;       :body (string-input-stream (json-str {:key1 "val1"}))
;       })

(defn -main []
  (run-jetty app {:port (if (nil? (System/getenv "PORT"))
                          8000 ; localhost or heroku?
                          (Integer/parseInt (System/getenv "PORT")))}) )

