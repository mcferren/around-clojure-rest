;; the top section includes all of the libraries
;; injected so that we can use their namespace

(ns recursiftion.dao_dictionary
  (:require [clojure.string]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.conversion :refer [from-db-object]]
            [clojure.pprint :refer [pprint]])
  (:import [com.mongodb MongoOptions ServerAddress]
           [org.bson.types.ObjectId])
  )



;; connect using connection URI stored in an env variable, in this case, MONGOHQ_URL
; (let [uri               (System/genenv "MONGOHQ_URL")
(def mongo-uri "mongodb://mcferren:Broadway$19@linus.mongohq.com:10028/app30824309")
; (def mongo-uri "mongodb://mcferren:Broadway$19@linus.mongohq.com:10028/app30824309?maxPoolSize=128&waitQueueMultiple=5;waitQueueTimeoutMS=150;socketTimeoutMS=5500&autoConnectRetry=true;safe=false&w=1;wtimeout=2500;fsync=true")



;; FIRST checks if there is present a userobject in the user mongo collection that matches in id;;
;; IF there is, then return that userobject
;; ELSE there is not, then create an empty user object prepopulated with just a category history and MAYBE SOME STRANGERS????
(defn checkOrInsertUser [userobject]
  (let [uobject userobject
        uri mongo-uri
          {:keys [conn db]} (mg/connect-via-uri uri)
        ucoll "users"]

      (do 
        (if (mc/any? db ucoll {:_id (get-in uobject [:id])})
            (mc/find-one-as-map db ucoll {:_id (get-in uobject [:id])})
            (mc/insert-and-return db ucoll {:_id (get-in uobject [:id]) 
                                            :name (get-in uobject [:name]) 
                                            :picture (get-in uobject [:picture])
                                            :entitytype "user"
                                            :questionsauthored [] 
                                            :questionsanswered []
                                            :strangerlikes []
                                            :strangerfriends []
                                            :categoryhistory ["training" "math" "dating" "commercial" "personal" "disney" "WORLD"]}))

        ; (mc/update db ucoll {:_id useridinput} {:upsert true});;;;
        ; (mc/update db ucoll {useridinput "{}"} {$set {:questions-authored "[]" :questions-ansered "[]"}} {:upsert true})
      )))


;; Generic method to query a mongodb collection in batch with an array of ids
(defn getEntityBatchById [entityarrayofids entitytype]
  (let [earray entityarrayofids 
        etype entitytype
        uri mongo-uri
            {:keys [conn db]} (mg/connect-via-uri uri)]

        (mc/find-maps db etype {:_id {$in earray}})
    ))



;; QUERIES the "questions" mongo collection for all id's in an array of question ids
;; RETURNS an array of question objects that match ids with that array
(defn getQuestionBatchById [questionarrayofids]
  (let [qarray questionarrayofids
        uri mongo-uri
            {:keys [conn db]} (mg/connect-via-uri uri)
        qcoll "questions"]

        (mc/find-maps db qcoll {:_id {$in qarray}})
    ))


;; Generic method to query a mongodb collection in batch with an array of ids
(defn getQuestionBatchByAuthorId [authorid]
  (let [aid authorid
        uri mongo-uri
            {:keys [conn db]} (mg/connect-via-uri uri)
        qcoll "questions"]

        (mc/find-maps db qcoll {:author aid})
    ))


;; FIRST queries the database for the like or friend object by id
;; IF it finds something, then returns it
;; ELSE it creates a new empty object
(defn getEntity [entityobject entitytype]
  (let [eobject entityobject
        eid (or
              (get-in eobject [:id])
              (get-in eobject [:_id]))
        uri mongo-uri
            {:keys [conn db]} (mg/connect-via-uri uri)
        typecoll entitytype]

        (do
            (if (mc/any? db typecoll {:_id eid})
                (mc/find-one-as-map db typecoll {:_id eid})     
                {
                  :id
                  (get-in eobject [:id])

                  :name
                  (get-in eobject [:name])

                  :picture
                  (get-in eobject [:picture])

                  :questionsanswered
                  []

                  :questionsauthored
                  []
                }
            ))))


;; INPUT is a question object with all the appropriate properties (NEED TO ADD VALIDATION)
;; QUERIES the database in multiple place input order to add the question object to the mongo questions collections
;; QUERIES also add the question id to the appropriate questions answered and questions authord propoerties
;; ******* a question authored is automatically made a question answered for the user who created the question
;; INSERTS and SORt also occurs with the category(ies) that the question has been tagged by
(defn insertQuestionAuthored [payload]
  (let [payloadinput payload
        uid (payloadinput :author)
        uri mongo-uri
            {:keys [conn db]} (mg/connect-via-uri uri)
        qcoll "questions"
        ucoll "users"
        ccoll "categories"
        qobject (mc/insert-and-return db qcoll {
                                                :author (get-in payloadinput [:author])
                                                :author_type (get-in payloadinput [:author_type])
                                                :categories (get-in payloadinput [:categories])
                                                :correct_answer (get-in payloadinput [:correct_answer])
                                                :options (get-in payloadinput [:options])
                                                :question (get-in payloadinput [:question])
                                                :timestamp (System/currentTimeMillis)
                                                })
        qid (qobject :_id)]

        (do 
            ;; INSERT FOR USER'S "QUESTIONS-AUTHORED" CHILD BLOCK
            (mc/update db ucoll {:_id uid} {$push {:questionsauthored qid}})

            ;; INSERT FOR USER'S "QUESTIONS-ANSWERED" CHILD BLOCK TOO - IF THEY ASKED THEN WE COUNT IT AS AN ANSWER TOO
            (mc/update db ucoll {:_id uid} {$push {:questionsanswered qid}})


            ;; INSERT INTO CATEGORIES MONGO COLLECTION
            (mc/insert-batch db ccoll 
              (filter identity 
                (map (fn [element] ;; validate whether they category is present before insert
                        (if (not (mc/any? db ccoll {:_id element}))
                            {:_id element}
                        )
                     )
                     (get-in payloadinput [:categories]) ;; iterating over array of category strings
                )
              )
            )

            ;; RETURN THE QUESTION OBJECT
            qobject
        )
    ))


;; UPDATES query result for user id by reordering (adding) the array of category state history
;; INPUT IS A USERID AND AN ARRAY OF CATEGORY TITLE STRINGS TO REARRANGE (UNSHIFT TO TOP POSITION)
;; RETURNS AN ARRAY OF CATEGORY TITLE STRINGS WHICH IS SORTED APPROPRIATELY
(defn updateCategoryHistory [userid questioncategories]
  (let [uid userid
        qcat questioncategories
        uri mongo-uri
            {:keys [conn db]} (mg/connect-via-uri uri)
        ucoll "users"]

        (do 
          ;; THIS BLOCK BASICALLY CUTS LIKE A DECK OF CARDS
          (mc/update db ucoll {:_id uid} {$pullAll {:categoryhistory qcat}})
          (mc/update db ucoll {:_id uid} {$pushAll {:categoryhistory qcat}})
          (mc/find-maps db ucoll {:_id uid} ["categoryhistory"])
        )
  )
)

;; simple crud Batch find by the cateogry property
(defn getQuestionsByCategory [categoryarrayofsearchstrings]
  (let [carrayofsearchstrings categoryarrayofsearchstrings
        uri mongo-uri
            {:keys [conn db]} (mg/connect-via-uri uri)
        qcoll "questions"]
        
        (mc/find-maps db qcoll {:categories {$in carrayofsearchstrings}})
    )
)

;; simple crud Batch find by an array of cateogry strings
;; RETURN can be multiple question objects
(defn getCategorySearch [searchstringarray]
  (let [ssarray searchstringarray
        uri mongo-uri
            {:keys [conn db]} (mg/connect-via-uri uri)
        ccoll "categories"
        returnarray []]

        (do
          (distinct
            (flatten
              (map (fn [outerelement]
                       (map (fn [innerelement]
                                (get-in innerelement [:_id]))
                            (mc/find-maps db ccoll {:_id {$regex (str ".*" outerelement ".*")}})
                       ))
                   ssarray
              );;
            )
          )
        )
    ))




















;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;
;; ******************************* BELOW ARE FUNCTIONS FOR AN APP CALLED PARROT ****************************** ;;
;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;
;; ************** EVERYTHING IS IN THE SAME REPO BECAUSE aROUND IS AN INPUT MECHANISM FOR PARROT ************* ;;
;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;
;; *********************************************************************************************************** ;;


(let [uri               mongo-uri
      {:keys [conn db]} (mg/connect-via-uri uri)
      coll "skittles"]
;   ; (mc/insert db "skittles" { :_id (ObjectId.) :first_name "John" :last_name "Lennon" })
    (mc/remove db coll)
      (mc/insert-and-return db coll {:name "blue" :age 30}))



; inserts one word into the database
(defn insertword [word color payload]
  (let [wordinput word
        colorinput color
        payloadinput payload
        uri mongo-uri
          {:keys [conn db]} (mg/connect-via-uri uri)
        coll "skittles"]

        ; (binding [*out* *err*]
        ;             (println (type {:word wordinput :color colorinput :payload payloadinput})))
    (mc/insert-and-return db coll {:word wordinput :color colorinput :payload payloadinput})      
  )
)


(defn insertPayload [payload]
  (let [payloadinput payload
        uri mongo-uri
          {:keys [conn db]} (mg/connect-via-uri uri)
        qcoll "questions"]

          (mc/insert-and-return db qcoll {:payload payloadinput})
    ))

(defn acceptpayload [payload]
  (let [payloadinput payload
        uri mongo-uri
          {:keys [conn db]} (mg/connect-via-uri uri)
        coll "skittles"]

    (mc/insert-and-return db coll {:payload payloadinput})  
    ))

; gets all from the database
(defn getall []
  (let [uri mongo-uri
      		{:keys [conn db]} (mg/connect-via-uri uri)
        coll "skittles"]

    ; (mc/find db coll {:name "blue"})	
    (from-db-object (mc/find-maps db coll { }) true)	
  )
)
