(ns mysema.ttl-reader-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [mysema.ttl-reader :refer :all])
  (:import [java.io File]
           (mysema.ttl Splitter)))


(deftest word-splitter-test

  (is (= (Splitter/splitToWords "a .")
         ["a" "."]))

  (is (= (Splitter/splitToWords "a \"\"")
         ["a" "\"\""]))

  (is (= (Splitter/splitToWords "a \"\\\"\"")
         ["a" "\"\"\""]))

  (is (= (Splitter/splitToWords "a \"\",")
         ["a" "\"\""]))

  (is (= (Splitter/splitToWords "a \"\" .")
         ["a" "\"\"" "."]))

  (is (= (Splitter/splitToWords "a \"\".")
         ["a" "\"\"" "."]))

  (is (= (Splitter/splitToWords "a \"\"\"\"\"\"")
         ["a" "\"\""]))

  (is (= (Splitter/splitToWords "a \"\"\"\"\"\",")
         ["a" "\"\""]))

  (is (= (Splitter/splitToWords "a \"\"\"\"\"\" .")
         ["a" "\"\"" "."]))

  (is (= (Splitter/splitToWords "a \"\"\"\"\"\".")
         ["a" "\"\"" "."]))

  (is (= (Splitter/splitToWords "a \"\"\"\\\"\"\"\"")
         ["a" "\"\"\""]))

  (is (= (Splitter/splitToWords "a \"\"\"b\"\"\"")
         ["a" "\"b\""]))

  (is (= (Splitter/splitToWords "a \"\"\"b\"\"\"\"")
         ["a" "\"b\"\""]))

  ;; grr finto yso contains unescaped double quotes in long string format
  ;; against the turtle spec
  (is (= (Splitter/splitToWords "a \"\"\"\"bee\"\"\"\"")
         ["a" "\"\"bee\"\""]))

  (is (= (Splitter/splitToWords "a \"\"\"b \\\"\"\"\"@fi .")
         ["a" "\"b \"\"" "@fi" "."]))

  (is (= (Splitter/splitToWords "a b, c ,   \"d \"; \" f  \\\"\" \"g\", .")
         ["a" "b" "c" "\"d \"" ";" "\" f  \"\"" "\"g\"" "."]))

  (is (= (Splitter/splitToWords "    skos:prefLabel \"wood stoves\"@en,")
         ["skos:prefLabel" "\"wood stoves\"" "@en"]))

  )

(def data1
  ["<rdf:type>"
   "        a       <owl:Class> ;"
   "        <http://www.w3.org/2002/07/owl#DataTypeProperty>"
   "             <file:///foo> , <file:///boo> ;"
   "        <dc:dateCreated>"
   "            \"2013-01-28T12:58:13.077Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;"
   "        <skos:prefLabel>"
   "             \"web link\"@en , \"webblänk\"@sv , \"verkkolinkki\"@fi ."])

(def data2
  ["@prefix yso: <http://www.yso.fi/onto/yso/> ."
   ""
   "yso:p3956 a skos:Concept ;"
   "    skos:prefLabel \"wood stoves\"@en,"
   "        \"puuliedet\"@fi,"
   "        \"vedspisar\"@sv ;"
   "    skos:broader yso:p3635,"
   "        yso:p3955 ;"
   "    skos:closeMatch <http://id.loc.gov/authorities/subjects/sh85128435>,"
   "        allars:Y28737,"
   "        ysa:Y103119 ;"
   "    skos:inScheme yso: ."])


(deftest test-full-parse

  (is (= (doall (triple-seq data1))
         [(->Triple "rdf:type" "a" (->Res "owl:Class"))
          (->Triple "rdf:type" "owl:DataTypeProperty" (->Res "file:///foo"))
          (->Triple "rdf:type" "owl:DataTypeProperty" (->Res "file:///boo"))
          (->Triple "rdf:type" "dc:dateCreated" (->Lit "2013-01-28T12:58:13.077Z" nil "xsd:dateTime"))
          (->Triple "rdf:type" "skos:prefLabel" (->Lit "web link" "en" nil))
          (->Triple "rdf:type" "skos:prefLabel" (->Lit "webblänk" "sv" nil))
          (->Triple "rdf:type" "skos:prefLabel" (->Lit "verkkolinkki" "fi" nil))]))

  (is (= (doall (triple-seq data2))
         [(->Triple "@prefix" "yso:" (->Res "http://www.yso.fi/onto/yso/"))
          (->Triple "yso:p3956" "a" (->Res "skos:Concept"))
          (->Triple "yso:p3956" "skos:prefLabel" (->Lit "wood stoves" "en" nil))
          (->Triple "yso:p3956" "skos:prefLabel" (->Lit "puuliedet" "fi" nil))
          (->Triple "yso:p3956" "skos:prefLabel" (->Lit "vedspisar" "sv" nil))
          (->Triple "yso:p3956" "skos:broader" (->Res "yso:p3635"))
          (->Triple "yso:p3956" "skos:broader" (->Res "yso:p3955"))
          (->Triple "yso:p3956" "skos:closeMatch" (->Res "http://id.loc.gov/authorities/subjects/sh85128435"))
          (->Triple "yso:p3956" "skos:closeMatch" (->Res "allars:Y28737"))
          (->Triple "yso:p3956" "skos:closeMatch" (->Res "ysa:Y103119"))
          (->Triple "yso:p3956" "skos:inScheme" (->Res "yso:"))]))

  )



(comment


  (do
    (def w1 "small.ttl")
    (def bigfile "huge.ttl")

    (defn res [path] (io/resource path))
    (defn abs-path [res] (.getAbsolutePath (File. (.toURI res)))))


  (-> (ttl-reader (abs-path (res w1)) {}
                  (fn [coll]
                    (->> coll
                         (r/flatten)
                         (r/foldcat))))
      prn-triples)

  ;; Singlecore version
  ;; 8,9 sec, 4,36 million triples, 487t triples per sec
  (time
    (with-open [inp (clojure.java.io/reader bigfile)]
      (transduce (comp (map split-to-words)
                       cat
                       (triples-from-words identity)
                       (map (fn [_] 1)))
                 + 0
                 (line-seq inp))))

  ;; Multicore version
  ;; 1,9 sec, 4,36 million triples, 2 358t triples per sec
  (time
    (->> (iota/rec-seq bigfile (* 256 1024) [46 10])
         (r/map (fn [^String s]
                  (into [] (triples-from-words identity) (split-to-words s))))
         (r/flatten)
         (r/map (fn [_] 1))
         (r/fold +)))

  )