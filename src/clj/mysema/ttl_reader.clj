(ns mysema.ttl-reader
  (:require [iota :as iota]
            [clojure.core.reducers :as r])
  (:import (java.util ArrayList)
           (mysema.ttl Splitter)))

(def default-prefixes {"rdf:"  "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                       "dc:"   "http://purl.org/dc/elements/1.1/"
                       "owl:"  "http://www.w3.org/2002/07/owl#"
                       "foaf:" "http://xmlns.com/foaf/0.1/"
                       "xsd:"  "http://www.w3.org/2001/XMLSchema#"
                       "skos:" "http://www.w3.org/2004/02/skos/core#"})

;; TODO could the attribute conversions be cached to get some minor speed up?

(defn create-prefix-f
  "Shortens resource uri to 'prefix:localname' if the beginning of resource uri starts with
   some of the known uris. Tries longest uris first."
  [prefixes]
  (let [uri-starts (sort-by #(- (count %)) (vals prefixes))
        uri-to-prefix (zipmap (vals prefixes) (keys prefixes))]
    (fn [^String s]
      (when s
        (let [^String prefix (some (fn [^String uri] (when (.startsWith s uri) uri)) uri-starts)]
          (if prefix
            (str (get uri-to-prefix prefix) (.substring s (.length prefix)))
            s))))))

(def default-prefix-f (create-prefix-f default-prefixes))

(defrecord Res [v]
  Object
  (toString [_] (str "<" v ">"))
  Comparable
  (compareTo [a b] (compare (:v a) (:v b))))

(defrecord Lit [v lang dtype]
  Object
  (toString [_] (str "\"" v "\" " lang dtype)))

(defrecord Triple [s p o]
  Object
  (toString [_] (str "<" s "> <" p "> " o)))

(defn triple? [t] (instance? Triple t))

(defn delete-ends
  [^String s ^String start ^String end]
  (when s
    (if (and (.startsWith s start) (.endsWith s end))
      (.substring s (.length start) (- (.length s) (.length end)))
      s)))

(defn create-lit
  [prefixf ^String v ^String lang ^String datatype]
  (->Lit (delete-ends v "\"" "\"")
         (when lang (.substring lang 1))
         (when datatype (prefixf (delete-ends (.substring datatype 2) "<" ">")))))

(defn create-res [prefixf v] (->Res (prefixf (delete-ends v "<" ">"))))

(defn create-triple
  [prefixf s p o]
  (->Triple (prefixf (delete-ends s "<" ">"))
            (prefixf (delete-ends p "<" ">"))
            o))

(defn split-to-words [^String s] (Splitter/splitToWords s))

(defn triples-from-words
  "Stateful transducer which emits list of triples from input words."
  [prefixf]
  (fn [rf]
    (let [;; State
          s (volatile! nil)
          p (volatile! nil)
          o (volatile! nil)
          lang (volatile! nil)
          datatype (volatile! nil)
          objects (ArrayList.)

          ;; Functions
          lit? (fn [] (or @lang @datatype (.startsWith ^String @o "\"")))
          end? (fn [input] (or (= input ";") (= input ".")))
          clear-s (fn [] (vreset! s nil))
          clear-o (fn [] (vreset! o nil) (vreset! lang nil) (vreset! datatype nil))
          clear-p-o (fn [] (vreset! p nil) (clear-o) (.clear objects))

          ;; Emit multiple triples directly to reduction without creating intermediate collection
          catmap (fn [f] (cat ((map f) rf)))
          emit-triples (fn [result s p objects]
                         ((catmap (partial create-triple prefixf s p)) result objects))
          ]
      (fn
        ([] (rf))
        ([result] (clear-p-o) (clear-s) (rf result))
        ([result ^String input]

          ;(println "===============")
          ;(println "got" input)

          ;; Set the state based on word stream
         (cond
           (nil? @s) (vreset! s input)
           (nil? @p) (vreset! p input)
           (nil? @o) (vreset! o input)
           (.startsWith input "@") (vreset! lang input)
           (.startsWith input "^^") (vreset! datatype input)
           ;; We have a literal or resource to be saved
           :else (do (.add objects (if (lit?)
                                     (create-lit prefixf @o @lang @datatype)
                                     (create-res prefixf @o)))
                     ;; Reset previous object state
                     (clear-o)
                     ;; Set current value to object slot
                     ;; if we are still getting more objects
                     (when-not (end? input) (vreset! o input))))

          ;(when (> (.size objects) 500) (println "big objects array " (.size objects)))

          ;; Is it end of current predicate or subject
         (if (end? input)
           (let [result (emit-triples result @s @p objects)]
             (clear-p-o)
             ;; Change subject if ending on .
             (when (= input ".") (clear-s))
             result)

           ;; Or just keep on consuming words
           result
           ))
        ))))


(defn triple-seq
  "Lazy sequence of triples from the collection of lines"
  ([coll] (triple-seq default-prefixes coll))
  ([prefixes coll]
   (let [pref-f (create-prefix-f prefixes)]
     (sequence (comp (map split-to-words) cat (triples-from-words pref-f))
               coll))))


(defn ttl-reader
  "Reader with parallel parsing and arbitrary core.reducers
  transformations on way. Returns lists of triples to reduction, each
  list containing only triples with same subject."
  ([filename xf] (ttl-reader filename default-prefixes (* 256 1024) xf))
  ([filename prefixes xf] (ttl-reader filename prefixes (* 256 1024) xf))
  ([filename prefixes buf-size xf]
   (let [pref-f (create-prefix-f prefixes)]
     (->>
       ;; Splits file on entity boundary "./n"
       ;; TODO This might get broken on triple quoted strings!
       (iota/rec-seq filename buf-size [46 10])

       ;; Parse entity string as a list of triples
       (r/map (fn [s]
                (into [] (triples-from-words pref-f) (split-to-words s))))

       xf))))


(defn print-triples
  "Pretty print triple list"
  [triples]
  (doseq [t triples]
    (println (str t))))