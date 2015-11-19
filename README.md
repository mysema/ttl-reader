# Ttl reader

Very fast Turtle data reader using core.reducers paraller loading with Iota memmapped files.

This is not fully Turtle spec compliant as I it covers the spec just enough to load all the TTL files I needed. I have used it to prepare some 6 million triples to Datomic, so it can do some real job.

I tried to use [CRG Turtle parser](https://github.com/quoll/crg-turtle), which has fully spec compliant parsing, but it totally choked on my 550mb TTL file.

Using reader is simple, here's an example on how to calculate triples using single threaded transducer with regular clojure reader:

```clojure
(time
    (with-open [inp (clojure.java.io/reader bigfile)]
      (transduce (comp (map split-to-words)
                       cat
                       (triples-from-words identity)
                       (map (fn [_] 1)))
                 + 0
                 (line-seq inp))))
"Elapsed time: 9129.697126 msecs"
=> 4363205
```
And this is same with multicore version using Iota and core.reducers to get as fast as possible:
```clojure
(time
  (->> (iota/rec-seq bigfile (* 256 1024) [46 10])  ; Iota is getting vector of bytes as split points for it's chunks
       (r/map 
         (fn [^String s]
           (into [] (triples-from-words identity) (split-to-words s))))
       (r/flatten)
       (r/map (fn [_] 1))
       (r/fold +)))
"Elapsed time: 2095.141824 msecs"
=> 4363205
```
So it's reading almost 2,4 million triples per second, not bad. In comparison it took 38 minutes on CRG Turtle parser to count triples from the same file.

Here's some real world examples on processing an ontology. The speed of the parser makes it really convient to make ad hoc queries directly into triple-files.

```clojure

(defn limit-coll
  [limit coll]
  (if (and limit (pos? limit)) (r/take limit coll) coll))

(defn offset-coll
  [offset coll]
  (if (and offset (pos? offset)) (r/drop offset coll) coll))

(defn count-entities
  [filename ns-uris filter]
  (time
    (ttl-reader filename ns-uris
                (fn [coll] ;; coll is a list of triples with the same subject
                  (->> coll
                       (r/filter filter)
                       (r/map (fn [_] 1))
                       (r/fold +))))))

(defn query-entities
  [filename ns-uris filter limit offset]
  (time
    (ttl-reader filename ns-uris
                (fn [coll]
                  (->> coll
                       (r/filter filter)
                       (offset-coll offset)
                       (limit-coll limit)
                       (r/flatten)
                       (r/foldcat))))))

(defn is-of-type?
  [& types]
  (let [types-set (set types)]
    (fn [triples]
      (contains? types-set (:v (:o (first triples)))))))

(defn contains-attr?
  [& attrs]
  (let [attr-set (set attrs)]
    (fn [triples]
      (some #(contains? attr-set (:p %)) triples))))

(count-entities ontologyfile {} (is-of-type? "skos:Concept"))
(-> (query-entities ontologyfile {} (contains-attr? "kaunokki:tekija") 5 0) 
    prn-triples)
```


# License

Copyright © 2015 Mysema Ltd

Distributed under the Eclipse Public License version 1.0
