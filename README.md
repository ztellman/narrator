![](https://dl.dropboxusercontent.com/u/174179/narrator/storyteller.png)

Narrator is a library for reducing streams of time-ordered data into concise, high-level descriptions.

### usage

Narrator transforms streams of data into periodically sampled values.  This is most easily done using `narrator.query/query-seq`:

```clj
> (use 'narrator.query)
nil
> (require '[narrator.operators :as n])
> (query-seq 
    [:foo n/sum] 
    (repeat 10 {:foo 1))
10
```

Here, we've composed `:foo` and `narrator.operators/sum` together, and applied them to the sequence.  Since we haven't defined a period, `query-seq` consumes the entire sequence, first mapping `:foo` over each element, and then summing the result.  However, we can also do a periodic analysis of the sequence:


```clj
> (query-seq 
    [:foo n/rate] 
    {:period 5, :timestamp :foo} 
    (map #(hash-map :foo %) (range 10)))
({:timestamp 5, :value 5} {:timestamp 10, :value 5})
```

Here we've defined a `:timestamp` function that will give us the time at which each message occurred, and a `:period` at which we want to emit the result.  In this case we're asking for the rate of message over that period, which in this case is one message for each unit of time.

The operators given to `query-seq` can either be normal Clojure functions, which will be mapped over the messages, or special operators that filter or aggregate messages.  These can be used alone, or composed in any order:

```clj
> (query-seq inc (range 5))
(1 2 3 4 5)
> (query-seq n/rate (range 5))
5
> (query-seq [n/rate inc] (range 5))
6
```

Notice in the last example we're incrementing the output of `rate`, which is a periodic operator.  We can even compose multiple periodic operators together, though this may give us odd results:

```clj
> (query-seq [n/rate n/rate] (range 5))
1
```

Since there's only one value emitted by the first `rate`, this will always return `1`, no matter what the input.

We can also do mutliple simultaneous analyses:

```clj
> (query-seq 
    {:sum [:foo n/sum], :rate n/rate} 
    (map #(hash-map :foo %) (range 10)))
{:rate 10, :sum 45}
```

Maps are valid syntax anywhere within a descriptor, and can even be nested:

```clj
> (query-seq 
    [:foo {:rate n/rate, :sum n/sum}]
    (map #(hash-map :foo %) (range 10)))
{:sum 45, :rate 10}

> (query-seq
    {:a {:b [inc n/sum], :c [dec n/sum]}}
    (range 10))
{:a {:c 35, :b 55}}
```

We can also split on arbitrary fields within the data:

```clj
> (query-seq 
    (n/group-by even? n/rate)
    (range 1000))
{true 500, false 500}

> (query-seq
    (n/group-by #(rem % 4) [n/rate inc])
    (range 1000))
{3 251, 1 251, 2 251, 0 251}
```

Here we've used `by` to split the stream along a facet defined by the given function, and then applied the given operators to each subset of the data.

The structural query descriptors work great when we know the structure of the data ahead of time, but what about when the structure is nested, even arbitrarily so?  In these cases, we can use `recur`, which feeds the messages back into the same descriptor:

```clj
> (def x {:name "foo"
          :children [{:name "bar"
                      :children [{:name "quux"}]}
                     {:name "baz"}
                     {:name "baz"}]})
#'x
> (query-seq
    (n/group-by :name 
      {:rate n/rate
       :children [:children n/concat n/recur]})
    [x])
{"foo" {:rate 1
        :children {"bar" {:rate 1
                          :children {"quux" {:rate 1, :children {}}}
                                     "baz" {:rate 2, :children {}}}}}}
```

In this operator, we group each task by their `:name`, first counting their frequency, but also taking the list of `:children`, concatenating it such that each element is propagated forward as an individual message, and then fed back into the same query.

### available operators

`rate`, `sum`, `group-by`, `concat`, and `recur` are demonstrated above.

`filter` behaves much like the Clojure function:

```clj
> (query-seq (n/filter even?) (range 10))
(0 2 4 6 8)
```

...

### defining your own operators

...

### license

Copyright © 2013 Zachary Tellman

Distributed under the [MIT License](http://opensource.org/licenses/MIT)