![](https://dl.dropboxusercontent.com/u/174179/narrator/storyteller.png)

Narrator is for analyzing and aggregating streams of data.  Stream processing is automatically parallelized wherever possible, and the provided operators are designed to be memory-efficient, allowing for high-throughput analysis of large historical data sets or unbounded realtime streams.

### usage

```clj
[narrator "0.1.0"]
```

Narrator transforms streams of data into periodically sampled values.  This is most easily done using `narrator.query/query-seq`:

```clj
> (use 'narrator.query)
nil
> (require '[narrator.operators :as n])
> (query-seq 
    [:foo (n/sum)] 
    (repeat 10 {:foo 1}))
10
```

Here, we've composed `:foo` and `narrator.operators/sum` together, and applied them to the sequence.  `sum` and other operators can be called with a map of options, such as `(sum {:clear-on-reset? false})`, but as a convenience, if they are called with no parameters the parentheses can be omitted.  `sum` and `(sum)` are interchangeable.

Since we haven't defined a period, `query-seq` consumes the entire sequence, first mapping `:foo` over each element, and then summing the result.  However, we can also do a periodic analysis of the sequence:


```clj
> (query-seq 
    n/rate
    {:period 5, :timestamp :foo} 
    (for [n (range 10)]
      {:foo n}))
({:timestamp 5, :value 5} 
 {:timestamp 10, :value 5})
```

Here we've defined a `:timestamp` function that will give us the time at which each message occurred, and a `:period` at which we want to emit the result.  In this case we're asking for the rate of message over that period, which is one message for each unit of time.

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

Here we've used `group-by` to split the stream along a facet defined by the given function, and then applied the given operators to each subset of the data.

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
{"foo" {:rate 1,
        :children {"bar" {:rate 1,
                          :children {"quux" {:rate 1,
                                             :children nil}}},
                   "baz" {:rate 2,
                          :children nil}}}}
```

In this operator, we group each task by their `:name`, first counting their frequency, but also taking the list of `:children`, concatenating it such that each element is propagated forward as an individual message, and then fed back into the same query.

### core.async and lamina integration

If `core.async` is included in the classpath, then Narrator also provides a `narrator.query/query-channel` function, which behaves similarly to `query-seq`, but can also be used to process realtime streams.  A similar capability for [Lamina](https://github.com/ztellman/lamina) is provided via `narrator.query/query-lamina-channel`.

### available operators

`rate`, `sum`, `group-by`, `concat`, and `recur` are demonstrated above.

`filter` behaves much like the Clojure function:

```clj
> (query-seq (n/filter even?) (range 10))
(0 2 4 6 8)
```

`sample` can be used to periodically emit a representative sampling of the incoming stream:

```clj
> (query-seq (n/sample {:sample-size 10}) (range 1000))
(527 161 55 522 173 312 149 664 449 570)
```

If the values are numbers, `quantiles` can be used to give the statistical distribution of values:

```clj
> (query-seq n/quantiles (range 1000))
{0.999 999.0, 0.99 990.0, 0.95 950.0, 0.9 900.0, 0.5 500.0}
```

Note that these values are approximate, using the Q-Digest algorithm to extract a representative sample from the stream.

The mean value of the numbers can be determined using `mean`:

```clj
> (query-seq n/mean (range 1000))
499.5
```

The difference between successive values can be determined using `delta`.  The first value will always be emitted as-is.

```clj
> (query-seq n/delta (range 5 10))
(5 1 1 1 1)
```

To filter out only values which have changed, use `transitions`.

```clj
> (query-seq n/transitions [1 1 2 2 2 3 3])
(1 2 3)
```

The most recent value within a period, or overall, can be determined using `latest`:

```clj
> (query-seq n/latest (range 10))
10
```

When trying to remove duplicate values from large datasets, the memory cost can be quite high.  Using Bloom Filters, `quasi-distinct-by` allows approximate duplicate removal using much less memory, with tunable error rates.  The facet used for duplicate checks must be a string or keyword:

```clj
> (query-seq (n/quasi-distinct-by identity) [:a :a :b :c])
[:a :b :c]
```

Measuring cardinality of a large dataset can also be very memory intensive.  Using the HyperLogLog cardinality estimation algorithm, this can be greatly reduced.  `quasi-cardinality` allows estimation of the unique number of elements within a stream, where those elements may be strings, keywords, bytes, or numbers.

```clj
> (query-seq n/quasi-cardinality (concat (range 10) (range 10)))
10
```

A moving-windowed variant of any operator may be defined via `(moving interval operator)`.  This requires that a `:timestamp` be specified, so that the window may be moved.

```clj
> (map :value 
    (query-seq 
      (n/moving 3 n/rate) 
      {:timestamp identity :period 1} 
      (range 10)))
(0 1 2 3 3 3 3 3 3 3)
```

### defining your own operators

Narrator allows for two kinds of operators: processors and aggregators.  A processor will emit zero or more messages for each message it receives, while an aggregator will only emit one message per interval.

A processor can be defined as just a bare function, which will emit the result of the function for each message received.  To define an arbitrary mapping between incoming and outgoing messages, a `clojure.core.reducer` function may be given to `narrator.core/reducer-op`.

Many aggregators may be defined as a [monoid](http://en.wikipedia.org/wiki/Monoid), which is simpler than it may seem.  For instance, a sum aggregator may be defined using `narrator.core/monoid-aggregator`:

```clj
(monoid-aggregator 
  :initial (fn [] 0)
  :combine +)
```

In this, we've defined an initial state, and a function that combines any two messages or states.Since both messages and our aggregate values are numbers, our combiner function can simply be `+`.  However, if we wanted to aggregate a list of all messages, we will need to make each message look like a list.  Do do this, we can define a `pre-processor`:

```clj
(monoid-aggregator
  :initial (constantly (list))
  :pre-processor list
  :combine concat)
```

Here, we make sure each message is now a list of one message, before combining them all together via concatenation.

However, sometimes it's too inefficient or simply not possible to define our aggregator as a monoid. In this case, we can use `narrator.core/stream-aggregator-generator`:

```clj
(stream-aggregator-generator
  :combine (fn [sums] (reduce + sums))
  :ordered? false
  :create (fn []
            (let [cnt (atom 0)]
              (stream-aggregator
                :process (fn [msgs] (swap! cnt + (reduce + msgs)))
                :deref (fn [] (deref cnt))
                :reset (fn [] (reset! cnt 0))))))
```

This is equivalent to the sum example given above. Here, we've defined a **generator** that will create aggregators for our query.  The generator says it is not `:ordered?`, so the stream of messages can be processed in parallel.  It also defines a `:combine` function, which takes a sequence of values from the sub-aggregators and returns a single value.

The `:create` callback returns an instance of the aggregator via `narrator.core/stream-aggregator`.  This aggregator closes over an atom containing the sum, and defines a `:process` callback that takes a sequence of messages and adds them to the count.  It defines a `:deref` function that returns the current count, and an optional `:reset` function which is called at the beginning of each new query interval.

For each instance of the aggregator, the `:process` callback is guaranteed to only be called on one thread, so non-thread-safe operations and data structures can be used.  Likewise, `:deref` and `:reset` are guaranteed not to be called while messages are being processed, so no mutual exclusion is necessary. 

Given these invariants, very high throughput message processing is possible, with automatic parallelization wherever possible.  The built-in operators in Narrator take full advantage of this.

Full documentation can be found [here](http://ideolalia.com/narrator/).

### license

Copyright © 2013 Zachary Tellman

Distributed under the [MIT License](http://opensource.org/licenses/MIT)
