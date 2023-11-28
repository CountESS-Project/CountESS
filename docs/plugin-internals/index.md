---
layout: default
---
# CountESS: Plugin Internals

This document described the internal details of how CountESS plugins work.
You don't need to worry about this stuff to [use CountESS](../running-countess/)
and most [plugin authors](../writing-plugins/) won't need it either: just 
subclass one of the existing `countess.core.plugins` classes.
Rather, this document attempts to describe the internals of those classes and
why they are that way.

## Discussion

### Multiprocessing vs Threading

Computers these days have a lot of CPU cores, and it'd be disappointing for 
CountESS to use only one of them at a time.

As Python still struggles with
[the GIL](https://wiki.python.org/moin/GlobalInterpreterLock)
getting true multi-CPU performance out of CPython code requires
[`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html) 
rather than [`threading`](https://docs.python.org/3/library/threading.html) 
or [`asyncio`](https://docs.python.org/3/library/asyncio.html).

Multiprocessing brings its own inefficiencies, so plugins which are heavy
users of [numpy](https://numpy.org/) may be better off using threading as
numpy code is executed outside of the GIL.

### Queues

Data can be copied between threads and processes using
[`queue.Queue`](https://docs.python.org/3/library/queue.html#queue.Queue)
which is thread- safe or
[`multiprocessing.Queue`](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Queue) which is both thread- and multiprocess- safe.
Queues can be given a maximum size beyond which an attempt to put new data onto the queue will block.
This allows 'backpressure' between plugins to prevent excessive memory consumption.

[Iterators](https://docs.python.org/3/tutorial/classes.html#iterators)
allow a form of concurrent programming too, and have a very handy 
[`StopIteraton` exception](https://docs.python.org/3/library/exceptions.html#StopIteration)
to indicate that they are "finished".
Programming with iterators is particularly pleasant in Python because of
generators and the
[`yield`](https://docs.python.org/3/reference/simple_stmts.html#yield) syntax.

We want plugins to accept data in whatever order it has become ready, which indicates we need a single input queue for the plugin. But different upstream plugins may have different needs for multiprocessing vs threads.

### Finishing

Plugins may allocate resources while running and so it's very useful for
them to receive a signal indicating that no more data is going to come along
and that they can therefore release any resources.

[Iterators](https://docs.python.org/3/tutorial/classes.html#iterators) have a [`StopIteraton`](https://docs.python.org/3/library/exceptions.html#StopIteration) exception which performs this duty but queues do not.

To see if upstream processes are still able to communicate we can check `thread.is_alive()` / `process.is_alive()` for each upstream process. Note that data may still be in the queue though.

## Plugin Superclasses

These are all subclasses of `BasePlugin` and are available for your plugin classes to inherit from.

### InputPlugin

Input plugins have the job of reading files or similar so they don't have any data inputs.
Care must be taken not to overrun the available memory.

### SimplePlugin

The simplest plugins are transformers: they take a stream of data in and emit
a stream of data, and no particular synchronization is needed.  For example
a simple per-row operation can be done to dataframes in whatever order across
multiple threads or processes.

Whether parallelizing by threads or processes is more appropriate, or not parallelizing at all, depends on the underlying libraries used by the plugin. 

The [various types of `TransformPlugin`](../writing-plugins/#pandastransformplugin) are all subclasses of `SimplePlugin`.
 
### PrepareMapReduceFinalizePlugin

Not all plugins are this simple, some plugins need to gather more data in one place to calculate values across rows. Using a 
[MapReduce](https://en.wikipedia.org/wiki/MapReduce) technique avoids this limitation, by allowing the data to be processed in chunks and then the chunks combined. While quite a lot of data may still need to be gathered in one place the scale of the problem is reduced substantially.

| stage | purpose | example<br>calculating the mean of all values > 0 |
|:---:|:---:|:---:|
| Prepare | filter records and/or columns before data is sent to processes | discard any values <= 0<br>discard any empty dataframes |
| Map | data is processed across multiple processes into an intermediate form |  sum = sum(value)<br>count = count(value) | 
| Reduce | intermediate forms are reduced | sum = sum(sum)<br>count = sum(count)| 
| Finalize | intermediate form is translated to the final form | mean = sum / count |

### ProductPlugin

One plugin which works quite differently is `ProductPlugin`, which is used to
implement [joins](../included-plugins/#join).  Where we have a join where one stream of data is too large
to fit in memory, we can cache the other stream and join the two streams
piece by piece.

|          |      A[0]      |      A[1]      |      A[2]      | ... |
|:--------:|:--------------:|:--------------:|:--------------:|:---:|
| **B[0]** | A[0] JOIN B[0] | A[1] JOIN B[0] | A[2] JOIN B[0] | ... |
| **B[1]** | A[0] JOIN B[1] | A[1] JOIN B[1] | A[2] JOIN B[1] | ... |
| **...**  |      ...       |      ...       |       ...      | ... |

Complicating matters, the parts of each data source may arrive in whatever order. While parts are arriving from both sources we need to cache both sources to calculate the joins: when data from A arrives we join it with each cached item from B, and when data from B arrives we join it with each cached item from A.

| input | cache used | output |
|:---:|:---:|:---:|
| A[0] | (empty) | (nothing) |
| B[0] | A[0] | A[0] JOIN B[0] |
| B[1] | A[0] | A[0] JOIN B[1] |
| A[1] | B[0:1] | A[1] JOIN B[0]<br>A[1] JOIN B[1] |
| B[2] | A[0:1] | A[0] JOIN B[2]<br>A[1] JOIN B[2] |
| B[3] | A[0:1] | A[0] JOIN B[3]<br>A[1] JOIN B[3] |
| B[4] | A[0:1] | A[0] JOIN B[4]<br>A[1] JOIN B[4] |
| A[2] | B[0:5] | A[2] JOIN B[0]<br>A[2] JOIN B[1]<br>A[2] JOIN B[2]<br>A[2] JOIN B[3]<br>A[2] JOIN B[4] |
| B[5] | A[0:2] | A[0] JOIN B[5]<br>A[1] JOIN B[5]<br>A[2] JOIN B[5] |
| B[6] | A[0:2] | A[0] JOIN B[6]<br>A[1] JOIN B[6]<br>A[2] JOIN B[6] |
| ... | ... |

But once a source is "finished", we no longer need to keep a cache of the *other* source.  In the example above, if after receiving A[2] we discover than source A is finished, we can discard the cache of B and not cache any more entries from B as we will not need them again.  This means we can join a large data source with a small data source quite efficiently, even if we don't know the size of either source when we start.

The problem is, if there's a shared queue then we can't be sure that there's no more data in the queue until the queue is empty, which may never happen. So we need either to not have a shared queue or to have a sentinel value.

For this to work we need to know:
* Which source each received input is from
* When each source is "finished".

This is quite distinct from the usual plugin where we simple add input from all sources and don't care until *all* sources are finished.

## Plugin implementation

* every plugin gets its own thread.
* plugins can then spawn processes if they wish, and the thread can supervise.
* plugins have a get_queue() method which returns an input queue, for most plugins this is the one queue but for productplugin it'll be separate
* not a shared input queue and not a shared output queue
* process_queue(input_queue, output_queue)


```
class ProductPlugin:
    def __init__(self):
        self.source_queues = []

	def get_queue(source: Process|Thread):
	    self.source_queues.append((source, Queue())
	
	def get_data():
	    while self.source_queues:
		    for source, queue in self.source_queues[:]:
		        try:
		            yield queue.get(timeout=0.1)
		        except queue.Empty:
		            if not source.is_alive():
		                self.finished(source)
		                self.source_queues.remove((source, queue))

class NormalPlugin:

    def __init__(self):
        self.queue = multiprocessing.Queue()
        self.sources = []

    def get_queue(source: Process|Thread):
        self.sources.append(source)
        return self.queue

    def get_data():
        while True:
	        try:
	            yield self.queue.get(timeout=1)
	        except self.queue.Empty:
  	            if not any(source.is_alive() for source in self.sources):
	                break



```
