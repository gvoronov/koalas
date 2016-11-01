## Design Philosophy

I wanted a package that was reminiscent of Pandas toward this end:

1. A crucial feature is the support for varied datatypes across columns. Koalas implements this
via DataValue types.
2. Koalas has three primary data-structures Row, Series, and DataFrame

However I also wanted to remedy some of the annoyances I have had with Pandas and also wanted the
feel of dataframes to be more scala-like. So toward this end,

1. I structure dataframes as a collection of rows as opposed to a collection of columns, so that
most dataframe operations are implemented with map-like operations.
2. All datatypes in koalas are immutable. This is clearly more in the spirit of Scala.
Additionally, I always found Pandas hybrid approach somewhat annoying. I spend quite a bit of time
with bugs where I forget whether a given operation returns a copy or modifies the object in place.
Finally, it would be too soon if I ever see the Pandas SettingWithCopyWarning again.
3. This approach follows pretty closely with Spark, so anything written in this framework will
transfer quite easily.

I want to finally note, that the Koalas project was initiated as a first step towards an
implementation of a general purpose decision-tree framework (Eucalyptus). As such some of the
design decisions are particularly optimal towards this objective. For instance, a dataframe as a
collection of rows, will aggregating along a column more slowly than a dataframe as a collection
of columns, but will allow for faster partition into multiple dataframes as according to some
rule. Additionally the decision tree fit method will create many new dataframes from partitions
of the input dataframe. As these partitions are new instances, an immutable dataframe is no less
performant than a mutable dataframe.
