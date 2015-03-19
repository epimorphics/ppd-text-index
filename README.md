# conjunctive queries - Jena-686 and PPD text index 

In JENA-686 (https://issues.apache.org/jira/browse/JENA-686)
we explain a problem we had with doing text search on
addresses and failing to return any answers from Lucene
searches like:

> city:liverpool AND street:green

These are "conjunctive queries", the ANDs of other
(and in our case, basic field=value) queries.

The problem arose because at the time Jena indexed 
each property (`city`, `street` above) 
separately, so there is no single Lucene `Document` that 
matches both of `city` and `street`.

The size of our data makes the performance loss of
doing separate text queries, and then using *eg* a SPARQL filter
to combine them, unacceptable. Instead we arrange to create
multi-field `Document`s by exploiting the configurability
of `TextDocumentProducer`'s in a dataset assembly;
Jena generates index `Document`s using classes that
support the `TextDocProducer` interface. 

We exploited this feature of Jena and implemented
three new features inside our `ppd-text-index` 
application:

* a multi-field `Document`-generating `TextDocProducer`
* entities that report field addition
* a TextDocProducer handling multiple fields
* a batch indexing tool

Indexes produced by these features in combination
allow conjunctive queries and are used in
our live application. 

## configurable document producers

Adding quads to, or removing quads from, a dataset is
monitored by a `DatasetChanges` object. Incremental
indexing of a dataset -- such as is performed 
when a SPARQL update is applied to the dataset -- 
is implemented by a `DatasetChanges`
object which updates the index as the changes flow
through it. The updates to jena-text allow arbitrary
document producers to be plugged in: in particular
we modified Jena so that a `TextDocProducer` could
have access to the entire dataset as it indexed
changes.

Datasets constructed by the methods in
`TextDatasetFactory` may specify a `TextDocProducer`
object to use as the `DatasetChanges` monitor for that
dataset. If none is specified (or specified as `null`)
then `TextDocProducerTriples` is used, which preserves
the original (single-field) behaviour. 
Similarly, the `TextDataset` 
assembler vocabulary has a property `text:textDocProducer`, 
whose value must be a URI resource with scheme `java:` and 
body the name
of the document producer class, *eg*:

		:myDatasetRoot
		... 
		; text:textDocProducer \<java:com.epimorphics.lr.jena.query.text.TextDocProducerBatch\> 
		...
		.

The named class (call it `SomeProducer`) must exist and have 
one of two constructors:

		public SomeProducer(TextIndex ti)
		public SomeProducer(DatasetGraph dg, TextIndex ti)

The two-argument constructor (a feature we added to Jena) 
passes  in the `DatasetGraph`;
this allows the `TextDocProducer` to access the quads
in the dataset as necessary. The existing `TextDocProducer`s
(`TextDocProducerTriples` and `TextDocProducerEntities`)
supply the one-argument constructor, while the
`ppd-text-index` `TextDocProducerBatch` supplies the
two-argument constructor. 

## ExtendedEntities

An `ExtendedEntity` satisfies the same interface as `Entity`,
representing field/value pairs to be put into a `Document`,
but in addition reports whether a field/value pair is
appropriate for this `Entity`. This allows a `TextDocProducer`
to know whether it is necessary for an `ExtendedEntity` to
be flushed back into a `Document` or not.

## TextDocProducerBatch

A TextDocProducerBatch is a document producer
(*ie* a `DatasetChanges` instance for updating
indexes as a dataset is updated)
that handles multiple fields for a single document
and index document deletion when all fields are
deleted.

Adding a quad `(g, s, p, o)` to a dataset monitored by
causes `TextDocProducerBatch` to update the *single* 
entity corresponding to `(g, s)`.

Similarly, removing a quad updates the single 
document corresponding to `(g, s)` and, if that document
now has no fields, deletes the entity completely.

Internally, these operations are handled by the method:

> change(QuadAction, Node g, s, p, o)

using the method:

> ExtendedEntity.addProperty(EntityDefinition, Node p, v)

to determine if fields have really been added for
an `ADD` action or, for a `DELETE` action, whether
the entity has been updated and whether it now is
empty and hence to be removed from the index.

Removal of quads carries an overhead. If any
fields have been removed by the quad removal,
then the current document for this entity is discarded
and, if there are still some quads with this subject
in the dataset,
the entity has to be reconstituted. (This is because
there is no incremental removal on Lucene documents.)

Reconstituting the document means querying the dataset
for any quads with the given subject. If there are
many delete operations for a given subject, the same
(or very similar) queries will be re-issued for each
deletion.

To avoid this, TextDocProducerBatch processes the
changes in *batches* of quads with the same action and
subject. Thus the deletion of `N > 1` quads will
result in a *single* scan of the properties of a
given subject, rather than `N` scans.

## bulk indexer

A dataset built with `TextDocProducerBatch` will index
or deindex quads as they are added to or removed from 
the dataset. If all updates to the dataset are made
with `TextDocProducerBatch` as the `TextDocProducer`,
then the index will be up to
date. However if the dataset has been constructed by
*eg* copying an existing TDB, or the definitions of 
indexable fields change, we can re-index the database
from scratch using a bulk indexer.

`ppd-text-index` has a command-line interface to
a `TextIndexer` class:

> java -cp $PPD:$FUSEKI jena.textindexer --desc $FILE 

where

1. $PPD is the name of the `ppd-text-index` jar file;
2. $FUSEKI is the name of the Jena Fuseki jar file;
3. $FILE is the name of the file describing the dataset.

An example $FILE can been seen in the `ppd-text-index`
resources:

> src/test/resources/config-ppd-text.ttl

which differs from a normal jena-text assembler
description only in having a `TextDataset` resource
with a `text:textDocProducer` property.

The indexer runs through
all the subjects in the dataset, and in the same
way as `TextDocProducerBatch` creates entries for 
all the relevant literal
values of those subjects.

## Limitations

Currently the code to handle conjunctive queries
(specifically the `updateEntity` operation)
only works with Lucene indexes.

To avoid indexing a subject multiple times if
the scan does not group all the quads with the 
same subject together, the bulk indexer records all
the subjects seen so far, so additional Java heap 
space may be needed.

`TextDocProducerBatch` does not keep track of whether or
not it's in a transaction (*ie* between a `start()` and
its corresponding `finish()`; it assumes it's inside
an externally-constructed transaction.

The code only applies in the default graph.
