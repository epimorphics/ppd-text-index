# conjunctive queries - Jena-686 and PPD text index 

In JENA-686 (https://issues.apache.org/jira/browse/JENA-686)
we explain a problem we had with doing text search on
addresses and failing to return any answers from searches
like

> city:liverpool AND street:green

These are "conjunctive queries", the ANDs of other
(and in our case, basic field/value) queries.

The problem arises because the default text indexing in 
jena-text indexes each property (city, street above) 
separately, so there is no document that matches both of 
`city` and `street`.

With the scale of our data the performance loss of
doing separate text queries and then using *eg* a SPARQL filter
to combine them is unacceptable. Instead we 
extended jena-text to make conjunctive queries *possible*
and outside Jena, in our `ppd-text-index` code, used that extension to 
*implement* conjunctive queries by creating multi-field
documents.

We introduced two features in jena-text and three in
our `ppd-text-index` application:

* configurable TextDocProducer
* ability to delete documents in a Lucene index
* entities reporting successful field addition
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
document producers to be plugged in.

Datasets constructed by the methods in
`TextDatasetFactory` may specify a `TextDocProducer`
object to use as the `DatasetChanges` monitor for that
dataset. If none is specified (or specified as `null`)
then `TextDocProducerTriples` is used, which preserves
the original (single-field) behaviour. 
Similarly, the `TextDataset` 
assembler vocabulary is extended with a
new property `text:textDocProducer`, whose value must be
a URI resource with scheme `java:` and body the name
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

The two-argument constructor passes in the `DatasetGraph`;
this allows the `TextDocProducer` to access the quads
in the dataset as necessary. The existing `TextDocProducer`s
(`TextDocProducerTriples` and `TextDocProducerEntities`)
supply the one-argument constructor, while the
`ppd-text-index` `TextDocProducerBatch` supplies the
two-argument constructor. 

## deletion from Lucene indexes

The base jena-text indexing ignores the deletion of quads (because
this is the less common operation, and the spurious subjects generated
from the index entries for deleted triples are typically discarded
by filters in the SPARQL queries that are applied). To avoid debris
accumulating in the indexes, we added to `TextIndex` a method
`deleteDocuments` which is implemented in `TextIndexLucene` by
invoking the underlying Lucene `deleteDocuments` method. 

This allows TextDocProducers to drop index entries when there are
no more indexed fields for a given subject.

## ExtendedEntities

An `ExtendedEntity`, constructed with a specified subject node,
is intended to be updated using its method

> boolean addProperty(EntityDefinition ed, Node p, Node o)

which 

1. checks that the property `p` is a field property, and
2. checks that the value `o` is a literal, and
3. if so, adds `(p's URI, o's lexical form)` to the entity's map, and
4. returns `true` iff the addition was peformed.

Callers of `addProperty` can then track whether some
property has been added to the map, *ie*, that there
has been a change in the indexing for the subject.

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

using

> ExtendedEntity.addProperty(EntityDefinition, Node p, v)

(see `ExtendedEntity` above)
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
only works with Lucene indexes.

To avoid indexing a subject multiple times if
the scan does not group all the quads with the 
same subject together, the bulk indexer records all
the subjects seen so far, so additional Java heap 
space may be needed.


