(DRAFT, updates in progress)

# PPD text index

A text indexer for Jena Text to work the the price paid data. This
application currently requires an
[extended version of Jena](https://github.com/epimorphics/jena-config-doc-producer)
which allows the text document producer to be specified via the Assembler
description. In time, we aim to get these extensions folded back into the main
Jena codebase.


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
doing separate text queries and then using /eg/ a SPARQL filter
to combine them is unacceptable. Instead we minimally 
extended jena-text to make conjunctive queries /possible/
and outside Jena used that minimal extension to 
/implement/ conjunctive queries.

We introduced two features in jena-text and three in
the `ppd-text-index` used in our application:

* configurable TextDocProducer
* ability to delete documents in a Lucene index
* multi-property Entities
* TextDocProducer handling those Entities
* batch indexing

## configurable document producers

Changes to a dataset are monitored by calling the
`DatasetChanges.change(QuadAction,Node g, s, p, o)` method; this
is how on-the-fly (re-)indexing works, using the change
reports to modify the index. To change how on-the-fly
indexing works we need both a new kind of TextDocProducer
and a way of plugging it into a dataset on construction.

Methods in `TextDatasetFactory` have been given an
additional optional [1] `TextDocumentProducer` parameter
to supply the developers choice of document producer.
If `null` is used, then a default `TextDocProducerTriples`
is created and used, which is the original behaviour.

The `TextDataset` assembler vocabulary is extended with a
new property `text:textDocProducer`. It\'s value must be
a URI resource with scheme `java:` and body the name
of the document producer class:

> ... text:textDocProducer \<java:com.epimorphics.lr.jena.query.text.TextDocProducerBatch\> 

That class must have either a single-argument constructor taking
a `TextIndex` argument, or a two-argument constructor taking a
`DatasetGraph` and a `TextIndex` argument.  The `DatasetGraph` argument
allows a `TextDocumentProducer` to reach back into the dataset for quads
as described for `TextDocumentProducerBatch` (below).

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

To track the addition and removal of quads as updates are
made to a dataset we use a specialised kind of `Entity`,
the `ExtendedEntity`. An `ExtendedEntity` is updated by
its `addProperty(EntityDefinition,Node p, Node o)` method,
which only records properties that are part of the 
`EntityDefinition` and which have literal values. It
returns true iff the addition was successful.

## TextDocProducerBatch

A TextDocProducerBatch is a document producer
that handles index document deletion and multiple
fields for a single subject entity.

Adding a quad `(g, s, p, o)` to a dataset causes
TextDocProducerBatch to update the /single/ entity
corresponding to `(g, s)`.

Similarly removing a quad updates the single entity
corresponding to `(g, s)` and, if that entity now
has no fields, deletes the entity completely.

These operations are handled by the

> change(QuadAction, Node g, s, p, o)

using

> ExtendedEntity.addProperty(EntityDefinition, Node p, v)

to determine if fields have really been added for
an `ADD` action or, for a `DELETE` action, whether
the entity has been updated and whether it now is
empty and hence to be removed from the index.

Removal of quads carries an overhead. If any
fields have been removed by the quad removal,
then the current document for this entity is discarded
and, if there are still some quads with this subject,
the entity has to be reconstituted. (This is because
there is no incremental removal on Lucene documents.)

Reconstituting the document means querying the dataset
for any quads with the given subject. If there are
many delete operations for a given subject, the same
(or very similar) queries will be re-issued for each
deletion.

To avoid this, TextDocProducerBatch processes the
changes in /batches/ of quads with the same action and
subject. Thus the deletion of `N > 1` quads will
result in a /single/ scan of the properties of a
given subject, rather than `N` scans.

## bulk indexer

A dataset built with `TextDocProducerBatch` will index
or deindex quads as they are added to or removed from 
the dataset. If all updates to the database are made
with `TextDocProducerBatch` then the index will be up to
date. However if the dataset has been constructed by
eg copying an existing TDB, or the definitions of 
indexable fields change, we can re-index the database
from scratch using a bulk indexer.

`ppd-text-index` has a command-line interface to
a `TextIndexer` class. The indexer runs through
all the subjects in the dataset, and then -- using
the same tactics as `TextDocProducerBatch` --
creates entries for all the relevant literal
values of those subjects.

To avoid indexing a subject multiple times if
the scan does not group all the quads with the 
same subject together, the indexer records all
the subjects seen so far, so enough Java heap 
space must be allocated to the indexing command.

## Limitations

Currently the code to handle conjunctive queries
only works with Lucene indexes.

## History

Original version of this code by [Brian McBride](https://github.com/bwm-epimorphics).
Some cleanup and minor modifications and fixes by [Ian Dickinson](https://github.com/ijdickinson).
