/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epimorphics.lr.jena.query.text;

import java.io.IOException;
import java.util.*;

import org.apache.jena.query.text.*;
import org.apache.lucene.index.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.*;

/**
 * This document producer batches up consecutive sequences of quads with the
 * same subject to add to the index as a single entity.
 * To deal with subjects that do not present consecutively, we search the
 * triple store for other triples with the same subject before updating the
 * text entity.
 */
public class TextDocProducerBatch
    implements TextDocProducer
{
    /***********************************/
    /* Constants                       */
    /***********************************/

    /***********************************/
    /* Static variables                */
    /***********************************/

    private static Logger log = LoggerFactory.getLogger( TextDocProducerBatch.class );

    /***********************************/
    /* Instance variables              */
    /***********************************/

    private TextIndex indexer;
    private DatasetGraph dsg;

    List<Quad> queue = new ArrayList<Quad>();

    /* If true, the queue contains quads to add, otherwise quads to remove */
    boolean queueAdd = true;

    /* The subject currently being processed, to detect subject change boundaries */
    Node currentSubject;

    /***********************************/
    /* Constructors                    */
    /***********************************/

    public TextDocProducerBatch( DatasetGraph dsg, TextIndex textIndex ) {
        this.dsg = dsg;
        this.indexer = textIndex;
        log.debug( "Initialising TextDocProducerBatch" );
    }
    
    public TextDocProducerBatch( TextIndex textIndex ) {
    	this(null, textIndex);
    }

    /***********************************/
    /* External signature methods      */
    /***********************************/

    public void setTextIndex( TextIndex indexer ) {
        this.indexer = indexer;
    }

    public void setDatasetGraph( DatasetGraph dsg ) {
        this.dsg = dsg;
    }

    /**
     * @return The entity definition for the current indexer
     */
    public EntityDefinition entityDefinition() {
        return indexer.getDocDef();
    }

    @Override
    public void start() {
        log.debug( "TextDocProducerBatch.start()" );
        // indexer.startIndexing();
        startNewBatch( true );
    }

    @Override
    public void finish() {
        log.debug( "TextDocProducerBatch.finish()" );
        flush();
    }

	public void flush() {
        log.debug( "TextDocProducerBatch.flush()" );
		if (queueAdd) {
            addBatch();
        }
        else {
            removeBatch();
        }
		startNewBatch( true );
	}

    @Override
    public void change( QuadAction qaction, Node g, Node s, Node p, Node o ) {
        Quad quad = new Quad( g, s, p, o );
        switch( qaction ) {
            case ADD:
                indexNewQuad( quad );
                break;
            case DELETE:
                unIndex( quad );
                break;
            case NO_ADD:
                log.warn( "Saw change action NO_ADD, but ignoring it!" );
                break;
            case NO_DELETE:
                log.warn( "Saw change action NO_DELETE, but ignoring it!" );
                break;
        }
    }

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    protected void indexNewQuad( Quad quad ) {
        if (currentSubject == null) {
            currentSubject = quad.getSubject();
        }

        checkBatchBoundary( quad.getSubject(), true );
        queue.add( quad );
    }

    protected void checkBatchBoundary( Node subject, boolean add ) {
        if (!(currentSubject.equals( subject ) && queueAdd == add)) {
            if (add) {
                addBatch();
            }
            else {
                removeBatch();
            }
            startNewBatch( add );
            currentSubject = subject;
        }
    }

    protected void startNewBatch( boolean add ) {
        queue.clear();
        currentSubject = null;
        queueAdd = add;
    }

    protected void addBatch() {
        if (currentSubject != null && !queue.isEmpty()) {
            log.debug( "TextDocProducerBatch adding new batch for " + currentSubject );
            ExtendedEntity entity = new ExtendedEntity( entityDefinition(), null, currentSubject );
            int count = addQuads( queue.iterator(), entity );
            if (count > 0) {
                // add pre-existing fields to the entity
                // TODO check: should include graph ID in the find() here??
                count += addQuads( dsg.find( null, currentSubject, null, null ), entity );
                indexer.updateEntity( entity );
            }
        }
    }

    protected int addQuads( Iterator<Quad> iter, ExtendedEntity entity ) {
        int count = 0;

        while (iter.hasNext()) {
            Quad quad = iter.next();
            boolean added = entity.addProperty( entityDefinition(), quad.getPredicate(), quad.getObject() );
            if (added) {
                count++;
            }
        }

        return count;
    }

    /**
     * Remove a batch of quads that we have queued up. All of the quads will have the same
     * subject. Currently only works for Lucene indexes.
     */
    protected void removeBatch() {
        if (currentSubject != null && !queue.isEmpty()) {
            log.debug( "TextDocProducerBatch unindexing document for " + currentSubject );

            // TODO needs some work to make this a general capability
            if (indexer instanceof TextIndexLucene) {
                removeLuceneDocument( (TextIndexLucene) indexer );
            }
            else {
                log.warn( "Sorry, deleting is not yet supported on " + indexer.getClass().getName() );
            }
        }
    }

    /**
     * Remove a batch of same-subject quads from a Lucene index. Basic strategy is to
     * remove the document for the common subject, then re-index any properties that remain
     * in the quad store for that subject.
     *
     * @param indexerLucene
     */
    protected void removeLuceneDocument( TextIndexLucene indexerLucene ) {
        String key = currentSubject.isBlank() ? currentSubject.getBlankNodeLabel() : currentSubject.getURI();
        try {
			indexerLucene.getIndexWriter().deleteDocuments( new Term( "uri", key ) );
		} catch (IOException e) {
			throw new TextIndexException(e);
		}
        
        // there may be triples left that have current subject as subject
        // we need to put those back
        ExtendedEntity entity = new ExtendedEntity( entityDefinition(), null, currentSubject );

        // TODO check: should include graph ID in the find() here??
        int count = addQuads( dsg.find( null, currentSubject, null, null ), entity );
        if (count > 0) {
            indexer.updateEntity( entity );
        }
    }

    /**
     * In the case that a quad is being removed, we batch up the properties change
     * events until a boundary occurs, then delete that quad subject's document,
     * and re-add a document if any properties remain for the subject.
     * @param quad The quad being removed by this change event
     */
    protected void unIndex( Quad quad ) {
        if (currentSubject == null) {
            currentSubject = quad.getSubject();
        }

        checkBatchBoundary( quad.getSubject(), false );
        queue.add( quad );
    }

    /***********************************/
    /* Inner class definitions         */
    /***********************************/



}
