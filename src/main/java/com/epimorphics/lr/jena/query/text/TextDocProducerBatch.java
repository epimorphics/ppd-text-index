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
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.*;

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

    protected ThreadLocal<BatchState> state = new ThreadLocal<BatchState>() {
    	@Override protected BatchState initialValue() {
    		return new BatchState();
    	}    	
    };

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
    
    // Only for testing
    public ThreadLocal<BatchState> exposeBatchState() {
    	return state;
    }

    /**
     * @return The entity definition for the current indexer
     */
    public EntityDefinition entityDefinition() {
        return indexer.getDocDef();
    }

//    static int count = 0;
//    int index = ++count;
    
    @Override public void start() {
    	BatchState s = state.get();
        log.debug( "TextDocProducerBatch.start()" );
        s.start();
        // indexer.startIndexing();
        // s.reset(true, null);
    }

    @Override public void finish() {
        log.debug( "TextDocProducerBatch.finish()" );
        flush();
        state.set(null);
        state.remove();
    }

	public void flush() {
        log.debug( "TextDocProducerBatch.flush()" );
        BatchState s = state.get();
		if (s.queueAdd) {
            addBatch(s);
        }
        else {
            removeBatch(s);
        }
		s.reset(true, null);
	}

    @Override public void change( QuadAction qaction, Node g, Node s, Node p, Node o ) {
        Quad quad = new Quad( g, s, p, o );
        switch( qaction ) {
            case ADD:
                indexNewQuad( quad );
                break;
            case DELETE:
                unIndex( state.get(), quad );
                break;
            case NO_ADD:
//                log.warn( "Saw change action NO_ADD, but ignoring it!" );
                break;
            case NO_DELETE:
                log.warn( "Saw change action NO_DELETE, but ignoring it!" );
                break;
        }
    }

	@Override public void reset() {
		// Don't think we need anything here, depends on details of the
		// DatsetChange contract really.
	}

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    protected void indexNewQuad( Quad quad ) {
    	BatchState s = state.get();
        if (s.currentSubject == null) {
        	s.currentSubject = quad.getSubject();
        }

        checkBatchBoundary( s, quad.getSubject(), true );
        s.queue.add( quad );
    }

    protected void checkBatchBoundary( BatchState s, Node subject, boolean add ) {
        if (!(s.currentSubject.equals( subject ) && s.queueAdd == add)) {
            if (add) {
                addBatch(s);
            }
            else {
                removeBatch(s);
            }
			s.reset(add, subject);
        }
    }

    protected void startNewBatch( BatchState s, boolean add ) {
    	s.reset(add, null);
    }

    protected void addBatch(BatchState s) {
        if (s.hasBatch()) {
        	Node cs = s.currentSubject;
            log.debug( "TextDocProducerBatch adding new batch for " + cs );
            ExtendedEntity entity = new ExtendedEntity( entityDefinition(), null, cs );
            int count = addQuads( s.queue.iterator(), entity );
            if (count > 0) {
                // add pre-existing fields to the entity
                // TODO check: should include graph ID in the find() here??
                count += addQuads( dsg.find( null, cs, null, null ), entity );
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
    protected void removeBatch(BatchState s) {
        if (s.currentSubject != null && !s.queue.isEmpty()) {
            log.debug( "TextDocProducerBatch unindexing document for " + s.currentSubject );

            // TODO needs some work to make this a general capability
            if (indexer instanceof TextIndexLucene) {
                removeLuceneDocument( s, (TextIndexLucene) indexer );
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
    protected void removeLuceneDocument( BatchState s, TextIndexLucene indexerLucene ) {
        String key = s.currentSubject.isBlank() ? s.currentSubject.getBlankNodeLabel() : s.currentSubject.getURI();
        try {
			indexerLucene.getIndexWriter().deleteDocuments( new Term( "uri", key ) );
		} catch (IOException e) {
			throw new TextIndexException(e);
		}
        
        // there may be triples left that have current subject as subject
        // we need to put those back
        ExtendedEntity entity = new ExtendedEntity( entityDefinition(), null, s.currentSubject );

        // TODO check: should include graph ID in the find() here??
        int count = addQuads( dsg.find( null, s.currentSubject, null, null ), entity );
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
    protected void unIndex( BatchState s, Quad quad ) {
        if (s.currentSubject == null) {
        	s.currentSubject = quad.getSubject();
        }

        checkBatchBoundary( s, quad.getSubject(), false );
        s.queue.add( quad );
    }


}
