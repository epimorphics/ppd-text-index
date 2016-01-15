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

import com.epimorphics.lr.jena.query.text.BatchState.Mode;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.*;

/**

<p>
	TextDocProducerBatch monitors quads as they are added and deleted
	through the DatasetChanges interface it implements, and adjusts
	the TextIndex according to literal objects. Multiple quads with the
	same subject are dealt with together. In case such a batch isn't
	complete, ie there are already existing literals for its subject,
	we search the dataset graph for all relevant triples and allow
	for their literals. 
</p>

<p>
	Batch state is retained in a thread-local variable. The state
	records the ongoing sequence of quads with the same subject
	and mode (add <i>vs</i> delete quads). Initially this state
	is an empty list of quads, a model boolean, and the current
	subject (which is the subject of all the quads in the list). 
</p>

<p>
	The DatasetChanges API contains the methods start(), finish(),
	reset(), and change(). 
</p>

<p>
	Calling start() announces that a new sequence of updates follows.
	Any queued-up quads are discarded. A new batch state is created.
</p>

<p>
	Calling finish() announces that the sequence of updates has finished.
	Any pending quads are indexed. The batch state is discarded.	
</p>

<p>
	change() announces another quad to be added or removed. The most recent
	call to start() in this thread should not have been followed by a call to 
	finish(). If this quad shares subject and mode with the current state,
	then the quad is added to the pending quads. Otherwise the pending quads
	are used to update the index before being discarded and this quad becomes
	the first of the new pending quads.
</p>

<p>
	reset() announces that whatever accumulated batch state there is should
	be discarded without updating the index. (Complete batches will already
	have been used to update the index.)
</p>

<p>
	In normal use a TextDocProducerBatch sees API sequences of the
	form <code>(start, change*, finish)*</code>.
</p>

*/
public class TextDocProducerBatch
    implements TextDocProducer
{
    private static Logger log = LoggerFactory.getLogger( TextDocProducerBatch.class );

    private TextIndex indexer;
    private DatasetGraph dsg;

    protected ThreadLocal<BatchState> state = new ThreadLocal<BatchState>() {
    	@Override protected BatchState initialValue() {
    		return new BatchState();
    	}    	
    };

    public TextDocProducerBatch( DatasetGraph dsg, TextIndex textIndex ) {
        this.dsg = dsg;
        this.indexer = textIndex;
        log.debug( "Initialising TextDocProducerBatch" );
    }
    
    public TextDocProducerBatch( TextIndex textIndex ) {
    	this(null, textIndex);
    }
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
    
    @Override public void start() {
    	BatchState s = state.get();
        log.debug( "TextDocProducerBatch.start()" );
        s.start();
        // indexer.startIndexing();
    }

    @Override public void finish() {
        log.debug( "TextDocProducerBatch.finish()" );
        try { flush(); } finally { state.remove(); }
    }

	public void flush() {
        log.debug( "TextDocProducerBatch.flush()" );
        BatchState s = state.get();
		if (s.queueMode == Mode.ADD) {
            addBatch(s, indexer);
        }
        else if (s.queueMode == Mode.DELETE){
            removeBatch(s, indexer);
        }
		s.reset(Mode.NONE, null);
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
//                log.warn( "Saw change action NO_DELETE, but ignoring it!" );
                break;
        }
    }

	@Override public void reset() {
		state.remove();
	}

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    protected void indexNewQuad( Quad quad ) { 	
    	BatchState s = state.get();
        if (s.currentSubject == null) {
        	s.currentSubject = quad.getSubject();
        }

        checkBatchBoundary( s, quad.getSubject(), Mode.ADD );
        s.queue.add( quad );
    }

    protected void checkBatchBoundary( BatchState s, Node subject, Mode queueMode ) {
        if (!(s.currentSubject.equals( subject ) && s.queueMode == queueMode)) {
            if (s.queueMode == Mode.ADD) {
                addBatch(s, indexer);
            }
            else if (s.queueMode == Mode.DELETE){
                removeBatch(s, indexer);
            }
			s.reset(queueMode, subject);
        } else {
        }
    }

    protected void startNewBatch( BatchState s, Mode queueMode ) {
    	s.reset(queueMode, null);
    }

    protected void addBatch(BatchState s, TextIndex indexer) {
        if (s.hasBatch()) {
        	Node cs = s.currentSubject;
//            log.info( "TextDocProducerBatch adding new batch for " + s.queue );
            ExtendedEntity entity = new ExtendedEntity( entityDefinition(), Mode.ADD, null, cs );
            int count = addQuads( s.queue.iterator(), entity );
            if (count > 0) {
                // add pre-existing fields to the entity
                // TODO check: should include graph ID in the find() here??
                count += addQuads( dsg.find( null, cs, null, null ), entity );
                indexer.updateEntity( entity );
            }
        } else {
//        	System.err.println(">> no batch");
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
    protected void removeBatch(BatchState s, TextIndex indexer) {
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
        ExtendedEntity entity = new ExtendedEntity( entityDefinition(), Mode.DELETE, null, s.currentSubject );

        // TODO check: should include graph ID in the find() here??
        int count = addQuads( dsg.find( null, s.currentSubject, null, null ), entity );
        if (count > 0) {
            indexerLucene.updateEntity( entity );
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

        checkBatchBoundary( s, quad.getSubject(), Mode.DELETE );
        s.queue.add( quad );
    }


}
