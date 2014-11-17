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

import java.util.*;

import org.apache.jena.query.text.*;
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
    private boolean started;

    List<Quad> queue = new ArrayList<Quad>();
    Node currentSubject;

    /***********************************/
    /* Constructors                    */
    /***********************************/

    public TextDocProducerBatch( DatasetGraph dsg, TextIndex textIndex ) {
        this.dsg = dsg;
        this.indexer = textIndex;
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
        indexer.startIndexing();
        started = true;
        startNewBatch();
    }

    @Override
    public void finish() {
        addBatch();
        startNewBatch();
        indexer.finishIndexing();
        started = false;
    }

    @Override
    public void change( QuadAction qaction, Node g, Node s, Node p, Node o ) {
        Quad quad = new Quad( g, s, p, o );
        switch( qaction ) {
            case ADD:
                indexNewQuad( quad );
                break;
            case DELETE:
                log.warn( "Saw change action DELETE, but ignoring it!" );
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

        checkBatchBoundary( quad.getSubject() );
        queue.add( quad );
        checkSingletonBatch();
    }

    protected void checkBatchBoundary( Node subject ) {
        if (!currentSubject.equals( subject )) {
            addBatch();
            startNewBatch();
            currentSubject = subject;
        }
    }

    protected void startNewBatch() {
        queue.clear();
        currentSubject = null;
    }

    /**
     * I'm actually not quite sure under what circumstances this arises, but it's part
     * of the original code so I'm keeping it!
     */
    protected void checkSingletonBatch() {
        if (!started) {
            addBatch();
            startNewBatch();
        }
    }

    protected void addBatch() {
        if (currentSubject != null) {
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

    /***********************************/
    /* Inner class definitions         */
    /***********************************/



}
