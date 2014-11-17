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

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.*;

/**
 * this producer class batches up consecutive sequences of quads with the
 * same subject to add to the index as a single entity. It will also
 *  include in that entity existing properties of that subject in the
 * triple store.
 */
public class TextDocProducerBatch
    implements TextDocProducer
{
    private EntityDefinition defn;
    private TextIndex indexer;
    private DatasetGraph dsg;
    private boolean started;

    List<Quad> batch;
    Node subject;

    public void setDefn( EntityDefinition defn ) {
        this.defn = defn;
    }

    public void setTextIndex( TextIndex indexer ) {
        this.indexer = indexer;
    }

    public void setDatasetGraph( DatasetGraph dsg ) {
        this.dsg = dsg;
    }

    public void start() {
        indexer.startIndexing();
        started = true;
        startNewBatch();
    }

    public void finish() {
        addBatch();
        startNewBatch();
        indexer.finishIndexing();
        started = false;
    }

    public void change( QuadAction qaction, Node g, Node s, Node p, Node o ) {

        if (qaction != QuadAction.ADD)
            return;

        Quad quad = new Quad( g, s, p, o );

        if (subject == null) {
            subject = s;
            batch.add( quad );
        }
        else if (subject.equals( s )) {
            batch.add( quad );
        }
        else {
            addBatch();
            startNewBatch();
            subject = s;
            batch.add( quad );
        }

        // this is a single
        if (!started) {
            addBatch();
            startNewBatch();
        }

        // Entity entity = TextQueryFuncs.entityFromQuad(defn, g, s, p, o) ;
        // if ( entity != null )
        // // Null means does not match defn
        // indexer.addEntity(entity) ;
    }

    private void startNewBatch() {
        batch = new ArrayList<Quad>();
        subject = null;
    }

    private void addBatch() {
        if (subject == null) {
            // nothing to index
            return;
        }
        ExtendedEntity entity = new ExtendedEntity( defn, null, subject );
        int count = addQuads( batch.iterator(), entity );
        if (count == 0) {
            return;
        }

        // add pre-existing fields to the entity
        count += addQuads( dsg.find( null, subject, null, null ), entity );

        if (count > 0) {
            indexer.updateEntity( entity );
        }
    }

    private int addQuads( Iterator<Quad> iter, ExtendedEntity entity ) {
        int count = 0;
        for (; iter.hasNext();) {
            Quad quad = iter.next();
            boolean added = entity.addProperty( defn, quad.getPredicate(), quad.getObject() );
            if (added) {
                count++;
            }
        }
        return count;
    }
}
