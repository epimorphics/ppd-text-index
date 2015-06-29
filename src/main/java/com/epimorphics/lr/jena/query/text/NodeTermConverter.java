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

import org.apache.jena.query.text.TextIndexException;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.AnonId;

/**
 * Utility methods for converting between {@link Node}s and string terms that
 * can be stored in, and retrieved from, the text index.
 */
public class NodeTermConverter
{

    public static final String ANON_NODE_PREFIX = "_:";

    /**
     * Return a string that will be a text index term for the given subject.
     * @param s A node denoting a subject resource
     * @return A string term that denotes the node in the text index
     * @throws IllegalArgumentException if s is null
     * @throws TextIndexException unless s is a URI node
     */
    public static String subjectToTerm( Node s ) {
        if (s == null) {
            throw new IllegalArgumentException( "Subject node should not be null" );
        }

        if (!(s.isURI() || s.isBlank())) {
            throw new TextIndexException( "Subject should be a URI or a blank node: " + s );
        }

        return nodeToTerm( s );
    }

    /**
     * Return a string that will be the text index term for the given graph name.
     * @param g A node denoting a graph, or null
     * @return  A string term that denotes the node in the text index, or null
     * @throws TextIndexException unless g is a URI node
     */
    public static String graphNodeToTerm( Node g ) {
        if (g == null) {
            return null;
        }

        if (!(g.isURI() || g.isBlank())) {
            throw new TextIndexException( "Graph label should be either a URI or a blank node: " + g );
        }

        return nodeToTerm( g );
    }

    private static String nodeToTerm( Node n ) {
        return (n.isURI()) ? n.getURI() : ANON_NODE_PREFIX + n.getBlankNodeLabel();
    }

    /**
     * @return The graph node corresponding to the given term
     */
    public static Node termToNode( String v ) {
        if (v.startsWith( ANON_NODE_PREFIX )) {
            return NodeFactory.createAnon( new AnonId( v.substring( ANON_NODE_PREFIX.length() ) ) );
        }
        else {
            return NodeFactory.createURI( v );
        }
    }

}
