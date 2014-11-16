/*****************************************************************************
 * File:    NodeTermConverterTest.java
 * Project: lr-text-indexer
 * Created: 16 Nov 2014
 * By:      ian
 *
 * Copyright (c) 2014 Epimorphics Ltd. All rights reserved.
 *****************************************************************************/

// Package
///////////////

package com.epimorphics.lr.jena.query.text;


// Imports
///////////////

import static org.junit.Assert.*;

import org.apache.jena.query.text.TextIndexException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

/**
 * Unit tests for {@link NodeTermConverter}
 */
public class NodeTermConverterTest
{
    /***********************************/
    /* Constants                       */
    /***********************************/

    /***********************************/
    /* Static variables                */
    /***********************************/

    @SuppressWarnings( value = "unused" )
    private static final Logger log = LoggerFactory.getLogger( NodeTermConverterTest.class );

    /***********************************/
    /* Instance variables              */
    /***********************************/

    private Node l;
    private Node r;
    private Node a;

    /***********************************/
    /* Constructors                    */
    /***********************************/

    /***********************************/
    /* External signature methods      */
    /***********************************/

    @Before
    public void setUp() {
        l = ResourceFactory.createPlainLiteral( "foo" ).asNode();
        r = ResourceFactory.createResource( "http://test/foo" ).asNode();
        a = ResourceFactory.createResource().asNode();
    }

    @Test (expected=IllegalArgumentException.class )
    public void testSubjectToTerm0() {
        NodeTermConverter.subjectToTerm( null );
    }

    @Test (expected=TextIndexException.class )
    public void testSubjectToTerm1() {
        NodeTermConverter.subjectToTerm( l );
    }

    @Test
    public void testSubjectToTerm2() {
        String term = NodeTermConverter.subjectToTerm( r );
        assertNotNull( term );
    }

    @Test
    public void testGraphNodeToTerm0() {
        assertNull( NodeTermConverter.graphNodeToTerm( null ) );
    }

    @Test (expected=TextIndexException.class )
    public void testGraphNodeToTerm1() {
        NodeTermConverter.graphNodeToTerm( l );
    }

    @Test
    public void testGraphToTerm2() {
        String term = NodeTermConverter.graphNodeToTerm( a );
        assertNotNull( term );
    }

    @Test
    public void testTermToNode0() {
        String term = NodeTermConverter.subjectToTerm( r );
        Node n = NodeTermConverter.termToNode( term );
        assertEquals( r, n );
    }

    @Test
    public void testTermToNode1() {
        String term = NodeTermConverter.subjectToTerm( a );
        Node n = NodeTermConverter.termToNode( term );
        assertEquals( a, n );
    }

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    /***********************************/
    /* Inner class definitions         */
    /***********************************/

}

