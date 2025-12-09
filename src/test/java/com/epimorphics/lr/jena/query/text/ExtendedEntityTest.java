/*****************************************************************************
 * File:    ExtendedEntityTest.java
 * Project: lr-text-indexer
 * Created: 14 Nov 2014
 * By:      ian
 *
 * Copyright (c) 2014 Epimorphics Ltd. All rights reserved.
 *****************************************************************************/

// Package
///////////////

package com.epimorphics.lr.jena.query.text;


// Imports
///////////////

import static org.junit.jupiter.api.Assertions.*;

import org.apache.jena.query.text.EntityDefinition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epimorphics.lr.jena.query.text.BatchState.Mode;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.QuadAction;

/**
 * Unit tests for {@link ExtendedEntity}
 */
public class ExtendedEntityTest
{    
	private static final Logger log = LoggerFactory.getLogger( ExtendedEntityTest.class );

	private final Node s = node("http://test.com/s");
    private final Node p = node("http://test.com/p");
    private final Node o = node("http://test.com/o");
    private final Node l = literal("foo");

    private final Node g = node( "http://test.com/g" );

    private final EntityDefinition ed_withG = new EntityDefinition( "entity", "primary" ) ;
    private final EntityDefinition ed_noG = new EntityDefinition( "entity", "primary", "graph" );

    private Node node(String uri) {
    	return ResourceFactory.createResource(uri).asNode();
    }
    
    private Node literal(String form) {
    	return ResourceFactory.createPlainLiteral(form).asNode();
    }

    @Test
    public void testExtendedEntityEntityDefinitionNodeNode() {
        ExtendedEntity xe = new ExtendedEntity( ed_noG, Mode.NONE, null, s );
        assertEquals( "http://test.com/s", xe.getId() );
        assertNull( xe.getGraph() );

        xe = new ExtendedEntity( ed_noG, Mode.NONE, g, s );
        assertEquals( "http://test.com/s", xe.getId() );
        assertEquals( "http://test.com/g", xe.getGraph() );

        xe = new ExtendedEntity( ed_withG, Mode.NONE, null, s );
        assertEquals( "http://test.com/s", xe.getId() );
        assertNull( xe.getGraph() );

        xe = new ExtendedEntity( ed_withG, Mode.NONE, g, s );
        assertEquals( "http://test.com/s", xe.getId() );
        assertEquals( "http://test.com/g", xe.getGraph() );
    }

    @Test
    public void testExtendedEntityEntityDefinitionNodeNodeNodeNode_1() {
        // resource o is not suitable for indexing
        assertThrows(NotSuitableForIndexingException.class, () -> new ExtendedEntity(ed_withG, Mode.NONE, g, s, p, o));
    }

    @Test
    public void testExtendedEntityEntityDefinitionNodeNodeNodeNode_2() {
        // no field for l
        assertThrows(NotSuitableForIndexingException.class, () -> new ExtendedEntity( ed_withG, Mode.NONE, g, s, p, l ));
    }

    @Test
    public void testExtendedEntityEntityDefinitionNodeNodeNodeNode_3() {
        ed_withG.set( "foo_field", p );
        ExtendedEntity xe = new ExtendedEntity( ed_withG, Mode.NONE, g, s, p, l );
        assertEquals( "foo", xe.get( "foo_field" ));
    }

    @Test
    public void testExtendedEntityEntityDefinitionQuad_1() {
        Quad q = new Quad(g, s, p, o);
        assertThrows(NotSuitableForIndexingException.class,
                () -> new ExtendedEntity(ed_withG, Mode.NONE, q));
    }

    @Test
    public void testExtendedEntityEntityDefinitionQuad_2() {
        Quad q = new Quad( g, s, p, l );
        assertThrows(NotSuitableForIndexingException.class,
                () -> new ExtendedEntity( ed_withG, Mode.NONE, q ));
    }

    @Test
    public void testExtendedEntityEntityDefinitionQuad_3() {
        ed_withG.set( "foo_field", p );
        Quad q = new Quad( g, s, p, l );
        ExtendedEntity xe = new ExtendedEntity( ed_withG, Mode.NONE, q );
        assertEquals( "foo", xe.get( "foo_field" ));
    }

    @Test
    public void testAddProperty() {
        ExtendedEntity xe = new ExtendedEntity( ed_withG, Mode.NONE, g, s );
        assertNull( xe.get( "foo_field" ));

        assertFalse( xe.addProperty( ed_withG, p, o ));
        assertFalse( xe.addProperty( ed_withG, p, l ));

        ed_withG.set( "foo_field", p );
        log.info("Expecting a logged WARN message about foo_field");
        assertFalse( xe.addProperty( ed_withG, p, o ));
        assertTrue( xe.addProperty( ed_withG, p, l ));

        assertEquals( "foo", xe.get( "foo_field" ));
    }
}
