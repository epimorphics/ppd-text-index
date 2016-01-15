/*****************************************************************************
 * File:    ExtendedEntity.java
 * Project: lr-text-indexer
 * Created: 14 Nov 2014
 * By:      ian
 *
 * Copyright (c) 2014 Epimorphics Ltd. All rights reserved.
 *****************************************************************************/

// Package
///////////////

package com.epimorphics.lr.jena.query.text;


import org.apache.jena.atlas.logging.Log;
import org.apache.jena.query.text.*;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.FmtUtils;

import com.epimorphics.lr.jena.query.text.BatchState.Mode;

/**
 * Facade-pattern wrapper for {@link Entity}, which allows entities to
 * be created from nodes in graphs, and updated with additional
 * indexed fields over time.
 */
public class ExtendedEntity extends Entity
{
	/**
		When an ExtendedEntity is created it records whether it is being
		used for an ADD or DELETE operation, or NONE if it doesn't matter
		(or is both). This is primarily to leave an trail for testing.
	*/
	final Mode mode;
	
    /**
     * Construct a new extended entity from the given graph and subject nodes.
     *
     * @param defn The entity definition
     * @param g Node denoting the graph
     * @param s Node denoting the subject resource
     */
    public ExtendedEntity( EntityDefinition defn, Mode mode, Node g, Node s ) {
        super( NodeTermConverter.subjectToTerm( s ), NodeTermConverter.graphNodeToTerm( g ) );
        this.mode = mode;
        String graphField = defn.getGraphField();
        if (defn.getGraphField() != null)
            put( graphField, NodeTermConverter.graphNodeToTerm( g ) );
    }

    /**
     * Create an Entity from a quad (as g/s/p/o). Throws an exception if the quad is
     * not a candidate for indexing.
     *
     * @param defn Entity definition
     * @param g Node denoting the graph
     * @param s Node denoting the subject
     * @param p Node denoting the predicate
     * @param o Node denoting the object
     * @exception NotSuitableForIndexingException if the node cannot be indexed
     */
    public ExtendedEntity( EntityDefinition defn, Mode mode, Node g, Node s, Node p, Node o ) {
        this( defn, mode, g, s );

        if (!addProperty( defn, p, o )) {
            throw new NotSuitableForIndexingException();
        }
    }

    /**
     * Create an Entity from a quad. throws an exception if the quad is not a candidate
     * for indexing.
     *
     * @param defn Entity definition
     * @param quad Quad to be indexed
     * @exception NotSuitableForIndexingException if the quad cannot be indexed
     */
    public ExtendedEntity( EntityDefinition defn, Mode mode, Quad quad ) {
        this( defn, mode, quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject() );
    }
    
    public boolean equals(Object other) {
    	return other instanceof ExtendedEntity && same( (ExtendedEntity) other);
    }

    /**
     	This equality test is crude and only tests that the maps and the
     	modes of the two ExtendedEntitys are equal.
    */
    private boolean same(ExtendedEntity other) {
		return getMap().equals(other.getMap()) && mode == other.mode;
	}

	/***********************************/
    /* External signature methods      */
    /***********************************/

    /**
     * Add a text field to this entity for the given property, if it has a field
     * definition and has a literal value.
     *
     * @param defn Entity definition
     * @param p Node denoting the predicate
     * @param o Node denoting the object
     * @return True if a field was added for property p with literal value o
     */
    public boolean addProperty( EntityDefinition defn, Node p, Node o ) {
        boolean success = true;
        String field = defn.getField( p );

        if (field == null) {
            success = false;
        }
        else if (!o.isLiteral()) {
            Log.warn( TextQuery.class,
                      "Not a literal value for mapped field-predicate: " + field + " :: " +
                      FmtUtils.stringForString( field ) );
            success = false;
        }
        else {
            put( field, o.getLiteralLexicalForm() );
        }

        return success;
    }
}

