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

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.util.FmtUtils;

/**
 * Facade-pattern wrapper for {@link Entity}, which allows entities to
 * be created from nodes in graphs, and updated with additional
 * indexed fields over time.
 */
public class ExtendedEntity
extends Entity
{
    /***********************************/
    /* Constants                       */
    /***********************************/

    /***********************************/
    /* Static variables                */
    /***********************************/

    /***********************************/
    /* Instance variables              */
    /***********************************/

    /***********************************/
    /* Constructors                    */
    /***********************************/

    /**
     * Construct a new extended entity from the given graph and subject nodes.
     *
     * @param defn The entity definition
     * @param g Node denoting the graph
     * @param s Node denoting the subject resource
     */
    public ExtendedEntity( EntityDefinition defn, Node g, Node s ) {
        super( NodeTermConverter.subjectToTerm( s ), NodeTermConverter.graphNodeToTerm( g ) );

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
    public ExtendedEntity( EntityDefinition defn, Node g, Node s, Node p, Node o ) {
        this( defn, g, s );

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
    public ExtendedEntity( EntityDefinition defn, Quad quad ) {
        this( defn, quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject() );
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

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    /***********************************/
    /* Inner class definitions         */
    /***********************************/

}

