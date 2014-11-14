/*****************************************************************************
 * File:    NotSuitableForIndexing.java
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


/**
 * Exception to denote that a quad is not suitable for indexing
 * (e.g. because the object is not a literal, or because no field
 * is defined for the predicate).
 */
@SuppressWarnings( "serial" )
public class NotSuitableForIndexing
extends RuntimeException
{
    // no constructor, this is just a marker exception
}

