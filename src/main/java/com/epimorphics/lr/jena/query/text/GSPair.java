/*****************************************************************************
 * File:    GSPair.java
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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.hp.hpl.jena.graph.Node;

/**
 * A class representing the md5 hash of a pair of nodes, one a graph node,
 * the other a subject.
 *
 * The graph node can be null, the subject node can't,
 *
 */
class GSPair
{

    private static MessageDigest md = null;
    static {
        try {
            md = MessageDigest.getInstance( "MD5" );
        }
        catch (NoSuchAlgorithmException e) {
            // it won't happen, but if it does it will fail soon anyway
        }
    }
    private long l1, l2;

    GSPair( Node graph, Node subject ) {
        byte[] bytes = md.digest( (graph.toString() + subject.toString()).getBytes() );
        ByteBuffer bb = ByteBuffer.allocate( 16 );

        bb.put( bytes );
        l1 = bb.getLong( 0 );
        l2 = bb.getLong( 8 );
    }

    @Override
    public boolean equals( Object object ) {
        if (object instanceof GSPair) {
            GSPair gs = (GSPair) object;
            return gs.l1 == l1 && gs.l2 == l2;
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (int) l1 & 0xFFFFFFFF;
    }

}
