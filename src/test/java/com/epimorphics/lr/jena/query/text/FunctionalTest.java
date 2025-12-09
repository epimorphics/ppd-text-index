/*****************************************************************************
 * File:    FunctionalTest.java
 * Project: ppd-text-index
 * Created: 15 Dec 2014
 * By:      ian
 *
 * Copyright (c) 2014 Epimorphics Ltd. All rights reserved.
 *****************************************************************************/

package com.epimorphics.lr.jena.query.text;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.text.TextQuery;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jena.query.*;
import org.apache.jena.tdb.StoreConnection;
import org.apache.jena.tdb.base.file.Location;

/**
 * Functional test for test indexing, covering building an index from scratch
 * and updating the index using SPARQL updates (which is how the PPD index
 * gets updated).
 */
public class FunctionalTest extends SharedIndexTestSupport {
	
    /***********************************/
    /* Static variables                */
    /***********************************/

    private static final Logger log = LoggerFactory.getLogger( FunctionalTest.class );

    /***********************************/
    /* External signature methods      */
    /***********************************/

    /** Ensure assembler initialized. */
    @BeforeAll
    public static void setupClass() {
        TextQuery.init() ;
    }

    /** Reset the TDB dataset each time by deleting and reloading */
    @BeforeEach
    public void resetTDB() {
        log.debug( "Reset TDB" );
        StoreConnection.expel( Location.create(TDB_TEST_ROOT), true );

        File tdbDir = new File( TDB_TEST_ROOT );
        File indexDir = new File( TDB_INDEX_ROOT );

        try {
            if (tdbDir.exists()) {
                FileUtils.deleteDirectory( tdbDir );
            }
            if (indexDir.exists()) {
                FileUtils.deleteDirectory( indexDir );
            }
        }
        catch (IOException e) {
            log.error( e.getMessage(), e );
            throw new RuntimeException( e );
        }

        tdbDir.mkdir();
        indexDir.mkdir();

        ds = loadDataset( ASSEMBLER_CONFIG, PPD_BASE_TEST_DATA );
    }
    
    @AfterEach
    public void closeDataset() {
    	ds.close();
    }

    /**
     * Test that the base dataset is indexed as it is loaded.
     */
    @Test
    public void testCreateIndexForBaseDataset() {
        testQueryPostcodeCount( "query-town-dover.sparql", "CT17 9LD", 1 );
    }

    /**
     * Test that we are indexing addresses as single documents - match two
     * fields on one address
     */
    @Test
    public void testMultipleFieldsSingleDocument() {
        testQueryPostcodeCount( "query-town-and-paon.sparql", "NG19 6NA", 1);
    }

    /**
     * Test that index allows stop-words like 'the' to be indexed as parts of addresses
     */
    @Test
    public void testIndexAllowsStopWords() {
        testQueryPostcodeCount( "query-with-stop-word.sparql", "BB12 8NQ", 7 );
    }

    /**
     * Test that incremental adds are indexed
     */
    @Test
    public void testIndexIncrementalAdd() {
        testQueryPostcodeCount( "query-with-stop-word.sparql", "BB12 8NQ", 7 );
        SharedIndexTestSupport.updateData( ds, loadQuery( TEST_RESOURCES + "update-add.sparql" ) );
        testQueryPostcodeCount( "query-with-stop-word.sparql", "PAT JESS", 8 );
    }

    /**
     * Test that incremental deletes remove documents from the index
     */
    @Test
    public void testIndexIncrementalDelete() {
        testQueryPostcodeCount( "query-with-stop-word.sparql", "BB12 8NQ", 7 );
        SharedIndexTestSupport.updateData( ds, loadQuery( TEST_RESOURCES + "update-delete.sparql" ) );
        testQueryPostcodeCount( "query-with-stop-word.sparql", "AL9 5DQ", 6 );
    }

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    /**
     * Load a datafile into a newly created dataset, specified by the given assembler file
     * @param assemblerFile
     * @param dataFile
     * @return
     */
    public Dataset loadDataset( String assemblerFile, String dataFile ) {
        Dataset ds = null;
        try {
            ds = createDsFromAssembler( assemblerFile );

            ds.begin(ReadWrite.WRITE) ;
            try {
                RDFDataMgr.read( ds, TEST_RESOURCES + dataFile ) ;
                ds.commit() ;
            }
            finally {
                ds.end() ;
            }

            ds.begin( ReadWrite.READ );
            log.debug( "Load completed, dataset size is: " + ds.getNamedModel( "urn:x-arq:UnionGraph" ).size() );
            ds.end();
        }
        catch (Exception e) {
            log.error( e.getMessage() );
            log.error( e.getClass().getName() );
        }

        return ds;
    }


}
