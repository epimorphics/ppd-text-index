/*****************************************************************************
 * File:    FunctionalTest.java
 * Project: ppd-text-index
 * Created: 15 Dec 2014
 * By:      ian
 *
 * Copyright (c) 2014 Epimorphics Ltd. All rights reserved.
 *****************************************************************************/

package com.epimorphics.lr.jena.query.text;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.text.TextQuery;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.tdb.StoreConnection;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.update.*;

/**
 * Functional test for test indexing, covering building an index from scratch
 * and updating the index using SPARQL updates (which is how the PPD index
 * gets updated).
 */
public class FunctionalTest
{
    /***********************************/
    /* Constants                       */
    /***********************************/

    static final String TEST_RESOURCES = "src/test/resources/";

    static final String TDB_TEST_ROOT = "target/tdb-testing";
    static final String TDB_INDEX_ROOT = TDB_TEST_ROOT + "-index";

    static final String ASSEMBLER_CONFIG = "config-ppd-text.ttl";

    static final String TEXT_CONFIG_NS = "http://epimorphics.com/test/functional/ppd-text";

    static final String TEXT_CONFIG_ROOT = TEXT_CONFIG_NS + "#ds-with-lucene";

    static final String PPD_BASE_TEST_DATA = "landregistry_sample.nq";


    /***********************************/
    /* Static variables                */
    /***********************************/

    private static final Logger log = LoggerFactory.getLogger( FunctionalTest.class );

    /***********************************/
    /* Instance variables              */
    /***********************************/

    /** TDB dataset with assembler */
    Dataset ds;

    /***********************************/
    /* Constructors                    */
    /***********************************/

    /***********************************/
    /* External signature methods      */
    /***********************************/

    /** Ensure assembler initialized. */
    @BeforeClass
    public static void setupClass() {
        TextQuery.init() ;
    }

    /** Reset the TDB dataset each time by deleting and reloading */
    @Before
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
        updateData( ds, loadQuery( TEST_RESOURCES + "update-add.sparql" ) );
        testQueryPostcodeCount( "query-with-stop-word.sparql", "PAT JESS", 8 );
    }

    /**
     * Test that incremental deletes remove documents from the index
     */
    @Test
    public void testIndexIncrementalDelete() {
        testQueryPostcodeCount( "query-with-stop-word.sparql", "BB12 8NQ", 7 );
        updateData( ds, loadQuery( TEST_RESOURCES + "update-delete.sparql" ) );
        testQueryPostcodeCount( "query-with-stop-word.sparql", "AL9 5DQ", 6 );
    }

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    /**
     * Perform a test query as a select, and check that the results include a certain address
     * (identified by postcode) and matches a certain number of results
     *
     * @param sparql
     * @param expectedPostcode
     * @param expectedHits
     */
    public void testQueryPostcodeCount( String sparql, String expectedPostcode, int expectedHits ) {
        ResultSet results = queryData( ds, loadQuery( TEST_RESOURCES + sparql ) );
        assertTrue( "Query failed to return any results", results.hasNext() );

        int n = 0;
        boolean seenPostcode = false;

        while (results.hasNext()) {
            QuerySolution soln = results.next();
            n++;

            Literal l = soln.getLiteral( "ppd_propertyAddressPostcode" );
            String postcode = (l == null) ? null : l.getLexicalForm();
            seenPostcode = seenPostcode || (postcode != null && postcode.equals( expectedPostcode ));
        }

        assertTrue( "Expected to see postcode " + expectedPostcode, seenPostcode );
        assertEquals( "Expected " + expectedHits + " results", expectedHits, n );
    }

    /**
     * @return A dataset given an assembler description
     */
    public Dataset createDsFromAssembler( String assemblerFile ) {
        return DatasetFactory.assemble( TEST_RESOURCES + assemblerFile, TEXT_CONFIG_ROOT );
    }

    /**
     * Load a datafile into a newly created dataset, specified by the given assembler file
     * @param assemblerFile
     * @param dataFile
     * @return
     */
    @SuppressWarnings( "hiding" )
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

    /**
     * @return The resultset from running the given sparql query against the given dataset
     */
    @SuppressWarnings( "hiding" )
    public ResultSet queryData( Dataset ds, String sparql ) {
        ds.begin(ReadWrite.READ) ;
        ResultSet rs = null;

        try {
            Query q = QueryFactory.create( sparql );
            rs = QueryExecutionFactory.create( q, ds ).execSelect();
        }
        finally {
            ds.end() ;
        }

        return rs;
    }

    /**
     * Load a sparql query from a file
     * @param queryFile
     * @return
     */
    public String loadQuery( String queryFile ) {
        try {
            File file = new File( queryFile );
            assertTrue( "Query file does not exist: " + queryFile, file.exists() );

            return FileUtils.readFileToString( file );
        }
        catch (IOException e) {
            log.error( e.getMessage(), e );
            throw new RuntimeException( e );
        }
    }

    /**
     * Perform a sparql update
     */
    @SuppressWarnings( "hiding" )
    public void updateData( Dataset ds, String sparql ) {
        ds.begin(ReadWrite.WRITE) ;

        try {
            GraphStore graphStore = GraphStoreFactory.create(ds) ;
            UpdateAction.parseExecute( sparql, graphStore ) ;
            ds.commit() ;
        }
        finally {
            ds.end() ;
        }
    }

    /***********************************/
    /* Inner class definitions         */
    /***********************************/

}
