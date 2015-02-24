package com.epimorphics.lr.jena.query.text;

import java.io.File;
import java.io.IOException;

import lr.textindexer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.text.TextQuery;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.tdb.StoreConnection;
import com.hp.hpl.jena.tdb.base.file.Location;

public class TestTextIndexingCommand {
	
	static { TextQuery.init(); }

    private static final Logger log = LoggerFactory.getLogger( TestTextIndexingCommand.class );

    Dataset ds = null;
    
	@BeforeClass public static void runTextIndexer() throws IOException {	
		clearIndex();
		clearTDB();
		loadData();        
		clearIndex();
		StoreConnection.expel( Location.create(FunctionalTest.TDB_TEST_ROOT), true );
		textindexer.main("--desc", "src/test/resources/config-ppd-text.ttl");
	}
	
	static void clearIndex() throws IOException {
        File indexDir = new File( FunctionalTest.TDB_INDEX_ROOT );
        FileUtils.deleteDirectory( indexDir );
        indexDir.mkdir();
	}

	private static void clearTDB() throws IOException {
		clearIndex();
		File tdbDir = new File( FunctionalTest.TDB_TEST_ROOT );
        FileUtils.deleteDirectory( tdbDir );
        tdbDir.mkdir();
	}
	
	@Before public void openDataset() {
        ds = DatasetFactory.assemble
        		( FunctionalTest.TEST_RESOURCES + FunctionalTest.ASSEMBLER_CONFIG
            	, FunctionalTest.TEXT_CONFIG_ROOT 
            	);
	}
	
	@After public void closeDataset() {
		if (ds == null) {
			log.warn("Dataset was null");
		} else {
			ds.close();
		}
	}
	
	static void loadData() {
        Dataset ds = DatasetFactory.assemble
        	( FunctionalTest.TEST_RESOURCES + FunctionalTest.ASSEMBLER_CONFIG
        	, FunctionalTest.TEXT_CONFIG_ROOT 
        	);

        ds.begin(ReadWrite.WRITE) ;
        RDFDataMgr.read( ds, FunctionalTest.TEST_RESOURCES + FunctionalTest.PPD_BASE_TEST_DATA ) ;
        ds.commit() ;
        ds.end() ;            
        
        ds.close();
	}
	
	@Test public void emptyTest() {	
	}
	
	@Test public void testCreateIndexForBaseDataset() {
        testQueryPostcodeCount( "query-town-dover.sparql", "CT17 9LD", 1 );
    }    
	
	@Test public void testMultipleFieldsSingleDocument() {
        testQueryPostcodeCount( "query-town-and-paon.sparql", "NG19 6NA", 1);
    }
	
    @Test public void testIndexAllowsStopWords() {
        testQueryPostcodeCount( "query-with-stop-word.sparql", "BB12 8NQ", 7 );
    }
    
    public void testQueryPostcodeCount( String sparql, String expectedPostcode, int expectedHits ) {
        ResultSet results = queryData( ds, loadQuery( FunctionalTest.TEST_RESOURCES + sparql ) );
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
    
}
