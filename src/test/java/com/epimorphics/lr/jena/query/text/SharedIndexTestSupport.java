package com.epimorphics.lr.jena.query.text;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.jena.riot.RDFDataMgr;
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
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateAction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
    IntegrationBase contains constants and code used by FunctionalText
    and TextTextIndexingCommand.
*/
public class SharedIndexTestSupport {

	static final String TDB_TEST_ROOT = "target/tdb-testing";
	static final String TDB_INDEX_ROOT = TDB_TEST_ROOT + "-index";
	
	static final String TEXT_CONFIG_NS = "http://epimorphics.com/test/functional/ppd-text";
	static final String TEXT_CONFIG_ROOT = TEXT_CONFIG_NS + "#ds-with-lucene";
	
	static final String ASSEMBLER_CONFIG = "config-ppd-text.ttl";

	static final String TEST_RESOURCES = "src/test/resources/";
	static final String PPD_BASE_TEST_DATA = "landregistry_sample.nq";

    private static final Logger log = LoggerFactory.getLogger( FunctionalTest.class );

    protected Dataset ds = null;
    
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
    	Load a sparql query from the file named in <code>queryFile</code>.
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
		Perform the sparql update <code>sparql</code> on#
		the dataset <code>ds</code>. 
	*/
	public static void updateData( Dataset ds, String sparql ) {
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
	
    /**
    	queryData returns the ResultSet from running the query in
    	<code>sprqal</code> against the dataset <code>ds</code>.
    */
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
    	returns a dataset as specified by the assembly configuration
    	in the file named by <code>assemblerFile</code>.
    */
    public Dataset createDsFromAssembler( String assemblerFile ) {
        return DatasetFactory.assemble( TEST_RESOURCES + assemblerFile, TEXT_CONFIG_ROOT );
    }

    /**
		Return a dataset constructed according to the assembler
		and text config files.
    */
	protected static Dataset assembleDataset() {
		return DatasetFactory.assemble
			( TEST_RESOURCES + ASSEMBLER_CONFIG
			, TEXT_CONFIG_ROOT 
			);
	}

	/**
	    Create a TDB dataset according to the usual recipe and
	    initialise it with the PPD test data.
	*/
	protected static void loadDataIntoDataset() {
        Dataset ds = assembleDataset();
        ds.begin(ReadWrite.WRITE) ;
        RDFDataMgr.read( ds, TEST_RESOURCES + PPD_BASE_TEST_DATA ) ;
        ds.commit() ;
        ds.end() ;            
        ds.close();
	}
	
}
