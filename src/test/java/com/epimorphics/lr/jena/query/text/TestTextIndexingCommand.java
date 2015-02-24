package com.epimorphics.lr.jena.query.text;

import java.io.File;
import java.io.IOException;

import lr.textindexer;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.text.TextQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.tdb.StoreConnection;
import com.hp.hpl.jena.tdb.base.file.Location;

public class TestTextIndexingCommand extends SharedIndexTestSupport {
	
	static { TextQuery.init(); }

    private static final Logger log = LoggerFactory.getLogger( TestTextIndexingCommand.class );

	@BeforeClass public static void runTextIndexer() throws IOException {	
		clearIndex();
		clearTDB();
		loadDataIntoDataset();        
		clearIndex();
		StoreConnection.expel( Location.create(TDB_TEST_ROOT), true );
		textindexer.main("--desc", "src/test/resources/config-ppd-text.ttl");
	}
	
	protected static void clearIndex() throws IOException {
        File indexDir = new File( TDB_INDEX_ROOT );
        FileUtils.deleteDirectory( indexDir );
        indexDir.mkdir();
	}

	protected static void clearTDB() throws IOException {
		File tdbDir = new File( TDB_TEST_ROOT );
        FileUtils.deleteDirectory( tdbDir );
        tdbDir.mkdir();
	}
	
	@Before public void openDataset() {
        ds = assembleDataset();
	}
	
	@After public void closeDataset() {
		if (ds == null) {
			log.warn("Dataset was null");
		} else {
			ds.close();
		}
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
    
}
