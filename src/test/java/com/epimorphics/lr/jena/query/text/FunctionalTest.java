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

import org.apache.jena.query.text.TextQuery;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.tdb.TDB;

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

    static final String COMMON_SPARQL_PREFIXES =
            "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "prefix owl: <http://www.w3.org/2002/07/owl#>\n" +
            "prefix xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
            "prefix sr: <http://data.ordnancesurvey.co.uk/ontology/spatialrelations/>\n" +
            "prefix lrhpi: <http://landregistry.data.gov.uk/def/hpi/>\n" +
            "prefix lrppi: <http://landregistry.data.gov.uk/def/ppi/>\n" +
            "prefix skos: <http://www.w3.org/2004/02/skos/core#>\n" +
            "prefix lrcommon: <http://landregistry.data.gov.uk/def/common/>\n" +
            "PREFIX  text: <http://jena.apache.org/text#>\n" +
            "PREFIX  ppd:  <http://landregistry.data.gov.uk/def/ppi/>\n" +
            "PREFIX  lrcommon: <http://landregistry.data.gov.uk/def/common/>\n";

    static final String QUERY_TOWN_BRACKNELL =
            COMMON_SPARQL_PREFIXES +
            "SELECT  ?item ?ppd_propertyAddress ?ppd_hasTransaction ?ppd_pricePaid ?ppd_publishDate ?ppd_transactionDate ?ppd_transactionId ?ppd_estateType ?ppd_newBuild ?ppd_propertyAddressCounty ?ppd_propertyAddressDistrict ?ppd_propertyAddressLocality ?ppd_propertyAddressPaon ?ppd_propertyAddressPostcode ?ppd_propertyAddressSaon ?ppd_propertyAddressStreet ?ppd_propertyAddressTown ?ppd_propertyType ?ppd_recordStatus\n" +
            "WHERE\n" +
            "  { ?ppd_propertyAddress text:query _:b0 .\n" +
            "    _:b0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> lrcommon:town .\n" +
            "    _:b0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:b1 .\n" +
            "    _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"( bracknell )\" .\n" +
            "    _:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:b2 .\n" +
            "    _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> 3000000 .\n" +
            "    _:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .\n" +
            "    ?item ppd:propertyAddress ?ppd_propertyAddress .\n" +
            "    ?item ppd:hasTransaction ?ppd_hasTransaction .\n" +
            "    ?item ppd:pricePaid ?ppd_pricePaid .\n" +
            "    ?item ppd:publishDate ?ppd_publishDate .\n" +
            "    ?item ppd:transactionDate ?ppd_transactionDate .\n" +
            "    ?item ppd:transactionId ?ppd_transactionId\n" +
            "    OPTIONAL\n" +
            "      { ?item ppd:estateType ?ppd_estateType }\n" +
            "    OPTIONAL\n" +
            "      { ?item ppd:newBuild ?ppd_newBuild }\n" +
            "    OPTIONAL\n" +
            "      { ?ppd_propertyAddress lrcommon:county ?ppd_propertyAddressCounty }\n" +
            "    OPTIONAL\n" +
            "      { ?ppd_propertyAddress lrcommon:district ?ppd_propertyAddressDistrict }\n" +
            "    OPTIONAL\n" +
            "      { ?ppd_propertyAddress lrcommon:locality ?ppd_propertyAddressLocality }\n" +
            "    OPTIONAL\n" +
            "      { ?ppd_propertyAddress lrcommon:paon ?ppd_propertyAddressPaon }\n" +
            "    OPTIONAL\n" +
            "      { ?ppd_propertyAddress lrcommon:postcode ?ppd_propertyAddressPostcode }\n" +
            "    OPTIONAL\n" +
            "      { ?ppd_propertyAddress lrcommon:saon ?ppd_propertyAddressSaon }\n" +
            "    OPTIONAL\n" +
            "      { ?ppd_propertyAddress lrcommon:street ?ppd_propertyAddressStreet }\n" +
            "    OPTIONAL\n" +
            "      { ?ppd_propertyAddress lrcommon:town ?ppd_propertyAddressTown }\n" +
            "    OPTIONAL\n" +
            "      { ?item ppd:propertyType ?ppd_propertyType }\n" +
            "    OPTIONAL\n" +
            "      { ?item ppd:recordStatus ?ppd_recordStatus }\n" +
            "  }\n" +
            "LIMIT   100\n";

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

    /** Reset the TDB dataset each time */
    @Before
    public void resetTDB() {
        log.debug( "Reset TDB" );
        File tdbDir = new File( TDB_TEST_ROOT );
        File indexDir = new File( TDB_INDEX_ROOT );

        if (tdbDir.exists()) {
            log.debug( "Removing existing TDB directory" );
            tdbDir.delete();
        }
        if (indexDir.exists()) {
            log.debug( "Removing existing Lucene index directory" );
            tdbDir.delete();
        }

        tdbDir.mkdir();
        indexDir.mkdir();

        ds = loadDataset( ASSEMBLER_CONFIG, PPD_BASE_TEST_DATA );
    }

    @Test
    public void testCreateIndexForBaseDataset() {
        ResultSet results = queryData( ds, QUERY_TOWN_BRACKNELL );
        assertTrue( "Query failed to return any results", results.hasNext() );

        int n = 0;
        boolean seenPostcode = false;

        while (results.hasNext()) {
            QuerySolution soln = results.next();
            n++;

            log.debug( "solution " + n + " - postcode " + soln.getLiteral( "ppd_propertyAddressPostcode" ));
            seenPostcode = seenPostcode || soln.getLiteral( "ppd_propertyAddressPostcode" ).equals( "RG12 0UT" );
        }

        assertTrue( "Expected to see postcode", seenPostcode );
        assertEquals( 100, n );
    }

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    public Dataset createDsFromAssembler( String assemblerFile ) {
        Dataset ds = DatasetFactory.assemble( TEST_RESOURCES + assemblerFile, TEXT_CONFIG_ROOT );
        return ds ;
    }

    public Dataset loadDataset( String assemblerFile, String dataFile ) {
        log.debug( "Load begin" );
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

            TDB.sync( ds );
        }
        catch (Exception e) {
            log.error( e.getMessage() );
            log.error( e.getClass().getName() );
        }

        return ds;
    }

    public ResultSet queryData( Dataset ds, String sparql ) {
//        String qs = StrUtils.strjoinNL("SELECT * ", " { ?s text:query (rdfs:label 'X1') ;", "      rdfs:label ?label",
//                                       " }") ;

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



    /***********************************/
    /* Inner class definitions         */
    /***********************************/

}

//    @Test
//    public void buildText_01() {
//        createAssembler("text-config.ttl") ;
//    }
//
//    @Test
//    public void buildText_02() {
//        Dataset ds = createAssembler("text-config-union.ttl") ;
//        assertTrue(ds.getContext().isDefined(TextQuery.textIndex)) ;
//        assertTrue(ds.getContext().isDefined(TDB.symUnionDefaultGraph)) ;
//    }
//
//    @Test
//    public void buildText_03() {
//        createCode() ;
//    }
//
//    @Test
//    public void buildText_04() {
//        Dataset ds = createAssembler("text-config.ttl") ;
//        loadData(ds) ;
//        queryData(ds) ;
//    }
//
//    @Test
//    public void buildText_05() {
//        Dataset ds = createCode() ;
//        loadData(ds) ;
//        queryData(ds) ;
//    }
//

