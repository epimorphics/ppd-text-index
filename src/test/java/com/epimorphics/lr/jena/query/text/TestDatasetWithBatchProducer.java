package com.epimorphics.lr.jena.query.text;



import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.query.text.DatasetGraphText;
import org.apache.jena.query.text.assembler.TextAssembler;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDFS;

/**
 * Unit tests for a dataset that specifies a non-default document producer.
 */
public class TestDatasetWithBatchProducer {
    
	private static final String INDEX_PATH = "target/test/TestDatasetWithLuceneIndex";
    private static final File indexDir = new File(INDEX_PATH);

    private static final String SPEC_BASE = "http://example.org/spec#";
    private static final String SPEC_ROOT_LOCAL = "lucene_text_dataset";
    private static final String SPEC_ROOT_URI = SPEC_BASE + SPEC_ROOT_LOCAL;
    private static final String SPEC;

    private static Logger log = LoggerFactory.getLogger( TestDatasetWithBatchProducer.class );
    private Dataset dataset;
    
    static {
        SPEC = StrUtils.strjoinNL(
                    "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ",
                    "prefix ja:   <http://jena.hpl.hp.com/2005/11/Assembler#> ",
                    "prefix tdb:  <http://jena.hpl.hp.com/2008/tdb#>",
                    "prefix text: <http://jena.apache.org/text#>",
                    "prefix :     <" + SPEC_BASE + ">",
                    "",
                    "[] ja:loadClass    \"org.apache.jena.query.text.TextQuery\" .",
                    "text:TextDataset      rdfs:subClassOf   ja:RDFDataset .",
                    "text:TextIndexLucene  rdfs:subClassOf   text:TextIndex .",

                    ":" + SPEC_ROOT_LOCAL,
                    "    a              text:TextDataset ;",
                    "    text:dataset   :dataset ;",
                    "    text:index     :indexLucene ;",
                    "    text:textDocProducer <java:" + TextDocProducerBatch.class.getCanonicalName() + ">",
                    "    .",
                    "",
                    ":dataset",
                    "    a               ja:RDFDataset ;",
                    "    ja:defaultGraph :graph ;",
                    ".",
                    ":graph",
                    "    a               ja:MemoryModel ;",
                    ".",
                    "",
                    ":indexLucene",
                    "    a text:TextIndexLucene ;",
                    "    text:directory <file:" + INDEX_PATH + "> ;",
                    "    text:entityMap :entMap ;",
                    "    .",
                    "",
                    ":entMap",
                    "    a text:EntityMap ;",
                    "    text:entityField      \"uri\" ;",
                    "    text:defaultField     \"label\" ;",
                    "    text:map (",
                    "         [ text:field \"label\" ; ",
                    "           text:predicate rdfs:label",
                    "         ]",
                    "         [ text:field \"comment\" ; text:predicate rdfs:comment ]",
                    "         ) ."
                    );
    }

    @BeforeEach
    public void init() {
        Reader reader = new StringReader(SPEC);
        Model specModel = ModelFactory.createDefaultModel();
        specModel.read(reader, "", "TURTLE");
        TextAssembler.init();
        deleteOldFiles();
        indexDir.mkdirs();
        Resource root = specModel.getResource(SPEC_ROOT_URI);
        dataset = (Dataset) Assembler.general.open(root);
    }
    
    @AfterEach
    public void closeDataset() {
    	if (dataset == null) {
    		log.warn("dataset was not set up");
    	} else {
    		dataset.close();
    	}
    }

    @AfterAll
    public static void deleteOldFiles() {
        if (indexDir.exists()) {
            emptyAndDeleteDirectory(indexDir);
        }
    }

    @Test
    public void testConfiguresBatchProducer() {
        DatasetGraphText dsgText = (DatasetGraphText)dataset.asDatasetGraph() ;
        assertTrue(dsgText.getMonitor() instanceof TextDocProducerBatch) ;
    }

    @Test
    public void testEmptyUpdate() {
        dataset.begin(ReadWrite.WRITE);
        dataset.getDefaultModel();
        dataset.commit();
        // we should not get any exceptions
    }

    @Test
    public void testSimpleQuery() {
        dataset.begin(ReadWrite.WRITE);
        Model model = dataset.getDefaultModel();
        Resource subject =
                model.createResource("http://example.com/testDatasetWithBatchProducer/testSimpleQuery")
                     .addProperty(RDFS.label, "label" )
                     .addProperty(RDFS.comment, "comment" );
        dataset.commit();
        // now lets query the index
        String queryString = StrUtils.strjoinNL(
                "PREFIX rdfs: <" + RDFS.getURI() + ">",
                "PREFIX text: <http://jena.apache.org/text#>",
                "SELECT * {",
                "	?s text:query 'label:label ' .",
                "}");
        Set<String> expected = new HashSet<String>();
        expected.add(subject.getURI());
        doTestQuery(dataset, "testConjunctiveQueryAcrossFields", queryString, expected, 1);
    }

    @Test
    public void testProducerSeparatesResources() {
        dataset.begin(ReadWrite.WRITE);
        Model model = dataset.getDefaultModel();
        Resource subject1 =
                model.createResource("http://example.com/testDatasetWithBatchProducer/testProducerSeparatesResources/1")
                     .addProperty(RDFS.label, "label1" )
                     .addProperty(RDFS.comment, "comment1" );
        model.createResource("http://example.com/testDatasetWithBatchProducer/testProducerSeparatesResources/1")
             .addProperty(RDFS.label, "label2" )
             .addProperty(RDFS.comment, "comment2" );
        dataset.commit();

        // now let's query the index
        String queryString = StrUtils.strjoinNL(
                "PREFIX rdfs: <" + RDFS.getURI() + ">",
                "PREFIX text: <http://jena.apache.org/text#>",
                "SELECT * {",
                "	?s text:query 'label:label1' .",
                "}");
        Set<String> expected = new HashSet<String>();
        expected.add(subject1.getURI());
        doTestQuery(dataset, "testConjunctiveQueryAcrossFields", queryString, expected, 1);
    }

    @Test
    public void testConjunctiveQueryAcrossFields() {
        dataset.begin(ReadWrite.WRITE);
        Model model = dataset.getDefaultModel();
        Resource subject =
                model.createResource("http://example.com/testDatasetWithBatchProducer/ConjunctiveQueryAccossFields")
                     .addProperty(RDFS.label, "label" )
                     .addProperty(RDFS.comment, "comment" );
        dataset.commit();
        // now lets query the index
        String queryString = StrUtils.strjoinNL(
                "PREFIX rdfs: <" + RDFS.getURI() + ">",
                "PREFIX text: <http://jena.apache.org/text#>",
                "SELECT * {",
                "	?s text:query 'label:label AND comment:comment' .",
                "}");
        Set<String> expected = new HashSet<String>();
        expected.add(subject.getURI());
        doTestQuery(dataset, "testConjunctiveQueryAcrossFields", queryString, expected, 1);
    }

    @Test
    public void testProducerMergesExistingProperties() {
        dataset.begin(ReadWrite.WRITE);
        Model model = dataset.getDefaultModel();
        Resource subject1 =
                model.createResource("http://example.com/testDatasetWithBatchProducer/ConjunctiveQueryAccossFields")
                     .addProperty(RDFS.label, "label1" );
        dataset.commit();

        dataset.begin(ReadWrite.WRITE);
        model.createResource("http://example.com/testDatasetWithBatchProducer/ConjunctiveQueryAccossFields")
             .addProperty(RDFS.label, "label2" );
        dataset.commit();

        dataset.begin(ReadWrite.WRITE);
        subject1.addProperty(RDFS.comment, "comment1" );
        dataset.commit();

        // now lets query the index
        String queryString = StrUtils.strjoinNL(
                "PREFIX rdfs: <" + RDFS.getURI() + ">",
                "PREFIX text: <http://jena.apache.org/text#>",
                "SELECT * {",
                "	?s text:query 'label:label1 AND comment:comment1' .",
                "}");
        Set<String> expected = new HashSet<String>();
        expected.add(subject1.getURI());
        doTestQuery(dataset, "testConjunctiveQueryAcrossFields", queryString, expected, 1);
    }

    private static void emptyAndDeleteDirectory(File dir) {
        File[] contents = dir.listFiles() ;
        if (contents != null) {
            for (File content : contents) {
                if (content.isDirectory()) {
                    emptyAndDeleteDirectory(content) ;
                } else {
                    content.delete() ;
                }
            }
        }
        dir.delete() ;
    }

    private static void doTestQuery(Dataset dataset, String label, String queryString, Set<String> expectedEntityURIs, int expectedNumResults) {
        Query query = QueryFactory.create(queryString) ;
        try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            dataset.begin(ReadWrite.READ);
            ResultSet results = qexec.execSelect();

            int count;
            for (count = 0; results.hasNext(); count++) {
                String entityURI = results.next().getResource("s").getURI();
                assertTrue(expectedEntityURIs.contains(entityURI), label + ": unexpected result: " + entityURI);
            }
            assertEquals(expectedNumResults, count, label);
        } finally {
            dataset.end();
        }
    }
}
