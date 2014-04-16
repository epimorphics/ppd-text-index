package com.epimorphics.lr.jena.query.text;



import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.query.text.TextQuery;
import org.apache.jena.query.text.assembler.TextAssembler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * This class defines a setup configuration for a dataset that specifies a non-default document producer.
 */
public class TestDatasetWithBatchProducer {
	private static final String INDEX_PATH = "target/test/TestDatasetWithLuceneIndex";
	private static final File indexDir = new File(INDEX_PATH);
	
	private static final String SPEC_BASE = "http://example.org/spec#";
	private static final String SPEC_ROOT_LOCAL = "lucene_text_dataset";
	private static final String SPEC_ROOT_URI = SPEC_BASE + SPEC_ROOT_LOCAL;
	private static final String SPEC;
	private static Dataset dataset;
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
				    "    text:docProducer '" + TextDocProducerBatch.class.getCanonicalName() + "'",
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
	
	public static void init() {
		Reader reader = new StringReader(SPEC);
		Model specModel = ModelFactory.createDefaultModel();
		specModel.read(reader, "", "TURTLE");
		TextAssembler.init();			
		deleteOldFiles();
		indexDir.mkdirs();
		Resource root = specModel.getResource(SPEC_ROOT_URI);
		dataset = (Dataset) Assembler.general.open(root);
	}
	
	
	public static void deleteOldFiles() {
		if (indexDir.exists()) emptyAndDeleteDirectory(indexDir);
	}	

	@BeforeClass public static void beforeClass() {
		init();
	}	
	
	@AfterClass public static void afterClass() {
		deleteOldFiles();
	}
	
	@Test
	public void testConfiguresBatchProducer() {
		assertTrue(dataset.getContext().get(TextQuery.docProducer) instanceof TextDocProducerBatch );
	}
	
	@Test public void testSimpleQuery() {
		init();
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
	
	@Test public void testProducerSeparatesResources() {
		init();
		dataset.begin(ReadWrite.WRITE);
		Model model = dataset.getDefaultModel();
		Resource subject1 = 
				model.createResource("http://example.com/testDatasetWithBatchProducer/testProducerSeparatesResources/1")
		             .addProperty(RDFS.label, "label1" )
		             .addProperty(RDFS.comment, "comment1" );
		Resource subject2 = 
				model.createResource("http://example.com/testDatasetWithBatchProducer/testProducerSeparatesResources/1")
		             .addProperty(RDFS.label, "label2" )
		             .addProperty(RDFS.comment, "comment2" );
		dataset.commit();
		// now lets query the index
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
	
	@Test public void testConjunctiveQueryAcrossFields() {
		init();
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
	
	@Test public void testProducerMergesExistingProperties() {
		init();
		dataset.begin(ReadWrite.WRITE);
		Model model = dataset.getDefaultModel();
		Resource subject1 = 
				model.createResource("http://example.com/testDatasetWithBatchProducer/ConjunctiveQueryAccossFields")
		             .addProperty(RDFS.label, "label1" );
		dataset.commit();
		
		dataset.begin(ReadWrite.WRITE);
		Resource subject2 = 
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
	
	public static void doTestQuery(Dataset dataset, String label, String queryString, Set<String> expectedEntityURIs, int expectedNumResults) {
		Query query = QueryFactory.create(queryString) ;
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset) ;
		try {
			dataset.begin(ReadWrite.READ);
		    ResultSet results = qexec.execSelect() ;
		    
		    int count;
		    for (count=0; results.hasNext(); count++) {
		    	String entityURI = results.next().getResource("s").getURI();
		        assertTrue(label + ": unexpected result: " + entityURI, expectedEntityURIs.contains(entityURI));
		    }
		    assertEquals(label, expectedNumResults, count);
		} finally { qexec.close() ; dataset.end() ; }		
	}
}
