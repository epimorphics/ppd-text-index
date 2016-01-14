package com.epimorphics.lr.jena.threadtesting;

import java.io.File;
import java.io.IOException;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.text.Entity;
import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextIndex;
import org.apache.jena.query.text.TextIndexConfig;
import org.apache.jena.query.text.TextIndexLucene;
import org.apache.jena.sparql.core.QuadAction;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.vocabulary.RDFS;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Ignore;
import org.junit.Test;

import com.epimorphics.lr.jena.query.text.TextDocProducerBatch;

public class TestBatchBatching {

	static class TextIndexIntercept extends TextIndexLucene {

		public TextIndexIntercept(Directory text_dir, TextIndexConfig config) {
			super(text_dir, config);
		}

	    public void addEntity(Entity entity) {
	    	System.err.println(">> add " + entity);
	    	super.addEntity(entity);
	    }
	    public void updateEntity(Entity entity) {
	    	System.err.println(">> update " + entity);
	    	super.updateEntity(entity);
	    }
	    
	    public void deleteEntity(Entity entity) {
	    	System.err.println(">> delete " + entity);
	    	super.deleteEntity(entity);
	    }
	}

	static final Node G  = NodeFactory.createURI("eh:/G");
	static final Node S1 = NodeFactory.createURI("eh:/S1");
	static final Node S2 = NodeFactory.createURI("eh:/S2");
	static final Node P  = RDFS.label.asNode(); 
	static final Node Q  = NodeFactory.createURI("eh:/Q"); 
	static final Node O1 = NodeFactory.createLiteral("O1", (String) null);
	static final Node O2 = NodeFactory.createLiteral("O2", (String) null);
	static final Node O3 = NodeFactory.createLiteral("O3", (String) null);
	static final Node O4 = NodeFactory.createLiteral("O4", (String) null);
	static final Node O5 = NodeFactory.createLiteral("O5", (String) null);
	
	@Test @Ignore public void testBatching() throws IOException {
		String root = "./DATASET";
		String tdb_dir = root + "/TDB";
		Directory text_dir = FSDirectory.open(new File(root + "/textIndex"));
		EntityDefinition entDef = new EntityDefinition("uri", "text", RDFS.label);
		
		entDef.set("Q",  Q);
		
		Dataset base = TDBFactory.createDataset(tdb_dir); 
		TextIndexConfig config = new TextIndexConfig(entDef);
		
		TextIndex index = new TextIndexIntercept(text_dir, config) ;
	
		TextDocProducerBatch b = new TextDocProducerBatch(base.asDatasetGraph(), index);
	
		b.start();
		b.change(QuadAction.ADD, G, S1, P, O1);
		b.change(QuadAction.ADD, G, S1, Q, O2);
		b.change(QuadAction.ADD, G, S2, P, O2);
		b.change(QuadAction.ADD, G, S2, P, O1);
		b.change(QuadAction.ADD, G, S1, P, O1);
		b.finish();
	}
	
}
