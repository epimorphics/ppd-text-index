package com.epimorphics.lr.jena.threadtesting;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.text.Entity;
import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextIndexConfig;
import org.apache.jena.query.text.TextIndexLucene;
import org.apache.jena.sparql.core.QuadAction;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.vocabulary.RDFS;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Ignore;
import org.junit.Test;

import com.epimorphics.lr.jena.query.text.BatchState.Mode;
import com.epimorphics.lr.jena.query.text.ExtendedEntity;
import com.epimorphics.lr.jena.query.text.TextDocProducerBatch;

public class TestBatchBatching {

	static class TextIndexIntercept extends TextIndexLucene {

		public List<Entity> entityHistory = new ArrayList<Entity>();
		public List<String> modeHistory = new ArrayList<String>();
		
		public void clearHistory() {
			entityHistory.clear();
			modeHistory.clear();
		}
		
		public TextIndexIntercept(Directory text_dir, TextIndexConfig config) {
			super(text_dir, config);
		}

	    public void addEntity(Entity entity) {
//	    	System.err.println(">> add " + entity);
	    	entityHistory.add(entity);
	    	modeHistory.add("add");
	    	super.addEntity(entity);
	    }
	    public void updateEntity(Entity entity) {
//	    	System.err.println(">> update " + entity);
	    	entityHistory.add(entity);
	    	modeHistory.add("update");
	    	super.updateEntity(entity);
	    }
	    
	    public void deleteEntity(Entity entity) {
	    	System.err.println(">> delete " + entity);
	    	entityHistory.add(entity);
	    	modeHistory.add("delete");
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
	
	
	// Unignoring requires creating ./DATASET/TDB.
	@Test @Ignore public void testBatching() throws IOException {
		String root = "./DATASET";
		String tdb_dir = root + "/TDB";
		Directory text_dir = FSDirectory.open(new File(root + "/textIndex"));
		EntityDefinition entDef = new EntityDefinition("uri", "text", RDFS.label);
		
		entDef.set("Q",  Q);
		
		Dataset base = TDBFactory.createDataset(tdb_dir); 
		TextIndexConfig config = new TextIndexConfig(entDef);
		
		TextIndexIntercept index = new TextIndexIntercept(text_dir, config) ;
	
		TextDocProducerBatch b = new TextDocProducerBatch(base.asDatasetGraph(), index);
		
		//////////////////////////////////////////////////
		
		base.begin(ReadWrite.WRITE);
		Graph g = base.getDefaultModel().getGraph();
		List<Triple> content = g.find(Node.ANY, Node.ANY, Node.ANY).toList();
		for (Triple c: content) g.delete(c);
		base.commit();
		base.end();
		
		///////////////////////
		
		index.clearHistory();
		base.begin(ReadWrite.WRITE);
		b.start();
		b.change(QuadAction.ADD, G, S1, P, O1);
		b.change(QuadAction.ADD, G, S1, Q, O2);
		b.change(QuadAction.ADD, G, S2, P, O2);
		b.change(QuadAction.ADD, G, S2, Q, O1);
		b.finish();
		base.commit();
		base.end();

		ExtendedEntity A = new ExtendedEntity(entDef, Mode.ADD, G, S1); 
		A.addProperty(entDef, P, O1);
		A.addProperty(entDef, Q, O2);
		
		ExtendedEntity B = new ExtendedEntity(entDef, Mode.ADD, G, S2);
		B.addProperty(entDef, P, O2);
		B.addProperty(entDef, Q, O1);
		
		assertEquals(list("update", "update"), index.modeHistory);
		assertEquals(list(A, B), index.entityHistory);

		//////////////////////////////////////////////
		
		base.begin(ReadWrite.WRITE);
		g.add(new Triple(S2, Q, O2));
		base.commit();
		base.end();
		
		index.clearHistory();
		base.begin(ReadWrite.WRITE);
		b.start();
		b.change(QuadAction.ADD, G, S1, P, O1);
		b.change(QuadAction.DELETE, G, S2, Q, O2);
		b.finish();
		base.commit();
		base.end();

		ExtendedEntity C = new ExtendedEntity(entDef, Mode.ADD, G, S1); 
		C.addProperty(entDef, P, O1);

		ExtendedEntity D = new ExtendedEntity(entDef, Mode.DELETE, G, S2); 
		D.addProperty(entDef, Q, O2);
		
		assertEquals(list("update", "update"), index.modeHistory);
		assertEquals(list(C, D), index.entityHistory);
		
	}

	private <T> List<T> list(T... args) {
		return Arrays.asList(args);
	}
	
}
