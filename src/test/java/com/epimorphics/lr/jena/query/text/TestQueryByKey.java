package com.epimorphics.lr.jena.query.text;

import java.io.File;
import java.io.IOException;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.text.Entity;
import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextIndexConfig;
import org.apache.jena.query.text.TextIndexLucene;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

public class TestQueryByKey {

	@Test public void testIt() throws IOException {
	
		Directory d = FSDirectory.open(new File("." + "/textIndex"));
		
		TextIndexConfig c = new TextIndexConfig(new EntityDefinition("uri", "text"));
		
		TextIndexLucene ix = new TextIndexLucene(d, c);
		
		Node property = NodeFactory.createURI("eh:/P");
		
		Entity e = new Entity(property.getURI(), "what has it got in its pockets");
		
		System.err.println(ix.query(property, "*"));
		
		ix.addEntity(e);
		
		System.err.println(ix.query(property, "*"));		
		
		ix.close();
	}
}
