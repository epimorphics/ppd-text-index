package com.epimorphics.lr.jena.query.text;

import java.io.IOException;
import java.nio.file.FileSystems;

import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextIndexConfig;
import org.apache.jena.query.text.TextIndexLucene;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

public class TestQueryByKey {

	@Test public void testIt() throws IOException {
	
		Directory d = FSDirectory.open(FileSystems.getDefault().getPath("." + "/textIndex"));
		
		TextIndexConfig c = new TextIndexConfig(new EntityDefinition("uri", "text"));
		
		TextIndexLucene ix = new TextIndexLucene(d, c);
		
		
//		Node property = NodeFactory.createURI("eh:/P");
//		
//		Entity e = new Entity(property.getURI(), "what has it got in its pockets");
//		
//		System.err.println(ix.query(property, "*"));
//		
//		ix.addEntity(e);
//		
//		System.err.println(ix.query(property, "*"));		
		ix.commit();
		ix.close();
		
	}
}
