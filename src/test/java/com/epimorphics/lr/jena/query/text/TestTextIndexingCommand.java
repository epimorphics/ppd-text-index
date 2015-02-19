package com.epimorphics.lr.jena.query.text;

import lr.textindexer;

import org.junit.Ignore;
import org.junit.Test;

public class TestTextIndexingCommand {

	@Ignore @Test public void testTextIndexer() {
		textindexer.main("--desc", "src/test/resources/config-ppd-text.ttl");
	}
}
