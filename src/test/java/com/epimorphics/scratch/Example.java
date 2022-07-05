package com.epimorphics.scratch;

import java.util.Iterator;

import org.apache.jena.query.text.TextDocProducerTriples;
import org.apache.jena.query.text.TextIndex;

import org.apache.jena.graph.Node;
import org.apache.jena.query.text.changes.TextQuadAction;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.QuadAction;

public class Example extends TextDocProducerTriples {

	final DatasetGraph dg;
	
	public Example(DatasetGraph dg, TextIndex indexer) {
		super(indexer);
		this.dg = dg;
	}
	
	public void change(TextQuadAction qaction, Node g, Node s, Node p, Node o) {
		if (qaction == TextQuadAction.ADD) {
			if (alreadyHasOne(s, p)) super.change(qaction, g, s, p, o);
		}
	}

	private boolean alreadyHasOne(Node s, Node p) {
		int count = 0;
		Iterator<Quad> quads = dg.find( null, s, p, null );
		while (quads.hasNext()) { quads.next(); count += 1; }
		return count > 1;
	}

}
