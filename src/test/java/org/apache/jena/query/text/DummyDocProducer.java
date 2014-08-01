package org.apache.jena.query.text;

import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextDocProducer;
import org.apache.jena.query.text.TextIndex;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.QuadAction;

// dummy doc producer class for testing
public class DummyDocProducer implements TextDocProducer {

	int count;
	
	@Override
	public void start() {
		count = 0;		
	}

	@Override
	public void change(QuadAction qaction, Node g, Node s, Node p, Node o) {
		count++;
	}

	@Override
	public void finish() {}
	
	public int getNumQuads() { return count ;}

	@Override
	public void setDefn(EntityDefinition defn) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setTextIndex(TextIndex textIndex) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDatasetGraph(DatasetGraph dsg) {
		// TODO Auto-generated method stub
		
	}

}
