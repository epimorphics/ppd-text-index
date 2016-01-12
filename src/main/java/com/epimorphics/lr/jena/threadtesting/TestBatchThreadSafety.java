package com.epimorphics.lr.jena.threadtesting;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.text.Entity;
import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextHit;
import org.apache.jena.query.text.TextIndex;
import org.apache.jena.shared.Lock;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.QuadAction;
import org.apache.jena.sparql.util.Context;
import org.junit.Test;

import com.epimorphics.lr.jena.query.text.TextDocProducerBatch;

public class TestBatchThreadSafety {

	static class TextIndexTesting implements TextIndex {

		@Override public void close() {
			// TODO Auto-generated method stub
		}

		@Override public void prepareCommit() {
			// TODO Auto-generated method stub
		}

		@Override public void commit() {
			// TODO Auto-generated method stub
		}

		@Override public void rollback() {
			// TODO Auto-generated method stub
		}

		@Override public void addEntity(Entity entity) {
			// TODO Auto-generated method stub
		}

		@Override public void updateEntity(Entity entity) {
			// TODO Auto-generated method stub
		}

		@Override public void deleteEntity(Entity entity) {
			// TODO Auto-generated method stub
		}

		@Override public Map<String, Node> get(String uri) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public List<TextHit> query(Node property, String qs, int limit) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public List<TextHit> query(Node property, String qs) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public EntityDefinition getDocDef() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	static class DatasetGraphTesting implements DatasetGraph {

		@Override public Graph getDefaultGraph() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public Graph getGraph(Node graphNode) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public boolean containsGraph(Node graphNode) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override public void setDefaultGraph(Graph g) {
			// TODO Auto-generated method stub
			
		}

		@Override public void addGraph(Node graphName, Graph graph) {
			// TODO Auto-generated method stub
			
		}

		@Override public void removeGraph(Node graphName) {
			// TODO Auto-generated method stub
			
		}

		@Override public Iterator<Node> listGraphNodes() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public void add(Quad quad) {
			// TODO Auto-generated method stub
			
		}

		@Override public void delete(Quad quad) {
			// TODO Auto-generated method stub
			
		}

		@Override public void add(Node g, Node s, Node p, Node o) {
			// TODO Auto-generated method stub
			
		}

		@Override public void delete(Node g, Node s, Node p, Node o) {
			// TODO Auto-generated method stub
			
		}

		@Override public void deleteAny(Node g, Node s, Node p, Node o) {
			// TODO Auto-generated method stub
			
		}

		@Override public Iterator<Quad> find() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public Iterator<Quad> find(Quad quad) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public boolean contains(Node g, Node s, Node p, Node o) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override public boolean contains(Quad quad) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override public void clear() {
			// TODO Auto-generated method stub
			
		}

		@Override public boolean isEmpty() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override public Lock getLock() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public Context getContext() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override public long size() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override public void close() {
			// TODO Auto-generated method stub
		}
		
	}
	
	static final Node G = NodeFactory.createURI("eh:/G");
	static final Node S = NodeFactory.createURI("eh:/S");
	static final Node P = NodeFactory.createURI("eh:/P");
	static final Node O = NodeFactory.createURI("eh:/O");
	
	
	@Test public void testSafety() throws InterruptedException {
		TextIndex ti = new TextIndexTesting();
		DatasetGraph dsg = new DatasetGraphTesting();
		final TextDocProducerBatch b = new TextDocProducerBatch(dsg, ti);
		for (int i = 0; i < 10; i += 1) {
			System.gc(); 
			System.gc(); 
			long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory(); System.err.println(used);
			Thread t = new Thread(new Runnable() {

				@Override public void run() {
					b.start();
					b.change(QuadAction.ADD, G, S, P, O);
					b.finish();					
				}
				
			});
			t.start();
			t.join();
		}
		
		System.err.println(">> state: " + b.exposeBatchState());
		assertTrue(b.exposeBatchState().get() == null);
	}
}
