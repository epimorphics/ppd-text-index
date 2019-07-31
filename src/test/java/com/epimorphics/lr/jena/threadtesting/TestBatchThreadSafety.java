package com.epimorphics.lr.jena.threadtesting;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.query.text.Entity;
import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextHit;
import org.apache.jena.query.text.TextIndex;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.shared.Lock;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.QuadAction;
import org.apache.jena.sparql.util.Context;
import org.junit.Ignore;
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
//			System.err.println(">> add entity " + entity);
		}

		@Override public void updateEntity(Entity entity) {
//			System.err.println(">> update entity " + entity);
		}

		@Override public void deleteEntity(Entity entity) {
			System.err.println(">> update delete " + entity);
			// TODO Auto-generated method stub
		}

		@Override public Map<String, Node> get(String uri) {
			// TODO Auto-generated method stub
			return null;
		}

//		@Override public List<TextHit> query(Node property, String qs, int limit) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override public List<TextHit> query(Node property, String qs) {
//			// TODO Auto-generated method stub
//			return null;
//		}

		
		static final EntityDefinition ed = new EntityDefinition
			("URI", "fieldP", ResourceFactory.createResource("eh:/P"));
			;
		
		@Override public EntityDefinition getDocDef() {
			return ed;
		}

		@Override
		public List<TextHit> query(Node arg0, String arg1, String arg2, String arg3) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<TextHit> query(Node arg0, String arg1, String arg2, String arg3, int arg4) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<TextHit> query(Node arg0, String arg1, String arg2, String arg3, int arg4, String arg5) {
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
			return new Iterator<Quad>() {

				@Override
				public boolean hasNext() {
					// TODO Auto-generated method stub
					return false;
				}

				@Override
				public Quad next() {
					// TODO Auto-generated method stub
					return null;
				}};
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

		@Override
		public void begin(TxnType type) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void begin(ReadWrite readWrite) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean promote(Promote mode) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void commit() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void abort() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void end() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public ReadWrite transactionMode() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public TxnType transactionType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isInTransaction() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Graph getUnionGraph() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean supportsTransactions() {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
	
	static final Node G = NodeFactory.createURI("eh:/G");
	static final Node S1 = NodeFactory.createURI("eh:/S1");
	static final Node S2 = NodeFactory.createURI("eh:/S2");
	static final Node P = NodeFactory.createURI("eh:/P");
	static final Node O1 = NodeFactory.createLiteral("O1", (String) null);
	static final Node O2 = NodeFactory.createLiteral("O2", (String) null);
	static final Node O3 = NodeFactory.createLiteral("O3", (String) null);
	static final Node O4 = NodeFactory.createLiteral("O4", (String) null);
	static final Node O5 = NodeFactory.createLiteral("O5", (String) null);
	
	static final int THREADS = 20;
	static final int TIMES = 100000;
	static final int OBJECTS = 20;
	static final int SUBJECTS = 200;
	
	static final class ThreadVar {
		Thread thread;
	}
	
	boolean retain = false;
	
	@Test @Ignore public void testDoesNotRetainGarbage() throws InterruptedException {
		TextIndex ti = new TextIndexTesting();
		DatasetGraph dsg = new DatasetGraphTesting();
		final TextDocProducerBatch b = new TextDocProducerBatch(dsg, ti);
		final List<Thread> threads = new ArrayList<Thread>();
		
		final Random r = new Random();
//		for (long j = 0; j < 100000000; j += 1) r.nextInt();
		
		showMemory("before thread creation");
		
		for (int i = 0; i < THREADS; i += 1) {
			final int[] ii = new int[]{i};
			final ThreadVar tv = new ThreadVar();
			final Thread t = new Thread(new Runnable() {

				@Override public void run() {
					b.start();
					if (ii[0] == 0) {
//						System.err.println(">> updates");
						b.change(QuadAction.ADD, G, S1, P, O1);
						b.change(QuadAction.ADD, G, S1, P, O2);
						b.change(QuadAction.ADD, G, S2, P, O3);
						b.change(QuadAction.ADD, G, S2, P, O4);
						b.change(QuadAction.ADD, G, S2, P, O5);
						b.change(QuadAction.ADD, G, S2, P, O5);
					//
						for (int si = 0; si < SUBJECTS; si += 1) {	
							Node s = NodeFactory.createURI("eh:/S" + si);				
							for (int oi = 0; oi < OBJECTS; oi += 1) {
								Node jo = NodeFactory.createLiteral("O" + oi, (String) null);
								b.change(QuadAction.ADD, G, s, P, jo);
							}
						}
					} else {
//						System.err.println("Pretend query");
					}
					b.finish();
					synchronized (threads) { threads.add(tv.thread); }
				}
				
			});
			tv.thread = t;
			threads.add(t);	
		}
		
		List<Thread> allThreads = new ArrayList<Thread>(threads);
		
		
		for (int i = 0; i < TIMES; i += 1) synchronized (threads) {
			int w = r.nextInt(threads.size());
			Thread t = threads.remove(w);
			t.run();
		}
		
		for (int i = 0; i < THREADS; i += 1) {
			allThreads.get(i).join();
		}
		
		System.err.println(">> retained " + threads.size() + " threads.");
		
		showMemory("after all threads joined");
		
		if (retain) {
			System.err.println("");
			System.err.println(ti);
			System.err.println(dsg);
			System.err.println(b);
			System.err.println(threads);
		}
//		
////		System.err.println(">> state: " + b.exposeBatchState());
	}

	private void showMemory(String title) {
		severalGCs(); 
		long used = 
			Runtime.getRuntime().totalMemory() 
			- Runtime.getRuntime().freeMemory()
			; 
		System.err.println("");
		System.err.println(title);
		System.err.println("total memory: " + Runtime.getRuntime().totalMemory() );
		System.err.println("free  memory: " + Runtime.getRuntime().freeMemory() );
		System.err.println("used memory:  " + used);
	}


	private void severalGCs() {
		System.gc(); 
		System.gc(); 
		System.gc(); 
		System.gc();
	}
}
