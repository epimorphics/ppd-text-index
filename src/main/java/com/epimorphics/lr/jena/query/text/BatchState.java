package com.epimorphics.lr.jena.query.text;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
	Type for thread-local state used by batching.
*/
public class BatchState {

    List<Quad> queue = new ArrayList<Quad>();

    /* If true, the queue contains quads to add, otherwise quads to remove */
    boolean queueAdd = true;

    /* The subject currently being processed, to detect subject change boundaries */
    Node currentSubject;

	public void start() {
		clear();
	}
	
	public void finish() {
		clear();
	}
	
	protected void clear() {
		queue.clear();
		queueAdd = true;
		currentSubject = null;
	}
	
	/** clear the quere and set current subject and add mode */
	protected void reset(boolean queueAdd, Node currentSubject) {
		queue.clear();
		this.queueAdd = queueAdd;
		this.currentSubject = currentSubject;
	}

	/** true iff there is a subject and some quads queued up. */
	public boolean hasBatch() {
		// TODO Auto-generated method stub
		return currentSubject != null && !queue.isEmpty();
	}
    
}