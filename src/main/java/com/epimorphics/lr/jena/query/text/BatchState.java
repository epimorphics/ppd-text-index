package com.epimorphics.lr.jena.query.text;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
	Type for thread-local state used by batching.
*/
public class BatchState {

	public static enum Mode {NONE, ADD, DELETE}
	
    List<Quad> queue = new ArrayList<Quad>();

    Mode queueMode = Mode.NONE;

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
		queueMode = Mode.NONE;
		currentSubject = null;
	}
	
	/** clear the queue and set current subject and add mode */
	protected void reset(Mode queueMode, Node currentSubject) {
		queue.clear();
		this.queueMode = queueMode;
		this.currentSubject = currentSubject;
	}

	/** true iff there is a subject and some quads queued up. */
	public boolean hasBatch() {
		return currentSubject != null && !queue.isEmpty();
	}
    
}