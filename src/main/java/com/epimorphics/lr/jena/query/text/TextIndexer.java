package com.epimorphics.lr.jena.query.text;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jena.query.text.DatasetGraphText;
import org.apache.jena.query.text.Entity;
import org.apache.jena.query.text.EntityDefinition;
import org.apache.jena.query.text.TextIndex;
import org.apache.jena.query.text.TextIndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.sparql.core.Quad;

public class TextIndexer {
	
	private static Logger      log          = LoggerFactory.getLogger(TextIndexer.class) ;
	
	protected Dataset          dataset      = null ;
	protected DatasetGraphText datasetGraph = null ;
    protected TextIndex        textIndex    = null ;
    protected EntityDefinition entityDefinition ;
	
    public TextIndexer(Dataset dataset) {
		this.dataset = dataset;
		this.datasetGraph = (DatasetGraphText) dataset.asDatasetGraph() ;
		this.textIndex = datasetGraph.getTextIndex() ;
	        if (textIndex == null)
	            throw new TextIndexException("Dataset has no text index") ;
	    entityDefinition = textIndex.getDocDef() ;
	}
    
    public TextIndexer(DatasetGraphText datasetGraph) {
		this.datasetGraph = datasetGraph;
		this.dataset = DatasetFactory.create(datasetGraph);
		this.textIndex = datasetGraph.getTextIndex() ;
	    entityDefinition = textIndex.getDocDef() ;
	}
	
	public void index(ProgressMonitor progressMonitor) {	
		
        textIndex.startIndexing() ;

        // this is a bit crude and does not scale
        // options include
        //	- replace add with update - will update same resource multiple times
        //  - presort the quads and then do add - but this slower than current code
        
        Set<GSPair> processed = new HashSet<GSPair>();
                
        Iterator<Quad> quadIter = datasetGraph.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY) ;
        for ( ; quadIter.hasNext() ; ) {
            Quad quad = quadIter.next() ;
            GSPair gs = new GSPair(quad.getGraph(), quad.getSubject());
            if (processed.contains(gs))  continue ;  // already done this one.
            processed.add(gs);
            Entity entity = TextQueryFuncs.entity(entityDefinition, quad.getGraph(), quad.getSubject()) ;
            int count = addFieldsToEntity(entity, quad.getGraph(), quad.getSubject());
            if (count > 0) {
                textIndex.addEntity(entity) ;
                if (progressMonitor != null) {
                    progressMonitor.progressByN(count);                    
                }
            }
        }
        textIndex.finishIndexing();
    }
	
	// find all the properties of the of the subject in the graph 
	// that are indexed and add them to the entity
	
	private int addFieldsToEntity(Entity entity, Node graph, Node subject) {
		int count = 0;		
		Iterator<Quad> iter = datasetGraph.find(graph, subject, null, null);
		for ( ; iter.hasNext(); ) {
			Quad quad = iter.next();
			boolean added = TextQueryFuncs.addPropertyToEntity(entityDefinition, entity, quad.getPredicate(), quad.getObject());
			if (added)  {
				count++;
			}
		}		
		return count;	
	}

    private List<Node> getIndexedProperties() {
        List<Node> result = new ArrayList<Node>() ;
        for (String f : entityDefinition.fields()) {
            for ( Node p : entityDefinition.getPredicates(f) )
                result.add(p) ;
        }
        return result ;
    }
    /**
     * A class representing the md5 hash of a pair of nodes, one a graph node, the other a subject.
     * 
     * The graph node can be null, the subject node can't,
     *
     */
    protected static class GSPair {
    	
    	private static MessageDigest  md = null ;
    	static {
    		try {
				md = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				// it won't happen, but if it does it will fail soon anyway
			}
    	}
    	private long l1, l2;
    	
    	GSPair(Node graph, Node subject) {
    		byte[] bytes = md.digest((graph.toString() + subject.toString()).getBytes());
    		ByteBuffer bb = ByteBuffer.allocate(16);;
    		bb.put(bytes);
    		l1 = bb.getLong(0);
    		l2 = bb.getLong(8);
    	}
    	
    	@Override
    	public boolean equals(Object object) {
    		if ( object instanceof GSPair ) {
    			GSPair gs = (GSPair) object ;
    			return gs.l1 == l1 && gs.l2 == l2;
    		} else {
    			return false;
    		}
    	}
    	
    	@Override
    	public int hashCode() {
    		return (int) l1 & 0xFFFFFFFF;
    	}
    	
    }

    // TDBLoader has a similar progress monitor
    // Not used here to avoid making ARQ dependent on TDB
    // So potential to rationalise and put progress monitor in a common
    // utility class
     public static class ProgressMonitor {
        String progressMessage ;
        long   startTime ;
        long   progressCount ;
        long   intervalStartTime ;
        long   progressAtStartOfInterval ;
        long   reportingInterval = 10000 ; // milliseconds

        public ProgressMonitor(String progressMessage) {
            this.progressMessage = progressMessage ;
            start() ; // in case start not called
        }

        void start() {
            startTime = System.currentTimeMillis() ;
            progressCount = 0L ;
            startInterval() ;
        }

        private void startInterval() {
            intervalStartTime = System.currentTimeMillis() ;
            progressAtStartOfInterval = progressCount ;
        }

        void progressByOne() {
        	progressByN(1);
        }


        void progressByN(int n) {
            progressCount+= n;
            long now = System.currentTimeMillis() ;
            if (reportDue(now)) {
                report(now) ;
                startInterval() ;
            }
        }

        boolean reportDue(long now) {
            return now - intervalStartTime >= reportingInterval ;
        }

        private void report(long now) {
            long progressThisInterval = progressCount - progressAtStartOfInterval ;
            long intervalDuration = now - intervalStartTime ;
            long intervalSeconds = intervalDuration / 1000;
            if (intervalSeconds == 0) intervalSeconds = 1;
            long overallDuration = now - startTime ;
            String message = progressCount + " (" + progressThisInterval / intervalSeconds + " per second) "
                             + progressMessage + " (" + progressCount / Math.max(overallDuration / 1000, 1)
                             + " per second overall)" ;
            log.info(message) ;
        }

        public void close() {
            long overallDuration = System.currentTimeMillis() - startTime ;
            String message = progressCount + " (" + progressCount / Math.max(overallDuration / 1000, 1)
                             + " per second) " + progressMessage ;
            log.info(message) ;
        }
    }

}
