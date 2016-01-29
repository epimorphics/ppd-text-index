package com.epimorphics.lr.jena.query.text;

import java.util.*;

import org.apache.jena.query.text.*;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;

import com.epimorphics.lr.jena.query.text.BatchState.Mode;

/**
 * Supervisor object for the process of creating a new text index for
 * a given dataset or graph.
 */
public class TextIndexer
{
    /** The dataset graph we are processing */
    private DatasetGraphText datasetGraph;

    /**
     * Create an indexer for the given dataset
     * @param dataset A dataset to index
     * @throws TextIndexException if the dataset does not have an associated text index
     */
    public TextIndexer( Dataset dataset ) {
        if (!(dataset.asDatasetGraph() instanceof DatasetGraphText)) {
            throw new TextIndexException( "Cannot index a Dataset that does not have a text index" );
        }
        if (this.datasetGraph.getTextIndex() == null) {
            throw new TextIndexException( "Dataset has no text index" );
        }

        this.datasetGraph = (DatasetGraphText) dataset.asDatasetGraph();
    }

    /**
     * Create a text index for the given dataset graph
     * @param datasetGraph
     */
    public TextIndexer( DatasetGraphText datasetGraph ) {
        this.datasetGraph = datasetGraph;
    }

    /**
     * Perform the indexing of the graph/subject pairs in the {@link DatasetGraph}
     * that this indexer was constructed with.
     *
     * @param pm If non-null, report incremental progress
     */
    public void index( ProgressMonitor pm ) {
        TextIndex textIndex = currentTextIndex( datasetGraph );
        EntityDefinition entityDefinition = textIndex.getDocDef();

        // textIndex.startIndexing();
        		
        Node G = NodeFactory.createURI("eh:/G");
        Node S = NodeFactory.createURI("eh:/S");
        
        Iterator<Quad> quadIter = datasetGraph.find( Node.ANY, Node.ANY, Node.ANY, Node.ANY );
        while (quadIter.hasNext()) {
            Quad quad = quadIter.next();
            Node g = quad.getGraph();
            Node s = quad.getSubject();
            if (g.equals(G) && s.equals(S)) {
            	// already processed this local subject
            } else {
            	// new graph/subject pair to process
            	G = g;
            	S = s;
            	indexSubject( entityDefinition, g, s, textIndex, pm );
            }
            
        }

        // textIndex.finishIndexing();
    }
    
    public void alternative_index_think_about_it( ProgressMonitor pm ) {
        TextIndex textIndex = currentTextIndex( datasetGraph );
        EntityDefinition ed = textIndex.getDocDef();

        // textIndex.startIndexing();
        		
        Node G = NodeFactory.createURI("eh:/G");
        Node S = NodeFactory.createURI("eh:/S");
        
        ExtendedEntity e = new ExtendedEntity(ed, Mode.ADD, G, S);
        int count = 0;
        
        Iterator<Quad> quadIter = datasetGraph.find( Node.ANY, Node.ANY, Node.ANY, Node.ANY );
        
        /*
            Iterate through all the quads. Assume that any given (Graph, Subject) properties
            appear as a complete contiguous run. That means that we can accumulate a
            new entity while we are reading those quads. When the (Graph, Subject) pair changes,
            we can add the entity to the index and set up a new state with the new
            graph and subject, a count of zero, and a new entity to update.
         
        */
        while (quadIter.hasNext()) {
            Quad quad = quadIter.next();
            Node g = quad.getGraph(), s = quad.getSubject();
            Node p = quad.getPredicate(), o = quad.getObject();
            if (g.equals(G) && s.equals(S)) {
				if (e.addProperty(ed, p, o)) {
            		count += 1; 
            	}
            } else {
            	if (count > 0) {
            		textIndex.addEntity(e);
            		if (pm != null) pm.progressByN(count);
            	}
            	G = g;
            	S = s;
            	count = 0;
                e = new ExtendedEntity(ed, Mode.ADD, G, S);
                e.addProperty(ed, p, o);
            }
            
        }

        // textIndex.finishIndexing();
    }

    /**
     * @return The current text index
     * @param g The current dataset graph
     */
    protected TextIndex currentTextIndex( DatasetGraphText g ) {
        return g.getTextIndex();
    }

    /**
     * Create index entries for the properties of subject s in graph g, then mark
     * that pair as seen.
     *
     * @param def The text entity definition
     * @param g The current graph
     * @param s The current subject
     * @param index The current text index
     * @param pm Optional progress monitor
     */
    protected void indexSubject( EntityDefinition def, Node g, Node s, TextIndex index, ProgressMonitor pm ) {
        ExtendedEntity entity = new ExtendedEntity( def, Mode.ADD, g, s );
        int count = 0;
        // see(g,s);
        for (Iterator<Quad> i = datasetGraph.find( g, s, null, null ); i.hasNext();) {
            Quad quad = i.next();
            if (entity.addProperty( def, quad.getPredicate(), quad.getObject() )) {
                count++;
            }
        }

        if (count > 0) {
            index.addEntity( entity );
            if (pm != null) {
                pm.progressByN( count );
            }
        }
    }

}
