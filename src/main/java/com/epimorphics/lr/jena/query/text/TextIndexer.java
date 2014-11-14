package com.epimorphics.lr.jena.query.text;

import java.util.*;

import org.apache.jena.query.text.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.sparql.core.Quad;

public class TextIndexer
{

    static Logger log = LoggerFactory.getLogger( TextIndexer.class );

    protected Dataset dataset = null;
    protected DatasetGraphText datasetGraph = null;
    protected TextIndex textIndex = null;
    protected EntityDefinition entityDefinition;

    public TextIndexer( Dataset dataset ) {
        this.dataset = dataset;
        this.datasetGraph = (DatasetGraphText) dataset.asDatasetGraph();
        this.textIndex = datasetGraph.getTextIndex();
        if (textIndex == null)
            throw new TextIndexException( "Dataset has no text index" );
        entityDefinition = textIndex.getDocDef();
    }

    public TextIndexer( DatasetGraphText datasetGraph ) {
        this.datasetGraph = datasetGraph;
        this.dataset = DatasetFactory.create( datasetGraph );
        this.textIndex = datasetGraph.getTextIndex();
        entityDefinition = textIndex.getDocDef();
    }

    public void index( ProgressMonitor progressMonitor ) {

        textIndex.startIndexing();

        // this is a bit crude and does not scale
        // options include
        // - replace add with update - will update same resource multiple times
        // - presort the quads and then do add - but this slower than current
        // code

        Set<GSPair> processed = new HashSet<GSPair>();

        Iterator<Quad> quadIter = datasetGraph.find( Node.ANY, Node.ANY, Node.ANY, Node.ANY );
        for (; quadIter.hasNext();) {
            Quad quad = quadIter.next();
            GSPair gs = new GSPair( quad.getGraph(), quad.getSubject() );
            if (processed.contains( gs ))
                continue; // already done this one.
            processed.add( gs );
            ExtendedEntity entity = new ExtendedEntity( entityDefinition, quad.getGraph(), quad.getSubject() );
            int count = addFieldsToEntity( entity, quad.getGraph(), quad.getSubject() );
            if (count > 0) {
                textIndex.addEntity( entity );
                if (progressMonitor != null) {
                    progressMonitor.progressByN( count );
                }
            }
        }
        textIndex.finishIndexing();
    }

    // find all the properties of the of the subject in the graph
    // that are indexed and add them to the entity

    private int addFieldsToEntity( ExtendedEntity entity, Node graph, Node subject ) {
        int count = 0;
        Iterator<Quad> iter = datasetGraph.find( graph, subject, null, null );
        for (; iter.hasNext();) {
            Quad quad = iter.next();
            boolean added = entity.addProperty( entityDefinition, quad.getPredicate(), quad.getObject() );
            if (added) {
                count++;
            }
        }
        return count;
    }

    // TODO remove
//    private List<Node> getIndexedProperties() {
//        List<Node> result = new ArrayList<Node>();
//        for (String f : entityDefinition.fields()) {
//            for (Node p : entityDefinition.getPredicates( f ))
//                result.add( p );
//        }
//        return result;
//    }

}
