package edu.usc.cs.ir.cwork.solr;

import edu.usc.cs.ir.cwork.graph.Graph;
import edu.usc.cs.ir.cwork.graph.Vertex;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by tg on 11/5/15.
 */
public class GraphBuilder {

    public static final int INTERACTIVE_DELAY =  2000; //ms
    public static final Logger LOG = LoggerFactory.getLogger(GraphBuilder.class);
    public static final String ID_FIELD = "id";

    public Set<String> ignoreEdges = new HashSet<>();

    private Iterator<SolrDocument> docs;
    private String fieldName;

    public GraphBuilder(Iterator<SolrDocument> docs, String fieldName) {
        this.docs = docs;
        this.fieldName = fieldName;
    }

    public Set<Object> getEdges(Collection<Object> fieldValue) {
        Set<Object> result = new HashSet<>();
        for (Object val : fieldValue) {
            if (!ignoreEdges.contains(val)){
                result.add(val);
            }
        }
        return result;
    }

    public Graph build(){
        Map<Object, Set<Vertex>> edges = new HashMap<>();
        //Set<Vertex> allVertices = new HashSet<>();
        long count = 0;
        long st = System.currentTimeMillis();

        while (this.docs.hasNext()){
            SolrDocument next = docs.next();
            if (!next.containsKey(ID_FIELD)) {
                throw new IllegalStateException("No id field, " + next);
            }
            if (!next.containsKey(fieldName)) {
                throw new IllegalStateException("No  " + fieldName + " field, " + next);
            }
            String id = (String) next.getFieldValue(ID_FIELD);
            Collection<Object> values = next.getFieldValues(fieldName);
            Set<Object> thisEdges = getEdges(values);

            for (Object edge : thisEdges) {
                if (!edges.containsKey(edge)) {
                    edges.put(edge, new HashSet<>());
                }

                Vertex thisVertex = new Vertex(id);
                //connect this vertex with previous vertices
                Set<Vertex> vertices = edges.get(edge);
                vertices.forEach(v -> v.addUndirectedEdge(thisVertex));

                //its undirected, so no need to connect this side of vertex!!
                //just add
                vertices.add(thisVertex);

                //allVertices.add(thisVertex);
            }
            count++;
            if (System.currentTimeMillis() - st > INTERACTIVE_DELAY) {
                st = System.currentTimeMillis();
                LOG.info("Count = {}, Edges ={}", count, edges.size());
            }
        }
        LOG.info("Completed scanning edges, total vertices = {}", count);
       // return new Graph(fieldName, allVertices);
        return null;
    }


    public void setIgnoreEdges(Set<String> ignoreEdges) {
        this.ignoreEdges = ignoreEdges;
    }

    public static void main(String[] args) {

        SolrDocIterator iterator = new SolrDocIterator("http://localhost:8983/solr",
                "locations:*", 0, 1000, "id", "locations");
        System.out.println("Found " + iterator.getNumFound() + " docs");

        GraphBuilder builder  = new GraphBuilder(iterator, "locations");
        builder.setIgnoreEdges(new HashSet<>(Arrays.asList("Etc")));
        Graph graph = builder.build();
        System.out.println("Done");

    }
}
