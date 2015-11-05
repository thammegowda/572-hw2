package edu.usc.cs.ir.cwork.graph;

import java.util.HashSet;
import java.util.Set;

/**
 * Vertex Model
 */
public class Vertex {

    private String id;
    private double score;
    private Set<Vertex> edges;

    public Vertex(String id, double score) {
        this(id, score, new HashSet<>());
    }

    public Vertex(String id, double score, Set<Vertex> edges) {
        this.id = id;
        this.score = score;
        this.edges = edges;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public Set<Vertex> getEdges() {
        return edges;
    }

    public void addUndirectedEdge(Vertex vertex) {
        //undirected edge
        this.edges.add(vertex);
        vertex.edges.add(this);
    }

    public void setEdges(Set<Vertex> edges) {
        this.edges = edges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Vertex vertex = (Vertex) o;

        return !(id != null ? !id.equals(vertex.id) : vertex.id != null);

    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

}
