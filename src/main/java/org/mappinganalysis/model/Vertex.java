package org.mappinganalysis.model;

import java.util.HashSet;

/**
 * Vertex
 */
public class Vertex {

  /**
   * unique vertex id
   */
  private int id;

  /**
   * one possible label
   */
  private String label = "";

  /**
   * unique url
   */
  private String url = "";

  /**
   * edges expressed via vertex ids which are connected to the vertex
   */
  private HashSet<Integer> edges = new HashSet<>();

  /**
   * Constructor - build vertex with unique id
   * @param id id
   */
  public Vertex(Integer id) {
    this.id = id;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void addEdge(Integer edge) {
    edges.add(edge);
  }

  public Integer getId() {
    return id;
  }

  public String getLabel() {
    return label;
  }

  public String getUrl() {
    return url;
  }

  public HashSet<Integer> getEdges() {
    return edges;
  }

}
