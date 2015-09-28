package org.mappinganalysis.model;

import java.util.HashSet;

/**
 * Vertex
 */
public class Vertex {

  /**
   * source
   */
  private String source ="";
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
   * RDF type
   */
  private String type = "";
  /**
   * latitude
   */
  private double lat;
  /**
   * longitude
   */
  private double lon;

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

  /**
   * Constructor - Create vertex with additional information.
   * @param id vertex id
   * @param url vertex url
   * @param source data set where vertex comes from
   * @param label vertex label
   */
  public Vertex(int id, String url, String source, String label) {
    this.id = id;
    this.url = url;
    this.label = label;
    this.source = source;
  }

  /**
   * Set label of single vertex.
   * @param label label
   */
  public void setLabel(String label) {
    this.label = label;
  }

  /**
   * Set url of single vertex.
   * @param url url
   */
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * Add edge (vertex id of target) to a vertex.
   * @param edge id
   */
  public void addEdge(Integer edge) {
    edges.add(edge);
  }

  /**
   * Get vertex id of single vertex.
   * @return vertex id
   */
  public Integer getId() {
    return id;
  }

  /**
   * Get label of single vertex.
   * @return label
   */
  public String getLabel() {
    return label;
  }

  /**
   * Get url of single vertex.
   * @return url
   */
  public String getUrl() {
    return url;
  }

  /**
   * Get all edges for a vertex
   * @return set of target vertex ids
   */
  public HashSet<Integer> getEdges() {
    return edges;
  }

  /**
   * Get source data set name where vertex comes from
   * @return source string
   */
  public String getSource() {
    return source;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public double getLat() {
    return lat;
  }

  public void setLat(double lat) {
    this.lat = lat;
  }

  public double getLon() {
    return lon;
  }

  public void setLon(double lon) {
    this.lon = lon;
  }
}
