package org.mappinganalysis.model;

import com.google.common.collect.Sets;

import java.util.HashSet;

/**
 * Vertex
 */
public class Vertex {

  /**
   * source
   */
  private String source = "";
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
  private HashSet<String> typeSet = new HashSet<>();
  /**
   * Geonames rdf:type detail information
   */
  private String typeDetail = "";
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
  private HashSet<Integer> edges = null;
  private double ele;

  /**
   * Constructor - build vertex with unique id
   *
   * @param id id
   */
  public Vertex(Integer id) {
    this.id = id;
  }

  /**
   * Constructor - Create vertex with additional information.
   *
   * @param id     vertex id
   * @param url    vertex url
   * @param source data set where vertex comes from
   * @param label  vertex label
   */
  public Vertex(int id, String url, String source, String label) {
    this.id = id;
    this.url = url;
    this.label = label;
    this.source = source;
  }

  /**
   * Set label of single vertex.
   *
   * @param label label
   */
  public void setLabel(String label) {
    this.label = label;
  }

  /**
   * Set url of single vertex.
   *
   * @param url url
   */
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * Add edge (vertex id of target) to a vertex.
   *
   * @param edge id
   */
  public void addEdge(Integer edge) {
    if (edges == null) {
      this.edges = Sets.newHashSet();
    } else {
      this.edges = Sets.newHashSet(edges);
    }
    this.edges.add(edge);
  }

  /**
   * Get vertex id of single vertex.
   *
   * @return vertex id
   */
  public Integer getId() {
    return id;
  }

  /**
   * Get label of single vertex.
   *
   * @return label
   */
  public String getLabel() {
    return label;
  }

  /**
   * Get url of single vertex.
   *
   * @return url
   */
  public String getUrl() {
    return url;
  }

  /**
   * Get all edges for a vertex
   *
   * @return set of target vertex ids
   */
  public HashSet<Integer> getEdges() {
    return edges;
  }

  /**
   * Get source data set name where vertex comes from
   *
   * @return source string
   */
  public String getOntology() {
    return source;
  }

  public String toString() {
    String types = "";
    for (String s : typeSet) {
      types = types.concat(s).concat("\n");
    }
    return "SimpleVertex{" +
        "id=" + getId() +
        ", url=" + getUrl() +
        ", label=" + getLabel() +
        ", source=" + getOntology() +
        ", lat=" + getLat() +
        ", lon=" + getLon() +
        ", types=" + types +
        "}";
  }

  public HashSet<String> getTypeSet() {
    return typeSet;
  }

  public void addType(String type) {
    this.typeSet.add(type);
  }

  public double getLat() {
    return lat;
  }

  public String getTypeDetail() {
    return typeDetail;
  }

  public void setTypeDetail(String typeDetail) {
    this.typeDetail = typeDetail;
  }

  public double getEle() {
    return ele;
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

  public void setEle(double ele) {
    this.ele = ele;
  }
}