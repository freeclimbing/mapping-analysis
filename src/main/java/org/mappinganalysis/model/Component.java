package org.mappinganalysis.model;

import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * Component
 */
public class Component {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(Component.class);
  /**
   *  Vertices of component
   */
  private HashSet<Vertex> vertices = new HashSet<>();
  private int id;

  /**
   * Constructor
   * @param cc component id
   */
  public Component(int cc) {
    this.id = cc;
  }

  /**
   * Get set of vertices for a single component.
   * @return set of vertices
   */
  public HashSet<Vertex> getVertices() {
    return vertices;
  }

  /**
   * Get all component vertices as set of integers.
   * @return set of vertex ids
   */
  public Set<Integer> getVerticesAsInt() {
    Set<Integer> vertices = new HashSet<>();

    for (Vertex vertex : getVertices()) {
      vertices.add(vertex.getId());
    }

    return vertices;
  }

  /**
   * Get the unique component ID of the current component.
   * @return component id
   */
  public int getId() {
    return id;
  }

  /**
   * Add a vertex object to the component.
   * @param vertex vertex
   */
  public void addVertex(Vertex vertex) {
    vertices.add(vertex);
  }

  /**
   * Get single vertex by id.
   * @param id vertex id
   * @return vertex
   */
  public Vertex getVertex(int id) {
    for (Vertex vertex : vertices) {
      if (vertex.getId() == id) {
        return vertex;
      }
    }
    return null;
  }

  /**
   * Add a single vertex with properties to a component.
   * @param id vertex id
   * @param url vertex url
   * @param source data set where vertex comes from
   * @param label vertex label
   */
  public void addVertex(int id, String url, String source, String label) {
    Vertex vertex = new Vertex(id, url, source, label);
    vertices.add(vertex);
  }
}
