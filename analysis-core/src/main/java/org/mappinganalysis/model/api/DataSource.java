package org.mappinganalysis.model.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * Data Source
 */
public interface DataSource<VV, EE extends Vertex<VV, EE>> {
  /**
   * Reads input as Gelly graph.
   * @return graph
   */
  Graph getGraph() throws Exception;

  /**
   * Reads input as DataSet of Gelly vertices.
   * @return vertices
   */
  DataSet<Vertex<VV, EE>> getVertices() throws Exception;
}
