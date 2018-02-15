package org.mappinganalysis.graph.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;

/**
 * Create edges for a given set of vertices having component ids.
 */
public class EdgeComputationVertexCcSet
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Edge<Long, NullValue>> {

  private KeySelector<Vertex<Long, ObjectMap>, Long> keySelector;
  private EdgeComputationStrategy strategy = EdgeComputationStrategy.NONE;
  private Boolean isResultEdgeDistinct = true;
  private DataSet<Vertex<Long, ObjectMap>> vertices;

  /**
   * Create all distinct edges for a set of representatives.
   */
  public EdgeComputationVertexCcSet() {
    this.strategy = EdgeComputationStrategy.REPRESENTATIVE;
  }

  /**
   * Create all distinct edges for a set of vertices with cc ids.
   */
  public EdgeComputationVertexCcSet(
      KeySelector<Vertex<Long, ObjectMap>, Long> keySelector) {
    this(keySelector, EdgeComputationStrategy.ALL, true);
  }

  /**
   * Create edges for set of vertices having cc id - optionally create only as many edges
   * to connect all vertices within cc.
   * @param keySelector used cc id key selector
   * @param strategy if SIMPLE, only core edges will be computed in cc
   * @param isResultEdgeDistinct if false, no distinct check for edges
   */
  public EdgeComputationVertexCcSet(
      KeySelector<Vertex<Long, ObjectMap>, Long> keySelector,
      EdgeComputationStrategy strategy,
      Boolean isResultEdgeDistinct) {
    this.keySelector = keySelector;
    this.strategy = strategy;
    this.isResultEdgeDistinct = isResultEdgeDistinct;
  }

  /**
   * For simple edge creator, edges are always distinct.
   * @param keySelector used cc id key selector
   * @param strategy needs to be false
   */
  public EdgeComputationVertexCcSet(
      KeySelector<Vertex<Long, ObjectMap>, Long> keySelector,
      EdgeComputationStrategy strategy) {
    this.keySelector = keySelector;
    this.strategy = strategy;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> vertices) {
    this.vertices = vertices;
  }

  /**
   * For a set of vertices, create all single distinct edges which can be
   * created within a connected component and restrict them:
   * - only one edge between 2 vertices
   * - no edge from a -> a
   * @return edge set
   */
  @Override
  public DataSet<Edge<Long, NullValue>> createResult() {
    if (strategy.equals(EdgeComputationStrategy.ALL) && isResultEdgeDistinct) {
      return vertices
          .runOperation(new AllEdgesCreator(keySelector));
    } else if (strategy.equals(EdgeComputationStrategy.ALL) && !isResultEdgeDistinct) {
      throw new IllegalArgumentException("Not currently supported non-distinct: " + strategy);
    } else if (strategy.equals(EdgeComputationStrategy.SIMPLE)) {
      return vertices
          .runOperation(new SimpleEdgesCreator(keySelector));
    } else if (strategy.equals(EdgeComputationStrategy.REPRESENTATIVE)) {
      return vertices
          .runOperation(new RepresentativeEdgesCreator());
    } else {
      throw new IllegalArgumentException("Unsupported strategy: " + strategy);
    }
  }
}
