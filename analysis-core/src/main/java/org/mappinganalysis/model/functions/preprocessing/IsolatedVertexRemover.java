package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

/**
 * For a dataset of vertices, delete vertices which are not source or target of an edge.
 */
public class IsolatedVertexRemover<EV>
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private final DataSet<Tuple2<Long, Long>> edges;
//  private final Class<VV> vertexClass;
  private DataSet<Vertex<Long, ObjectMap>> initialVertices;

  public IsolatedVertexRemover(DataSet<Edge<Long, EV>> edges) {//}, Class<VV> vertexClass) {
    this.edges = edges.<Tuple2<Long, Long>>project(0, 1);
//    this.vertexClass = vertexClass;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    this.initialVertices = inputData;
  }

  /**
   * Check both source and target side of links if a matching vertex is found.
   * @return set of vertices having a match
   */
  @Override
  public DataSet<Vertex<Long, ObjectMap>> createResult() {

//    TypeHint hint = new TypeHint() {
//      @Override
//      public TypeInformation getTypeInfo() {
//        return TypeInformation.of(vertexClass);
//      }
//    };

    DataSet<Vertex<Long, ObjectMap>> left = initialVertices
        .join(edges)
        .where(0)
        .equalTo(0)
        .with((vertex, edge) -> vertex)
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    return initialVertices
        .join(edges)
        .where(0)
        .equalTo(1)
        .with((vertex, edge) -> vertex)
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .union(left)
        .distinct(0);
  }
}
