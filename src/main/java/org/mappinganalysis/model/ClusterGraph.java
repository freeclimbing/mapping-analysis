package org.mappinganalysis.model;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.VertexToTuple2Map;

/**
 * not yet used
 */
@Deprecated
public class ClusterGraph extends Tuple2<ClusterVertex, ClusterEdge> {

  private DataSet<ClusterVertex> vertices = null;
  private DataSet<ClusterEdge> edges = null;

  public ClusterGraph() {
  }

  public ClusterGraph(DataSet<ClusterVertex> vertices, DataSet<ClusterEdge> edges) {
    this.vertices = vertices;
    this.edges = edges;
  }

  public DataSet<ClusterVertex> getVertices() {
    return vertices;
  }

  public DataSet<ClusterEdge> getEdges() {
    return edges;
  }

  public void setPreVertices(DataSet<Vertex<Long, ObjectMap>> preVertices, String ccId) {
    this.vertices = preVertices.map(new ClusterComponentMapFunction(ccId));
  }

  //  public void setPreEdges(DataSet<Edge<Long, ObjectMap>> edges, String ccId) {
  //
  //  }


  public void setPostVertices(DataSet<Vertex<Long, ObjectMap>> postVertices, String ccId) {
    this.vertices = vertices.join(postVertices)
        .where(0)
        .equalTo(0)
        .with(new ClusterGraphJoinFunction(ccId));

  }

  private static class ClusterComponentMapFunction implements MapFunction<Vertex<Long,ObjectMap>, ClusterVertex> {
    private final String ccId;

    public ClusterComponentMapFunction(String ccId) {
      this.ccId = ccId;
    }

    @Override
    public ClusterVertex map(Vertex<Long, ObjectMap> vertex) throws Exception {
      return new ClusterVertex(vertex.getId(), (long) vertex.getValue().get(ccId));
    }
  }

  private static class ClusterGraphJoinFunction implements JoinFunction<ClusterVertex, Vertex<Long,ObjectMap>, ClusterVertex> {
    private final String ccId;

    public ClusterGraphJoinFunction(String ccId) {
      this.ccId = ccId;
    }

    @Override
    public ClusterVertex join(ClusterVertex clusterVertex, Vertex<Long, ObjectMap> vertex) throws Exception {
      clusterVertex.f1.f1 = (long) vertex.getValue().get(ccId);
      return clusterVertex;
    }
  }
}
