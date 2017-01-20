package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Vertex;

/**
 * Created by markus on 1/20/17.
 *
 * TODO
 */
public class VertexInternalTypeMapper<VV> implements CustomUnaryOperation<Vertex<Long, VV>, Vertex<Long, VV>> {
  private DataSet<Vertex<Long, VV>> initialVertices;

  @Override
  public void setInput(DataSet<Vertex<Long, VV>> inputData) {
    this.initialVertices = inputData;
  }

  @Override
  public DataSet<Vertex<Long, VV>> createResult() {
    return null;
  }
}
