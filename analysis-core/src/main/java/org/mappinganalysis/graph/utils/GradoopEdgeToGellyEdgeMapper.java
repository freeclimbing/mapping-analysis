package org.mappinganalysis.graph.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.properties.Property;
import org.mappinganalysis.util.Utils;

public class GradoopEdgeToGellyEdgeMapper
    implements MapFunction<org.gradoop.common.model.impl.pojo.Edge, Edge<Long, NullValue>> {
  private static final Logger LOG = Logger.getLogger(GradoopEdgeToGellyEdgeMapper.class);

  private final Edge<Long, NullValue> reuseEdge;

  public GradoopEdgeToGellyEdgeMapper() {
    reuseEdge = new Edge<>();
  }

  @Override
  public Edge<Long, NullValue> map(
      org.gradoop.common.model.impl.pojo.Edge value) throws Exception {
    boolean hasLeftRightVertexId = false;
    assert value.getProperties() != null;
    for (Property property : value.getProperties()) {
      if (property.getKey().equals("left")) {
        reuseEdge.setSource(property.getValue().getLong());
        hasLeftRightVertexId = true;
      } else if (property.getKey().equals("right")) {
        reuseEdge.setTarget(property.getValue().getLong());
        hasLeftRightVertexId = true;
      }
    }

    if (!hasLeftRightVertexId) {
      reuseEdge.setSource(
          Utils.getHash(value.getSourceId().toString()));
      reuseEdge.setTarget(
          Utils.getHash(value.getTargetId().toString()));
    }

    reuseEdge.setValue(NullValue.getInstance());
    return reuseEdge;
  }
}
