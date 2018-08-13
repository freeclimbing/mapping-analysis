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
  public Edge<Long, NullValue> map(org.gradoop.common.model.impl.pojo.Edge value) throws Exception {
    assert value.getProperties() != null;
    for (Property property : value.getProperties()) {
      System.out.println(property + " " +property.getValue().toString());
      if (property.getKey().equals("left")) {
        reuseEdge.setSource(property.getValue().getLong());
      } else if (property.getKey().equals("right")) {
        reuseEdge.setTarget(property.getValue().getLong());
      }
    }

    if (reuseEdge.getSource() == null) {
      Long hash = Utils.getHash(value.getSourceId().toString());
      reuseEdge.setSource(hash);
    }
    if (reuseEdge.getTarget() == null) {
      Long hash = Utils.getHash(value.getTargetId().toString());
      reuseEdge.setTarget(hash);
    }

    reuseEdge.setValue(NullValue.getInstance());
    return reuseEdge;
  }
}
