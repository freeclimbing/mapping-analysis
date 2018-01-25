package org.mappinganalysis.graph.utils;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.mappinganalysis.util.Utils;

public class GradoopToGellyEdgeJoinFunction
    implements JoinFunction<Edge, Vertex, Edge> {
  private static final Logger LOG = Logger.getLogger(GradoopToGellyEdgeJoinFunction.class);
  private int side;

  public GradoopToGellyEdgeJoinFunction(int side) {
    this.side = side;
  }

  @Override
  public Edge join(Edge edge, Vertex vertex) throws Exception {
//    LOG.info("recid prop value: " + vertex.getPropertyValue("recId").toString());
    if (vertex.hasProperty("recId")) {
      long recId = Utils.getIdFromNcId(vertex.getPropertyValue("recId").toString());
      if (side == 0) {
        edge.setProperty("left", recId);
      } else {
        edge.setProperty("right", recId);
      }
    }
    return edge;
  }
}
