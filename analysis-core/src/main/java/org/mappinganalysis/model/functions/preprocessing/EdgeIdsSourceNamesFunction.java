package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.EdgeIdsSourcesTuple;
import org.mappinganalysis.model.ObjectMap;

public class EdgeIdsSourceNamesFunction
    implements FlatJoinFunction<EdgeIdsSourcesTuple,
    Vertex<Long, ObjectMap>,
    EdgeIdsSourcesTuple> {
  private final int side;

  public EdgeIdsSourceNamesFunction(int side) {
    this.side = side;
  }

  @Override
  public void join(EdgeIdsSourcesTuple left,
                   Vertex<Long, ObjectMap> right,
                   Collector<EdgeIdsSourcesTuple> out) throws Exception {
    if (left != null && right != null) {
      left.checkSideAndUpdate(side, right.getValue().getOntology());
      out.collect(left);
    }
  }
}
