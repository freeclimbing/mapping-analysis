package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;

/**
 * Transformation from MusicTuple elements to Vertex<Long, ObjectMap>.
 */
public class FinalMergeMusicVertexCreator
    implements FlatJoinFunction<MergeMusicTuple, Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private final DataDomain domain;

  FinalMergeMusicVertexCreator(DataDomain domain) {
    this.domain = domain;
  }

  @Override
  public void join(MergeMusicTuple tuple,
                   Vertex<Long, ObjectMap> second,
                   Collector<Vertex<Long, ObjectMap>> out) throws Exception {
    Vertex<Long, ObjectMap> result = tuple.toVertex(domain);
    if (result != null) {
      out.collect(result);
    }
  }
}
