package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.ObjectMap;

/**
 * Provide a transformation from MusicTriplet to Triplet<Long, ObjectMap, NullValue>.
 */
class MusicTripletToTripletFunction
    implements MapFunction<MergeMusicTriplet, Triplet<Long, ObjectMap, NullValue>> {
  private DataDomain dataDomain;

  public MusicTripletToTripletFunction(DataDomain dataDomain) {
    this.dataDomain = dataDomain;
  }

  @Override
  public Triplet<Long, ObjectMap, NullValue> map(MergeMusicTriplet triplet) throws Exception {
    Vertex<Long, ObjectMap> source = triplet.getSrcTuple()
        .toVertex(dataDomain);

    Vertex<Long, ObjectMap> target = triplet.getTrgTuple()
        .toVertex(dataDomain);

    return new Triplet<>(source.getId(),
        target.getId(),
        source.getValue(),
        target.getValue(),
        NullValue.getInstance());
  }
}
