package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeMusicTriplet;

/**
 */
public class TransitionElementsFlatMapFunction<T>
    implements FlatMapFunction<T, Tuple2<Long, Long>> {
  private DataDomain domain;

  public TransitionElementsFlatMapFunction(DataDomain domain) {
    this.domain = domain;
  }

  @Override
  public void flatMap(T input, Collector<Tuple2<Long, Long>> out) throws Exception {
    if (domain == DataDomain.GEOGRAPHY) {
      MergeGeoTriplet triplet = (MergeGeoTriplet) input;
      Long min = triplet.getSrcId() < triplet.getTrgId()
          ? triplet.getSrcId() : triplet.getTrgId();
//            LOG.info(triplet.getSrcId() + " " + triplet.getTrgId() + " " + min);
      out.collect(new Tuple2<>(triplet.getSrcId(), min));
      out.collect(new Tuple2<>(triplet.getTrgId(), min));
    } else {
      MergeMusicTriplet triplet = (MergeMusicTriplet) input;
      Long min = triplet.getSrcId() < triplet.getTrgId()
          ? triplet.getSrcId() : triplet.getTrgId();
//            LOG.info(triplet.getSrcId() + " " + triplet.getTrgId() + " " + min);
      out.collect(new Tuple2<>(triplet.getSrcId(), min));
      out.collect(new Tuple2<>(triplet.getTrgId(), min));
    }


  }
}
