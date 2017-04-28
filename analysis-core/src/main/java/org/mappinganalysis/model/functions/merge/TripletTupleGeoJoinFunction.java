package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.JoinFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;

/**
 * Created by markus on 4/28/17.
 */
class TripletTupleGeoJoinFunction
    implements JoinFunction<MergeGeoTriplet, MergeGeoTuple, MergeGeoTriplet> {
  private final Integer position;

  public TripletTupleGeoJoinFunction(Integer position) {
    this.position = position;
  }

  @Override
  public MergeGeoTriplet join(MergeGeoTriplet triplet,
                              MergeGeoTuple newTuple) throws Exception {
    if (position == 0) {
      triplet.setSrcTuple(newTuple);
    } else if (position == 1) {
      triplet.setTrgTuple(newTuple);
    } else {
      throw new IllegalArgumentException("Unsupported position: " + position);
    }
//          LOG.info("LEFT DELTA JOIN " + triplet.toString());
    return triplet;
  }
}
