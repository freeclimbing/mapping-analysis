package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * GeoCode threshold in meter.
 */
public class GeoCodeThreshold
    implements FilterFunction<Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, ObjectMap> distanceThreshold) throws Exception {
    return ((double) distanceThreshold.getEdge().getValue().get(Utils.DISTANCE)) < Utils.GEO_COORD_INITIAL_THRESHOLD;
  }
}
