package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Filter coordinates where both latitude and longitude are 0 for either source or target resource.
 */
public class EmptyGeoCodeFilter implements FilterFunction<Triplet<Long, ObjectMap, NullValue>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {
    ObjectMap source = triplet.getSrcVertex().getValue();
    ObjectMap target = triplet.getTrgVertex().getValue();

    return isGeoPoint(source) && isGeoPoint(target);
  }

  private boolean isGeoPoint(ObjectMap props) {
    if (props.containsKey(Constants.LAT) && props.containsKey(Constants.LON)) {
      return ((props.getLatitude() == null)
          || (props.getLongitude() == null)) ? Boolean.FALSE : Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }
}
