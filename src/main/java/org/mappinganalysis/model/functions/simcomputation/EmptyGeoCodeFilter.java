package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
    if (props.containsKey(Utils.LAT) && props.containsKey(Utils.LON)) {
      // TODO how to handle multiple values in lat/lon correctly?
      return ((getDouble(props.getLatitude()) == null)
          || (getDouble(props.getLongitude()) == null)) ? Boolean.FALSE : Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  /**
   * deprecated? when bug lat/Lon fixed -its never a set because of input process
   * @param latOrLon
   * @return
   */
  private Double getDouble(Object latOrLon) {
    if (latOrLon instanceof Set) {
      return Doubles.tryParse(Iterables.get((Set) latOrLon, 0).toString());
    } else {
      return Doubles.tryParse(latOrLon.toString());
    }
  }
}
