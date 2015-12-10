package org.mappinganalysis.model.functions;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * Filter coordinates where both latitude and longitude are 0 for either source or target resource.
 */
public class EmptyGeoCodeFilter implements FilterFunction<Triplet<Long, FlinkVertex, NullValue>> {
  @Override
  public boolean filter(Triplet<Long, FlinkVertex, NullValue> triplet) throws Exception {

    Map<String, Object> source = triplet.getSrcVertex().getValue().getProperties();
    Map<String, Object> target = triplet.getTrgVertex().getValue().getProperties();

    return isGeoPoint(source) && isGeoPoint(target);
  }

  private boolean isGeoPoint(Map<String, Object> props) {
    if (props.containsKey(Utils.LAT) && props.containsKey(Utils.LON)) {
      Object lat = props.get(Utils.LAT);
      Object lon = props.get(Utils.LON);
      // TODO how to handle multiple values in lat/lon correctly?
      return ((getDouble(lat) == null) || (getDouble(lon) == null)) ? Boolean.FALSE : Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  private Double getDouble(Object latlon) {
    if (latlon instanceof List) {
      return Doubles.tryParse(((List) latlon).get(0).toString());
    } else {
      return Doubles.tryParse(latlon.toString());
    }
  }
}
