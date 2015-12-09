package org.mappinganalysis.io.functions;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.FlinkProperty;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.model.PropertyHelper;
import org.mappinganalysis.utils.Utils;

import java.util.Map;

/**
 * Create vertex with accumulated properties from single entry database result rows.
 */
public class PropertyCoGroupFunction implements CoGroupFunction<FlinkVertex,
    FlinkProperty, FlinkVertex> {
  public void coGroup(Iterable<FlinkVertex> vertices, Iterable<FlinkProperty> properties,
                      Collector<FlinkVertex> out) throws Exception {
    FlinkVertex vertex = Iterables.get(vertices, 0);
    Map<String, Object> vertexProperties = vertex.getProperties();
    boolean latitudeAdded = false;
    boolean longitudeAdded = false;

    for (FlinkProperty property : properties) {
      Object value = property.getPropertyValue();
      String key = property.getPropertyKey();
      if (latitudeAdded && key.equals(Utils.LAT) || longitudeAdded && key.equals(Utils.LON)) {
        continue;
      }

      if (property.getPropertyType().equals("double")) {
        value = Doubles.tryParse(value.toString());
      } else if (property.getPropertyType().equals("string")) {
        value = value.toString();
      }
      vertexProperties = PropertyHelper.addValueToProperties(vertexProperties, value, key);
      if (key.equals(Utils.LAT)) {
        latitudeAdded = true;
      }
      if (key.equals(Utils.LON)) {
        longitudeAdded = true;
      }
    }
    vertex.setProperties(vertexProperties);
    out.collect(vertex);
  }
}
