package org.mappinganalysis.io.functions;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.FlinkProperty;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Create vertex with accumulated properties from single entry database result rows.
 */
public class PropertyCoGroupFunction extends RichCoGroupFunction<Vertex<Long, ObjectMap>,
    FlinkProperty, Vertex<Long, ObjectMap>> {
  private LongCounter vertexCounter = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.VERTEX_COUNT_ACCUMULATOR, vertexCounter);
  }

  public void coGroup(Iterable<Vertex<Long, ObjectMap>> vertices, Iterable<FlinkProperty> properties,
                      Collector<Vertex<Long, ObjectMap>> out) throws Exception {
    try {
      Vertex<Long, ObjectMap> vertex = Iterables.get(vertices, 0);;
      ObjectMap vertexProperties = vertex.getValue();

      for (FlinkProperty property : properties) {
        String value = property.getPropertyValue();
        String key = property.getPropertyKey();
        if (vertexProperties.containsKey(Utils.LAT) && key.equals(Utils.LAT)
            || vertexProperties.containsKey(Utils.LON) && key.equals(Utils.LON)) {
          continue;
        }
        if (vertexProperties.containsKey(Utils.LABEL) && key.equals(Utils.LABEL)) {
          continue;
        }

        if (property.getPropertyType().equals("double")) {
          vertexProperties.addProperty(key, Doubles.tryParse(value));
        } else if (property.getPropertyType().equals("string")) {
          vertexProperties.addProperty(key, value);
        }
      }

      // skip no label vertices
      if (vertexProperties.containsKey(Utils.LABEL)) {
        vertex.setValue(vertexProperties);

        vertexCounter.add(1L);
        out.collect(vertex);
      }
    } catch (IndexOutOfBoundsException e) {
      // dont collect the vertex if the vertex does not exist
    }
  }
}
