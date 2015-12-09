package org.mappinganalysis.model.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.model.PropertyHelper;
import org.mappinganalysis.utils.Utils;

import java.util.Map;

import static org.simmetrics.builders.StringMetricBuilder.with;

/**
 * Merge properties of grouped entities based on "best data source" availability,
 * i.e., GeoNames > DBpedia > others
 */
public class BestDataSourceAllLabelsGroupReduceFunction
    implements GroupReduceFunction<Vertex<Long, FlinkVertex>, Vertex<Long, FlinkVertex>> {
  @Override
  public void reduce(Iterable<Vertex<Long, FlinkVertex>> vertices,
                     Collector<Vertex<Long, FlinkVertex>> collector) throws Exception {
    FlinkVertex result = new FlinkVertex();
    Map<String, Object> resultProps = Maps.newHashMap();
    boolean isRepresentative = false;

    for (Vertex<Long, FlinkVertex> vertex : vertices) {
      if (!isRepresentative) {
        result.setId(vertex.getId());
        isRepresentative = true;
      }
      resultProps = PropertyHelper
          .addValueToProperties(resultProps, vertex.getValue(), Utils.CL_VERTICES);

      if (vertex.getValue().hasLabel()) {
        resultProps = PropertyHelper
            .addValueToProperties(resultProps, vertex.getValue().getLabel(), Utils.LABEL, true);
      }

      createRepresentativeProperties(resultProps, vertex);
    }
    result.setProperties(resultProps);
    collector.collect(new Vertex<>(result.getId(), result));
  }

  private void createRepresentativeProperties(Map<String, Object> resultProps,
                                              Vertex<Long, FlinkVertex> vertex) {
    Map<String, Object> properties = vertex.getValue().getProperties();

    boolean latLonGnFound = false;
    boolean latLonDbpFound = false;
    boolean typeGnFound = false;
    boolean typeDbpFound = false;
    if (properties.containsKey(Utils.ONTOLOGY)) {
      if (properties.get(Utils.ONTOLOGY).equals(Utils.GN_NAMESPACE)) {
        if (properties.containsKey(Utils.LAT) && properties.containsKey(Utils.LON)) {
          setLatLon(resultProps, properties);
          latLonGnFound = true;
        }
        if (properties.containsKey(Utils.TYPE_INTERN) && !properties.get(Utils.TYPE_INTERN).equals("-1")) {
          resultProps.put(Utils.TYPE_INTERN, properties.get(Utils.TYPE_INTERN));
          typeGnFound = true;
        }
      }
      if (properties.get(Utils.ONTOLOGY).equals(Utils.DBP_NAMESPACE)) {
        if (properties.containsKey(Utils.LAT) && properties.containsKey(Utils.LON) && !latLonGnFound) {
          setLatLon(resultProps, properties);
          latLonDbpFound = true;
        }
        if (properties.containsKey(Utils.TYPE_INTERN)
            && !properties.get(Utils.TYPE_INTERN).equals("-1") && !typeGnFound) {
          resultProps.put(Utils.TYPE_INTERN, properties.get(Utils.TYPE_INTERN));
          typeDbpFound = true;
        }
      }
    }
    if (properties.containsKey(Utils.LAT) && properties.containsKey(Utils.LON)
        && !latLonDbpFound && !latLonGnFound) {
      setLatLon(resultProps, properties);
    }
    if (properties.containsKey(Utils.TYPE_INTERN) && !properties.get(Utils.TYPE_INTERN).equals("-1")
        && !typeGnFound && !typeDbpFound) {
      resultProps.put(Utils.TYPE_INTERN, properties.get(Utils.TYPE_INTERN));
    }
  }

  private Map<String, Object> setLatLon(Map<String, Object> result, Map<String, Object> properties) {
    result.put(Utils.LAT, properties.get(Utils.LAT));
    result.put(Utils.LON, properties.get(Utils.LON));
    return result;
  }
}
