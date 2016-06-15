package org.mappinganalysis.model.functions.preprocessing;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.TypeDictionary;
import org.mappinganalysis.utils.Utils;

import java.util.Set;

/**
 * Map types of imported resources to an internal dictionary of harmonized type values.
 */
public class InternalTypeMapFunction extends RichMapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private ListAccumulator<String> types = new ListAccumulator<>();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.TYPES_COUNT_ACCUMULATOR, types);
  }
  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    ObjectMap properties = vertex.getValue();
    Set<String> resultTypes = Sets.newHashSet();

    if (properties.containsKey(Utils.GN_TYPE_DETAIL)) {
      resultTypes = getDictValue(properties.get(Utils.GN_TYPE_DETAIL).toString());
    }
    if (properties.containsKey(Utils.TYPE) &&
        (resultTypes.isEmpty() || resultTypes.contains(Utils.NO_TYPE))) {
      resultTypes = getDictValues(properties.getTypes(Utils.TYPE));
    }
    if (resultTypes.isEmpty()) {
      resultTypes = Sets.newHashSet(Utils.NO_TYPE);
    }

    types.add(resultTypes.toString());
    properties.put(Utils.TYPE_INTERN, resultTypes);
    return vertex;
  }

  private static Set<String> getDictValue(String value) {
    return getDictValues(Sets.newHashSet(value));
  }

  private static Set<String> getDictValues(Set<String> values) {
    Set<String> resultTypes = Sets.newHashSet();

    for (String value : values) {
      if (TypeDictionary.PRIMARY_TYPE.containsKey(value)) {
        resultTypes.add(TypeDictionary.PRIMARY_TYPE.get(value));
      }
    }
    for (String value : values) {
      if (TypeDictionary.SECONDARY_TYPE.containsKey(value)) {
        resultTypes.add(TypeDictionary.SECONDARY_TYPE.get(value));
      }
    }

    return resultTypes.isEmpty() ? Sets.newHashSet(Utils.NO_TYPE) : resultTypes;
  }
}
