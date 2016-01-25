package org.mappinganalysis.model.functions.preprocessing;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.TypeDictionary;
import org.mappinganalysis.utils.Utils;

import java.util.Map;
import java.util.Set;

/**
 * Map types of imported resources to an internal dictionary of harmonized type values.
 */
public class InternalTypeMapFunction extends RichMapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private ListAccumulator<String> statsCounter = new ListAccumulator<>();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.TYPES_COUNT_ACCUMULATOR, statsCounter);
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    Map<String, Object> properties = vertex.getValue();
    String resultType = null;
    if (properties.containsKey(Utils.GN_TYPE_DETAIL)) {
      resultType = getDictValue(properties.get(Utils.GN_TYPE_DETAIL).toString());
    }
    if (properties.containsKey(Utils.TYPE) && (resultType == null || resultType.equals(Utils.TYPE_NOT_FOUND))) {
      resultType = getInternalType(properties.get(Utils.TYPE));
    }
    if (resultType == null) {
      resultType = Utils.NO_TYPE_AVAILABLE;
    }
    statsCounter.add(resultType);
    properties.put(Utils.TYPE_INTERN, resultType);
    return vertex;
  }

  /**
   * get relevant key and translate with custom dictionary for internal use
   */
  private String getInternalType(Object property) {
    String resultType;
    if (property instanceof Set) {
      Set<String> values = Sets.newHashSet((Set<String>) property);
      resultType = getDictValue(values);
    } else {
      resultType = getDictValue(property.toString());
    }
    return resultType;
  }

  private static String getDictValue(String value) {
    return getDictValue(Sets.newHashSet((String) value));
  }

  private static String getDictValue(Set<String> values) {
    for (String value : values) {
      if (TypeDictionary.PRIMARY_TYPE.containsKey(value)) {
        return TypeDictionary.PRIMARY_TYPE.get(value);
      }
    }

    for (String value : values) {
      if (TypeDictionary.SECONDARY_TYPE.containsKey(value)) {
        return TypeDictionary.SECONDARY_TYPE.get(value);
      }
    }

    for (String value : values) {
      if (TypeDictionary.TERTIARY_TYPE.containsKey(value)) {
        return TypeDictionary.TERTIARY_TYPE.get(value);
      }
    }

    return Utils.TYPE_NOT_FOUND;
  }
}
