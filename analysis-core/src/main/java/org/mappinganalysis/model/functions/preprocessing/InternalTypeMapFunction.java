package org.mappinganalysis.model.functions.preprocessing;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.TypeDictionary;

import java.util.Set;

/**
 * Map types of imported resources to an internal dictionary of harmonized type values.
 * TODO richmap no longer needed
 */
public class InternalTypeMapFunction extends RichMapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(InternalTypeMapFunction.class);

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    ObjectMap properties = vertex.getValue();
    Set<String> resultTypes = Sets.newHashSet();

    if (properties.containsKey(Constants.GN_TYPE_DETAIL)) {
//      LOG.info("###itm: " + vertex.getId() + " ### gn");
      resultTypes = getDictValue(properties.get(Constants.GN_TYPE_DETAIL).toString());
    }
    if (properties.containsKey(Constants.TYPE) &&
        (resultTypes.isEmpty() || resultTypes.contains(Constants.NO_TYPE))) {
//      LOG.info("###itm: " + vertex.toString() + " ### normal");
      resultTypes = getDictValues(properties.getTypes(Constants.TYPE));
    }
    if (resultTypes.isEmpty()) {
//      LOG.info("###itm: " + vertex.getId() + " ### notype");
      resultTypes = Sets.newHashSet(Constants.NO_TYPE);
    }
//    LOG.info("###itm: " + vertex.toString() + " ### " + resultTypes.toString());

    properties.put(Constants.TYPE_INTERN, resultTypes);
    properties.remove(Constants.TYPE);
    properties.remove(Constants.GN_TYPE_DETAIL);
//    LOG.info("###inttypemap###2 " + vertex.toString());

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

    return resultTypes.isEmpty() ? Sets.newHashSet(Constants.NO_TYPE) : resultTypes;
  }
}
