package org.mappinganalysis.model.functions.decomposition.typegroupby;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;
import java.util.Set;

/**
 * filter vertices with overlapping types into one hash component, even
 * "Mountain -- Mountain, Island -- Island" is working
 *
 * old component id cc_id is removed at the end in favor of hashCcId
 */
class HashCcIdOverlappingFunction
    implements GroupReduceFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(HashCcIdOverlappingFunction.class);

  @Override
  public void reduce(Iterable<Vertex<Long, ObjectMap>> input,
                     Collector<Vertex<Long, ObjectMap>> out) throws Exception {
    Set<Vertex<Long, ObjectMap>> vertexSet = Sets.newHashSet(input);
    HashMap<String, Long> typeHashDict = Maps.newHashMap();

    // all vertices need to be processed, hash is stored for each different type
    // NOTE: hash changes for different runs because of use of set, but result is correct
    for (Vertex<Long, ObjectMap> tmpVertex : vertexSet) {
//      LOG.info(tmpVertex.getId() + " #######################");
      Set<String> types = tmpVertex.getValue().getTypes(Constants.COMP_TYPE);
      Long hash = null;
      for (String type : types) {
        if (typeHashDict.containsKey(type)) {
          if (hash == null) {
//            LOG.info(tmpVertex.getId() + " type: " + type + " hash: null");
            hash = typeHashDict.get(type);
          } else {
//            LOG.info(tmpVertex.getId() + " type: " + type + " hash: " + hash);
            typeHashDict.put(type, hash);
          }
        } else {
          if (hash == null) {
//            LOG.info(tmpVertex.getId() + " not contains type: " + type + " hash: null");
            typeHashDict.put(type, Utils.getHash(
                type.concat(tmpVertex.getId().toString())));
            hash = Utils.getHash(type.concat(tmpVertex.getId().toString()));
          } else {
//            LOG.info(tmpVertex.getId() + " not contains type: " + type + " hash: " + hash);
            typeHashDict.put(type, hash);
          }
        }
      }
//      LOG.info(tmpVertex.getId() + "###hashDict### " + typeHashDict.toString());
    }

    for (Vertex<Long, ObjectMap> vertex : vertexSet) {
      String rndVertexType = vertex.getValue()
          .getTypes(Constants.COMP_TYPE).iterator().next();

      vertex.getValue()
          .put(Constants.HASH_CC, typeHashDict.get(rndVertexType));
      vertex.getValue().remove(Constants.COMP_TYPE);
      vertex.getValue().remove(Constants.CC_ID);
//      LOG.info("###hashOverlap###: " + vertex.toString());
      out.collect(vertex);
    }
  }
}
