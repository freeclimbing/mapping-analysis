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
 * new group reduce filters vertices with overlapping
 * types correctly together into one hashCc
 *
 * TODO test
 */
class HashCcIdOverlappingFunction implements GroupReduceFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(HashCcIdOverlappingFunction.class);

  @Override
  public void reduce(Iterable<Vertex<Long, ObjectMap>> input, Collector<Vertex<Long, ObjectMap>> out) throws Exception {
    Set<Vertex<Long, ObjectMap>> tmpVertices = Sets.newHashSet(input);
    HashMap<String, Long> hashDictionary = Maps.newHashMap();

    for (Vertex<Long, ObjectMap> tmpVertex : tmpVertices) {
//      LOG.info(tmpVertex.getId() + " #######################");
      Set<String> types = tmpVertex.getValue().getTypes(Constants.COMP_TYPE);
      Long hash = null;
      for (String type : types) {
        if (hashDictionary.containsKey(type)) {
          if (hash == null) {
//            LOG.info(tmpVertex.getId() + " type: " + type + " hash: null");
            hash = hashDictionary.get(type);
          } else {
//            LOG.info(tmpVertex.getId() + " type: " + type + " hash: " + hash);
            hashDictionary.put(type, hash);
          }
        } else {
          if (hash == null) {
//            LOG.info(tmpVertex.getId() + " not contains type: " + type + " hash: null");
            hashDictionary.put(type, Utils.getHash(type.concat(tmpVertex.getId().toString())));
            hash = Utils.getHash(type.concat(tmpVertex.getId().toString()));
          } else {
//            LOG.info(tmpVertex.getId() + " not contains type: " + type + " hash: " + hash);
            hashDictionary.put(type, hash);
          }
        }
      }
//      LOG.info(tmpVertex.getId() + "###hashDict### " + hashDictionary.toString());
    }

    for (Vertex<Long, ObjectMap> vertex : tmpVertices) {
      String rndVertexType = vertex.getValue()
          .getTypes(Constants.COMP_TYPE).iterator().next();

      vertex.getValue()
          .put(Constants.HASH_CC, hashDictionary.get(rndVertexType));
      vertex.getValue().remove(Constants.COMP_TYPE);
//      LOG.info("###hashOverlap###: " + vertex.toString());
      out.collect(vertex);
    }
  }
}
