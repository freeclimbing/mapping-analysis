package org.mappinganalysis.model.functions.typegroupby;

import com.google.common.collect.Maps;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.Collections;
import java.util.HashMap;

public class NoTypeVertexUpdateFunction extends VertexUpdateFunction<Long, ObjectMap, ObjectMap> {
  @Override
  public void updateVertex(Vertex<Long, ObjectMap> vertex,
                           MessageIterator<ObjectMap> inMessages) throws Exception {
    if (!vertex.getValue().containsKey(Utils.TMP_TYPE) && hasNoType(vertex.getValue())) {
      ObjectMap maxCcIdMap = null;

      // save already existing sim values
      HashMap<Long, Double> options;
      if (vertex.getValue().containsKey(Utils.VERTEX_OPTIONS)) {
        options = (HashMap<Long, Double>) vertex.getValue().get(Utils.VERTEX_OPTIONS);
      } else {
        options = Maps.newHashMap();
      }

      // get max sim from neighbors + vertex
      double bestSim = 0.0;
      for (ObjectMap msg : inMessages) {
        long vertexCcId = (long) vertex.getValue().get(Utils.HASH_CC);
        long neighborCcId = (long) msg.get(Utils.HASH_CC);
        if (vertexCcId != neighborCcId) {
          double newSim = (double) msg.get(Utils.AGGREGATED_SIM_VALUE);
          options.put((long) msg.get(Utils.VERTEX_ID), newSim);

          if (newSim > bestSim) {
            bestSim = newSim;
            if (msg.containsKey(Utils.TMP_TYPE) || !hasNoType(msg)) {
              maxCcIdMap = msg;
            } else if (hasNoType(msg)) {
              maxCcIdMap = neighborCcId < vertexCcId ? msg : vertex.getValue();
            }
          }
        } else {
          // cc is equal, neighbor options could have better similarity ...
          if (msg.containsKey(Utils.VERTEX_OPTIONS)) {
            HashMap<Long, Double> tmp = (HashMap<Long, Double>) msg.get(Utils.VERTEX_OPTIONS);
            for (Long key : tmp.keySet()) {
              options.put(key, tmp.get(key));
            }
//              LOG.info(tmp.toString());
          }

          // ... but both values cannot be next option, because already in same cc
          options.remove(vertex.getId());
          options.remove(msg.get(Utils.VERTEX_ID));
        }
      }

      // if neighbor is already in cc, neighbors of neighbors may be interesting
      for (Long key : options.keySet()) {
        if (options.get(key).equals(Collections.max(options.values()))) {
          for (ObjectMap msg : inMessages) {
            if (msg.get(Utils.VERTEX_ID).equals(key)) {
              maxCcIdMap = msg;
            }
          }
        }
      }

      // save new cc_id on vertex
      if (maxCcIdMap != null) {
        vertex.getValue().put(Utils.HASH_CC, maxCcIdMap.get(Utils.HASH_CC));
        vertex.getValue().put(Utils.VERTEX_OPTIONS, options);
        if (maxCcIdMap.containsKey(Utils.TMP_TYPE)) {
          vertex.getValue().put(Utils.TMP_TYPE, maxCcIdMap.get(Utils.TMP_TYPE));
        }
        if (maxCcIdMap.containsKey(Utils.TYPE_INTERN)) {
          vertex.getValue().put(Utils.TMP_TYPE, maxCcIdMap.get(Utils.TYPE_INTERN));
        }
        setNewVertexValue(vertex.getValue());
      }
    }
  }

  private boolean hasNoType(ObjectMap map) {
    return map.get(Utils.TYPE_INTERN).equals(Utils.NO_TYPE_AVAILABLE)
        || map.get(Utils.TYPE_INTERN).equals(Utils.NO_VALUE);
  }
}
