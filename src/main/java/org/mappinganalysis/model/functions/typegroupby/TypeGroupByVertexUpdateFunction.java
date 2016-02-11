package org.mappinganalysis.model.functions.typegroupby;

import com.google.common.collect.Maps;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.Collections;
import java.util.HashMap;

public class TypeGroupByVertexUpdateFunction extends VertexUpdateFunction<Long, ObjectMap, ObjectMap> {
  private static final Logger LOG = Logger.getLogger(TypeGroupByVertexUpdateFunction.class);

  @Override
  public void updateVertex(Vertex<Long, ObjectMap> vertex, MessageIterator<ObjectMap> inMessages)
      throws Exception {
    if (!vertex.getValue().containsKey(Utils.TMP_TYPE) && hasNoType(vertex.getValue())) {
      ObjectMap newBestValue = null;
      HashMap<Long, Double> options = initOptions(vertex);
      long vertexCcId = (long) vertex.getValue().get(Utils.HASH_CC);
      LOG.info("Working on vertex: " + vertexCcId);

      // get max sim from neighbors + vertex
      double bestSim = 0.0;
      for (ObjectMap msg : inMessages) {
        long neighborCcId = (long) msg.get(Utils.HASH_CC);
        LOG.info("Got message from: " + msg.get(Utils.VERTEX_ID));
        if (vertexCcId != neighborCcId) {
          double newSim = (double) msg.get(Utils.AGGREGATED_SIM_VALUE);
          options.put((long) msg.get(Utils.VERTEX_ID), newSim);

          if (newSim > bestSim) {
            bestSim = newSim;
            if (msg.containsKey(Utils.TMP_TYPE) || !hasNoType(msg)) {
              newBestValue = msg;
            } else if (hasNoType(msg)) {
              newBestValue = neighborCcId < vertexCcId ? msg : vertex.getValue();
            }
          }
        } else {
          // cc is equal, neighbor options could have better similarity ...
          addNeighborOptions(vertex, options, msg);
        }
      }

      // if neighbor is already in cc, neighbors of neighbors may be interesting
      for (Long key : options.keySet()) {
        if (options.get(key).equals(Collections.max(options.values()))) {
          for (ObjectMap msg : inMessages) {
            if (msg.get(Utils.VERTEX_ID).equals(key)) {
              newBestValue = msg;
            }
          }
        }
      }

      // save new cc_id on vertex
      if (newBestValue != null) {
        vertex.getValue().put(Utils.HASH_CC, newBestValue.get(Utils.HASH_CC));
        vertex.getValue().put(Utils.VERTEX_OPTIONS, options);
        if (newBestValue.containsKey(Utils.TMP_TYPE)) {
          vertex.getValue().put(Utils.TMP_TYPE, newBestValue.get(Utils.TMP_TYPE));
        }
        if (newBestValue.containsKey(Utils.TYPE_INTERN) && !hasNoType(newBestValue)) {
          vertex.getValue().put(Utils.TMP_TYPE, newBestValue.get(Utils.TYPE_INTERN));
        }
        LOG.info("### set new vert value: " + vertex.getValue());
        setNewVertexValue(vertex.getValue());
      }
    }
  }

  private HashMap<Long, Double> initOptions(Vertex<Long, ObjectMap> vertex) {
    HashMap<Long, Double> options;
    if (vertex.getValue().containsKey(Utils.VERTEX_OPTIONS)) {
      options = (HashMap<Long, Double>) vertex.getValue().get(Utils.VERTEX_OPTIONS);
    } else {
      options = Maps.newHashMap();
    }
    return options;
  }

  private void addNeighborOptions(Vertex<Long, ObjectMap> vertex, HashMap<Long, Double> options, ObjectMap msg) {
    if (msg.containsKey(Utils.VERTEX_OPTIONS)) {
      HashMap<Long, Double> tmp = (HashMap<Long, Double>) msg.get(Utils.VERTEX_OPTIONS);
      for (Long key : tmp.keySet()) {
        options.put(key, tmp.get(key));
      }
    }

    // ... but both values cannot be next option, because already in same cc
    options.remove(vertex.getId());
    options.remove(msg.get(Utils.VERTEX_ID));
  }

  private boolean hasNoType(ObjectMap map) {
    return map.get(Utils.TYPE_INTERN).equals(Utils.NO_TYPE_AVAILABLE)
        || map.get(Utils.TYPE_INTERN).equals(Utils.NO_VALUE);
  }
}
