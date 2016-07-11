package org.mappinganalysis.model.functions.typegroupby;

import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.HashMap;

/**
 * @deprecated
 */
public class TypeGroupByVertexUpdateFunction extends VertexUpdateFunction<Long, ObjectMap, ObjectMap> {
  private static final Logger LOG = Logger.getLogger(TypeGroupByVertexUpdateFunction.class);

  @Override
  public void updateVertex(Vertex<Long, ObjectMap> vertex, MessageIterator<ObjectMap> inMessages)
      throws Exception {
//    LOG.info("potentiallyUpdatingVertex: " + vertex.toString());
    if (!vertex.getValue().containsKey(Utils.TMP_TYPE) && vertex.getValue().hasTypeNoType(Utils.COMP_TYPE)) {
      HashMap<Long, Double> options = initOptions(vertex);

      ObjectMap newBestValue = findNewBestValue(vertex, inMessages, options);

      // save new cc_id on vertex
      if (!newBestValue.isEmpty()) {
        updateVertexValue(vertex, newBestValue, options);
//          LOG.info("### set vert " + vertex.getId() + " value to: " + vertex.getValue());
        setNewVertexValue(vertex.getValue());
      }
    }
  }

  private ObjectMap findNewBestValue(Vertex<Long, ObjectMap> vertex,
                                     MessageIterator<ObjectMap> inMessages, HashMap<Long, Double> options) {
    ObjectMap newBestValue = new ObjectMap();
    long vertexCcId = vertex.getValue().getHashCcId();

//    LOG.info("findNewBestValue on vertexCCid: " + vertexCcId + " vertex: " + vertex.getId());

    double bestSim = 0D;
    // get max sim from neighbors + vertex
    for (ObjectMap msg : inMessages) {
      long msgCcId = msg.getHashCcId();
//      LOG.info("vertex " + vertex.getId() + " got message from: " + msg.get(Utils.VERTEX_ID) + " " + msg.getHashCcId());

      if (vertexCcId != msgCcId) {
        double newSim = (double) msg.get(Utils.AGGREGATED_SIM_VALUE);
//        options.put((long) msg.get(Utils.VERTEX_ID), newSim);

        boolean isSpecialCondition = newBestValue.hasTypeNoType(Utils.COMP_TYPE)
            && Doubles.compare(bestSim, 0D) != 0
            && Doubles.compare(newSim, bestSim) == 0;

//        if (isSpecialCondition) {
//          LOG.info("isSpecialCondition vertex: " + vertex.toString());
//          LOG.info("isSpecialCondition bestValue: " + newBestValue.toString());
//        }
        // does happen, check!?
//        Preconditions.checkArgument(!isSpecialCondition, "if ever true, fail and check (for big dataset)");

//        LOG.info("Doubles.compare(newSim, bestSim) > 0 - " + (Doubles.compare(newSim, bestSim) > 0));
//
//        LOG.info("newBestValue.hasTypeNoType(Utils.COMP_TYPE) - " + newBestValue.hasTypeNoType(Utils.COMP_TYPE));
//        LOG.info("Doubles.compare(bestSim, 0D) != 0 - " + (Doubles.compare(bestSim, 0D) != 0));
//        LOG.info("Doubles.compare(newSim, bestSim) == 0) - " + (Doubles.compare(newSim, bestSim) == 0));
//
//        LOG.info("!newBestValue.isEmpty() - " + !newBestValue.isEmpty());
//        LOG.info("Doubles.compare(newSim, bestSim) == 0 - " + (Doubles.compare(newSim, bestSim) == 0));
//        if (!newBestValue.isEmpty()) {
//          LOG.info("msgCcId < newBestValue.getHashCcId() - " + (msgCcId < newBestValue.getHashCcId()) + " " + msgCcId + " " + newBestValue.getHashCcId());
//        }

        // if newSim equals bestSim, we always want to choose the same result (lowest cc id), not the first msg cc id
        // if no type is given, the lowest cc id of msg and vertex is chosen
        if (Doubles.compare(newSim, bestSim) > 0 || isSpecialCondition) {
          bestSim = newSim;
          if (msg.containsKey(Utils.TMP_TYPE) || !msg.hasTypeNoType(Utils.COMP_TYPE)) {
//            LOG.info("tmp type or has type: " + msg);
            newBestValue = msg;
          } else if (msg.hasTypeNoType(Utils.COMP_TYPE)) {
//            LOG.info("has no type: " + msg);
            newBestValue = msgCcId < vertexCcId ? msg : vertex.getValue();
//            LOG.info("nbv has no type: " + newBestValue.toString());
          }
        } else if (msg.containsKey(Utils.TMP_TYPE) || !msg.hasTypeNoType(Utils.COMP_TYPE)
            && !newBestValue.isEmpty() // same similarity, both have types / tmp types, take lowest hashCcId
            && Doubles.compare(newSim, bestSim) == 0
            && msgCcId < newBestValue.getHashCcId()) {
//          LOG.info("same sim take lower cc: " + msg);
          newBestValue = msg;
        }
      } else {
        // cc is equal, neighbor options could have better similarity ...
//        addNeighborOptions(vertex, options, msg);
      }
    }
    return newBestValue;
  }

  private void updateVertexValue(Vertex<Long, ObjectMap> vertex,
                                 ObjectMap newBestValue, HashMap<Long, Double> options) {
    vertex.getValue().put(Utils.HASH_CC, newBestValue.getHashCcId());
//    vertex.getValue().put(Utils.VERTEX_OPTIONS, options);
    if (newBestValue.containsKey(Utils.TMP_TYPE)) {
      vertex.getValue().put(Utils.TMP_TYPE, newBestValue.get(Utils.TMP_TYPE));
    }
    if (newBestValue.containsKey(Utils.COMP_TYPE) && !newBestValue.hasTypeNoType(Utils.COMP_TYPE)) {
      vertex.getValue().put(Utils.TMP_TYPE, newBestValue.get(Utils.COMP_TYPE));
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
}
