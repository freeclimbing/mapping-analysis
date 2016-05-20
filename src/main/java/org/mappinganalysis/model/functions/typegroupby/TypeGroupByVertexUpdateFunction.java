package org.mappinganalysis.model.functions.typegroupby;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class TypeGroupByVertexUpdateFunction extends VertexUpdateFunction<Long, ObjectMap, ObjectMap> {
  private static final Logger LOG = Logger.getLogger(TypeGroupByVertexUpdateFunction.class);
  private ArrayList<Long> vertexList = Lists.newArrayList(2434L, 1134L, 1135L, 7214L);

  @Override
  public void updateVertex(Vertex<Long, ObjectMap> vertex, MessageIterator<ObjectMap> inMessages)
      throws Exception {
    if (!vertex.getValue().containsKey(Utils.TMP_TYPE) && vertex.getValue().hasNoType(Utils.COMP_TYPE)) {
      HashMap<Long, Double> options = initOptions(vertex);

      ObjectMap newBestValue = findNewBestValue(vertex, inMessages, options);

      // TODO check for big dataset if needed
//      // if neighbor is already in cc, neighbors of neighbors may be interesting
//      for (Long key : options.keySet()) {
//        if (options.get(key).equals(Collections.max(options.values()))) {
//          for (ObjectMap msg : inMessages) {
//            if (msg.get(Utils.VERTEX_ID).equals(key)) {
//              Preconditions.checkArgument(false, msg);
//              newBestValue = msg;
//            }
//          }
//        }
//      }

      // save new cc_id on vertex
      if (!newBestValue.isEmpty()) {
        updateVertexValue(vertex, newBestValue, options);
        if (vertexList.contains(vertex.getId()))
          LOG.info("### set vert value to: " + vertex.getValue());
        setNewVertexValue(vertex.getValue());
      }
    }
  }

  private ObjectMap findNewBestValue(Vertex<Long, ObjectMap> vertex,
                                     MessageIterator<ObjectMap> inMessages, HashMap<Long, Double> options) {
    ObjectMap newBestValue = new ObjectMap();
    long vertexCcId = vertex.getValue().getHashCcId();

    if (vertexList.contains(vertex.getId()))
      LOG.info("Working on vertexCCid: " + vertexCcId + " (vertex: " + vertex.getId());

    double bestSim = 0D;
    // get max sim from neighbors + vertex
    for (ObjectMap msg : inMessages) {
      long msgCcId = msg.getHashCcId();
      if (vertexList.contains(vertex.getId()))
        LOG.info("Got message from: " + msg.get(Utils.VERTEX_ID) + " " + msg.get(Utils.AGGREGATED_SIM_VALUE));

      if (vertexCcId != msgCcId) {
        double newSim = (double) msg.get(Utils.AGGREGATED_SIM_VALUE);
//        options.put((long) msg.get(Utils.VERTEX_ID), newSim);

        boolean isSpecialCondition = newBestValue.hasNoType(Utils.COMP_TYPE) && Doubles.compare(bestSim, 0D) != 0
            && Doubles.compare(newSim, bestSim) == 0;
        if (isSpecialCondition) {
          LOG.info("isSpecialCondition vertex: " + vertex.toString());
          LOG.info("isSpecialCondition bestValue: " + newBestValue.toString());
        }
        // does happen, check!?
//        Preconditions.checkArgument(!isSpecialCondition, "if ever true, fail and check (for big dataset)");

        LOG.info("Doubles.compare(newSim, bestSim) > 0 - " + (Doubles.compare(newSim, bestSim) > 0));

        LOG.info("newBestValue.hasNoType(Utils.COMP_TYPE) - " + newBestValue.hasNoType(Utils.COMP_TYPE));
        LOG.info("Doubles.compare(bestSim, 0D) != 0 - " + (Doubles.compare(bestSim, 0D) != 0));
        LOG.info("Doubles.compare(newSim, bestSim) == 0) - " + (Doubles.compare(newSim, bestSim) == 0));

        LOG.info("!newBestValue.isEmpty() - " + !newBestValue.isEmpty());
        LOG.info("Doubles.compare(newSim, bestSim) == 0 - " + (Doubles.compare(newSim, bestSim) == 0));
        if (!newBestValue.isEmpty()) {
          LOG.info("msgCcId < newBestValue.getHashCcId() - " + (msgCcId < newBestValue.getHashCcId()) + " " + msgCcId + " " + newBestValue.getHashCcId());
        }
        // if newSim equals bestSim, we always want to choose the same result (lowest cc id), not the first msg cc id
        // if no type is given, the lowest cc id of msg and vertex is chosen
        if (Doubles.compare(newSim, bestSim) > 0 || isSpecialCondition) {
          bestSim = newSim;
          if (msg.containsKey(Utils.TMP_TYPE) || !msg.hasNoType(Utils.COMP_TYPE)) {
            if (vertexList.contains(vertex.getId()))
              LOG.info("tmp type or has type: " + msg);
            newBestValue = msg;
          } else if (msg.hasNoType(Utils.COMP_TYPE)) {
            if (vertexList.contains(vertex.getId()))
              LOG.info("has no type: " + msg);
            // triggers
//            Preconditions.checkArgument(false, "has no type case " + msg.toString() + " vertex: " + vertex.toString()
//            + " newbestvalue: " + newBestValue.toString());
            newBestValue = msgCcId < vertexCcId ? msg : vertex.getValue();
          }
        } else if ((msg.containsKey(Utils.TMP_TYPE) || !msg.hasNoType(Utils.COMP_TYPE))
            && !newBestValue.isEmpty() // same similarity, both have types / tmp types, take lowest hashCcId
            && Doubles.compare(newSim, bestSim) == 0
            && msgCcId < newBestValue.getHashCcId()) {
            if (vertexList.contains(vertex.getId()))
              LOG.info("same sim take lower cc: " + msg);
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
    if (newBestValue.containsKey(Utils.COMP_TYPE) && !newBestValue.hasNoType(Utils.COMP_TYPE)) {
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
