package org.mappinganalysis.model.functions;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Aggregate all similarity values based on weight based metric.
 */
public class AggSimValueEdgeMapFunction implements MapFunction<Edge<Long, ObjectMap>, Edge<Long, ObjectMap>> {

  private static final Logger LOG = Logger.getLogger(AggSimValueEdgeMapFunction.class);

  double trigramWeight = 0.45;
  double typeWeight = 0.25;
  double geoWeight = 0.3;

  @Override
  public Edge<Long, ObjectMap> map(Edge<Long, ObjectMap> edge) throws Exception {
    ObjectMap value = edge.getValue();

    double aggregatedSim;
    if (value.containsKey(Utils.TRIGRAM)) {
      aggregatedSim = trigramWeight * (float) value.get(Utils.TRIGRAM);
    } else {
      aggregatedSim = 0;
    }
    if (value.containsKey(Utils.TYPE_MATCH)) {
      aggregatedSim += typeWeight * (float) value.get(Utils.TYPE_MATCH);
    }
    if (value.containsKey(Utils.DISTANCE)) {
      Object object = value.get(Utils.DISTANCE);
      if (object instanceof Double) {
        aggregatedSim += (Double) object * geoWeight;
      } else {
        LOG.info("Error" + object.getClass().toString());
      }
//      aggregatedSim += geoWeight * (Double) value.get(Utils.DISTANCE); // TODO why is this not working?
    }

    value.put(Utils.AGGREGATED_SIM_VALUE, aggregatedSim);
    return edge;
  }
}
