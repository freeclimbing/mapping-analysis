package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.GeoDistance;

import java.math.BigDecimal;

/**
 * Return triple including the distance between 2 geo points as edge value.
 */
public class GeoCodeSimMapper implements MapFunction<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(GeoCodeSimMapper.class);
  private final double maxDistInMeter;

  public GeoCodeSimMapper(double maxDistInMeter) {
    this.maxDistInMeter = maxDistInMeter;
  }

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long,
      ObjectMap, NullValue> triplet) throws Exception {
    ObjectMap source = triplet.getSrcVertex().getValue();
    ObjectMap target = triplet.getTrgVertex().getValue();

    Double distance = GeoDistance.distance(source.getLatitude(),
        source.getLongitude(), target.getLatitude(), target.getLongitude());

    ObjectMap property = new ObjectMap();
    if (distance >= maxDistInMeter) {
      property.put(Constants.SIM_DISTANCE, 0D);
    } else {
      BigDecimal tmpResult = null;
      double tmp = 1D - (distance / maxDistInMeter);
      tmpResult = new BigDecimal(tmp);
      double result = tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
      property.put(Constants.SIM_DISTANCE, result);
    }

    if (LOG.isDebugEnabled()) {
      LOG.info("geoSim: " + triplet.getSrcVertex().getId() + " "
          + triplet.getTrgVertex().getId() + " " + property.get(Constants.SIM_DISTANCE));
    }

    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            property));
  }
}
