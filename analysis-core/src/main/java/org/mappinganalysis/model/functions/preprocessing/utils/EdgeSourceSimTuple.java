package org.mappinganalysis.model.functions.preprocessing.utils;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 *  Tuple representation for an edge with contained information
 *  - cc id
 *  - source vertex id
 *  - target vertex id
 *  - starting vertex data source(s) (int)
 *  - neighbor vertex data source(s) (int)
 *  - edge similarity
 */
public class EdgeSourceSimTuple
    extends Tuple6<Long, Long, Long, Integer, Integer, Double> {
  public EdgeSourceSimTuple() { // needed
  }

  EdgeSourceSimTuple(Long ccId, Long source, Long target, int srcDatasource, int trgDataSource, Double edgeSim) {
    this.f0 = ccId;
    this.f1 = source;
    this.f2 = target;
    this.f3 = srcDatasource;
    this.f4 = trgDataSource;
    this.f5 = edgeSim;
  }

  public long getCcId() {
    return f0;
  }

  public long getSrcId() {
    return f1;
  }

  public long getTrgId() {
    return f2;
  }

  int getSrcDataSource() {
    return f3;
  }

  int getTrgDataSource() {
    return f4;
  }

  public double getLinkSim() {
    return f5;
  }
}
