package org.mappinganalysis.model.functions.incremental;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.config.Config;

/**
 * Create representatives for vertices within incremental setting.
 * Adds blocking label, clustered vertices and cluster sources to representative.
 */
public class RepresentativeCreator
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(RepresentativeCreator.class);

  private DataSet<Vertex<Long, ObjectMap>> initialVertices;
  private DataDomain domain;
  private BlockingStrategy blockingStrategy;

  public RepresentativeCreator(DataDomain domain, BlockingStrategy blockingStrategy) {
    this.domain = domain;
    this.blockingStrategy = blockingStrategy;
  }

  /**
   * Create representatives for vertices within incremental setting.
   * Adds blocking label, clustered vertices and cluster sources to representative.
   */
  public RepresentativeCreator(Config config) {
    this.domain = config.getDataDomain();
    this.blockingStrategy = config.getBlockingStrategy();
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    this.initialVertices = inputData;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> createResult() {
    return initialVertices
        .map(new IntermediateVertexReprMapFunction(domain, blockingStrategy));
  }

}
