package org.mappinganalysis.io.debug;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.NeighborTuple;

public class PrintNeighborTuple extends Printer<NeighborTuple> {
  private static final Logger LOG = Logger.getLogger(PrintNeighborTuple.class);

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public PrintNeighborTuple(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(NeighborTuple n) {
    return String.format("(%s,%s,%s,%s)",
        n.getVertexId(),
        n.getSimilarity(),
        n.getTypes(),
        n.getCompId());
  }

  @Override
  protected Logger getLogger() {
    LOG.setLevel(Level.DEBUG);
    return LOG;
  }
}
