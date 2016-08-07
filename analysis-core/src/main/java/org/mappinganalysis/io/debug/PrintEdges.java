package org.mappinganalysis.io.debug;

import org.apache.flink.graph.Edge;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

public class PrintEdges extends Printer<Edge<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(PrintEdges.class);

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public PrintEdges(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(Edge<Long, ObjectMap> edge) {
    return String.format("(%s,%s,%s)",
        edge.getSource(),
        edge.getTarget(),
        edge.getValue().toString());
  }

  @Override
  protected Logger getLogger() {
    LOG.setLevel(Level.DEBUG);
    return LOG;
  }
}
