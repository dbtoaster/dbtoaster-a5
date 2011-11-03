package org.dbtoaster.experiments.clustermgmt;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbtoaster.experiments.clustermgmt.events.UnifiedEvent;
import org.dbtoaster.experiments.common.CommonSkeleton;
import org.dbtoaster.experiments.common.GenericSubscriber;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventSender;
import com.espertech.esperio.csv.CSVInputAdapter;

public class ClusterMgmtSkeleton extends CommonSkeleton {

  private static final Log log = LogFactory.getLog(ClusterMgmtSkeleton.class);

  EventSender dispatch;

  public ClusterMgmtSkeleton() {
    super(getDefaultConfiguration());
    dispatch = runtime.getEventSender("Dispatch");
  }
  
  public static Configuration getDefaultConfiguration() {
    Configuration config = new Configuration();
    config.getEngineDefaults().getLogging().setEnableQueryPlan(true);
    config.addEventType("Dispatch", UnifiedEvent.class);
    return config;
  }
  
  public static void main(String[] args) throws InterruptedException {
    OptionParser parser = new OptionParser() { 
      {
        acceptsAll( Arrays.asList( "h", "?" ), "show help" );
        acceptsAll( Arrays.asList("q", "query") ).withRequiredArg().
          required().
          describedAs( "query script" ).
          ofType( String.class );
        
        acceptsAll( Arrays.asList("s", "sample") ).withRequiredArg().
          describedAs( "output sample frequency" ).
          ofType( Integer.class );

        acceptsAll( Arrays.asList("r", "result") ).withRequiredArg().
          required().
          describedAs( "result query id (from the last)" ).
          ofType ( Integer.class );

        acceptsAll( Arrays.asList("u", "unified") ).withRequiredArg().
          describedAs( "unified events file" ).
          ofType( String.class );
        
        acceptsAll( Arrays.asList("f", "field") ).withRequiredArg().
          describedAs( "field order" ).
          ofType( String.class );
      }
    };
    
    String queryFile = null;
    List<Integer> queryIds = new LinkedList<Integer>();
    int sampleFreq = 10;
    
    String unifiedFile = null;

    String[] unifiedFieldOrder = { "eventType", "rackid", "load" };

    try {
      OptionSet options = parser.parse(args);
      
      boolean hasInputs = options.valueOf("u") != null ;

      if ( options.has("h") || options.has("?") || !hasInputs ) {
        parser.printHelpOn(System.out);
        System.exit(1);
      }
      
      queryFile = (String) options.valueOf("q");
      for (Object o : options.valuesOf("r")) {
        queryIds.add((Integer) o);
      }

      if ( options.has( "u" ) && options.valueOf("u") != null )
        unifiedFile = (String) options.valueOf("u");

      if ( options.has( "s" ) && options.valueOf("s") != null )
        sampleFreq = (Integer) options.valueOf("s");

      if ( options.has("f") ) {
        unifiedFieldOrder = options.valuesOf("f").toArray(new String[0]);
        log.info("Overriding field order to: "+unifiedFieldOrder);
      }

    } catch (IOException e) { e.printStackTrace(); }
    
    ClusterMgmtSkeleton s = new ClusterMgmtSkeleton();

    log.info("using script "+queryFile);
    List<EPStatement> epStmts = null;
    epStmts = s.setupScript(queryFile);
    if ( !epStmts.isEmpty() ) {
      for (Integer queryId : queryIds) {
        if ( queryId <= 0 || queryId > epStmts.size() ) {
          log.error("invalid query id: "+queryId);
          System.exit(1);
        }
  
        EPStatement stmt = epStmts.get(epStmts.size()-queryId);
        log.info("Logging query(-"+queryId+"): "+stmt.getText());
        GenericSubscriber subscriber = new GenericSubscriber(sampleFreq);
        stmt.setSubscriber(subscriber);
      }
    }
    
    if ( unifiedFile != null ) {
      log.info("loading events from "+unifiedFile);
      CSVInputAdapter src = s.setupCSVSource(unifiedFile, "Dispatch", unifiedFieldOrder);
      src.start();
    }
  }

}
