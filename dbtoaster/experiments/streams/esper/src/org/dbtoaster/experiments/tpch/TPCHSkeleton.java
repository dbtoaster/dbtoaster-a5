package org.dbtoaster.experiments.tpch;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbtoaster.experiments.common.CommonSkeleton;
import org.dbtoaster.experiments.common.GenericSubscriber;
import org.dbtoaster.experiments.tpch.events.InsertCustomer;
import org.dbtoaster.experiments.tpch.events.InsertLineitem;
import org.dbtoaster.experiments.tpch.events.InsertNation;
import org.dbtoaster.experiments.tpch.events.InsertOrders;
import org.dbtoaster.experiments.tpch.events.InsertPart;
import org.dbtoaster.experiments.tpch.events.InsertPartsupp;
import org.dbtoaster.experiments.tpch.events.InsertRegion;
import org.dbtoaster.experiments.tpch.events.InsertSupplier;
import org.dbtoaster.experiments.tpch.events.UnifiedEvent;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventSender;
import com.espertech.esperio.csv.CSVInputAdapter;

public class TPCHSkeleton extends CommonSkeleton {

  private static final Log log = LogFactory.getLog(TPCHSkeleton.class);

  EventSender dispatch;
  /*
  EventSender insertLineitem;
  EventSender insertOrders;
  EventSender insertCustomer;
  EventSender insertSupplier;
  EventSender insertPart;
  EventSender insertPartsupp;
  EventSender insertNation;
  EventSender insertRegion;
  */
  
  public TPCHSkeleton() {
    super(getDefaultConfiguration());
    dispatch = runtime.getEventSender("Dispatch");
    /*
    insertLineitem = runtime.getEventSender("InsertLineitem");
    insertOrders = runtime.getEventSender("InsertOrders");
    insertCustomer = runtime.getEventSender("InsertCustomer");
    insertSupplier = runtime.getEventSender("InsertSupplier");
    insertPart = runtime.getEventSender("InsertPart");
    insertPartsupp = runtime.getEventSender("InsertPartsupp");
    insertNation = runtime.getEventSender("InsertNation");
    insertRegion = runtime.getEventSender("InsertRegion");
    */
  }
  
  public static Configuration getDefaultConfiguration() {
    Configuration config = new Configuration();
    config.getEngineDefaults().getLogging().setEnableQueryPlan(true);
    config.addEventType("Dispatch", UnifiedEvent.class);
    /*
    config.addEventType("InsertLineitem", InsertLineitem.class);
    config.addEventType("InsertOrders", InsertOrders.class);
    config.addEventType("InsertCustomer", InsertCustomer.class);
    config.addEventType("InsertSupplier", InsertSupplier.class);
    config.addEventType("InsertPart", InsertPart.class);
    config.addEventType("InsertPartsupp", InsertPartsupp.class);
    config.addEventType("InsertNation", InsertNation.class);
    config.addEventType("InsertRegion", InsertRegion.class);
    */
    return config;
  }

  static LinkedList<String> matchFileEvent(String baseDir, String fileName) {
    LinkedList<String> r = null;
    File f = new File(baseDir, fileName);
    if ( f.exists() ) {
      r = new LinkedList<String>();
      String baseName = f.getName();
      
      log.info("matching "+baseName);

      if ( baseName.contains("lineitem") ) {
        r.add("InsertLineitem");
        r.addAll(Arrays.asList(lineitemFields));
      }
      else if ( baseName.contains("orders") ) {
        r.add("InsertOrders");
        r.addAll(Arrays.asList(ordersFields));
      }
      else if ( baseName.contains("customer") ) {
        r.add("InsertCustomer");
        r.addAll(Arrays.asList(customerFields));
      }
      else if ( baseName.contains("supplier") ) {
        r.add("InsertSupplier");
        r.addAll(Arrays.asList(supplierFields));
      }
      else if ( baseName.contains("partsupp") ) {
        r.add("InsertPartsupp");
        r.addAll(Arrays.asList(partsuppFields));
      }
      else if ( baseName.contains("part") ) {
        r.add("InsertPart");
        r.addAll(Arrays.asList(partFields));
      }
      else if ( baseName.contains("nation") ) {
        r.add("InsertNation");
        r.addAll(Arrays.asList(nationFields));
      }
      else if ( baseName.contains("region") ) {
        r.add("InsertRegion");
        r.addAll(Arrays.asList(regionFields));
      }
    } else {
      log.error("invalid data file: "+f.getAbsolutePath());
    }
    return r;
  }
  
  public static void main(String[] args) {
    OptionParser parser = new OptionParser() { 
      {
        acceptsAll( Arrays.asList( "h", "?" ), "show help" );
        acceptsAll( Arrays.asList("q", "query") ).withRequiredArg().
          required().
          describedAs( "query script" ).
          ofType( String.class );

        acceptsAll( Arrays.asList("b", "basedir") ).withRequiredArg().
          describedAs( "Esper bin directory (where the dataset should be present)" ).
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

        acceptsAll( Arrays.asList("i","inputfile") ).withRequiredArg().
          describedAs( "input data files" ).
          ofType( String.class );
      }
    };    

    String queryFile = null;
    String baseDir = System.getProperty("user.dir");
    List<Integer> queryIds = new LinkedList<Integer>();
    int sampleFreq = 10;

    String unifiedFile = null;
    List<String> dataFiles = new LinkedList<String>();

    try {
      OptionSet options = parser.parse(args);
      if ( options.has("h") || options.has("?")
           || !(options.has("i") || options.has("u")) )
      {
        parser.printHelpOn(System.out);
        System.exit(1);
      }
      
      queryFile = (String) options.valueOf("q");
      for (Object o : options.valuesOf("r")) {
        queryIds.add((Integer) o);
      }

      if ( options.has( "s" ) && options.valueOf("s") != null )
        sampleFreq = (Integer) options.valueOf("s");

      if ( options.has( "b" ) && options.valueOf("b") != null )
        baseDir = (String) options.valueOf("b");

      if ( options.has( "u" ) && options.valueOf("u") != null )
        unifiedFile = (String) options.valueOf("u");

      for (Object o : options.valuesOf("i") ) {
        if ( o instanceof String ) dataFiles.add((String) o);
        else log.warn("invalid option type: "+o.getClass().getName());
      }
      
    } catch (IOException e) { e.printStackTrace(); }

    // Setup continuous queries and a subscriber to the last statement.
    TPCHSkeleton t = new TPCHSkeleton();

    List<EPStatement> epStmts = null;
    log.info("using script "+queryFile);
    epStmts = t.setupScript(queryFile);
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

    // Set up data files.
    List<CSVInputAdapter> sources = new LinkedList<CSVInputAdapter>();
    
    if ( unifiedFile != null ) {
      log.info("loading events from "+unifiedFile);
      sources.add(t.setupCSVSource(unifiedFile, "Dispatch", unifiedFields));
    }
    else
    {
      if ( !dataFiles.isEmpty() ) {
        for (String f : dataFiles) {
          LinkedList<String> evtAndFields = matchFileEvent(baseDir, f);
          if ( evtAndFields != null && !evtAndFields.isEmpty() ) {
            String evt = evtAndFields.pop();
            log.info("Found data for "+evt);
            sources.add(t.setupCSVSource(f, evt,
                evtAndFields.toArray(new String[0])));
          } else {
            log.error("failed to match event type for file "+f);
          }
        }
      } else {
        log.error("no data files found!");
      }
    }
    
    // Process data.
    for (CSVInputAdapter src : sources) { src.start(); }
  }
  
  
  private static final String[] lineitemFields = {
    "orderkey", "partkey", "suppkey",
    "linenumber", "quantity", "extendedprice", "discount", "tax",
    "returnflag", "linestatus",
    "shipdate", "commitdate", "receiptdate",
    "shipinstruct", "shipmode", "comment",
  };
  
  private static final String[] ordersFields = {
    "orderkey", "custkey",
    "orderstatus", "totalprice", "orderdate",
    "orderpriority", "clerk", "shippriority", "comment"
  };
  
  private static final String[] customerFields = {
    "custkey", "name", "address", "nationkey",
    "phone", "acctbal", "mktsegment", "comment"
  };
  
  private static final String[] supplierFields = {
    "suppkey", "name", "address", "nationkey",
    "phone", "acctbal", "comment"     
  };
  
  private static final String[] partFields = {
    "partkey", "name", "mfgr", "brand", "type", "size",
    "container", "retailprice", "comment"
  };
  
  private static final String[] partsuppFields = {
    "partkey", "suppkey", "availqty", "supplycost", "comment"
  };
  
  private static final String[] nationFields = {
    "nationkey", "name", "regionkey", "comment"
  };
  
  private static final String[] regionFields = {
    "regionkey", "name", "comment"
  };
  
  private static final String[] unifiedFields = {
    "streamname", "event",
    "acctbal", "address", "availqty", "brand",
    "clerk", "comment", "commitdate", "container", "custkey",
    "discount", "extendedprice", "linenumber", "linestatus",
    "mfgr", "mktsegment", "name", "nationkey",
    "orderdate", "orderkey", "orderpriority", "orderstatus",
    "partkey", "phone", "quantity",
    "receiptdate", "regionkey", "retailprice", "returnflag",
    "shipdate", "shipinstruct", "shipmode", "shippriority", "size", "suppkey", "supplycost",
    "tax", "totalprice", "type"
  };
}
