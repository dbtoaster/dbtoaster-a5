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

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventSender;
import com.espertech.esperio.csv.CSVInputAdapter;

public class TPCHSkeleton extends CommonSkeleton {

  private static final Log log = LogFactory.getLog(TPCHSkeleton.class);

  EventSender insertLineitem;
  EventSender insertOrders;
  EventSender insertCustomer;
  EventSender insertSupplier;
  EventSender insertPart;
  EventSender insertPartsupp;
  EventSender insertNation;
  EventSender insertRegion;
  
  public TPCHSkeleton() {
    super(getDefaultConfiguration());
    insertLineitem = runtime.getEventSender("InsertLineitem");
    insertOrders = runtime.getEventSender("InsertOrders");
    insertCustomer = runtime.getEventSender("InsertCustomer");
    insertSupplier = runtime.getEventSender("InsertSupplier");
    insertPart = runtime.getEventSender("InsertPart");
    insertPartsupp = runtime.getEventSender("InsertPartsupp");
    insertNation = runtime.getEventSender("InsertNation");
    insertRegion = runtime.getEventSender("InsertRegion");
  }
  
  public static Configuration getDefaultConfiguration() {
    Configuration config = new Configuration();
    config.addEventType("InsertLineitem", InsertLineitem.class);
    config.addEventType("InsertOrders", InsertOrders.class);
    config.addEventType("InsertCustomer", InsertCustomer.class);
    config.addEventType("InsertSupplier", InsertSupplier.class);
    config.addEventType("InsertPart", InsertPart.class);
    config.addEventType("InsertPartsupp", InsertPartsupp.class);
    config.addEventType("InsertNation", InsertNation.class);
    config.addEventType("InsertRegion", InsertRegion.class);
    return config;
  }

  static LinkedList<String> matchFileEvent(String fileName) {
    LinkedList<String> r = null;
    File f = new File(fileName);
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
      else if ( baseName.contains("part") ) {
        r.add("InsertPart");
        r.addAll(Arrays.asList(partFields));
      }
      else if ( baseName.contains("partsupp") ) {
        r.add("InsertPartsupp");
        r.addAll(Arrays.asList(partsuppFields));
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
        acceptsAll( Arrays.asList("i","inputfile") ).withRequiredArg().
          required().
          describedAs( "input data files" ).
          ofType( String.class );
      }
    };    

    String queryFile = null;
    List<String> dataFiles = new LinkedList<String>();

    try {
      OptionSet options = parser.parse(args);
      if ( options.has("h") || options.has("?") || !options.has("i") )
      {
        parser.printHelpOn(System.out);
      }
      
      queryFile = (String) options.valueOf("q");
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
      GenericSubscriber subscriber = new GenericSubscriber(1);
      epStmts.get(epStmts.size()-1).setSubscriber(subscriber);
    }

    // Push data files.
    if ( !dataFiles.isEmpty() ) {
      List<CSVInputAdapter> sources = new LinkedList<CSVInputAdapter>();

      for (String f : dataFiles) {
        LinkedList<String> evtAndFields = matchFileEvent(f);
        if ( evtAndFields != null && !evtAndFields.isEmpty() ) {
          String evt = evtAndFields.pop();
          log.info("Found data for "+evt);
          sources.add(t.setupCSVSource(f, evt,
              evtAndFields.toArray(new String[0])));
        } else {
          log.error("failed to match event type for file "+f);
        }
      }

      for (CSVInputAdapter src : sources) { src.start(); }

    } else {
      log.error("no data files found!");
    }
  }
  
  
  private static final String[] lineitemFields = {
    "orderkey", "partkey", "suppkey",
    "linenumber", "quantity", "extendedprice", "discount", "tax",
    "returnflag", "linestatus",
    "shipDate", "commitDate", "receiptDate",
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
  
}
