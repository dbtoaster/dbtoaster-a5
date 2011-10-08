package org.dbtoaster.experiments.finance;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbtoaster.experiments.common.CommonSkeleton;
import org.dbtoaster.experiments.common.GenericSubscriber;
import org.dbtoaster.experiments.finance.events.DeleteAsksEntry;
import org.dbtoaster.experiments.finance.events.DeleteBidsEntry;
import org.dbtoaster.experiments.finance.events.InsertAsksEntry;
import org.dbtoaster.experiments.finance.events.InsertBidsEntry;
import org.dbtoaster.experiments.finance.events.OrderBookEntry;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventSender;
import com.espertech.esperio.csv.CSVInputAdapter;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class OrderBookSkeleton extends CommonSkeleton {

  private static final Log log = LogFactory.getLog(OrderBookSkeleton.class);

  EventSender insertBids;
  EventSender insertAsks;
  EventSender deleteBids;
  EventSender deleteAsks;

  public OrderBookSkeleton() {
    super(getDefaultConfiguration());
    insertBids = runtime.getEventSender("InsertBids");
    insertAsks = runtime.getEventSender("InsertAsks");
    deleteBids = runtime.getEventSender("DeleteBids");
    deleteAsks = runtime.getEventSender("DeleteAsks");
  }
  
  public static Configuration getDefaultConfiguration() {
    Configuration config = new Configuration();
    config.addEventType("InsertBids", InsertBidsEntry.class);
    config.addEventType("InsertAsks", InsertAsksEntry.class);
    config.addEventType("DeleteBids", DeleteBidsEntry.class);
    config.addEventType("DeleteAsks", DeleteAsksEntry.class);
    return config;
  }
  
  public class EsperTest {
    String[] testCommon = {
      "create window Bids.win:keepall() as "+
      "(timestamp double, orderId int, brokerId int, price double, volume double)",

      "insert into Bids "+
      "select timestamp, orderId, brokerId, price, volume from InsertBids",
    
      "on DeleteBids as old "+
      "delete from Bids where Bids.orderId = old.orderId "+
      "and Bids.timestamp=old.timestamp"
    };
    
    String[] contentOnInsertTrigger = {
      "on Bids as trig select b1.* from Bids as b1"
    };
    
    String[] contentOnDeleteTrigger = {
      "on DeleteBids select b1.* from Bids as b1",
    };

    String[] queryOnInsertTrigger = {
      "on Bids "+
      "select sum(b1.price*b1.volume) as vwap from Bids as b1 "+
      "where (select sum(b3.volume) from Bids as b3) > "+
         "(select sum(b2.volume) from Bids as b2 where b2.price > b1.price)"
    };

    // Fails.
    String[] queryOnDeleteTrigger = {
      "on DeleteBids "+
      "select sum(b1.price*b1.volume) as vwap from Bids as b1 "+
      "where (select sum(b3.volume) from Bids as b3) > "+
         "(select sum(b2.volume) from Bids as b2 where b2.price > b1.price)",
    };
    
    // Bug examples
    String[] bugExampleTriggers = {
      // Esper subquery bug example. Two volumes are different, even though
      // they access the same aliased table.
      "on DeleteBids "+
      "select sum(b1.volume), (select sum(b2.volume) from Bids as b2) from Bids as b1",
      
      // Esper does not support joins in triggers.
      /*
      "on DeleteBids "+
      "select * from Bids as b1, Bids as b2 where b2.price > b1.price"
      */
      
      // More nested query bugs, the istream has equal sums, but the rstream
      // has different sums.
      "select irstream sum(b1.volume), "+
      "  (select sum(b2.volume) from Bids as b2) from Bids as b1",
    };
  };
  
  public void generate(boolean bids) {
    LinkedList<OrderBookEntry> entries = new LinkedList<OrderBookEntry>();
    for (int i = 0; i < 10; ++i) {
      OrderBookEntry e = null;
      if ( bids ) { e = new InsertBidsEntry(InsertBidsEntry.randomEntry()); }
      else { e = new InsertAsksEntry(InsertAsksEntry.randomEntry()); }
      log.info("inserting "+e);
      if ( bids ) insertBids.sendEvent(e);
      else insertAsks.sendEvent(e);
      entries.addFirst(e);
    }

    for (OrderBookEntry e : entries) {
      log.info("deleting "+e);
      if ( bids ) deleteBids.sendEvent(new DeleteBidsEntry(e));
      else deleteAsks.sendEvent(new DeleteAsksEntry(e));
    }
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

        acceptsAll( Arrays.asList("ib", "insertbids") ).withRequiredArg().
          describedAs( "insert bids file" ).
          ofType( String.class );

        acceptsAll( Arrays.asList("ia", "insertasks") ).withRequiredArg().
          describedAs( "insert asks file" ).
          ofType( String.class );

        acceptsAll( Arrays.asList("db", "deletebids") ).withRequiredArg().
          describedAs( "delete bids file" ).
          ofType( String.class );

        acceptsAll( Arrays.asList("da", "deleteasks") ).withRequiredArg().
          describedAs( "delete asks file" ).
          ofType( String.class );
        
        acceptsAll( Arrays.asList("f", "field") ).withRequiredArg().
          describedAs( "field order" ).
          ofType( String.class );
      }
    };
    
    String queryFile = null;
    int queryId = -1;
    int sampleFreq = 10;
    
    String insertBidsFile = null;
    String insertAsksFile = null;
    String deleteBidsFile = null;
    String deleteAsksFile = null;

    String[] fieldOrder = { "timestamp", "orderId", "brokerId", "volume", "price" };

    try {
      OptionSet options = parser.parse(args);
      if ( options.has("h") || options.has("?") ||
           (options.valueOf("ib") == null && options.valueOf("ia") == null) )
      {
        parser.printHelpOn(System.out);
      }
      
      queryFile = (String) options.valueOf("q");
      queryId = (Integer) options.valueOf("r");
      
      if ( options.has( "s" ) && options.valueOf("s") != null )
        sampleFreq = (Integer) options.valueOf("s");

      if ( options.has("ib") && options.valueOf("ib") != null )
        insertBidsFile = (String) options.valueOf("ib");
      
      if ( options.has("ia") && options.valueOf("ia") != null )
        insertAsksFile = (String) options.valueOf("ia");
      
      if ( options.has("db") && options.valueOf("db") != null )
        deleteBidsFile = (String) options.valueOf("db");
      
      if ( options.has("da") && options.valueOf("da") != null )
        deleteAsksFile = (String) options.valueOf("da");

      if ( options.has("f") ) {
        fieldOrder = options.valuesOf("f").toArray(new String[0]);
        log.info("Overriding field order to: "+fieldOrder);
      }

    } catch (IOException e) { e.printStackTrace(); }
    
    OrderBookSkeleton s = new OrderBookSkeleton();

    List<EPStatement> epStmts = null;

    /*
    List<String> stmts = new LinkedList<String>();
    EsperTest t = s.new EsperTest();
    stmts.addAll(Arrays.asList(t.testCommon));
    stmts.addAll(Arrays.asList(t.queryOnInsertTrigger));
    epStmts = s.setupStatements(stmts); 
    */
    
    log.info("using script "+queryFile);
    epStmts = s.setupScript(queryFile);
    if ( !epStmts.isEmpty() ) {
      if ( queryId <= 0 || queryId > epStmts.size() ) {
        log.error("invalid query id: "+queryId);
        System.exit(1);
      }

      GenericSubscriber subscriber = new GenericSubscriber(sampleFreq);
      epStmts.get(epStmts.size()-queryId).setSubscriber(subscriber);
    }
    
    if ( !(insertBidsFile == null && insertAsksFile == null 
           && deleteBidsFile == null && deleteAsksFile == null) )
    {
      List<CSVInputAdapter> sources = new LinkedList<CSVInputAdapter>();
      
      if ( insertBidsFile != null ) {
        log.info("loading insert bids from "+insertBidsFile);
        sources.add(s.setupCSVSource(insertBidsFile, "InsertBids", fieldOrder));
      }
      
      if ( insertAsksFile != null ) {
        log.info("loading insert asks from "+insertAsksFile);
        sources.add(s.setupCSVSource(insertAsksFile, "InsertAsks", fieldOrder));
      }

      if ( deleteBidsFile != null ) {
        log.info("loading delete bids from "+deleteBidsFile);
        sources.add(s.setupCSVSource(deleteBidsFile, "DeleteBids", fieldOrder));
      }
      
      if ( deleteAsksFile != null ) {
        log.info("loading delete asks from "+deleteAsksFile);
        sources.add(s.setupCSVSource(deleteAsksFile, "DeleteAsks", fieldOrder));
      }

      for ( CSVInputAdapter src : sources ) { src.start(); }

    } else {
      s.generate(true);
      s.generate(false);
    }
  }
}
