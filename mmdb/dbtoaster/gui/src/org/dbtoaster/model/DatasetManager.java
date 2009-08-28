package org.dbtoaster.model;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

// Class to hold dataset configuration for demo.
// TODO: represent internals with Eclipse Datatools SQL model.
// TODO: set up gui element to support definition.

public class DatasetManager
{

    public class Dataset
    {
        class Relation
        {
            public String streamType;
            public LinkedHashMap<String, String> fields;
            public String sourceType;
            public LinkedList<String> defaultSourceConstructorArgs; 
            public String tupleType;
            public String adaptorType;
            public LinkedHashMap<String, String> bindings;
            public String thriftNamespace;
            public String instancePrefix;
            public AtomicInteger instanceCounter;
            
            public Relation(String srt, LinkedHashMap<String, String> f,
                    String st, String sca, String tt, String at,
                    LinkedHashMap<String, String> bd, String tns, String ip)
            {
                streamType = srt;
                fields = f;
                sourceType = st;
                tupleType = tt;
                adaptorType = at;
                bindings = bd;
                thriftNamespace = tns;
                instancePrefix = ip;
                instanceCounter = new AtomicInteger();

                defaultSourceConstructorArgs = new LinkedList<String>(); 
                String[] args = sca.split(",");
                for (String a : args)
                {
                    if ( !a.isEmpty() )
                        defaultSourceConstructorArgs.add(a);
                }
            }
            
            public String getInstance()
            {
                return instancePrefix +
                    Integer.toString(instanceCounter.getAndIncrement());
            }
        };

        public class DatasetLocations
        {
            HashMap<String, String> relationFiles;
            HashMap<String, String> handlerInputFiles;
            
            String defaultFileDirectory;
            String defaultHandlerFileDirectory;
            HashMap<String, String> defaultFileNames;
            
            HashMap<String, Character> inputDelimiters;

            public DatasetLocations()
            {
                relationFiles = new HashMap<String, String>();
                handlerInputFiles = new HashMap<String, String>();
                defaultFileNames = new HashMap<String, String>();
                inputDelimiters = new HashMap<String, Character>();
            }
            
            public void setDefaultLocations(String fileDir,
                String handlerFileDir, HashMap<String, String> relationLocations,
                Character defaultDelimiter)
            {
                defaultFileDirectory = fileDir;
                defaultHandlerFileDirectory = handlerFileDir;
                defaultFileNames = relationLocations;
                
                for (Map.Entry<String, String> e : defaultFileNames.entrySet())
                {
                    inputDelimiters.put(e.getKey(), defaultDelimiter);
                }
            }
            
            public void addRelation(String relName, String fileName, Character delimiter)
            {
                if ( !defaultFileNames.containsKey(relName) ) {
                    defaultFileDirectory = "";
                    defaultFileNames.put(relName, fileName);
                }
                
                relationFiles.put(relName, fileName);
                inputDelimiters.put(relName, delimiter);
            }
            
            public void addHandlerInput(String relName, String fileName)
            {
                handlerInputFiles.put(relName, fileName);
            }

            public String getLocation(String relName)
            {
                String r = null;
                if ( relationFiles.containsKey(relName) )
                    r = relationFiles.get(relName);
                else if ( defaultFileNames.containsKey(relName) ) {
                    r = (defaultFileDirectory.isEmpty()?
                        "" : defaultFileDirectory + "/") +
                        defaultFileNames.get(relName);
                }
                
                return r;
            }
            
            public String getHandlerInputLocation(String relName)
            {
                String r = null;
                if ( handlerInputFiles.containsKey(relName) )
                    r = handlerInputFiles.get(relName);
                else if ( defaultFileNames.containsKey(relName) ) {
                    r = (defaultHandlerFileDirectory.isEmpty()?
                        "" : defaultHandlerFileDirectory + "/") +
                        defaultFileNames.get(relName);
                }
                
                return r;
            }
        
            public Character getSourceDelimiter(String relName)
            {
                Character r = null;
                if ( inputDelimiters.containsKey(relName) )
                    r = inputDelimiters.get(relName);
                return r;
            }
        }

        // relation name => field name * type
        HashMap<String, Relation> relations;

        DatasetLocations locations;
        
        public Dataset()
        {
            relations = new HashMap<String, Relation>();
            locations = new DatasetLocations();
        }
        
        void addRelation(String streamType, String name,
                LinkedHashMap<String, String> fieldsAndTypes,
                String sourceType, String sourceArgs, String tupleType,
                String adaptorType, LinkedHashMap<String, String> bindings,
                String thriftNamespace, String instancePrefix)
        {
            if ( !relations.containsKey(name) ) {
                Relation r = new Relation(streamType, fieldsAndTypes,
                    sourceType, sourceArgs, tupleType,
                    adaptorType, bindings, thriftNamespace, instancePrefix);
                relations.put(name, r);
            }
        }

        public Set<String> getRelationNames()
        {
            return relations.keySet();
        }

        public boolean hasRelation(String name) {
            return relations.containsKey(name);
        }
        
        public String getStreamType(String name)
        {
            String r = null;
            if (relations.containsKey(name)) r = relations.get(name).streamType;
            return r;
        }

        public LinkedHashMap<String, String> getRelationFields(String name)
        {
            LinkedHashMap<String, String> r = null;
            if (relations.containsKey(name)) r = relations.get(name).fields;
            return r;
        }
    
        public String getSourceType(String relation)
        {
            String r = null;
            if (relations.containsKey(relation))
                r = relations.get(relation).sourceType;
            return r;
        }
        
        public LinkedList<String> getDefaultConstructorArgs(String relation)
        {
            LinkedList<String> r = null;
            if (relations.containsKey(relation))
                r = relations.get(relation).defaultSourceConstructorArgs;
            return r;
        }
        
        public String getSourceInstance(String relation)
        {
            String r = null;
            if (relations.containsKey(relation))
                r = relations.get(relation).getInstance();
            return r;
        }
        
        public String getTupleType(String relation)
        {
            String r = null;
            if (relations.containsKey(relation))
                r = relations.get(relation).tupleType;
            return r;
        }
        
        public String getAdaptorType(String relation)
        {
            String r = null;
            if (relations.containsKey(relation))
                r = relations.get(relation).adaptorType;
            return r;
        }
        
        public LinkedHashMap<String, String> getBindings(String relation)
        {
            LinkedHashMap<String, String> r = null;
            if (relations.containsKey(relation))
                r = relations.get(relation).bindings;
            return r;
        }
        
        public String getThriftNamespace(String relation)
        {
            String r = null;
            if ( relations.containsKey(relation) )
                r = relations.get(relation).thriftNamespace;
            return r;
        }

    }
    
    private LinkedHashMap<String, Dataset> datasets;
    private static DatasetManager singleton = null;

    private DatasetManager()
    {
        datasets = new LinkedHashMap<String, Dataset>();
    }
    
    public static DatasetManager getDatasetManager()
    {
        if ( singleton == null )
            singleton = new DatasetManager();
        
        return singleton;
    }

    public void addDataset(String name, Dataset ds)
    {
        if (datasets.containsKey(name))
            System.out.println("Dataset " + name + " already exists!");

        else datasets.put(name, ds);
    }

    public Dataset getDataset(String name)
    {
        Dataset r = null;
        if (datasets.containsKey(name)) r = datasets.get(name);
        return r;
    }

    public Set<String> getDatasetNames()
    {
        return datasets.keySet();
    }

    public boolean hasDataset(String name)
    {
        return datasets.containsKey(name);
    }

    public boolean hasDatasetRelation(String name, String relName)
    {
        boolean r = false;
        if (name == null || name.isEmpty())
        {
            for (Map.Entry<String, Dataset> e : datasets.entrySet())
            {
                System.out.println("Checking dataset " + e.getKey());
                r = (r || e.getValue().hasRelation(relName));
            }
        }
        else r = hasDataset(name) && getDataset(name).hasRelation(relName);

        return r;
    }

    public String getRelationDataset(String relName)
    {
        String r = null;
        for (Map.Entry<String, Dataset> e : datasets.entrySet())
        {
            if ( e.getValue().hasRelation(relName) ) {
                r = e.getKey();
                break;
            }
        }
        
        return r;
    }
    
    public String getRelationLocation(String name, String relName)
    {
        String r = null;
        Dataset ds = getDataset(name);
        if ( ds != null ) {
            r = ds.locations.getLocation(relName);
        }
        
        return r;
    }

    public String getRelationHandlerInputLocation(String name, String relName)
    {
        String r = null;
        Dataset ds = getDataset(name);
        if ( ds != null ) {
            r = ds.locations.getHandlerInputLocation(relName);
        }
        
        return r;
    }

    public Character getSourceDelimiters(String name, String relName)
    {
        Character r = null;
        Dataset ds = getDataset(name);
        if ( ds != null ) {
            r = ds.locations.getSourceDelimiter(relName);
        }
        
        return r;
    }
    
    // Find first instance of relation.
    public LinkedHashMap<String, String> getRelationFields(String relName)
    {
        LinkedHashMap<String, String> r = null;
        for (Dataset ds : datasets.values())
        {
            if (ds.hasRelation(relName))
            {
                System.out.println("Found fields for " + relName);
                r = ds.getRelationFields(relName);
                return r;
            }
        }

        return r;
    }

    // VLDB demo specific code for default datasets.
    public final static String OrderbookDataset = "ORDERBOOK_DATA";
    public final static String OrderbookHandlerDataset = "ORDERBOOK_HANDLER_DATA";

    public final static String TpchDataset = "TPCH_DATA";
    public final static String TpchHandlerDataset = "TPCH_HANDLER_DATA";

    public static DatasetManager initDemoDatasetManager(DBToasterWorkspace dbtws)
    {
        // TODO: create a new directory of clean files in ~/datasets.
        String historicalDatasetPath = dbtws.getPath(OrderbookDataset);
        String historicalHandlerDatasetPath = dbtws.getPath(OrderbookHandlerDataset);

        // TODO: create a new directory of clean files in ~/datasets.
        String tpchDatasetPath = dbtws.getPath(TpchDataset);
        String tpchHandlerDatasetPath = dbtws.getPath(TpchHandlerDataset);
        
        DatasetManager aDM = getDatasetManager();

        LinkedHashMap<String, String> bookFieldsAndTypes =
            new LinkedHashMap<String, String>();

        bookFieldsAndTypes.put("t", "int");
        bookFieldsAndTypes.put("id", "int");
        bookFieldsAndTypes.put("broker_id", "int");
        bookFieldsAndTypes.put("p", "int");
        bookFieldsAndTypes.put("v", "int");
        
        LinkedHashMap<String, String> adaptorBindings =
            new LinkedHashMap<String, String>();
        
        adaptorBindings.put("t", "t");
        adaptorBindings.put("id", "id");
        adaptorBindings.put("broker_id", "broker_id");
        adaptorBindings.put("p", "price");
        adaptorBindings.put("v", "volume");
        
        // Historical algo execution
        Dataset historicalOrderbook = aDM.new Dataset();

        historicalOrderbook.addRelation(
            "file", "bids", bookFieldsAndTypes,
            "DBToaster::DemoDatasets::OrderbookFileStream",
            "\"" + (historicalDatasetPath.isEmpty()?
                "" : historicalDatasetPath + "/") + "20081201.csv\",10000",
            "DBToaster::DemoDatasets::OrderbookTuple",
            "DBToaster::DemoDatasets::OrderbookTupleAdaptor", adaptorBindings,
            "datasets",
            "BidsOrderbook");
        
        historicalOrderbook.addRelation(
            "file", "asks", bookFieldsAndTypes,
            "DBToaster::DemoDatasets::OrderbookFileStream",
            "\"" + (historicalDatasetPath.isEmpty()?
                "" : historicalDatasetPath + "/") + "20081201.csv\",10000",
            "DBToaster::DemoDatasets::OrderbookTuple",
            "DBToaster::DemoDatasets::OrderbookTupleAdaptor", adaptorBindings,
            "datasets",
            "AsksOrderbook");

        // TODO: split historical files into separate files for bids/asks.
        HashMap<String, String> historicalOrderbookLocations =
            new HashMap<String, String>();

        historicalOrderbookLocations.put("bids", "20081201.csv");
        historicalOrderbookLocations.put("asks", "20081201.csv");
        
        Character orderbookDelim = ',';
        
        historicalOrderbook.locations.setDefaultLocations(
            historicalDatasetPath, historicalHandlerDatasetPath,
            historicalOrderbookLocations, orderbookDelim);

        aDM.addDataset("orderbook", historicalOrderbook);

        // Live algo execution
        Dataset liveOrderbook = aDM.new Dataset();

     // TODO: remove arguments for network socket streams. These should be host/ports
        liveOrderbook.addRelation(
            "socket", "bids", bookFieldsAndTypes,
            "DBToaster::DemoDatasets::OrderbookSocketStream",
            "\"20081201.csv\",10000",
            "DBToaster::DemoDatasets::OrderbookTuple",
            "DBToaster::DemoDatasets::OrderbookTupleAdaptor", adaptorBindings,
            "datasets",
            "BidsOrderbook");
        
        liveOrderbook.addRelation(
            "socket", "asks", bookFieldsAndTypes,
            "DBToaster::DemoDatasets::OrderbookSocketStream",
            "\"20081201.csv\",10000",
            "DBToaster::DemoDatasets::OrderbookTuple",
            "DBToaster::DemoDatasets::OrderbookTupleAdaptor", adaptorBindings,
            "datasets",
            "AsksOrderbook");
        
        //aDM.addDataset("lo", liveOrderbook);

        // Note: no locations for live orderbook dataset.

        // SSB dataset
        LinkedHashMap<String, String> liFieldsAndTypes =
            new LinkedHashMap<String, String>();
        
        liFieldsAndTypes.put("orderkey", "int");
        liFieldsAndTypes.put("partkey", "int");
        liFieldsAndTypes.put("suppkey", "int");
        liFieldsAndTypes.put("linenumber", "int");
        liFieldsAndTypes.put("quantity", "double");
        liFieldsAndTypes.put("extendedprice", "double");
        liFieldsAndTypes.put("discount", "double");
        liFieldsAndTypes.put("tax", "double");
        liFieldsAndTypes.put("returnflag", "string");
        liFieldsAndTypes.put("linestatus", "string");
        liFieldsAndTypes.put("shipdate", "string");
        liFieldsAndTypes.put("commitdate", "string");
        liFieldsAndTypes.put("receiptdate", "string");
        liFieldsAndTypes.put("shipinstruct", "string");
        liFieldsAndTypes.put("shipmode", "string");
        liFieldsAndTypes.put("comment", "string");
        
        LinkedHashMap<String, String> ordFieldsAndTypes =
            new LinkedHashMap<String, String>();
        
        ordFieldsAndTypes.put("orderkey", "int");
        ordFieldsAndTypes.put("custkey", "int");
        ordFieldsAndTypes.put("orderstatus", "string");
        ordFieldsAndTypes.put("totalprice", "double");
        ordFieldsAndTypes.put("orderdate", "string");
        ordFieldsAndTypes.put("orderpriority", "string");
        ordFieldsAndTypes.put("clerk", "string");
        ordFieldsAndTypes.put("shippriority", "int");
        ordFieldsAndTypes.put("comment", "string");
        
        LinkedHashMap<String, String> ptFieldsAndTypes =
            new LinkedHashMap<String, String>();

        ptFieldsAndTypes.put("partkey", "int");
        ptFieldsAndTypes.put("name", "string");
        ptFieldsAndTypes.put("mfgr", "string");
        ptFieldsAndTypes.put("brand", "string");
        ptFieldsAndTypes.put("type", "string");
        ptFieldsAndTypes.put("size", "int");
        ptFieldsAndTypes.put("container", "string");
        ptFieldsAndTypes.put("retailprice", "double");
        ptFieldsAndTypes.put("comment", "string");
        
        LinkedHashMap<String, String> csFieldsAndTypes =
            new LinkedHashMap<String, String>();
        
        csFieldsAndTypes.put("custkey", "int");
        csFieldsAndTypes.put("name", "string");
        csFieldsAndTypes.put("address", "string");
        csFieldsAndTypes.put("nationkey", "int");
        csFieldsAndTypes.put("phone", "string");
        csFieldsAndTypes.put("acctbal", "double");
        csFieldsAndTypes.put("mktsegment", "string");
        csFieldsAndTypes.put("comment", "string");

        LinkedHashMap<String, String> spFieldsAndTypes =
            new LinkedHashMap<String, String>();
        
        spFieldsAndTypes.put("suppkey", "int");
        spFieldsAndTypes.put("name", "string");
        spFieldsAndTypes.put("address", "string");
        spFieldsAndTypes.put("nationkey", "int");
        spFieldsAndTypes.put("phone", "string");
        spFieldsAndTypes.put("acctbal", "double");
        spFieldsAndTypes.put("comment", "string");
        
        LinkedHashMap<String, String> psFieldsAndTypes =
            new LinkedHashMap<String, String>();
        
        psFieldsAndTypes.put("partkey", "int");
        psFieldsAndTypes.put("suppkey", "int");
        psFieldsAndTypes.put("availqty", "int");
        psFieldsAndTypes.put("supplycost", "double");
        psFieldsAndTypes.put("comment", "string");
        
        LinkedHashMap<String, String> ntFieldsAndTypes =
            new LinkedHashMap<String, String>();
        
        ntFieldsAndTypes.put("nationkey", "int");
        ntFieldsAndTypes.put("name", "string");
        ntFieldsAndTypes.put("regionkey", "int");
        ntFieldsAndTypes.put("comment", "string");

        LinkedHashMap<String, String> rgFieldsAndTypes =
            new LinkedHashMap<String, String>();

        rgFieldsAndTypes.put("regionkey", "int");
        rgFieldsAndTypes.put("name", "string");
        rgFieldsAndTypes.put("comment", "string");
        
        
        LinkedHashMap<String, String> liAdaptorBindings =
            new LinkedHashMap<String, String>();

        liAdaptorBindings.put("orderkey", "orderkey");
        liAdaptorBindings.put("partkey", "partkey");
        liAdaptorBindings.put("suppkey", "suppkey");
        liAdaptorBindings.put("linenumber", "linenumber");
        liAdaptorBindings.put("quantity", "quantity");
        liAdaptorBindings.put("extendedprice", "extendedprice");
        liAdaptorBindings.put("discount", "discount");
        liAdaptorBindings.put("tax", "tax");
        liAdaptorBindings.put("returnflag", "returnflag");
        liAdaptorBindings.put("linestatus", "linestatus");
        liAdaptorBindings.put("shipdate", "shipdate");
        liAdaptorBindings.put("commitdate", "commitdate");
        liAdaptorBindings.put("receiptdate", "receiptdate");
        liAdaptorBindings.put("shipinstruct", "shipinstruct");
        liAdaptorBindings.put("shipmode", "shipmode");
        liAdaptorBindings.put("comment", "comment");
        
        LinkedHashMap<String, String> ordAdaptorBindings =
            new LinkedHashMap<String, String>();
        
        ordAdaptorBindings.put("orderkey", "orderkey");
        ordAdaptorBindings.put("custkey", "custkey");
        ordAdaptorBindings.put("orderstatus", "orderstatus");
        ordAdaptorBindings.put("totalprice", "totalprice");
        ordAdaptorBindings.put("orderdate", "orderdate");
        ordAdaptorBindings.put("orderpriority", "orderpriority");
        ordAdaptorBindings.put("clerk", "clerk");
        ordAdaptorBindings.put("shippriority", "shippriority");
        ordAdaptorBindings.put("comment", "comment");

        LinkedHashMap<String, String> ptAdaptorBindings =
            new LinkedHashMap<String, String>();
        
        ptAdaptorBindings.put("partkey", "partkey");
        ptAdaptorBindings.put("name", "name");
        ptAdaptorBindings.put("mfgr", "mfgr");
        ptAdaptorBindings.put("brand", "brand");
        ptAdaptorBindings.put("type", "type");
        ptAdaptorBindings.put("size", "size");
        ptAdaptorBindings.put("container", "container");
        ptAdaptorBindings.put("retailprice", "retailprice");
        ptAdaptorBindings.put("comment", "comment");
        
        LinkedHashMap<String, String> csAdaptorBindings =
            new LinkedHashMap<String, String>();
        
        csAdaptorBindings.put("custkey", "custkey");
        csAdaptorBindings.put("name", "name");
        csAdaptorBindings.put("address", "address");
        csAdaptorBindings.put("nationkey", "nationkey");
        csAdaptorBindings.put("phone", "phone");
        csAdaptorBindings.put("acctbal", "acctbal");
        csAdaptorBindings.put("mktsegment", "mktsegment");
        csAdaptorBindings.put("comment", "comment");
        
        LinkedHashMap<String, String> spAdaptorBindings =
            new LinkedHashMap<String, String>();
        
        spAdaptorBindings.put("suppkey", "suppkey");
        spAdaptorBindings.put("name", "name");
        spAdaptorBindings.put("address", "address");
        spAdaptorBindings.put("nationkey", "nationkey");
        spAdaptorBindings.put("phone", "phone");
        spAdaptorBindings.put("acctbal", "acctbal");
        spAdaptorBindings.put("comment", "comment");

        LinkedHashMap<String, String> psAdaptorBindings =
            new LinkedHashMap<String, String>();
        
        psAdaptorBindings.put("partkey", "partkey");
        psAdaptorBindings.put("suppkey", "suppkey");
        psAdaptorBindings.put("availqty", "availqty");
        psAdaptorBindings.put("supplycost", "supplycost");
        psAdaptorBindings.put("comment", "comment");

        LinkedHashMap<String, String> ntAdaptorBindings =
            new LinkedHashMap<String, String>();
        
        ntAdaptorBindings.put("nationkey", "nationkey");
        ntAdaptorBindings.put("name", "name");
        ntAdaptorBindings.put("regionkey", "regionkey");
        ntAdaptorBindings.put("comment", "comment");

        LinkedHashMap<String, String> rgAdaptorBindings =
            new LinkedHashMap<String, String>();

        rgAdaptorBindings.put("regionkey", "regionkey");
        rgAdaptorBindings.put("name", "name");
        rgAdaptorBindings.put("comment", "comment");

        
        // Historical algo execution
        Dataset tpchDataset = aDM.new Dataset();

        tpchDataset.addRelation(
            "file", "lineitem", liFieldsAndTypes,
            "DBToaster::DemoDatasets::LineitemStream",
            "\"" + (tpchDatasetPath.isEmpty()? "" : (tpchDatasetPath + "/")) +
                "lineitem.tbl.a\",DBToaster::DemoDatasets::parseLineitemField, 16, 10000, 512",
            "DBToaster::DemoDatasets::lineitem",
            "DBToaster::DemoDatasets::LineitemTupleAdaptor", liAdaptorBindings,
            "datasets",
            "lineitem");
        
        tpchDataset.addRelation(
            "file", "orders", ordFieldsAndTypes,
            "DBToaster::DemoDatasets::OrderStream",
            "\"" + (tpchDatasetPath.isEmpty()? "" : (tpchDatasetPath + "/")) +
                "/orders.tbl.a\",DBToaster::DemoDatasets::parseOrderField, 9, 10000, 512",
            "DBToaster::DemoDatasets::order",
            "DBToaster::DemoDatasets::OrderTupleAdaptor", ordAdaptorBindings,
            "datasets",
            "order");
        
        tpchDataset.addRelation(
            "file", "part", ptFieldsAndTypes,
            "DBToaster::DemoDatasets::PartStream",
            "\"" + (tpchDatasetPath.isEmpty()? "" : (tpchDatasetPath + "/")) +
                "/part.tbl.a\",DBToaster::DemoDatasets::parsePartField, 9, 10000, 512",
            "DBToaster::DemoDatasets::part",
            "DBToaster::DemoDatasets::PartTupleAdaptor", ptAdaptorBindings,
            "datasets",
            "part");
        
        tpchDataset.addRelation(
            "file", "customer", csFieldsAndTypes,
            "DBToaster::DemoDatasets::CustomerStream",
            "\"" + (tpchDatasetPath.isEmpty()? "" : (tpchDatasetPath + "/")) +
                "/customer.tbl.a\",DBToaster::DemoDatasets::parseCustomerField, 8, 10000, 512",
            "DBToaster::DemoDatasets::customer",
            "DBToaster::DemoDatasets::CustomerTupleAdaptor", csAdaptorBindings,
            "datasets",
            "customer");
        
        tpchDataset.addRelation(
            "file", "supplier", spFieldsAndTypes,
            "DBToaster::DemoDatasets::SupplierStream",
                "\"" + (tpchDatasetPath.isEmpty()? "" : (tpchDatasetPath + "/")) +
                "/supplier.tbl.a\",DBToaster::DemoDatasets::parseSupplierField, 7, 10000, 512",
            "DBToaster::DemoDatasets::supplier",
            "DBToaster::DemoDatasets::SupplierTupleAdaptor", spAdaptorBindings,
            "datasets",
            "supplier");
        
        tpchDataset.addRelation(
            "file", "partsupp", psFieldsAndTypes,
            "DBToaster::DemoDatasets::PartSuppStream",
                "\"" + (tpchDatasetPath.isEmpty()? "" : (tpchDatasetPath + "/")) +
                "/partsupp.tbl.a\",DBToaster::DemoDatasets::parsePartSuppField, 5, 10000, 512",
            "DBToaster::DemoDatasets::partsupp",
            "DBToaster::DemoDatasets::PartSuppTupleAdaptor", psAdaptorBindings,
            "datasets",
            "partsupp");
        
        tpchDataset.addRelation(
            "file", "nation", ntFieldsAndTypes,
            "DBToaster::DemoDatasets::NationStream",
            "\"" + (tpchDatasetPath.isEmpty()? "" : (tpchDatasetPath + "/")) +
                "nation.tbl.a\",DBToaster::DemoDatasets::parseNationField, 4, 10000, 512",
            "DBToaster::DemoDatasets::nation",
            "DBToaster::DemoDatasets::NationTupleAdaptor", ntAdaptorBindings,
            "datasets",
            "nation");
        
        tpchDataset.addRelation(
            "file", "region", rgFieldsAndTypes,
            "DBToaster::DemoDatasets::RegionStream",
            "\"" + (tpchDatasetPath.isEmpty()? "" : (tpchDatasetPath + "/")) 
                + "/region.tbl.a\",DBToaster::DemoDatasets::parseRegionField, 3, 10000, 512",
            "DBToaster::DemoDatasets::region",
            "DBToaster::DemoDatasets::RegionTupleAdaptor", rgAdaptorBindings,
            "datasets",
            "region");

        HashMap<String, String> tpchLocations = new HashMap<String, String>();
        tpchLocations.put("lineitem", "lineitem.tbl.a");
        tpchLocations.put("order", "orders.tbl.a");
        tpchLocations.put("part", "part.tbl.a");
        tpchLocations.put("customer", "customer.tbl.a");
        tpchLocations.put("supplier", "supplier.tbl.a");
        tpchLocations.put("nation", "nation.tbl");
        tpchLocations.put("region", "region.tbl");
        tpchLocations.put("partsupp", "partsupp.tbl.a");
        
        Character tpchDelimiter = '|';
        tpchDataset.locations.setDefaultLocations(
            tpchDatasetPath, tpchHandlerDatasetPath, tpchLocations,
            tpchDelimiter);

        aDM.addDataset("tpch", tpchDataset);
        
        // TODO: LinearRoad dataset

        return aDM;
    }

}
