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

        // relation name => field name * type
        HashMap<String, Relation> relations;

        public Dataset()
        {
            relations = new HashMap<String, Relation>();
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
    public static DatasetManager initDemoDatasetManager()
    {
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
            "\"20081201.csv\",10000",
            "DBToaster::DemoDatasets::OrderbookTuple",
            "DBToaster::DemoDatasets::OrderbookTupleAdaptor", adaptorBindings,
            "datasets",
            "BidsOrderbook");
        
        historicalOrderbook.addRelation(
            "file", "asks", bookFieldsAndTypes,
            "DBToaster::DemoDatasets::OrderbookFileStream",
            "\"20081201.csv\",10000",
            "DBToaster::DemoDatasets::OrderbookTuple",
            "DBToaster::DemoDatasets::OrderbookTupleAdaptor", adaptorBindings,
            "datasets",
            "AsksOrderbook");

        aDM.addDataset("orderbook", historicalOrderbook);

        // Live algo execution
        Dataset liveOrderbook = aDM.new Dataset();

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


        // TODO: SSB dataset

        // TODO: LinearRoad dataset

        return aDM;
    }
}
