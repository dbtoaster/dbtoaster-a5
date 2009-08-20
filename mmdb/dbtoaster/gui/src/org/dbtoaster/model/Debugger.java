package org.dbtoaster.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Debugger
{
    String queryName;

    String binaryPath;
    HashMap<String, String> binaryEnv;
    
    ProcessBuilder debuggerProcess;
    Process currentDebugger;

    DBToasterDebugger debugger;

    // Profiler client
    TBinaryProtocol.Factory debuggerProtocolFactory;
    int currentPort;
    String currentHost;

    // Assumes DBToaster produces Java debugging clients with namespace:
    // DBToaster.Debugger.<query name>.*
    class DBToasterDebugger
    {
        public final static String debuggerPackagePrefix = "DBToaster.Debugger";
        
        public final static String debuggerClassNameSuffix = "Debugger";
        
        public final static String debuggerClientClassNameSuffix =
            debuggerClassNameSuffix + "$Client";
        
        public final static String debuggerConstantsClassNameSuffix = "Constants";

        final static String streamIdPrefix = "stream";
        final static String streamIdSuffix = "Id";

        final static String stepPrefix = "step_";
        final static String stepNPrefix = "stepn_";
        final static String getPrefix = "get_";

        ClassLoader debuggerLoader;
        Class<?> debuggerClass;
        Class<?> debuggerConstantsClass;

        HashMap<String, HashMap<String, Method> > methodPrefixes;

        HashMap<String, Method> stepMethods;
        HashMap<String, Method> stepNMethods;
        HashMap<String, Method> getMethods;
        
        // Runtime members.
        Object client;
        TTransport transport;
        boolean terminated;
        
        public DBToasterDebugger(String queryName, File clientBase)
        {
            // Load class and methods.
            String debuggerPackageBase = debuggerPackagePrefix + "." + queryName;

            try {
                debuggerLoader = URLClassLoader.newInstance(
                    new URL[] { clientBase.toURI().toURL() },
                        getClass().getClassLoader());
    
                String debuggerClassName = 
                    debuggerPackageBase + "." + debuggerClassNameSuffix;

                Class<?> dc = debuggerLoader.loadClass(debuggerClassName);
                
                String debuggerClientClassName =
                    debuggerPackageBase + "." + debuggerClientClassNameSuffix;
                
                for (Class<?> c : dc.getDeclaredClasses() ) {
                    if ( c.getName().equals(debuggerClientClassName) )
                        debuggerClass = c;
                }
                
                if ( debuggerClass == null ) {
                    System.err.println(
                        "Failed to find debugger client interface " +
                        debuggerClientClassName);
                }

                String debuggerConstantsClassName =
                    debuggerPackageBase + "." + debuggerConstantsClassNameSuffix;
                debuggerConstantsClass = debuggerLoader.loadClass(debuggerConstantsClassName);

                if ( debuggerConstantsClass == null ) {
                    System.err.println(
                        "Failed to find debugger constants " +
                        debuggerConstantsClassName);
                }

            } catch (Exception e) {
                String msg = "Failed to load debugger client for " +
                    queryName + ": " + e.getMessage();
                System.err.println(msg);
                e.printStackTrace();
            }

            if ( !(debuggerClass == null || debuggerConstantsClass == null) )
            {
                stepMethods = new HashMap<String, Method>();
                stepNMethods = new HashMap<String, Method>();
                getMethods = new HashMap<String, Method>();
                
                methodPrefixes = new HashMap<String, HashMap<String,Method>>();
                
                methodPrefixes.put(stepPrefix, stepMethods);
                methodPrefixes.put(stepNPrefix, stepNMethods);
                methodPrefixes.put(getPrefix, getMethods);
                
                for (Method m : debuggerClass.getMethods())
                {
                    String methodName = m.getName();
                    for (Map.Entry<String, HashMap<String,Method>> e :
                            methodPrefixes.entrySet())
                    {
                        if ( methodName.startsWith(e.getKey()) )
                            e.getValue().put(methodName, m);
                    }
                }
            }
            else {
                // Require both classes otherwise we can't retrieve
                // DBToaster stream dispatcher ids.
                debuggerClass = null;
                debuggerConstantsClass = null;
                System.err.println("Could not find debugger classes for " + queryName);
            }
        }

        void reset()
        {
            client = null;
            transport = null;
            terminated = false;
        }

        public boolean valid()
        {
            return !(debuggerClass == null || debuggerConstantsClass == null);
        }

        public void instantiateDebugger(TProtocol protocol)
        {
            reset();
            if ( !(stepMethods.isEmpty() && stepNMethods.isEmpty() && getMethods.isEmpty()) )
            {
                try {
                    transport = protocol.getTransport();
                    if ( !transport.isOpen() ) transport.open();

                    Constructor<?> clientConstructor =
                        debuggerClass.getConstructor(protocol.getClass());
                    client = clientConstructor.newInstance(protocol);
                } catch (Exception e)
                {
                    System.out.println("Unable to create DBToaster profiler.");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
        
        // Step method metadata.
        public Class<?> getStepDebugTupleType(String relationName)
        {
            Class<?> r = null;
            String key = stepPrefix + relationName;
            if ( stepMethods.containsKey(key) ) {
                Method m = stepMethods.get(key);
                Class<?>[] args = m.getParameterTypes();
                if ( args.length == 1 )
                    r = args[0];
                else {
                    System.err.println("Invalid step method: " + m.getName() +
                        " (takes " + args.length + " args, instead of 1)");
                }
            }
            else {
                System.err.println("No step method found for " + key);
            }

            return r;
        }
        
        public Class<?> getStepDataTupleType(String relationName)
        {
            Class<?> r = null;
            Class<?> debugType = getStepDebugTupleType(relationName);
            if ( debugType != null ) {
                try {
                    Field dataField = debugType.getField("data");
                    if ( dataField != null )
                        r = dataField.getType();
                    else
                        System.err.println("java.lang.Class.getField(name) returned null!");
                } catch (Exception e) {
                    String msg = "Error accessing 'data' field in " +
                        debugType.getName() + "(" + e.getMessage() + ")";
                    System.err.println(msg);
                    e.printStackTrace();
                }
            }
            else {
                System.err.println("No debug tuple type found for " + relationName);
            }
            return r;
        }
        
        public int getStreamId(String relationName)
        {
            int r = -1;
            String streamIdFieldName = streamIdPrefix + relationName + streamIdSuffix;
            Field idField;
            try {
                idField = debuggerConstantsClass.getField(streamIdFieldName);
                if ( Modifier.isStatic(idField.getModifiers()) ) {
                    r = idField.getInt(null);
                }
                
            } catch (Exception e) {
                String msg = "Unable to get stream id for " + relationName;
                System.err.println(msg);
                e.printStackTrace();
            }

            return r;
        }

        // Datastructure metadata
        public Class<?> getDatastructureClassType(String dsName)
        {
            Class<?> r = null;
            String key = getPrefix + dsName;
            if ( getMethods.containsKey(key) ) {
                Method m = getMethods.get(key);
                r = m.getReturnType();
            }
            return r;
        }
        
        public Class<?> getDatastructureKeyClassType(String dsName)
        {
            Class<?> r = null;
            String key = getPrefix + dsName;
            if ( getMethods.containsKey(key) ) {
                Method m = getMethods.get(key);
                
                Type genRT = m.getGenericReturnType();
                if ( genRT instanceof ParameterizedType )
                {
                    ParameterizedType paramRT = (ParameterizedType) genRT;
                    Type[] paramRTArgs = paramRT.getActualTypeArguments();
                    
                    if ( paramRTArgs.length == 0 )
                        System.err.println("Could not find actual parameterized types!");
                    
                    else if ( !(paramRTArgs[0] instanceof java.lang.Class<?>) )
                    {
                        System.err.println(
                            "Unable to handle nested parameterized return types.");
                    }
                    else
                    {
                        Class<?> argClass = null;
                        if ( paramRTArgs[0] instanceof java.lang.Class<?> )
                            argClass = (Class<?>) paramRTArgs[0];
                        else {
                        }
                        
                        r = argClass;
                    }
                }
                else
                    r = m.getReturnType();
                
                System.out.println(dsName + " key type: " + r);
            }
            return r;
        }

        // Debugger invocation.
        public void step(String relationName, Object tuple)
        {
            String key = stepPrefix + relationName;
            if ( stepMethods.containsKey(key) ) {
                try {
                    Method m = stepMethods.get(key);
                    m.invoke(client, tuple);
                } catch (Exception e) {
                    System.err.println("Failed to execute " + key);
                    e.printStackTrace();
                }
            }
            else {
                System.err.println("Could not find step method " + key);
            }
        }
        
        public void stepN(String relationName, int n)
        {
            String key = stepNPrefix + relationName;
            if ( stepNMethods.containsKey(key) ) {
                try {
                    Method m = stepMethods.get(key);
                    m.invoke(client, n);
                } catch (Exception e) {
                    System.err.println("Failed to execute " + key);
                    e.printStackTrace();
                }
            }
            else {
                System.err.println("Could not find stepN method " + key);
            }
        }
        
        public Object getDatastructure(String dsName)
        {
            Object r = null;
            String key = getPrefix + dsName;
            if ( getMethods.containsKey(key) ) {
                try {
                    Method m = getMethods.get(key);
                    r = m.invoke(client);
                } catch (Exception e) {
                    System.err.println("Failed to execute " + key);
                    e.printStackTrace();
                }
            }
            else {
                System.err.println("Could not find get method " + key);
            }
            
            return r;
        }
    
        public LinkedList<String> getInputRelationNames()
        {
            LinkedList<String> r = new LinkedList<String>();
            for (Map.Entry<String, Method> e : stepMethods.entrySet())
            {
                String methodName = e.getKey();
                r.add(methodName.substring(stepPrefix.length(), methodName.length()));
            }
            
            return r;
        }
        
        public LinkedList<String> getDatastructureNames()
        {
            LinkedList<String> r = new LinkedList<String>();
            for (Map.Entry<String, Method> e : getMethods.entrySet())
            {
                String methodName = e.getKey();
                r.add(methodName.substring(getPrefix.length(), methodName.length()));
            }
            return r;
        }
    };
    
    public Debugger() {}

    public Debugger(String queryName, String debuggerBinary, String debuggerClientBase)
    {
        this.queryName = queryName;

        File binaryFile = new File(debuggerBinary);
        if ( binaryFile.exists() && binaryFile.isFile() && binaryFile.canExecute() )
        {
            binaryPath = binaryFile.getAbsolutePath();
            binaryEnv = new HashMap<String, String>();
            binaryEnv.putAll(System.getenv());

            debuggerProcess = new ProcessBuilder(binaryFile.getAbsoluteFile().toString());
            currentDebugger = null;
            debuggerProtocolFactory = new TBinaryProtocol.Factory();
            
            System.out.println("Loading debugger client from " + debuggerClientBase);

            File clientBase = new File(debuggerClientBase);
            debugger = new DBToasterDebugger(queryName, clientBase);
            if ( !debugger.valid() )
            {
                debugger = null;
                System.err.println("Invalid DBToasterDebugger for " + queryName);
            }
            else
                System.out.println("Successfully loaded debugger client!");
        }
        else {
            System.err.println("Invalid debugger binary: " + binaryFile.toString());
        }
    }

    public String getBinaryPath() { return binaryPath; }

    public String startDebugging(String debugLogFile, int debuggerServicePort)
    {
        // Run query binary.
        String status = null;

        if ( debuggerProcess != null ) {
            LinkedList<String> args = new LinkedList<String>();
            args.add(Integer.toString(debuggerServicePort));
            debuggerProcess.command(args);

            try {
                debuggerProcess.redirectErrorStream(true);
                currentPort = debuggerServicePort;
                currentHost = "127.0.0.1";
                currentDebugger = debuggerProcess.start();
            } catch (IOException e)
            {
                String fa = "";
                for (String a : args) fa += (fa.isEmpty()? "" : " ") + a;
                
                status = "IOException while running: "+fa;
                System.err.println(status);
                e.printStackTrace();
            }
        }
        
        if ( status != null ) return status;

        // Connect debugger client.
        if ( currentDebugger != null ) 
        {
            TSocket s = new TSocket(currentHost, currentPort);
            TProtocol protocol = debuggerProtocolFactory.getProtocol(s);
            debugger.instantiateDebugger(protocol);
        
            try {
                BufferedReader logReader = new BufferedReader(
                        new InputStreamReader(currentDebugger.getInputStream()));
                    
                Writer logWriter = new BufferedWriter(new FileWriter(debugLogFile));
                String line = "";
                while ( (line = logReader.readLine()) != null )
                    logWriter.write(line + "\n");
    
                logWriter.close();
                logReader.close();
                
                int rs = currentDebugger.waitFor();
                if ( rs != 0 ) status = "Query returned non-zero exit status";
            } catch (IOException e) {
                status = "IOException while running query.";
                e.printStackTrace();
            } catch (InterruptedException e) {
                status = "Query interrputed while running...";
                e.printStackTrace();
            }
        }
     
        currentDebugger = null;
        return status;
    }
    
    // Interface to GUI.
    public LinkedList<String> getInputRelationNames() {
        LinkedList<String> r = null;
        if ( debugger != null ) r = debugger.getInputRelationNames();
        return r;
    }

    public LinkedList<String> getDBToasterMapNames() {
        LinkedList<String> r = null;
        if ( debugger != null ) r = debugger.getDatastructureNames();
        return r;
    }

    // Step method metadata.
    public Class<?> getStepDebugTupleType(String relationName)
    {
        Class<?> r = null;
        if ( debugger != null )
            r = debugger.getStepDebugTupleType(relationName);
        return r;
    }
    
    public Class<?> getStepDataTupleType(String relationName)
    {
        Class<?> r = null;
        if ( debugger != null )
            r = debugger.getStepDataTupleType(relationName);
        return r;
    }
    
    public int getStreamId(String relationName)
    {
        int r = -1;
        if ( debugger != null ) r = debugger.getStreamId(relationName);
        return r;
    }

    // Datastructure retrieval metadata
    public Class<?> getDatastructureClassType(String dsName)
    {
        Class<?> r = null;
        if ( debugger !=  null)
            r = debugger.getDatastructureClassType(dsName);
        return r;
    }

    public Class<?> getDatastructureKeyClassType(String dsName)
    {
        Class<?> r = null;
        if ( debugger !=  null)
            r = debugger.getDatastructureKeyClassType(dsName);
        return r;
    }
    
    // Debugger invocation.

    public void step(String relationName, Object tuple)
    {
        if ( debugger != null ) debugger.step(relationName, tuple);
    }
    
    public void stepn(String relationName, int n)
    {
        if ( debugger != null ) debugger.stepN(relationName, n);
    }
    
    public Object get(String dsName)
    {
        Object r = null;
        if ( debugger != null ) r = debugger.getDatastructure(dsName);
        return r;
    }
}
