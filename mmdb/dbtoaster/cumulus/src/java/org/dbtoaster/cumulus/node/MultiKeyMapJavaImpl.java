package org.dbtoaster.cumulus.node;

import java.io.*;
import java.util.*;
import com.sleepycat.je.*;

public class MultiKeyMapJavaImpl {
  public static final int DEFAULT_CACHE_SIZE = 128*1024*1024;
  protected static Environment env = null;
  
  protected final int                         numkeys;
  protected final Integer                     wildcard;
  protected final Double                      defaultValue;
  protected final String                      basepath;
  protected final String                      dbName;
  protected HashMap<DatabaseEntry,MKPattern>  patterns;
  protected Database                          basemap;
    
  private static ArrayList<Integer[]> translateArray(Integer[][] array){
    ArrayList<Integer[]> ret = new ArrayList<Integer[]>();
    for(Integer[] entry : array) { ret.add(entry); }
    return ret;
  }
  
  public MultiKeyMapJavaImpl(int numkeys, Integer[][] patterns){
    this(numkeys, translateArray(patterns));
  }
  
  public MultiKeyMapJavaImpl(int numkeys, Integer[][] patterns, String dbName, Double defaultValue){
    this(numkeys, translateArray(patterns), dbName, defaultValue);
  }
  
  public MultiKeyMapJavaImpl(int numkeys, List<Integer[]> patterns){
    this(numkeys, patterns, "", 0.0);
  }
  
  public MultiKeyMapJavaImpl(int numkeys, List<Integer[]> patterns, String dbName, Double defaultValue){
    this(numkeys, patterns, dbName, "/tmp", defaultValue, null);
  }
  
  public MultiKeyMapJavaImpl(int numkeys, List<Integer[]> patterns, String dbName, String basepath, Double defaultValue, Integer wildcard){
    this.numkeys = numkeys;
    this.wildcard = wildcard;
    this.defaultValue = defaultValue;
    this.basepath = basepath;
    this.dbName = dbName;
    this.patterns = new HashMap<DatabaseEntry,MKPattern>();
    
    if(env == null){
      EnvironmentConfig envConfig = new EnvironmentConfig();
      envConfig.setAllowCreate(true);
      envConfig.setLocking(false);
      envConfig.setTransactional(false);
      envConfig.setCacheSize(DEFAULT_CACHE_SIZE);
      env = new Environment(new File(basepath), envConfig);
    }
    
    String primaryName = basepath + "/db_" + dbName + "_primary.db";
    System.out.println("Creating db at : " + primaryName);
    DatabaseConfig dbConf = new DatabaseConfig();
    dbConf.setAllowCreate(true);
    this.basemap = env.openDatabase(null, primaryName, dbConf);
    
    for(Integer[] pattern : patterns){
      add_pattern(pattern);
    }
  }
  
  public void add_pattern(Integer[] pattern){
    String secondaryName = basepath + "/db_" + dbName + "_" + patterns.size() + ".db";
    System.out.println("Creating secondary db at : " + secondaryName);
    patterns.put(serializeKey(pattern), new MKPattern(pattern, env, secondaryName, basemap));
  }
  
  public Double get(Integer[] key){
    DatabaseEntry entry = new DatabaseEntry();
    if(basemap.get(null, serializeKey(key), entry, LockMode.DEFAULT) == OperationStatus.SUCCESS){
      return deserializeValue(entry);
    } else {
      return defaultValue;
    }
  }
  
  public void put(Integer[] key, Double value){
    basemap.put(null, serializeKey(key), serializeValue(value));
  }
  
  public boolean has_key(Integer[] key){
    DatabaseEntry entry = new DatabaseEntry();
    entry.setPartial(0, 0, true);
    return basemap.get(null, serializeKey(key), entry, LockMode.DEFAULT) == OperationStatus.SUCCESS;
  }
  
  public MKFullCursor fullScan(){
    return new MKFullCursor(basemap.openCursor(null, CursorConfig.DEFAULT));
  }
  
  public MKCursor scan(Integer[] partialKey){
    int cnt = 0;
    for(Integer dim : partialKey){
      if(dim != wildcard) { cnt ++; }
    }
    Integer[] pattern = new Integer[cnt];
    cnt = 0;
    for(int i = 0; i < partialKey.length; i++){
      if(partialKey[i] != wildcard) { pattern[cnt] = i; cnt++; }
    }
    return patterns.get(serializeKey(pattern)).getCursor(partialKey, basemap);
  }
  
  protected void cleanup(){
    
  }
  
  protected static void serializeKey(Integer[] key, DatabaseEntry entry){
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for(Integer dim : key) { 
      sb.append(sep);
      sb.append(dim);
      sep = ",";
    }
    entry.setData(sb.toString().getBytes());
  }
  
  protected static DatabaseEntry serializeKey(Integer[] key){
    DatabaseEntry entry = new DatabaseEntry();
    serializeKey(key, entry);
    return entry;
  }
  
  protected static Integer[] deserializeKey(DatabaseEntry key){
    String preSplitKey = new String(key.getData());
    String[] splitKey = preSplitKey.split(",");
    Integer[] parsed = new Integer[splitKey.length];
    for(int i = 0; i < parsed.length; i++){
      parsed[i] = Integer.parseInt(splitKey[i]);
    }
    return parsed;
  }
  
  protected static Double deserializeValue(DatabaseEntry entry){
    return new Double(new String(entry.getData()));
  }
  
  protected static DatabaseEntry serializeValue(Double entry){
    return new DatabaseEntry(entry.toString().getBytes());
  }
  
  protected class MKPattern implements SecondaryKeyCreator {
    protected final Integer[] pattern;
    protected final SecondaryDatabase index;
    private Integer[] patternBuffer;
    
    public MKPattern(Integer[] pattern, Environment env, String databaseName, Database primaryDatabase){
      this.pattern = pattern;
      patternBuffer = new Integer[pattern.length];
      
      SecondaryConfig dbConfig = new SecondaryConfig();
      dbConfig.setAllowCreate(true);
      dbConfig.setSortedDuplicates(true);
      dbConfig.setKeyCreator(this);
      index = env.openSecondaryDatabase(null, databaseName, primaryDatabase, dbConfig);
    }
    
    public MKCursor getCursor(Integer[] partialKey, Database primary){
      DatabaseEntry entry = new DatabaseEntry();
      entry.setPartial(0, 0, true);
      SecondaryCursor c = index.openCursor(null, CursorConfig.DEFAULT);
      c.getSearchKey(createSecondaryKey(partialKey), entry, LockMode.DEFAULT);
      return new MKCursor(c, primary);
    }
    
    public DatabaseEntry createSecondaryKey(Integer[] key){
      DatabaseEntry result = new DatabaseEntry();
      createSecondaryKey(key, result);
      return result;
    }
    
    public void createSecondaryKey(Integer[] pkey, DatabaseEntry skey){
      for(int i = 0; i < pattern.length; i++){
        patternBuffer[i] = pkey[pattern[i]];
      }
      MultiKeyMapJavaImpl.serializeKey(patternBuffer, skey);
    }
    
    public boolean createSecondaryKey(SecondaryDatabase secondary, DatabaseEntry key, DatabaseEntry data, DatabaseEntry result){
      Integer[] original = MultiKeyMapJavaImpl.deserializeKey(key);
      createSecondaryKey(original, result);
      return true;
    }
  }
  
  protected class MKFullCursor {
    protected Cursor c;
    protected DatabaseEntry key, data;
    protected boolean first;
    
    public MKFullCursor(Cursor c){
      this.c = c;
      this.first = false;
      key = new DatabaseEntry();
      data = new DatabaseEntry();
    }
    
    public boolean next(){
      if (first) {
        return c.getFirst(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
      } else {
        return c.getNextDup(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
      }
    }
    
    public Integer[] key(){
      return MultiKeyMapJavaImpl.deserializeKey(key);
    }
    
    public Double value(){
      return MultiKeyMapJavaImpl.deserializeValue(data);
    }
    
    public void close(){
      c.close();
    }
  }
  
  protected class MKCursor {
    protected SecondaryCursor c;
    protected Database primary;
    protected DatabaseEntry key, pKey, data;
    protected boolean first;
    
    public MKCursor(SecondaryCursor c, Database primary){
      this.c = c;
      this.primary = primary;
      key = new DatabaseEntry();
        key.setPartial(0, 0, true);
      pKey = new DatabaseEntry();
      data = new DatabaseEntry();
      first = true;
    }
    
    public void replace(Double value){
      primary.put(null, pKey, MultiKeyMapJavaImpl.serializeValue(value));
    }
    
    public boolean next(){
      if (first) { 
        first = false;
        return c.getCurrent(key, pKey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
      } else {
        return c.getNextDup(key, pKey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
      }
    }
    
    public Integer[] key(){
      return MultiKeyMapJavaImpl.deserializeKey(pKey);
    }
    
    public Double value(){
      return MultiKeyMapJavaImpl.deserializeValue(data);
    }
    
    public void close(){
      c.close();
    }
  }
}
