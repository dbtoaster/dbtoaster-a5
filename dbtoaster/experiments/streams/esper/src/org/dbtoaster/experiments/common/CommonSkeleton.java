package org.dbtoaster.experiments.common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esperio.AdapterInputSource;
import com.espertech.esperio.csv.CSVInputAdapter;
import com.espertech.esperio.csv.CSVInputAdapterSpec;

public class CommonSkeleton {

  protected EPAdministrator admin;
  protected EPRuntime runtime;

  public CommonSkeleton(String configFile) {
    initialize(configFile);
  }
  
  public CommonSkeleton(Configuration config) {
    start(config);
  }

  public void initialize(String configFile) {
    Configuration config = null;
    if ( configFile != null ) {
      config = new Configuration();
      config.configure(configFile);
    }
    start(config);
  }
  
  public void start(Configuration config) {
    EPServiceProvider service = EPServiceProviderManager.getDefaultProvider(config);
    admin = service.getEPAdministrator();
    runtime = service.getEPRuntime();
  }

  public void terminate() {
    EPServiceProvider service = EPServiceProviderManager.getDefaultProvider();
    service.destroy();
  }
  
  public List<EPStatement> setupStatements(List<String> stmts) {
    List<EPStatement> epStmts = new LinkedList<EPStatement>();
    for (String stmt : stmts) { epStmts.add(admin.createEPL(stmt)); }
    return epStmts;
  }

  public List<EPStatement> setupScript(String scriptFile) {
    if ( scriptFile == null ) return null;
    BufferedReader scriptReader = null;
    try {
      scriptReader = new BufferedReader(new FileReader(scriptFile));
    } catch (FileNotFoundException e) { e.printStackTrace(); }
    
    if ( scriptReader == null ) return null;

    String line;
    String lastStmt = "";
    List<String> stmts = new LinkedList<String>();
    
    try {
      while ( (line = scriptReader.readLine()) != null ) {
        lastStmt += (lastStmt.isEmpty()? "" : " ")+line;
        if ( line.endsWith(";") ) {
          lastStmt = lastStmt.substring(0, lastStmt.length()-1);
          stmts.add(lastStmt.trim());
          lastStmt = "";
        }
      }
    } catch (IOException e) { e.printStackTrace(); }
    
    // Skip the last unfinished statement in the file.
    //if ( lastStmt.trim() != "" ) stmts.add(lastStmt);

    return setupStatements(stmts);
  }
  
  public CSVInputAdapter setupCSVSource(String sourceFile,
                                        String sourceEventType,
                                        String[] fieldOrder)
  {
    EPServiceProvider s = EPServiceProviderManager.getDefaultProvider();
    CSVInputAdapterSpec spec = new CSVInputAdapterSpec(
        new AdapterInputSource(sourceFile), sourceEventType);
    spec.setPropertyOrder(fieldOrder);
    return new CSVInputAdapter(s, spec);
  }
}
