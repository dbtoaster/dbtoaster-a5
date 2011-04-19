#include <iostream>
#include <fstream>
#include <list>
#include <map>
#include <queue>
#include <sstream>
#include <vector>
#include <tr1/tuple>

#include <boost/ptr_container/ptr_list.hpp>
#include <boost/ptr_container/ptr_map.hpp>

#include "llvm/Module.h"
#include "llvm/ValueSymbolTable.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Bitcode/ReaderWriter.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/GenericValue.h"
#include "llvm/Linker.h"
#include "llvm/System/Path.h"
#include "llvm/Support/Mangler.h"
#include "llvm/Constants.h"
#include "llvm/DerivedTypes.h"
#include "llvm/System/DynamicLibrary.h"
#include "llvm/ModuleProvider.h"

#define LLVM_GPP "/home/anton/software/llvm-gcc/bin/g++"
#define LLVM_GPP_FLAGS " -emit-llvm -c -I/home/anton/software/boost/include/boost-1_39"

#define MAIN_SYMBOL "main"
#define REPOSITORY "."

using namespace std;
using namespace tr1;
using namespace llvm;
using namespace boost;

typedef int QueryId;
typedef int StreamId;

QueryId qid = 0;
QueryId genQueryId() { return ++qid; }


// Hacked up name mangler
string mangleName(string functionName, vector<string> functionArgTypes) {
  ostringstream o_ss;
  o_ss << functionName.length();
  string r = "_Z" + o_ss.str() + functionName;
  for (vector<string>::iterator i = functionArgTypes.begin();
       i != functionArgTypes.end(); ++i)
  {
    if ( *i == "int" ) 
      r += "i";
    else if ( *i == "float" )
      r += "f";
    else if ( *i == "double" )
      r += "d";
  }
  if ( functionArgTypes.size() == 0 ) r += "v";
  return r;
}

// Struct for runnable queries, including LLVM execution code.
struct QueryManifest {
  list<string> handlers;
  list<string> libraries;
};

QueryManifest loadQueryManifest(string manifestFile)
{
  QueryManifest r;

  ifstream mf(manifestFile.c_str());
  int linenum = 0;
  while ( mf.good() ) {
    char buf[256];
    mf.getline(buf, sizeof(buf));

    string line(buf);
    string::size_type l = line.length();

    if ( line.substr(0, 8) == "handler," ) {
      r.handlers.push_back(line.substr(8, l-8));
    }
    else if ( line.substr(0, 8) == "library," ) {
      r.libraries.push_back(line.substr(8, l-8));
    }
    else if ( mf.eof() ) break;
    else {
      cout << "Invalid manifest entry at line "
          << linenum << ": " << line << endl;
      r.handlers.clear();
      r.libraries.clear();
      mf.close();
      return r;
    }

    cout << "Read line: " << line << endl;

    ++linenum;
  }

  mf.close();
  return r;
}

struct ExecHandler {
  QueryId queryId;
  string handlerName;
  Function* handler;
  ModuleProvider* query;

  ExecHandler(QueryId qid, string name, Function* f, ModuleProvider* mp)
    : queryId(qid), handlerName(name), handler(f), query(mp)
  {}

  void runHandler(ExecutionEngine* handlerRunner) {
    // TODO: here we're checking argument types. Compile this away with LLVM.
    const FunctionType *FTy = handler->getFunctionType();
    std::vector<GenericValue> args;

    if ( FTy->getNumParams() > 0 ) {

      cout << "Fn: " << FTy->getNumParams() << endl;

      FunctionType::param_iterator p_it = FTy->param_begin();
      FunctionType::param_iterator p_end = FTy->param_end();
      for (int i = 0; p_it != p_end; ++p_it, ++i) {

	cout << "Param " << i << " type : " << (*p_it)->getDescription() << endl;

	// TODO: fill in argument values from stream.
	Type::TypeID pt = (*p_it)->getTypeID();
	GenericValue a;
	if ( pt == Type::IntegerTyID ) {
	  a.IntVal = APInt(32, 0);
	  args.push_back(a);	  
	} 
	else if ( pt == Type::FloatTyID ) {
	  a.FloatVal = 0.0;
	  args.push_back(a);
	}
	else if ( pt == Type::DoubleTyID ) { 
	  a.DoubleVal = 0.0;
	  args.push_back(a);
	}
	else {
	  cerr << "Unsupported argument type " << (*p_it)->getDescription() << endl;
	}
      }
    
      cout << "Args: " << args.size() << endl;
    }

    cout << "Running handler " << handlerName << endl;

    //handler->dump();

    GenericValue rv = handlerRunner->runFunction(handler, args);

    cout << "Ran handler." << endl;
  }

  // TODO: clean termination, e.g. on query removal
  bool done() { return false; }

  // Accessors
  ModuleProvider* getQuery() { return query; }
};


struct ExecQuery {
  QueryId id;
  ModuleProvider* query;
  Function* queryEntry;
  char** queryEnv;

  ExecQuery(QueryId q, ModuleProvider* m, Function* f, char **envp)
    : id(q), query(m), queryEntry(f), queryEnv(envp)
  {}

  void runMain(ExecutionEngine* queryRunner) {
    if ( queryEntry == NULL ) {
      cout << "Null entry function for query!" << endl;
      return;
    }

    string query_progname = "test_query";
    std::vector<string> args;
    args.push_back(query_progname);

    cout << "Running query main..." << endl;
    int rc = queryRunner->runFunctionAsMain(queryEntry, args, queryEnv);
    cout << "Ran query main, return code: " << rc << endl;
  }

};


struct Catalog {
  typedef ptr_map<QueryId, ModuleProvider>     QueryModules;
  typedef ptr_map<QueryId, ExecHandler>         QueryMains;
  typedef ptr_map<QueryId, list<ExecHandler> >  QueryHandlers;
  typedef ptr_map<StreamId, list<ExecHandler> > StreamListeners;

  QueryModules    queryModules;
  QueryMains      queryMains;
  QueryHandlers   queryHandlers;
  StreamListeners streamListeners;

  // Accessors
  list<ExecHandler>* getQueryHandlers(QueryId id) {
    list<ExecHandler>* r = NULL;
    QueryHandlers::iterator found = queryHandlers.find(id);
    if ( found != queryHandlers.end() ) {
      r = found->second;
    }

    return r;
  }

  // Bulk method for adding all handlers from a query
  void addQuery(QueryId id, Module* query,
		Function* mainf, const map<string, Function*>& handlers)
  {
    ExistingModuleProvider* mp = new ExistingModuleProvider(query);
    queryModules.insert(id, mp);
    queryMains.insert(id, new ExecHandler(id, "main", mainf, mp));
    map<string, Function*>::const_iterator h_it = handlers.begin();
    map<string, Function*>::const_iterator h_end = handlers.end();
    for (; h_it != h_end; ++h_it) {
      ExecHandler h(id, h_it->first, h_it->second, mp);
      queryHandlers[id].push_back(h);
    }

    // TODO: streams
  }

  void printFunctions() {
    QueryModules::iterator qm_it = queryModules.begin();
    QueryModules::iterator qm_end = queryModules.end();

    for (; qm_it != qm_end; ++qm_it) {
      Module* query = qm_it->second->getModule();

      Module::iterator fn_it = query->begin();
      Module::iterator fn_end = query->end();
      for (; fn_it != fn_end; ++fn_it) {
	cout << "Function: " << fn_it->getName() << endl;
      }
    }
  }

  void printSymbols() {
    QueryModules::iterator qm_it = queryModules.begin();
    QueryModules::iterator qm_end = queryModules.end();

    for (; qm_it != qm_end; ++qm_it) {
      Module* query = qm_it->second->getModule();
      ValueSymbolTable::ValueMap::iterator sym_it =
	query->getValueSymbolTable().begin();
    
      ValueSymbolTable::ValueMap::iterator sym_end =
	query->getValueSymbolTable().end();
    
      for (; sym_it != sym_end; ++sym_it) {
	cout << "Found symbol '" <<  sym_it->getKeyData() << "'" << endl;
      }
    }
  }

  void printLibraries() {
    QueryModules::iterator qm_it = queryModules.begin();
    QueryModules::iterator qm_end = queryModules.end();

    for (; qm_it != qm_end; ++qm_it) {
      Module* query = qm_it->second->getModule();
      
      cout << "Dependent on " << query->lib_size() << " libraries." << endl;
      Module::lib_iterator lib_it = query->lib_begin();
      Module::lib_iterator lib_end = query->lib_end();

      for (; lib_it != lib_end; ++lib_it) {
	cout << "Lib: " << *lib_it << endl;
      }
    }
  }

  void printModules() {
    QueryModules::iterator qm_it = queryModules.begin();
    QueryModules::iterator qm_end = queryModules.end();

    for (; qm_it != qm_end; ++qm_it) {
      Module* query = qm_it->second->getModule();

      cout << "Printing module " << query->getModuleIdentifier()
	   << "..." << endl;

      query->dump();
    }
  }

};

struct Scheduler {
  Catalog& catalog;

  queue<ExecHandler> workQueue;
  ExecutionEngine* handlerRunner;

  Scheduler(Catalog& c) : catalog(c), handlerRunner(NULL) {}

  void runOnce() {
    // Simple round-robin scheduling
    // TODO: clean up with list data structure rather than queues.
    queue<ExecHandler> nextRound;
    while (!workQueue.empty()) {
      ExecHandler& h = workQueue.front();
      workQueue.pop();
      
      h.runHandler(handlerRunner);
      
      if ( !h.done() )
	nextRound.push(h);
    }

    while ( !nextRound.empty() ) {
      workQueue.push(nextRound.front());
      nextRound.pop();
    }
  }

  // Scheduler thread run function
  // TODO: could be multi-threaded provided synchronized
  // access to work queue.
  void run() {

    while (1) {
      runOnce();
      // TODO: wait for notification of new work
    }
  }

  // Note, this should be called by a connection thread,
  // synchronized on the work queue.
  void addHandler(ExecHandler& h) {
    if ( handlerRunner == NULL )
      handlerRunner = ExecutionEngine::create(h.getQuery());

    else
      handlerRunner->addModuleProvider(h.getQuery());

    workQueue.push(h);

    // TODO: wake up the scheduler thread
  }
};

struct Compiler {
  Catalog& catalog;
  Scheduler& scheduler;

  // Source code file * manifest file
  typedef tuple<string, string> QuerySource;
  queue<QuerySource> sourceQueue;

  Compiler(Catalog& c, Scheduler& s) : catalog(c), scheduler(s) {}

  // TODO: invoke from network message
  void compileQuery(string sourceFileData, string manifestFileData)
  {
    // Write data to files.
    string sourceFileName = tempnam(REPOSITORY, "dbtSource");
    ofstream sourceFile(sourceFileName.c_str());
    sourceFile << sourceFileData;
    sourceFile.close();

    string manifestFileName = tempnam(REPOSITORY, "dbtManifest");
    ofstream manifestFile(manifestFileName.c_str());
    manifestFile << manifestFileData;
    manifestFile.close();

    // Enqueue file names.
    sourceQueue.push(make_tuple(sourceFileName, manifestFileName));

    // TODO: wake up compiler thread.
  }

  void run()
  {
    while (1) {
      while ( !sourceQueue.empty() ) {
	QuerySource qs = sourceQueue.front();
	sourceQueue.pop();

	string sourceFile = get<0>(qs);
	string manifestFile = get<1>(qs);

	registerQuery(sourceFile, manifestFile);
      }

      // TODO: wait for notification of new compile task.
    }
  }

  // Helper functions
  void registerQuery(string sourceFile, string manifestFile)
  {
    QueryManifest qmf = loadQueryManifest(manifestFile);

    if ( !qmf.handlers.empty() ) {
      Module* queryModule = compileToBitcode(sourceFile, qmf.libraries);
      if ( queryModule == NULL ) {
	cout << "LLVM compilation failed!" << endl;
	return;
      }

      QueryId id = genQueryId();
      Function* mainf = getQueryMain(id, queryModule);
      map<string, Function*> handlers =
	getCompiledHandlers(id, queryModule, qmf.handlers);

      // Add to catalog
      catalog.addQuery(id, queryModule, mainf, handlers);

      // TODO: synchronize w/ worker threads
      list<ExecHandler>* catalogHandlers = catalog.getQueryHandlers(id);
      list<ExecHandler>::iterator ch_it = catalogHandlers->begin();
      list<ExecHandler>::iterator ch_end = catalogHandlers->end();
      for (; ch_it != ch_end; ++ch_it)
	scheduler.addHandler(*ch_it);
    }
    else {
      cout << "No handlers found in manifest " << manifestFile << endl;
    }
  }

  // Initialize LLVM execution engine, JITting the query code, 
  // and retrieve function pointers according to mangled names.
  map<string, Function*> getCompiledHandlers(
      QueryId id, Module* query, list<string> handlers)
  {
    map<string, Function*> r;

    // Load handlers
    list<string>::iterator h_it = handlers.begin();
    list<string>::iterator h_end = handlers.end();

    for (; h_it != h_end; ++h_it) {
      string functionName;
      vector<string> functionArgTypes;
      string line = *h_it;

      string::iterator fe = line.begin();
      string::iterator fs = line.begin();
      unsigned int pos = 0;

      while (1) {
	if (*fe == ',' || fe == line.end() ) {
	  string field(fs,fe);
	  // Function name 
	  if ( pos == 0 ) { functionName = field; }

	  // Return val type (ignored for now).
	  else if ( pos == 1 ) { }

	  // Function arg types
	  else { functionArgTypes.push_back(field); }

	  if ( fe == line.end() ) break;
	  fs = fe+1;
	  ++pos;
	}
	++fe;
      }

      string mangledName = mangleName(functionName, functionArgTypes);
      cout << "Retrieving " << functionName << " as " << mangledName << endl;

      Function* f = query->getFunction(mangledName);
      if ( f != NULL ) {
	r[functionName] = f;
	cout << "Found function " << functionName << endl;
      }
      else
	cout << "Failed to find " << functionName << " as " << mangledName << endl;
    }

    cout << "Found " << r.size()
	 << " query handlers for query " << id << endl;

    return r;
  }

  Function* getQueryMain(QueryId id, Module* query) {
    return query->getFunction(MAIN_SYMBOL);
  }

  // LLVM JIT example
  Module* compileToBitcode(string cppFilename, const list<string>& libraries)
  {
    string::size_type idx = cppFilename.find('.');
    string bcFilename = cppFilename.substr(0, idx+1) + "o";

    string buildCmd = string() + LLVM_GPP + LLVM_GPP_FLAGS + " " +
      cppFilename;

    cout << "Compiled C++ for " << cppFilename << endl;

    int retval = system(buildCmd.c_str());
    if ( retval != 0 ) return NULL;

    MemoryBuffer* queryMB = MemoryBuffer::getFile(bcFilename.c_str());
    Module* r = ParseBitcodeFile(queryMB);

    cout << "Compiled bitcode for " << cppFilename << endl;

    // TODO: test linking against external libraries, e.g. boost, etc
    string queryName = "testQuery";
    Linker l(queryName, r);
    l.addSystemPaths();

    const vector<sys::Path> libPaths = l.getLibPaths();
    vector<sys::Path>::const_iterator path_it = libPaths.begin();
    vector<sys::Path>::const_iterator path_end = libPaths.end();

    cout << "Found " << libPaths.size() << " library paths." << endl;
    for (; path_it != path_end; ++path_it)
      cout << "Lib path " << path_it->toString() << endl;

    // Hack loading libstdc++ first.
    string stdcLoadError;
    sys::Path stdcPath = l.FindLib("stdc++.so");
    bool stdcLoadFailed = sys::DynamicLibrary::
	LoadLibraryPermanently(stdcPath.toString().c_str(), &stdcLoadError);

    if ( stdcLoadFailed  ) {
      cout << "Failed to link in stdc++ " << " error: " << stdcLoadError << endl;
      delete r;
      delete queryMB;
      return NULL;
    }
    else {
      cout << "Linked in stdc++, path: " << stdcPath.toString() << endl;
    }

    list<string>::const_iterator lib_it = libraries.begin();
    list<string>::const_iterator lib_end = libraries.end();

    bool native = false;

    for (; lib_it != lib_end; ++lib_it) {
      string libName = *lib_it;
      sys::Path libPath = l.FindLib(libName);

      /*
       * This form of linking doesnt seem to work for boost...
       if ( !libPath.isEmpty() ) {
       if ( l.LinkInArchive(libPath, native) ) {
       cout << "Failed to link in " << libName
       << " error: " << l.getLastError() << endl;
       delete r;
       delete queryMB;
       return NULL;
       }
       else {
       cout << "Linked in " << libName << ", path (" << native << "): "
       << libPath.toString() << endl;
       }
       }
       else {
       // Assume library is necessary and abort, cleaning up.
       cout << "Failed to find library " << libName << endl;
       delete r;
       delete queryMB;
       return NULL;
       }
      */

      string dllSuffix = sys::Path::GetDLLSuffix();
      libPath.eraseSuffix();
      libPath.appendSuffix(dllSuffix.substr(1, dllSuffix.length()-1));
      cout << "Dynamic lib for " << libName << ": " << libPath << endl;

      string loadError;
      bool loadFailed = sys::DynamicLibrary::
	LoadLibraryPermanently(libPath.toString().c_str(), &loadError);

      if ( loadFailed  ) {
	cout << "Failed to link in " << libName
	     << " error: " << loadError << endl;
	delete r;
	delete queryMB;
	return NULL;
      }
      else {
	cout << "Linked in " << libName << ", path: "
	     << libPath.toString() << endl;
      }
    }

    r = l.releaseModule();
  
    set<string> undefs;
    getAllUndefinedSymbols(r, undefs);
    cout << "Undefined " << undefs.size() << endl;
    set<string>::iterator un_it = undefs.begin();
    set<string>::iterator un_end = undefs.end();
    for (; un_it != un_end; ++un_it)
      cout << "Undefined: " << (*un_it) << endl;

    return r;
  }

  // Get undefined symbols.
  // Copied from LLVM source code.
  void getAllUndefinedSymbols(Module *M, set<string> &UndefinedSymbols) {
    set<string> DefinedSymbols;
    UndefinedSymbols.clear();

    // If the program doesn't define a main, try pulling one in from a .a file.
    // This is needed for programs where the main function is defined in an
    // archive, such f2c'd programs.
    Function *Main = M->getFunction("main");
    if (Main == 0 || Main->isDeclaration())
      UndefinedSymbols.insert("main");

    for (Module::iterator I = M->begin(), E = M->end(); I != E; ++I)
      if (I->hasName()) {
	if (I->isDeclaration())
	  UndefinedSymbols.insert(I->getName());
	else if (!I->hasLocalLinkage()) {
	  assert(!I->hasDLLImportLinkage()
		 && "Found dllimported non-external symbol!");
	  DefinedSymbols.insert(I->getName());
	}      
      }

    for (Module::global_iterator I = M->global_begin(), E = M->global_end();
	 I != E; ++I)
      if (I->hasName()) {
	if (I->isDeclaration())
	  UndefinedSymbols.insert(I->getName());
	else if (!I->hasLocalLinkage()) {
	  assert(!I->hasDLLImportLinkage()
		 && "Found dllimported non-external symbol!");
	  DefinedSymbols.insert(I->getName());
	}      
      }

    for (Module::alias_iterator I = M->alias_begin(), E = M->alias_end();
	 I != E; ++I)
      if (I->hasName())
	DefinedSymbols.insert(I->getName());

    // Prune out any defined symbols from the undefined symbols set...
    for (set<string>::iterator I = UndefinedSymbols.begin();
	 I != UndefinedSymbols.end(); )
      if (DefinedSymbols.count(*I))
	UndefinedSymbols.erase(I++);  // This symbol really is defined!
      else
	++I; // Keep this symbol in the undefined symbols list
  }

};


struct Runtime {
  Catalog catalog;
  Scheduler scheduler;
  Compiler compiler;

  Runtime()
    : scheduler(catalog), compiler(catalog,scheduler)
  {
    // TODO: set up scheduler thread
    // TODO: set up compiler thread, binding to network socket
  }

  void test() {
    compiler.registerQuery("query_test.cpp", "query_test.dbtmf");
    scheduler.runOnce();
  }
};

int main(int argc, char** argv, char **envp)
{
  Runtime dbToaster;
  dbToaster.test();
  return 0;
}
