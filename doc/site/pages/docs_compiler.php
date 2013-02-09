
<div class="codeblock">$&gt; dbtoaster [options] &lt;input file 1&gt; [&lt;input file 2&gt; [...]]
</div>

<a name="options"></a>
<?= chapter("Command Line Options") ?>
<dl>
<dt class="code">-c &lt;target file&gt;</dt>
<dd>Compile the query into a standalone binary.  By default, the C++ code generator will be used with G++ to generate the binary.  An alternate compiled target language (currently, C++ or Scala) may be selected using the <span class="code">-l</span> flag.</dd>

<dt class="code">-l &lt;language&gt;</dt>
<dd>Compile the query into the specified target language (see below).  By default, the query will be interpreted.  The use of this flag overrides any previous <span class="code">-l</span> or <span class="code">-r</span>.</dd>

<dt class="code">-o &lt;output file&gt;</dt>
<dd>Redirect the compiler's output to the specified file.  If used in conjunction with <span class="code">-c</span>, the source code for the compiled binary will be directed to this file.  The special output filename '<span class="code">-</span>' refers to stdout.  By default, output is directed to stdout, or discarded if the <span class="code">-c</span> flag is used.</dd>

<dt class="code">-r</dt>
<dd>Run the query (queries) in interpreter mode, overriding any target language previously specified by a <span class="code">-l</span>.  This is the default.</dd>

<dt class="code">-F &lt;optimization&gt;</dt>
<dd>Activate the specified optimization flag.  These are documented below.</dd>

<dt class="code">-O1 | -O2 | -O3</dt>
<dd>Set the optimization level to 1, 2, or 3 respectively.  At optimization level 1, compilation is faster and generated code is (usually) easier to understand and follow.  At optimization level 3, compilation is slower, but more efficient code is produced.  Optimization level 2 is the default.  Overrides any prior <span class="code">-O</span> flags provided on the command line.</dd>

<dt class="code">-I &lt;dir&gt;</dt>
<dd>When invoking a second-stage compiler with the <span class="code">-c</span> flag, add <span class="code">dir</span> to the include file search path.</dd>

<dt class="code">-L &lt;dir&gt;</dt>
<dd>When invoking a second-stage compiler with the <span class="code">-c</span> flag, add <span class="code">dir</span> to the library file search path.</dd>

<dt class="code">-D &lt;macro&gt;</dt>
<dd>When invoking a second-stage compiler with the <span class="code">-c</span> flag, define the preprocessor macro <span class="code">macro</span>.</dd>

<dt class="code">-g &lt;arg&gt;</dt>
<dd>When invoking a second-stage compiler with the <span class="code">-c</span> flag, pass through the argument <span class="code">arg</span>.</dd>

<dt class="code">--depth &lt;level&gt;</dt>
<dd>Limit the compiler's maximum recursive depth. By default, DBToaster compiles queries with the depth set to infinity. </dd>

<dt class="code">--custom-prefix &lt;prefix&gt;</dt>
<dd>Prefix all dbtoaster-generated symbols with this character string.  Use this if DBToaster generates a symbol that conflicts with a symbol in user code.  (Default: "__")</dd>

</dl>

<a name="languages"></a>
<?= chapter("Supported Languages") ?>
<table border=1>
  <tr>
    <th align="center">Language</th>
    <th align="center">Commandline Name</th>
    <th align="center">Output Format</th>
    <th align="center">Description</th>
  </tr>

  <tr>
    <td align="center">DBT Relational Calculus</td>
    <td align="center" class="code">calc</td>
    <td align="center">output</td>
    <td align="left">DBToaster's internal query representation.  This is a direct translation of the input queries.</td>
  </tr>
  
  <tr>
    <td align="center">M3</td>
    <td align="center" class="code">m3</td>
    <td align="center">output</td>
    <td align="left">A map-mantenance messages program.  This is the set of triggers (written in DBT Relational Calculus) that will incrementally maintain the input queries and all supporting datastructures.</td>
  </tr>

  <tr>
    <td align="center">C++</td>
    <td align="center" class="code">cpp</td>
    <td align="center">output/compiled</td>
    <td align="left">A C++ class implementing the queries.</td>
  </tr>

  <tr>
    <td align="center">Scala</td>
    <td align="center" class="code">scala</td>
    <td align="center">output/compiled</td>
    <td align="left">A Scala class implementing the queries.</td>
  </tr>

</table>

<a name="opt_flags"></a>
<?= chapter("Optimization Flags"); ?>

These flags are passed to the dbtoaster compiler with the <span class="code">-F</span> flag.  The <span class="code">-O1</span> and <span class="code">-O3</span> flags each activate a subset of these flags. <span class="code">-O2</span> is used by default (no optimization flags active).

<dl>
  
  <dt class="code">HEURISTICS-ENABLE-INPUTVARS</dt>
  <dd>Enable experimental support for incremental view caches.  Queries with joins (and correlations) on inequality predicates are implemented in a way that corresponds roughly to nested-loop one-way joins in stream processing (a tree-based implementation is in development).  If this flag is on, the compiler will cache and incrementally maintain the results of this one-way join.  This is typically a bad idea, since the cost of maintaining the cached values is often higher than the cost of the nested loop scan.  However, if the domains of the variables appearing in the join predicate are small, this flag can drastically improve performance (e.g., for the VWAP example query).  Future versions of DBToaster will include a cost-based optimizer that automatically applies this flag when appropriate.  This optimization is not activated by default at any optimization level.</dd>  

  <dt class="code">HEURISTICS-PULL-OUT-VALUES</dt>
  <dd>Prevent value terms (variables and comparisons) from being materialized inside maps. In certain cases (e.g., mddb/query2.sql), this option reduces the number of generated maps and speed-ups the compilation time at the expense of doing more computation at runtime.</dd>  

  <dt class="code">EXPRESSIVE-TLQS</dt>
  <dd>By default, each user-provided (top-level) query is materialized as a single map. If this flag is turned on, the compiler will materialize top-level queries as multiple maps (if it is more efficient to do so), and only combine them on request.  For more complex queries (in particular nested aggregate, and  AVG aggregate queries), this results in faster processing rates, and if fresh results are required less than once per update, a lower overall computational cost as well.  However, because the final evaluation of the top-level query is not performed until a result is requested, access latencies are higher.  This optimization is not activated by default at any optimization level.</dd>
  
  <dt class="code">IGNORE-DELETES</dt>
  <dd>Do not generate code for deletion triggers.  The resulting programs will be simpler, and sometimes have fewer datastructures, but will not support deletion events.  This optimization is not activated by default at any optimization level.</dd>
  
  <dt class="code">HEURISTICS-ALWAYS-UPDATE</dt>
  <dd>In some cases, it is slightly more efficient to re-evaluate expressions from scratch rather than maintaining them with their deltas (for example, certain queries containing nested aggregates).  Normally the compiler's heuristics will make a best-effort guess about whether to re-evaluate or incrementally maintain the expression.  If this flag is on, the compiler will incrementally maintain all expressions and never re-evaluate.</dd>  
  
  <dt class="code">HASH-STRINGS</dt>
  <dd>Do not use strings during evalation. All strings are immediately replaced by their integer hashes (using each runtime's native hashing mechanism) as soon as they are parsed.  This makes query evalation faster, but is not guaranteed to produce correct results if a hash collision occurs.  Furthermore, strings that would normally appear in the output are output as their integer hash values instead.  This optimization is not activated by default at any optimization level.</dd>    

  <dt class="code">COMPILE-WITH-STATIC</dt>
  <dd>Perform static linking on compiled binaries (e.g., invoke gcc with <span class="code">-static</span>).  The resulting binaries will be faster the first time they are run.  This optimization is not activated by default at any optimization level.</dd>  

  <dt class="code">CALC-DONT-CREATE-ZEROES</dt>
  <dd>Avoid creating empty relation terms during pre-evaluation.  Empty relation terms are aggressively propagated thoughout expressions in which they occur, and may result in expressions that do not need to be incrementally maintained (because they are guaranteed to be always empty).  Activating this flag is only useful if you want to inspect the generated Calculus/M3 code by hand. This optimization is not activated by default at any optimization level.</dd>  
  
  <dt class="code">AGGRESSIVE-FACTORIZE</dt>
  <dd>When optimizing expressions in DBToaster relational calculus, perform factorization as aggressively as possible.  For some queries, particularly those with nested subqueries, this can generate much more efficient code.  However, it makes compilation slower on some queries.  This optimization is automatically activated by <span class="code">-O3</span>.</dd>
  
  <dt class="code">AGGRESSIVE-UNIFICATION</dt>
  <dd>When optimizing expressions in DBToaster relational calculus, inline lifted variables wherever possible, even if the lift term can not be eliminated entirely.  This can produce substantially tighter code for queries with lots of constants, but slightly increases compilation time.  This optimization is automatically activated by <span class="code">-O3</span>.</dd>

  <dt class="code">DELETE-ON-ZERO</dt>
  <dd>In generated code, when a map value becomes 0, remove the value from the map.  Resulting programs are more efficient over long stretches of insertions and deletions.  This optimization is automatically activated by <span class="code">-O3</span>.</dd>

  <dt class="code">COMPILE-WITHOUT-OPT</dt>
  <dd>Request that the second-stage compiler disable any unnecessary optimizations (e.g., by default, GCC is invoked with <span class="code">-O3</span>, but not if this flag is active). This optimization is automatically activated by <span class="code">-O1</span>.</dd>

  <dt class="code">WIDE-TUPLE</dt>
  <dd>Use nested tuples in generated C++ programs. This option is mostly used at lower compilation levels (depth 0 or 1) to overcome the Boost limitation that tuples may contain at most 50 attributes.</dd>

  <dt class="code">WEAK-EXPR-EQUIV</dt>
  <dd>When testing for expression equivalence, perform only a naive structural comparison rather than a (at least quadratic, and potentially exponential) matching.  This accelerates compilation, but may result in the creation of duplicate maps.  This optimization is automatically activated by <span class="code">-O1</span>.</dd>
    
  <dt class="code">CALC-NO-OPTIMIZE</dt>
  <dd>Do not apply calculus optimizations that simplify delta expressions. This option prevents range restrictions from being propragated through expressions, which usually leads to significantly worse performance. The resulting code is close to what the naive recursive incremental algorithm would produce. This flag is not activated by default at any optimization level.</dd>
    
  <dt class="code">CALC-NO-DECOMPOSITION</dt>
  <dd>Do not apply query decomposition when computing deltas. This option is not activated by default at any optimization level.</dd>
        
  <dt class="code">K3-NO-OPTIMIZE</dt>
  <dd>Do not apply functional optimizations. This optimization is automatically activated by <span class="code">-O1</span>.</dd>
  
  <dt class="code">DUMB-LIFT-DELTAS</dt>
  <dd>When computing the viewlet transform, use the delta rule for lifts precisely as described in the PODS10 paper.  If this flag is <b>not</b> active, a postprocessing step is applied to lift deltas, that range-restricts the resulting expression to only those tuples that are affected. This optimization is automatically activated by <span class="code">-O1</span>.</dd>

</dl>