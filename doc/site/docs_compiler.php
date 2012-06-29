
<div class="codeblock">$&gt; dbtoaster [options] &lt;input file 1&gt; [&lt;input file 2&gt; [...]]
</div>

<a name="options"/>
<?= chapter("Command Line Options") ?>
<dl>
<dt class="code">-c &lt;target file&gt;</dt>
<dd>Compile the query into a standalone binary.  By default, the C++ code generator will be used with G++ to generate the binary.  An alternate compiled target language (currently, C++, OCaml, or Scala) may be selected using the <span class="code">-l</span> flag.</dd>

<dt class="code">-l &lt;language&gt;</dt>
<dd>Compile the query into the specified target language (see below for a list of valid languages).  By default, the query will be interpreted.</dd>

<dt class="code">-o &lt;output file&gt;</dt>
<dd>Redirect the compiler's output to the specified file.  If used in conjunction with <span class="code">-c</span>, the source code for the compiled binary will be directed to this file.  Passing in <span class="code">-</span> as the output file will direct output to stdout.  By default, output is directed to stdout, or discarded if the <span class="code">-c</span> flag is used.</dd>

<dt class="code">-r</dt>
<dd>Run the query (queries) in interpreter mode.  This is the default, the flag is used to override any previous -l flags.</dd>

<dt class="code">-F &lt;optimization&gt;</dt>
<dd>Activate the specified optimization flag.  These are documented below.</dd>

<dt class="code">-O1 | -O2 | -O3</dt>
<dd>Set the optimization level to 1, 2, or 3 respectively.  At optimization level 1, compilation is faster and generated code is (usually) easier to understand and follow.  At optimization level 3, compilation is slower, but more efficient code is produced.  Optimization level 2 is the default.</dd>
</dl>

<a name="languages"/>
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
    <td align="left">A C++ implementation of the K3 execution plan.</td>
  </tr>

  <tr>
    <td align="center">Scala</td>
    <td align="center" class="code">scala</td>
    <td align="center">output/compiled</td>
    <td align="left">A Scala implementation of the K3 execution plan.</td>
  </tr>

</table>

<a name="opt_flags"/>
<?= chapter("Optimization Flags"); ?>

These flags are passed to the dbtoaster compiler with the <span class="code">-F</span> flag.  The <span class="code">-O1</span>, <span class="code">-O2</span>, and <span class="code">-O3</span> flags each activate a subset of these flags. <span class="code">-O2</span> is used by default.

<dl>
  <dt class="code">IGNORE-DELETES</dt>
  <dd>Do not generate code for deletion triggers.  The resulting programs will be simpler, and sometimes have fewer datastructures, but will not support deletion events.  Not activated by default at any optimization level.</dd>
  
  <dt class="code">HEURISTICS-ALWAYS-UPDATE</dt>
  <dd>In some cases, it is slightly more efficient to re-evaluate parts of the expression tree rather than computing an incremental update.  If this flag is on, the compiler will always compute updates incrementally, regardless. Not activated by default at any optimization level.</dd>  
  
  <dt class="code">HASH-STRINGS</dt>
  <dd>Do not use strings during evalation. All strings are replaced by their integer hash (using each runtime's native hashing mechanism).  This makes query evalation faster, but is not guaranteed to produce correct results if a hash collision occurs. Not activated by default at any optimization level.</dd>  

  <dt class="code">EXPRESSIVE-TLQS</dt>
  <dd>By default, each user-provided (top-level) query is materialized as a single map. If this flag is turned on, the compiler will materialize top-level queries as multiple maps (if it is more efficient to do so), and only combine them when requested to do so.  For more complex queries (in particular queries that use the AVG aggregate), this results in faster processing rates and a lower overall computational cost.  However, because the final evaluation of the top-level query is only performed only on request, access latencies are higher.  Not activated by default at any optimization level.</dd>  

  <dt class="code">COMPILE-WITH-STATIC</dt>
  <dd>Perform static linking on compiled binaries (e.g., invoke gcc with <span class="code">-static</span>).  The resulting binaries will be faster the first time they are run.  Not activated by default at any optimization level.</dd>  

  <dt class="code">CALC-DONT-CREATE-ZEROES</dt>
  <dd>Avoid creating empty relation terms during pre-evaluation.  Empty relation terms are aggressively propagated thoughout expressions in which they occur, and may result in expressions that do not need to be incrementally maintained (because they are guaranteed to be always empty).  Activating this flag is only useful if you want to inspect the generated Calculus/M3 code by hand. Not activated by default at any optimization level.</dd>  

  <dt class="code">UNIFY-EXPRESSIONS</dt>
  <dd>Unify lift terms more aggressively.  Without this optimization, the compiler will not attempt to unify lift terms containing full expressions (i.e., nested aggregates), since it is rarely possible to actually unify these.  In some cases, however, this can produce simpler (and thus, more efficient) M3 programs. Activated by <span class="code">-O3</span>.</dd>
  
  <dt class="code">COMPILE-WITHOUT-OPT</dt>
  <dd>Request that the second-stage compiler disable any unnecessary optimizations (e.g., by default, GCC is invoked with <span class="code">-O3</span>, but not if this flag is active). Activated by <span class="code">-O1</span>.</dd>


  <dt class="code">WEAK-EXPR-EQUIV</dt>
  <dd>When testing for expression equivalence, perform only a naive structural comparison rather than a (at least quadratic, and potentially exponential) matching.  This accelerates compilation, but may result in the creation of duplicate maps.  Activated by <span class="code">-O1</span>.</dd>
    
  <dt class="code">K3-NO-OPTIMIZE</dt>
  <dd>Do not apply functional optimizations. Activated by <span class="code">-O1</span>.</dd>
  
  <dt class="code">DUMB-LIFT-DELTAS</dt>
  <dd>When computing the viewlet transform, use the delta rule for lifts precisely as described in the PODS10 paper.  If this flag is <b>not</b> active, a postprocessing step is applied to lift deltas, that range-restricts the resulting expression to only those tuples that are affected. Activated by <span class="code">-O1</span>.</dd>
  
  <dt class="code">CALC-DONT-CREATE-ZEROES</dt>
  <dd></dd>

</dl>