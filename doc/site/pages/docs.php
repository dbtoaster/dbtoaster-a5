<p>
The DBToaster compiler is used to generate incremental maintenance (M3) programs.  M3 programs can be executed in the following ways:
<ul>
<li><b>Interpreter</b>: The DBToaster compiler includes an internal M3 interpreter.  When the interpreter is used, the M3 program is evaluated and results are printed at the end of evaluation.  The interpreter is not especially efficient and should not be used in production systems, but is useful for query development and testing.</li>

<li><b>Standalone Binaries</b>: The DBToaster compiler can produce standalone binaries that evaluate the M3 program.  This requires invoking a second stage compiler (gcc or scalac) to generate the final binary.  This mode is far more efficient than the interpreter, but compilation is slower.</li>

<li><b>Source Code</b>: The DBToaster compiler can also produce source code that can be linked into your own binary.</li>
</ul>

For best performance, generate either Standalone Binaries or Source Code.
</p>

<?= chapter("The DBToaster Interpreter") ?>
<p>
To use DBToaster to evaluate queries in its internal interpreter, invoke it with the <tt>-r</tt> flag and one or more SQL query files.  The output of all queries in the file will be printed once all data has been processed.  If any of the queries do not terminate (e.g., one or more data sources are sockets), then pressing control-c will terminate the process and print the most recent query results.  

For example, we can run the sample query RST (included in the distribution) on the interpreter.

<div class="codeblock">$&gt; cat queries/simple/rst.sql
CREATE STREAM R(A int, B int) 
  FROM FILE 'data/simple/tiny/r.dat' LINE DELIMITED CSV;

CREATE STREAM S(B int, C int) 
  FROM FILE 'data/simple/tiny/s.dat' LINE DELIMITED CSV;

CREATE STREAM T(C int, D int)
  FROM FILE 'data/simple/tiny/t.dat' LINE DELIMITED CSV;

SELECT sum(A*D) AS AtimesD FROM R,S,T WHERE R.B=S.B AND S.C=T.C;

$&gt; dbtoaster -r queries/simple/rst.sql
Processing time: 0.0206508636475
---------- Results ------------
AtimesD: 306
</div>
</p>

<?= chapter("Generating Standalone Binaries")?>
<p>
To use DBToaster to create a standalone binary, invoke it with <tt>-c [binary name]</tt>.  The binary can be invoked directly.  Like the interpreter, it will print the results of all queries once all data has been processed.  </p>

<div class="codeblock">$&gt; dbtoaster queries/simple/rst.sql -c rst
$&gt; ./rst
&lt;?xml version="1.0" encoding="UTF-8" standalone="yes" ?&gt;
&lt;!DOCTYPE boost_serialization&gt;
&lt;boost_serialization signature="serialization::archive" version="9"&gt;
&lt;ATIMESD&gt;306&lt;/ATIMESD&gt;
&lt;/boost_serialization&gt;
</div>

<p>
Note that in order to compile binaries, DBToaster will invoke g++.  DBToaster relies on pthreads, several Boost libraries ("program_options", "serialization", "system", "filesystem", "chrono", and "thread"), and a custom DBToaster library.  These must all be in your binary, include, and library search paths.  The -I and -L flags may be used to pass individual include and library paths (respectively) to g++, or the environment variables DBT_HDR, and DBT_LIB may be used to store a colon-separated list of search paths.</p>

<p>
To produce a scala binary, invoke dbtoaster with <tt>-l scala</tt>, and the <tt>-c [binary name]</tt> flag as above.  DBToaster will produce <tt>[binary name].jar</tt>, which can be run using java as a normal scala program.
</p>

<div class="codeblock">$&gt; dbtoaster queries/simple/rst.sql -l scala -c rst
$&gt; scala -classpath "rst.jar:lib/dbt_scala/dbtlib.jar" \
                       org.dbtoaster.RunQuery
Run time: 0.261 s
&lt;ATIMESD&gt;306 &lt;/ATIMESD&gt;
</div>

<?= chapter("Generating Source Code") ?>
<p>DBToaster's primary role is the construction of code that can be linked in to existing applications.  To generate a source file in C++ or Scala, invoke it with <tt>-l [language]</tt>, replacing <tt>[language]</tt> with <tt>cpp</tt> or <tt>scala</tt>.  If the optional <tt>-o</tt> flag is used to direct the generated code into a particular file, the target language will be auto-detected from the file suffix (".scala" for Scala, and ".h", ".hpp", or ".cpp" for C++).</p>

<div class="codeblock">$&gt; dbtoaster queries/simple/rst.sql -o rst.cpp
$&gt; dbtoaster queries/simple/rst.sql -o rst.scala
$&gt; ls
rst.hpp      rst.scala
</div>

See the individual target language documentation pages for details.

