<p>
DBToaster operates in three modes: (1) Interpreter mode, which compiles and processes queries internally, within the DBToaster binary.  (2) Standalone Binary mode, which compiles queries into standalone binaries, which may be executed individually.  (3) Source Code mode, which compiles queries into datastructures that can be linked into and instantiated within your own projects.
</p>

<?= chapter("Interpreter Mode") ?>
<p>
To use DBToaster in interpreter mode, invoke it with the <tt>-r</tt> flag and one or more SQL query files.  The output of all queries in the file will be printed once all data has been processed.  If any of the queries do not terminate (e.g., one or more data sources are sockets), then pressing control-c will terminate the process and print the most recent query results.

<div class="codeblock">
$&gt; dbtoaster -r test/queries/simple/rst.sql
Processing time: 0.0309669971466
AtimesD: 1.87533670489e+13
</div>
</p>

<?= chapter("Standalone Binary Mode")?>
<p>
To use DBToaster to create a standalone binary, invoke it with <tt>-c [binary name]</tt>.  The binary can be invoked directly.  Like the interpreter, it will print the results of all queries once all data has been processed.  </p>

<div class="codeblock">
$&gt; dbtoaster test/queries/simple/rst.sql -c rst
$&gt; ./rst
&lt;?xml version="1.0" encoding="UTF-8" standalone="yes" ?&gt;
&lt;!DOCTYPE boost_serialization&gt;
&lt;boost_serialization signature="serialization::archive" version="9"&gt;
&lt;ATIMESD&gt;18753367048934&lt;/ATIMESD&gt;
&lt;/boost_serialization&gt;
</div>

<p>
Note that in order to compile binaries, DBToaster will invoke g++.  DBToaster relies on pthreads, several Boost libraries ("program_options", "serialization", "system", "filesystem", "chrono", and "thread"), and a custom DBToaster library.  These must all be in your binary, include, and library search paths.  The -I and -L flags may be used to pass individual include and library paths (respectively) to g++, or the environment variables DBT_HDR, and DBT_LIB may be used to store a colon-separated list of search paths.</p>

<p>
To produce a scala binary, invoke dbtoaster with <tt>-l scala</tt>, and the <tt>-c [binary name]</tt> flag as above.  DBToaster will produce <tt>[binary name].jar</tt>, which can be run using java as a normal scala program.
</p>

<div class="codeblock">
$&gt; dbtoaster test/queries/simple/rst.sql -l scala -c rst
$&gt; java -cp [path_to_dbt_lib]:[path_to_scala_lib] -jar rst.jar
&lt;?xml version="1.0" encoding="UTF-8" standalone="yes" ?&gt;
&lt;!DOCTYPE boost_serialization&gt;
&lt;boost_serialization signature="serialization::archive" version="9"&gt;
&lt;ATIMESD&gt;18753367048934&lt;/ATIMESD&gt;
&lt;/boost_serialization&gt;
</div>

<?= chapter("Source Code Mode") ?>
<p>DBToaster's primary role is the construction of code that can be linked in to existing applications.  To generate a source file in C++ or Scala, invoke it with <tt>-l [language]</tt>, replacing <tt>[language]</tt> with <tt>cpp</tt> or <tt>scala</tt>.  If the optional <tt>-o</tt> flag is used to direct the generated code into a particular file, the target language will be auto-detected from the file suffix (".scala" for Scala, and ".h", ".hpp", or ".cpp" for C++).</p>

<div class="codeblock">
$&gt; dbtoaster test/queries/simple/rst.sql -o rst.hpp
$&gt; dbtoaster test/queries/simple/rst.sql -o rst.scala
$&gt; ls
rst.hpp      rst.scala
</div>

See the individual target language documentation pages for details.

