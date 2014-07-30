<p>
The DBToaster compiler is used to generate incremental maintenance (M3) programs.  M3 programs can be executed in the following ways:
<ul>
<li><b>Standalone Binaries</b>: The DBToaster compiler can produce standalone binaries that evaluate the M3 program.  This requires invoking a second stage compiler (g++ or scalac) to generate the final binary.</li>

<li><b>Source Code</b>: The DBToaster compiler can also produce source code that can be linked into your own binary.</li>
</ul>

</p>

<?= chapter("Requirements")?>

Several dependencies are needed to run DBToaster.

<?= section("Windows")?>
We tested DBToaster successfully on:
<ul>
  <li>Oracle JDK 7 (or above)</li>
  <li><a href="http://cygwin.com/">Cygwin</a></li>
  <li>gcc-g++ 4.8 (or above)</li>
  <li><a href="http://scala-lang.org/download/2.10.2.html">Scala 2.10.2</a></li>
</ul>

<?= section("OS X")?>

<ul>
  <li>Oracle JDK 7 (or above)</li>
  <li>gcc-g++ 4.8 (or above)</li>
  <li><a href="http://scala-lang.org/download/2.10.2.html">Scala 2.10.2</a></li>
</ul>

<p>
   For installing gcc (if you are using <a href="http://brew.sh/">Homebrew</a>), the following commands can be used:
</p>

<div class="codeblock">
$> brew tap homebrew/versions
$> brew install gcc48
</div>

<p>
   In order to tell DBToaster to use a custom C++ compiler, create a symlink <tt>gpp</tt> in the DBToaster directory that points to the compiler:
</p>

<div class="codeblock">$> ln -s /usr/local/bin/g++-4.8 gpp</div>

<p>
   DBToaster detects that this link exists and uses it instead of <tt>g++</tt>.
</p>

<?= section("Linux")?>
<p>
   The easiest way to use DBToaster on Linux is to install the following dependencies with the package manager of your choice:
</p>

<ul>
  <li>Oracle JDK 7 (or above)</li>
  <li>gcc-g++ 4.8 (or above)</li>
  <li><a href="http://scala-lang.org/download/2.10.2.html">Scala 2.10.2</a></li>
</ul>

<p>
   <i>Note:</i> Other versions of the tools/packages might work as well but have not been tested.
</p>


<?= chapter("Evaluating a simple query")?>

DBToaster provides the <tt>-r</tt> flag which generates, compiles and evaluates a query in one simple step.
This is a convenient way to check whether DBToaster and its dependencies have successfully been installed.

<p>
   The following command evaluates the <tt>rst</tt> query that ships with DBToaster:
</p>

<div class="codeblock">CREATE STREAM R(A int, B int) 
  FROM FILE 'examples/data/simple/r.dat' LINE DELIMITED csv;

CREATE STREAM S(B int, C int) 
  FROM FILE 'examples/data/simple/s.dat' LINE DELIMITED csv;

CREATE STREAM T(C int, D int)
  FROM FILE 'examples/data/simple/t.dat' LINE DELIMITED csv;

SELECT sum(A*D) AS AtimesD FROM R,S,T WHERE R.B=S.B AND S.C=T.C;

$&gt; bin/dbtoaster -r examples/queries/simple/rst.sql
&lt;snap&gt;
&nbsp;&nbsp;&nbsp;&nbsp;&lt;ATIMESD&gt;306&lt;/ATIMESD&gt;
&lt;/snap&gt;

real  0m0.019s
user  0m0.002s
sys   0m0.002s
</div>

<?= chapter("Generating Standalone Binaries")?>
<p>
To use DBToaster to create a standalone binary, invoke it with <tt>-c [binary name]</tt>.  The binary can be invoked directly. It will print the results of all queries once all data has been processed.  </p>

<p>The following command line will generate the <tt>rst</tt> executable:
<div class="codeblock">$&gt; bin/dbtoaster examples/queries/simple/rst.sql -c rst
</div>
</p>
<p>
Running the <tt>rst</tt> executable will produce the following output:
<div class="codeblock">$&gt; ./rst
&lt;snap&gt;
&nbsp;&nbsp;&nbsp;&nbsp;&lt;ATIMESD&gt;306&lt;/ATIMESD&gt;
&lt;/snap&gt;
</div>
</p>

<p>
To produce a scala jar file, invoke dbtoaster with <tt>-l scala</tt>, and the <tt>-c [binary name]</tt> flag as above.  DBToaster will produce <tt>[binary name].jar</tt>, which can be run as a normal scala program. For more details, please refer to <?= mk_link(null, "docs", "scala"); ?> 
</p>

<?= chapter("Generating Source Code") ?>

<p>DBToaster's primary role is the construction of code that can be linked in to existing applications.  To generate a source file in C++ or Scala, invoke it with <tt>-l [language]</tt>, replacing <tt>[language]</tt> with <tt>cpp</tt> or <tt>scala</tt>.  If the optional <tt>-o</tt> flag is used to direct the generated code into a particular file, the target language will be auto-detected from the file suffix (".scala" for Scala, and ".h", ".hpp", or ".cpp" for C++).</p>

<div class="codeblock">$&gt; bin/dbtoaster examples/queries/simple/rst.sql -o rst.cpp
$&gt; bin/dbtoaster examples/queries/simple/rst.sql -o rst.scala
$&gt; ls
rst.hpp      rst.scala
</div>

See <?= mk_link(null, "docs", "scala"); ?> and <?= mk_link(null, "docs", "cpp"); ?> for details.

