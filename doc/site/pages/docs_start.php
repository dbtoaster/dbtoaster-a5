<p>
The DBToaster compiler can generate C++ or Scala code in two different forms:
<ul>
<li><b>Standalone Binary</b> - Generating a binary that incrementally evaluates a given SQL query requires invoking a second stage compiler (g++ or scalac).</li>

<li><b>Source Code</b> - Generated code can be easily embedded into user applications.</li>

</ul>

</p>

<?= chapter("Evaluating a simple query")?>

DBToaster provides the <tt>-r</tt> flag that generates, compiles, and runs the generated program in one simple step.
This is a convenient way to check whether DBToaster and its dependencies have been successfully installed.

<p>
   The following command evaluates the <tt>rst</tt> query on the toy dataset that ships with DBToaster:
</p>

<div class="codeblock">
$&gt; cat examples/queries/simple/rst.sql
CREATE STREAM R(A int, B int) 
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
Invoke DBToaster with <tt>-c [binary name]</tt> to create a standalone binary. By default, the compiler uses the C++ backend to produce an executable binary. Once invoked, the program prints out the results of all the queries contained in the input file after processing the whole input data.</p>

<p>The following command uses the C++ backend to generate the <tt>rst</tt> executable:
<div class="codeblock">$&gt; bin/dbtoaster examples/queries/simple/rst.sql -c rst
</div>
</p>
<p>
Running the <tt>rst</tt> executable produces the following output:
<div class="codeblock">$&gt; ./rst
&lt;snap&gt;
&nbsp;&nbsp;&nbsp;&nbsp;&lt;ATIMESD&gt;306&lt;/ATIMESD&gt;
&lt;/snap&gt;
</div>
</p>

<p>
To produce a Scala jar file, invoke DBToaster with <tt>-l scala</tt> and the <tt>-c [binary name]</tt> flag as above.  DBToaster will produce <tt>[binary name].jar</tt>, which can be run as a normal Scala program. For more details, please refer to <?= mk_link(null, "docs", "scala"); ?>. 
</p>

<?= chapter("Generating Source Code") ?>

<p>DBToaster's primary role is to generate code that can be embedded into user applications.  To produce a source file in C++ or Scala, invoke the compiler with <tt>-l [language]</tt>, replacing <tt>[language]</tt> with <tt>cpp</tt> or <tt>scala</tt>.  If the optional <tt>-o</tt> flag is used to redirect the generated code into a file, the target language will be auto-detected from the file suffix (".scala" for Scala and ".h", ".hpp", or ".cpp" for C++).</p>

<div class="codeblock">$&gt; bin/dbtoaster examples/queries/simple/rst.sql -o rst.cpp
$&gt; bin/dbtoaster examples/queries/simple/rst.sql -o rst.scala
$&gt; ls
rst.hpp      rst.scala
</div>

See <?= mk_link(null, "docs", "cpp"); ?> and <?= mk_link(null, "docs", "scala"); ?> for details.

