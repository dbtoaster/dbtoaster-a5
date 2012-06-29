<p>DBToaster includes a standard library of functions that may used in SQL queries.  Functions are called using a C-like syntax:
<div class="codeblock">SELECT listmin(A, B) FROM R;</div>
See the <a href="index.php?page=docs&subpage=sql#select">DBT-SQL Documentation</a> for information about calling functions.</p>

<hr/>
<?= chapter("<span class=\"code\">listmin, listmax</span>") ?>
Compute the minimum or maximum of a fixed-size list of values.

<div class="codeblock">listmin (val1, val2, [val3, [val4, [...]]])
listmax (val1, val2, [val3, [val4, [...]]])</div>

<h4>Inputs</h4>
<dl>
<dt class="code">val1, val2, ...: any</dt>
<dd>An arbitrary number of value expressions.  All values in the list must be of the same type, or escalateable to the same type (e.g., Int -> Float), and must be comparable.</dd>
</dl>

<h4>Output</h4>
Returns the minimum or maximum element of <span class="code">val1, val2, ...</span>.  The return type is the type of all of the elements of the list.

<h4>Notes</h4>
This library function is supported on the Interpreter, C++, and Scala runtimes.

<hr/>
<?= chapter("<span class=\"code\">date_part</span>") ?>
Extract the parts of a date.  Identical to <span class="code">EXTRACT(datepart FROM date)</span>.

<div class="codeblock">date_part ('year', date)
date_part ('month', date)
date_part ('day', date)</div>

<h4>Inputs</h4>
<dl>
<dt class="code">datepart: string literal</dt>
<dd>One of the string literals 'year', 'month', or 'day'.  Dictates which part of the date to extract.  datepart is case-insensitive.</dd>
<dt class="code">date: date</dt>
<dd>The day to extract a component of.</dd>
</dl>

<h4>Output</h4>
Returns the requested year, month, or date part of a date, as an integer.

<h4>Notes</h4>
This library function is supported on the Interpreter and C++ runtimes.

<hr/>
<?= chapter("<span class=\"code\">regexp_match</span>") ?>
Perform regular expression matching.  

<div class="codeblock">regexp_match (pattern, string)</div>

<h4>Inputs</h4>
<dl>
<dt class="code">pattern: string</dt>
<dd>A regular expression pattern (see Notes below).</dd>
<dt class="code">string: string</dt>
<dd>The string to match against.</dd>
</dl>

<h4>Output</h4>
Returns the integer 1 if the match succeeds, and the integer 0 otherwise.

<h4>Notes</h4>
<p>This library function is supported on the Interpreter and C++ runtimes.</p>

<p><b>Note:</b> Regular expressions are compiled and evaluated using each target runtime's native regular expression library.  The Interpreter uses Ocaml's built in Str module.  The C++ code generator uses <span class="code">regex(3)</span>.</p>

<hr/>
<?= chapter("<span class=\"code\">substring</span>") ?>
Extract a substring.

<div class="codeblock">substring (string, start, len)</div>

<h4>Inputs</h4>
<dl>
<dt class="code">string: string</dt>
<dd>The string to extract a substring from.</dd>
<dt class="code">start: int</dt>
<dd>The first character of string to return.  0 is the first character of the string.</dd>
<dt class="code">len: int</dt>
<dd>The length of the substring to return.</dd>
</dl>

<h4>Output</h4>
Returns the substring of string of length len, starting at index start.

<h4>Notes</h4>
<p>This library function is supported on the Interpreter and C++ runtimes.</p>

<hr/>
<?= chapter("<span class=\"code\">cast_int,cast_float,cast_string,cast_date</span>") ?>
Typecasting operations.

<div class="codeblock">cast_int(val)
cast_float(val)
cast_string(val)
cast_date(val)</div>

<h4>Inputs</h4>
<dl>
<dt class="code">val: any</dt>
<dd>The value to cast.  The value's type must be one that is castable to the target type.</dd>
</dl>

<h4>Output</h4>
Returns the cast value of val as follows.  If val is already of the target type, it is passed through unchanged.
<table border>
<tr><th>From</th><th>To</th><th>Notes</th></tr>
<tr><th>int</th><th>float</th><td> The floating point representation of the input.</td></tr>
<tr><th>int</th><th>string</th><td> The string representation of the input, as determined by the runtime's standard stringification functionality.</td></tr>
<tr><th>float</th><th>int</th><td> The integer representation of the input.  Non-integer values will be truncated.</td></tr>
<tr><th>float</th><th>string</th><td> The string representation of the input, as determined by the runtime's standard stringification functionality.</td></tr>
<tr><th>date</th><th>string</th><td> The string representation of the input, in the SQL-standard <span class="code">YYYY-MM-DD</span> date format.</td></tr>
<tr><th>string</th><th>int</th><td> The input parsed as an integer, using the runtime's standard integer parsing functionality. <br/> (Ocaml: int_of_string, C++: atoi(3))</td></tr>
<tr><th>string</th><th>float</th><td> The input parsed as a float, using the runtime's standard integer parsing functionality. <br/> (Ocaml: float_of_string, C++: atof(3))</td></tr>
<tr><th>string</th><th>date</th><td> The input parsed as a date.  The input string must be in SQL-standard <span class="code">YYYY-MM-DD</span> date format, or a runtime error will be generated.</td></tr>
</table>

<h4>Notes</h4>
<p>This library function is supported on the Interpreter and C++ runtimes.</p>