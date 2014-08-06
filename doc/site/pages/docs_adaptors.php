<p>DBToaster adaptors transform events from external sources into an appropriate format and deliver them to the query engine. The current release supports two file adaptors in both the C++ and Scala backends:
<ul>
    <li>CSV - for reading in string-delimited input files</li>
    <li>Order Book - for reading in stock market historical data</li>
</ul>
</p>

DBToaster also allows users to build custom adaptors for processing input streams. Such adaptors can build their own events and feed them to the query engine by calling the generated trigger functions. See <?= mk_link(null, "docs", "customadaptors"); ?> for more information.

<?= chapter("CSV") ?>
A simple string-delimited adaptor.  Fields are separated using the delimiter passed in the <b>delimiter</b> parameter.  If not provided, comma (",") will be used as a default delimiter.<br/>

The optional deletions parameter can be used to generate a single stream of both insertions and deletions.  When set to "true", the input source is assumed to have an extra, leading column.  When the value in this column is 0, the record is treated as a deletion.  When the value is 1, the record is treated as an insertion.<br/>

Fields are parsed based on the type of the corresponding column in the relation.  Ints, floats, and strings are parsed as-is.  Dates are expected to be formatted in the SQL-standard <tt>[yyyy]-[mm]-[dd]</tt> format.<br/>

<div class="codeblock">CREATE STREAM R(A int, B int) FROM FILE 'r.dat' 
LINE DELIMITED CSV (delimiter := '|');
</div>

<?= chapter("Order Book") ?>
An adaptor that allows reading in stock trade historical data. It assumes that all the input records obey the following predefined schema: 
<i>&lt;timestamp : float, message_id : int, action_id : char, volume : float, price : float&gt;</i>.  Insertions and deletions are triggered for each record as follows:<br/>
<ul>
  <li>If action_id is 'b', and the orderbook adaptor was instantiated with the parameter book := 'bids', an insertion will be generated for the record.</li>
  <li>If action_id is 'a', and the orderbook adaptor was instantiated with the parameter book := 'asks', an insertion will be generated for the record.</li>
  <li>If action_id is 'd', and the orderbook had previously inserted a record with the same message_id, a deletion will be generated for the record.</li>
</ul>

Records will be instantiated into a relation with schema &lt;T float, ID int, BROKER_ID int, VOLUME float, PRICE float&gt;.  All fields except BROKER_ID are taken directly from the input stream.  BROKER_IDs are assigned in a range from 0 to the integer value of the brokers parameter.  The value of BROKER_ID is assigned randomly, using rand() by default, or deterministically from the value of ID if the deterministic parameter is set to 'yes'.  

<div class="codeblock">CREATE STREAM bids(T float, ID int, BROKER_ID int, VOLUME float, PRICE float) 
FROM FILE 'history.csv' 
LINE DELIMITED orderbook (book := 'bids', brokers := '10', deterministic := 'yes');
</div>

<?= chapter("Summary") ?>
<table border>

<tr><th>Adaptor</th><th>Parameter</th><th>Optional</th><th>Description</th></tr>

<tr>
<td rowspan="2">CSV</td>
<td><div class="code">delimiter</div></td>
<td>yes</td>
<td>A string delimiter used to extract fields from a record. 
    If not specified, the default value is ','.</td>
</td></tr>

<tr> 
<td><div class="code">deletions</div></td>
<td>yes</td>
<td>If set to "true", use the first field of the input file to distinguish between rows for insertion and rows for deletion.  A 0 in the first column triggers a deletion event.  A 1 in the first column triggers an inertion event.  The first column is stripped off of the record before further parsing is performed.</td>
</tr>

<tr><td rowspan="3">Order Book</td>
<td><div class="code">action_id</div></td>
<td>no</td>
<td>The value of this parameter may be 'bids' or 'asks', and determines for which orderbook events will be generated.</td>
</tr>

<tr><td><div class="code">brokers</div></td>
<td>yes</td>
<td>The number of brokers to simulate.  By default, 10 brokers will be used.</td>
</tr>

<tr><td><div class="code">deterministic</div></td>
<td>yes</td>
<td>If the value of this parameter is 'yes', broker ids will be generated deterministically based on the message id.  By default, broker ids will be generated randomly using the rand() system call or equivalent.</td>
</tr>

</table>
