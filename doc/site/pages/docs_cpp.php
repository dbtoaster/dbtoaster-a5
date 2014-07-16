<div class="warning">Warning: This BETA API is not final, and subject to change before release.</div>

<p>
   <i>Note:</i> To compile and run queries using the C++ backend requires g++ 4.7 or above. Please refer to <?= mk_link("Getting Started", "docs"); ?> for details 
</p>

<a name="quickstart"></a>
<?= chapter("Quickstart Guide") ?>

<p>
DBToaster generates C++ code for incrementally maintaining the results of a given set of queries 
if CPP is specified as the output language (<tt>-l cpp</tt> command line option). In this case DBToaster 
produces a C++ header file containing a set of datastructures (<tt>tlq_t</tt>, <tt>data_t</tt> and
<tt>Program</tt>) required for executing the sql program. 
</p>
<p>
Let's consider the following sql query:
<div class="codeblock">$&gt; cat examples/queries/simple/rs_example1.sql
CREATE TABLE R(A int, B int) 
  FROM FILE 'examples/data/tiny/r.dat' LINE DELIMITED
  CSV (fields := ',');

CREATE STREAM S(B int, C int) 
  FROM FILE 'examples/data/tiny/s.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT SUM(r.A*s.C) as RESULT FROM R r, S s WHERE r.B = s.B;
</div>

The corresponding C++ header file can be obtained by running:

<div class="codeblock">$&gt; bin/dbtoaster examples/queries/simple/rs_example1.sql -l cpp -o rs_example1.hpp</div>
</p>

<p>
Alternatively, DBToaster can build a standalone binary (if the <tt>-c [binary name]</tt> flag is present) by compiling 
the generated header file against <tt>lib/dbt_c++/main.cpp</tt>, which provides code for executing the 
sql program and printing the results. 
</p>

<p>
Running the compiled binary will result in the following output:
<div class="codeblock">$&gt; ./rs_example1
&lt;snap&gt;
        &lt;RESULT&gt;156&lt;/RESULT&gt;
&lt;/snap&gt;
</div>
If the generated binary is run with the <tt>--async</tt> flag, it will also print intermediary results as frequently
as possible while the sql program is running in a separate thread.

<div class="codeblock">$&gt; ./rs_example1 --async
Initializing program:
Running program:
&lt;snap&gt;
        &lt;RESULT&gt;0&lt;/RESULT&gt;
&lt;/snap&gt;
&lt;snap&gt;
        &lt;RESULT&gt;0&lt;/RESULT&gt;
&lt;/snap&gt;
&lt;snap&gt;
        &lt;RESULT&gt;0&lt;/RESULT&gt;
&lt;/snap&gt;
&lt;snap&gt;
        &lt;RESULT&gt;0&lt;/RESULT&gt;
&lt;/snap&gt;
&lt;snap&gt;
        &lt;RESULT&gt;9&lt;/RESULT&gt;
&lt;/snap&gt;
&lt;snap&gt;
        &lt;RESULT&gt;74&lt;/RESULT&gt;
&lt;/snap&gt;
&lt;snap&gt;
        &lt;RESULT&gt;141&lt;/RESULT&gt;
&lt;/snap&gt;
Printing final result:
&lt;snap&gt;
        &lt;RESULT&gt;156&lt;/RESULT&gt;
&lt;/snap&gt;
</div>
</p>




<a name="apiguide"></a>
<?= chapter("C++ API Guide") ?>

<p>
The DBToaster C++ codegenerator produces a header file containing 3 main type definitions in the <tt>dbtoaster</tt> namespace:
<tt>tlq_t</tt>, <tt>data_t</tt> and <tt>Program</tt>. Additionally <tt>snapshot_t</tt> is pre-defined as a garbage collected
pointer to <tt>tlq_t</tt>. What follows is a brief description of these types, while a more detailed presentation can be found
in the <a href="#codereference">Reference</a> section. 
</p>

<p>
<b><tt>tlq_t</tt></b> encapsulates the materialized views directly needed for computing the results and offers functions for retrieving 
them.
</p>

<p>
<b><tt>data_t</tt></b> extends <tt>tlq_t</tt> with auxiliary materialized views needed for maintaining the results and offers trigger 
functions for incrementally updating them.
</p>

<p>
<b><tt>Program</tt></b> represents the execution engine of the sql program. It encapsulates a <tt>data_t</tt> object and provides 
implementations to a set of abstract functions of the <tt>IProgram</tt> class used for running the program. 
Default implementations for some of these functions are inherited from the <tt>ProgramBase</tt> class while others 
are generated depending on the previously defined <tt>tlq_t</tt> and <tt>data_t</tt> types.
</p>

<a name="execprogram"></a>
<?= section("Executing the Program") ?>

<p>
The execution of a program can be controlled through the functions: <tt>IProgram::init()</tt>, 
<tt>IProgram::run()</tt>, <tt>IProgram::is_finished()</tt>, <tt>IProgram::process_streams()</tt>
and <tt>IProgram::process_stream_event()</tt>.
</p>

<p>
<dl>
<dt class="api">virtual void IProgram::init() </dt><dd> Loads the tuples of static tables and performs initialization
of materialized views based on that data. The definition of this functions is generated as part of the
<tt>Program</tt> class.
</dd>
<dt class="api">void IProgram::run( bool async = false ) </dt><dd> Executes the program by invoking the
<tt>Program::process_streams()</tt> function. If parameter <tt>async</tt> is set to <tt>true</tt>
the execution takes place in a separate thread. This is a standard function defined by the <tt>IProgram</tt>
class.
</dd>
<dt class="api">bool IProgram::is_finished() </dt><dd> Tests whether the program has finished or not. Especially
relevant when the program is run in asynchronous mode. This is a standard function defined by the <tt>IProgram</tt>
class.
</dd>
<dt class="api">virtual void IProgram::process_streams() </dt><dd> Reads stream events from various sources and invokes 
the <tt>IProgram::process_stream_event()</tt> on each event. Default implementation of this function 
(<tt>ProgramBase::process_streams()</tt>) reads events from the sources specified in the sql program.
</dd>
<dt class="api">virtual void IProgram::process_stream_event(event_t&amp; ev) </dt><dd> Processes each stream event passing
through the system. Default implementation of this function (<tt>ProgramBase::process_stream_event()</tt>) does 
incremental maintenance work by invoking the trigger function corresponding to the event type <tt>ev.type</tt> 
for stream <tt>ev.id</tt> with the arguments contained in <tt>ev.data</tt>.
</dd>
</dl>
</p>


<a name="retrieveresults"></a>
<?= section("Retrieving the Results") ?>

<p>
The <b><tt>snapshot_t IProgram::get_snapshot()</tt></b> function returns a snapshot of the results of the program. 
The query results can then be obtained by calling the appropriate <tt>get_<i>TLQ_NAME</i>()</tt> function on the 
snapshot object as described in the reference of <a href="#tlq_t"><tt>tlq_t</tt></a>. If the program is 
running in asynchronous mode it is guaranteed that the taken snapshot is consistent.
</p>
<p>
Currently, the mechanism for taking snapshots is trivial, in that a snapshot consists of a full copy of the 
<tt>tlq_t</tt> object associated with the program. Consequently, the time required to obtain such a snapshot
is linear in the size of the results set.
</p>

<a name="basicexample"></a>
<?= section("Basic Example") ?>

<p>
We will use as an example the C++ code generated for the <tt>rs_example1.sql</tt> sql program introduced above. In the interest
of clarity some implementation details are omitted.
<div class="codeblock">$&gt; bin/dbtoaster examples/queries/simple/rs_example1.sql -l cpp -o rs_example1.hpp
#include &lt;lib/dbt_c++/program_base.hpp&gt;

namespace dbtoaster {

    /* Definitions of auxiliary maps for storing materialized views. */
    ...
    ...
    ...

    /* Type definition providing a way to access the results of the sql */
    /* program */
    struct tlq_t{
        tlq_t()
        {}
    
        ...
        
        /* Functions returning / computing the results of top level */
        /* queries */
        long get_RESULT(){
            ...
        }

    protected:

        /* Data structures used for storing/computing top level queries */
        ...
    };
    
    /* Type definition providing a way to incrementally maintain the */
    /* results of the sql program */
    struct data_t : tlq_t{
        data_t()
        {}
    
        /* Registering relations and trigger functions */
        void register_data(ProgramBase&lt;tlq_t&gt;&amp; pb) {
            ...
        }

        /* Trigger functions for table relations */
        void on_insert_R(long R_A, long R_B) {
            ...
        }
        
        /* Trigger functions for stream relations */
        void on_insert_S(long S_B, long S_C) {
            ...
        }
        
        void on_delete_S(long S_B, long S_C) {
            ...
        }
        
        void on_system_ready_event() {
            ...
        }

    private:

        /* Data structures used for storing materialized views */
        ...
    };

    /* Type definition providing a way to execute the sql program */
    class Program : public ProgramBase&lt;tlq_t&gt;
    {
    public:
        Program(int argc = 0, char* argv[] = 0) : 
                ProgramBase&lt;tlq_t&gt;(argc,argv) 
        {
            data.register_data(*this);

            /* Specifying data sources */
            ...
        }

        /* Imports data for static tables and performs view */
        /* initialization based on it. */
        void init() {
            process_tables();
            data.on_system_ready_event();
        }
    
        /* Saves a snapshot of the data required to obtain the results */
        /* of top level queries. */
        snapshot_t take_snapshot(){
            return snapshot_t( new tlq_t((tlq_t&amp;)data) );
        }
    
    private:
        data_t data;
    };

}
}</div>
</p>

<p>
Below is an example of how the API can be used to execute the sql program and
print its results:
<div class="codeblock">#include "rs_example1.hpp"

int main(int argc, char* argv[]) {
    bool async = argc > 1 &amp;&amp; !strcmp(argv[1],"--async");
    
    dbtoaster::Program p;
    dbtoaster::Program::snapshot_t snap;

    cout &lt;&lt; "Initializing program:" &lt;&lt; endl;
    p.init();

    cout &lt;&lt; "Running program:" &lt;&lt; endl;
    p.run( async );
    while( !p.is_finished() )
    {
       snap = p.get_snapshot();
       cout &lt;&lt; "RESULT: " &lt;&lt; snap->get_RESULT() &lt;&lt; endl;
    }

    cout &lt;&lt; "Printing final result:" &lt;&lt; endl;
    snap = p.get_snapshot();
    cout &lt;&lt; "RESULT: " &lt;&lt; snap->get_RESULT() &lt;&lt; endl;

    return 0;
}
</div>
</p>

<a name="customexecution"></a>
<?= section("Custom Execution") ?>

<p>
<b>Custom event processing</b> can be performed on each stream event if the virtual function 
<tt>void IProgram::process_stream_event(event_t&amp; ev)</tt> is overriden while still delegating
the basic processing task of an event to <tt>Program::process_stream_event()</tt>.
</p>
<p>Example: Custom event processing.
<div class="codeblock">namespace dbtoaster{
    class CustomProgram_1 : public Program
    {
    public:        
        void process_stream_event(event_t&amp; ev) {
            cout &lt;&lt; "on_" &lt;&lt; event_name[ev.type] &lt;&lt; "_";
            cout &lt;&lt; get_relation_name(ev.id) &lt;&lt; "(" &lt;&lt; ev.data &lt;&lt; ")" &lt;&lt; endl;

            Program::process_stream_event(ev);
        }        
    };
}
</div>
</p>

<p>
Stream events can be manually read from <b>custom sources</b> and fed into the system by overriding the virtual function
<tt>void IProgram::process_streams()</tt> and calling <tt>process_stream_event()</tt> for each event read.
</p>

<p>Example: Custom event sourcing.
<div class="codeblock">namespace dbtoaster{
    class CustomProgram_2 : public Program
    {
    public:        
        void process_streams() {
            
            for( long i = 1; i &lt;= 10; i++ ) {
                event_args_t ev_args;
                ev_args.push_back(i);
                ev_args.push_back(i+10);
                event_t ev( insert_tuple, get_relation_id("S"), ev_args);

                process_stream_event(ev);
            }
        }        
    };
}
</div>
</p>



<a name="codereference"></a>
<?= chapter("C++ Generated Code Reference") ?>

<a name="tlq_t"></a>
<?= section("<tt>struct tlq_t</tt>") ?>

<p>
The <tt>tlq_t</tt> contains all the relevant datastructures for computing the results of the sql program, also called
the top level queries. It provides a set of functions named <tt>get_<i>TLQ_NAME</i></tt> that return the top level query
result labeled <tt><i>TLQ_NAME</i></tt>. For our example the <tt>tlq_t</tt> produced has a function named <tt>get_RESULT</tt> 
that returns the query result corresponding to <tt>SELECT SUM(r.A*s.C) as RESULT ...</tt> in <tt>rs_example1.sql</tt>.
</p>

<?=subsection("Queries computing collections")?>
<p>
In the example above the result consisted of a single value. 
If however our query has a <tt>GROUP BY</tt> clause its result is a collection and
the corresponding <tt>get_RESULT</tt> function will return either a <tt>MultiHashMap</tt>.
</p>

<p>
Let's consider the following example:
<div class="codeblock">$&gt; cat examples/queries/simple/rs_example2.sql
CREATE STREAM R(A int, B int) 
  FROM FILE 'examples/data/tiny/r.dat' LINE DELIMITED
  CSV (fields := ',');

CREATE STREAM S(B int, C int) 
  FROM FILE 'examples/data/tiny/s.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT r.B, SUM(r.A*s.C) as RESULT_1, SUM(r.A+s.C) as RESULT_2 FROM R r, S s WHERE r.B = s.B GROUP BY r.B;
</div>
The generated code defines two collection types <tt>RESULT_1_map</tt> and <tt>RESULT_2_map</tt> and two corresponding
entry types: <tt>RESULT_1_entry</tt> and <tt>RESULT_2_entry</tt>. These entry structures have a set of key fields
corresponding to the <tt>GROUP BY</tt> clause, in our case <tt>R_B</tt>, and an additional value field, <tt>__av</tt>,
storing the aggregated value of the top level query for each key in the collection. Finally, <tt>tlq_t</tt> contains
two functions <tt>get_RESULT_1</tt> and <tt>get_RESULT_2</tt> returning the top level query results as <tt>RESULT_1_map</tt>
and <tt>RESULT_2_map</tt> objects.

<div class="codeblock">    /* Definitions of auxiliary maps for storing materialized views. */
    struct RESULT_1_entry {
        long R_B; long __av;
        ...
    };
    typedef multi_index_container&lt;RESULT_1_entry, ... &gt; RESULT_1_map;

    ...
    
    struct RESULT_2_entry {
        long R_B; long __av;
        ...
    };
    typedef multi_index_container&lt;RESULT_2_entry, ... &gt; RESULT_2_map;
    
    ...
    
    /* Type definition providing a way to access the results of the sql program */
    struct tlq_t{
        tlq_t()
        {}
    
        /* Serialization Code */
        ...

        /* Functions returning / computing the results of top level queries */
        RESULT_1_map&amp; get_RESULT_1(){
            ...
        }
        RESULT_2_map&amp; get_RESULT_2(){
            ...
        }

    protected:

        /* Data structures used for storing / computing top level queries */
        RESULT_1_map RESULT_1;
        RESULT_2_map RESULT_2;

    };
</div>
If the given query has no aggregates the <tt>COUNT(*)</tt> aggregate will be computed by default and 
consequently the resulting collections will be guaranteed not to have any duplicate keys.
</p>

<? /*
<?=subsection("Partial Materialization")?>
<p>
Some of the work involved in maintaining the results of a query can be saved by performing partial materialization
and only computing the final results when invoking <tt>tlq_t</tt>'s <tt>get_<i>TLQ_NAME</i></tt> functions. This
behaviour is especially desirable when the rate of querying the results is lower than the rate of updates, and
can be enabled through the <tt>-F EXPRESSIVE-TLQS</tt> command line flag.
<br/>
Below is an example of a query where partial materialization is indeed beneficial.

<div class="codeblock">$&gt; cat examples/queries/simple/r_lift_of_count.sql
CREATE STREAM R(A int, B int)
FROM FILE 'examples/data/tiny/r.dat' LINE DELIMITED
csv ();

SELECT r2.C FROM (
  SELECT r1.A, COUNT(*) AS C FROM R r1 GROUP BY r1.A
) r2;
</div>

<b>Generated <tt>tlq_t</tt> without <tt>-F EXPRESSIVE-TLQS</tt>:</b> We can see that <tt>get_COUNT()</tt>
simply returns the materialized view of the results.
<div class="codeblock">$&gt; bin/dbtoaster examples/queries/simple/r_lift_of_count.sql -l cpp

    ...

    /* Type definition providing a way to access the results of the sql program * /
    struct tlq_t{
        tlq_t()
        {}
        
        ...

        /* Functions returning / computing the results of top level queries * /
        COUNT_map& get_COUNT(){
            COUNT_map& __v_1 = COUNT;
            return __v_1;
        }

    protected:

        /* Data structures used for storing / computing top level queries * /
        COUNT_map COUNT;

    };

    ...

</div>

<b>Generated <tt>tlq_t</tt> with <tt>-F EXPRESSIVE-TLQS</tt>:</b> We can see that <tt>get_COUNT()</tt>
perfoms some final computation for constructing the end result in a temporary <tt>std::map</tt> before returning it.
We should remark that <tt>tlq_t</tt> no longer contains the full materialized view of the results <tt>COUNT_map COUNT;</tt>
but a partial materialization <tt>COUNT_1_E1_1_map COUNT_1_E1_1;</tt> used by <tt>get_COUNT()</tt> in computing 
the final query result.
<div class="codeblock">$&gt; bin/dbtoaster examples/queries/simple/r_lift_of_count.sql -l cpp -F EXPRESSIVE-TLQS

    ...

    /* Type definition providing a way to access the results of the sql program * /
    struct tlq_t{
        tlq_t()
        {}
        
        ...

        /* Functions returning / computing the results of top level queries * /
        map&lt;long,long&gt; get_COUNT(){
            map&lt;long,long&gt; __v_41;
            /* Result computation based on COUNT_1_E1_1 * /
            return __v_41;
        }

    protected:

        /* Data structures used for storing / computing top level queries * /
        COUNT_1_E1_1_map COUNT_1_E1_1;

    };

    ...

</div>

</p>
*/ ?>
<a name="data_t"></a>
<?= section("<tt>struct data_t</tt>")?>

<p>
The <tt>data_t</tt> contains all the relevant datastructures and trigger functions for incrementally maintaining the results
 of the sql program.
</p>
<p>
For each stream based relation <tt><i>STREAM_X</i></tt>, present in the sql program, it provides a pair of trigger functions named 
<tt>on_insert_<i>STREAM_X</i>()</tt> and <tt>on_delete_<i>STREAM_X</i>()</tt>  that incrementally maintain the query results in the event of 
an insertion/deletion of a tuple in <tt><i>STREAM_X</i></tt>. If generating code for the query presented above (<tt>rs_example1.sql</tt>) 
the <tt>data_t</tt> produced has the trigger functions <tt>void on_insert_S(long S_B, long S_C) / void on_delete_S(long S_B, long S_C)</tt>.
</p>
<p>
For static table based relations only the insertion trigger is required and will get called when processing the static tables 
in the initialization phase of the program.
</p>



<a name="program"></a>
<?= section("<tt>class Program</tt>") ?>

<p>
Finally, <tt>Program</tt> is a class that implements the <tt>IProgram</tt> interface and provides the basic functionalities
for reading static table tuples and stream events from their sources, initializing the relevant datastructures, running the sql 
program and retrieving its results. 
</p>
