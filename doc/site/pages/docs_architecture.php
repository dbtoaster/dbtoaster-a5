<div style="width:100%;text-align:center;"><img src="schematic.png" style="width:80%;"></div>

<p>DBToaster is a generative compiler and as you see in the above image, whenever you want to incrementally maintain a new SQL view, which is actually a query, you will use DBToaster to generate the incremental view maintenance program (IVM).</p>

<p>Afterwards, you can plug the generated IVM program into your own application(s). You only need to pass your data stream, which consists of all insert, update and delete operations on your data tables, to these generated programs.</p>

<p>In return, the IVM programs will always maintain the fresh result of your view and on your request, the latest result will be returned to your application.</p>

<p>The DBToaster is internally divided into two main components: Frontend and Backend.</p>

<div style="width:100%;text-align:center;"><img src="internal_arch.png" style="width:100%;"></div>

<p>As it is presented in the image above, the task of Frontend is to parse the given SQL program and convert it into an internal calculus for IVM and then it is compiled and optimized and finally converted into an intermediate IVM language that is specific to DBToaster.</p>

<p>Then, Backend will accept the output of the Frontend and after parsing it, it would try to optimize it using <a href="https://github.com/epfldata/lms/tree/booster-develop-0.3">Lightweight Modular Staging (LMS)</a> and then will use either C++ or Scala code generator to produce the concrete output program in C++ or Scala, respectively.</p>

<p>In addition, based on the parameters given by the user, the Backend might apply a second stage compiler to compile the generated program for producing the executable binary or byte-code.</p>
