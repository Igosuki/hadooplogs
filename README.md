<h1>Description</h1>
<p>A project that aims at customizing Haddop to normalize and centralize logging facilities over an heterogeneous IS</p>

<h1>Hadoop Logs project manual</h1>

<h2>Operations with Hive</h2>

<p>Add a custom function</p>
<pre><code>
create temporary function custom_udf as 'load.utils.CustomUDF';
select custom_udf(title) from titles group by custom_udf(title);
</code></pre>

<p>To deploy the project jar temporarily, launch the ant build (hive and hadoop being in your classpath), 
copy the jar on a local path on the server and execute :</p>
<pre><code>add jar /path/to/jar/jar_name.jar;</code></pre>
 
