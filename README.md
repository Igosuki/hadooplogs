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
 
<p>To execute a script file simply</p>
<pre><code>hive -f script</code></pre>
 
<p>To build the jar</p>
<pre>ant clean dist remember to change the hive and hadoop directories in the build file according to your setup</pre>
 
<p>To execute the project's scripts and see the log</p>
<pre><code>hive -f scriptname -hiveconf hive.root.logger=DEBUG,console</code></pre>
<pre>Things to have on the local system : the ant built jar file, the files </pre>
<pre>If the files are external, you don't need to do anything, if however they are internal (meaning hive will store them on the hdfs) : </pre>
<pre><code>hadoop dfs -mkdir /the/path/to/store</code></pre>

<p>To execute the project's scripts with another parse configuration</p>
<pre><code>hive -f scriptname -hiveconf hive.root.logger=DEBUG,console -hiveconf hadoop.csvinput.format=conffile</code></pre>
<pre>External normalizing configurations are not yet supported with thrift because there is no default file naming convention yet</pre>
<pre>Where conffile is the classpath of your xml file </pre>
<pre>The configuration schema and xml examples can be found in src\main\resources\properties\jobs\<pre>

