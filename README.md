# P-LUPOSDATE Cloud Semantic Web Database Management System

This is a [repository of P-LUPOSDATE](https://github.com/luposdate/p-luposdate), a Cloud Semantic Web Database Management System developed by [IFIS](http://www.ifis.uni-luebeck.de/) at the [University of Lübeck](http://www.uni-luebeck.de/).

It is based on [LUPOSDATE](https://github.com/luposdate/luposdate).

P-LUPOSDATE supports the RDF query language SPARQL 1.1.

## Usage

The following programs can be started:
- HBaseLoader: This program imports triple (in N3 format). The data can be loaded per HBase API (Option 1) or with bulk-load (Option 2), which is recommended, with the following call: $ java -jar hbaseLoader.jar <path> <load-option> <size of the blocks of HBase triples>
- QueryExecuter: This program executes arbitrary SPARQL queries, the paths of which must be given as parameters as well as other parameters like the Bloomfilter application: $ java -jar queryExecuter.jar <number of reduce nodes> <Bloomfilter option: both, first, second> <output of triple set: size or nosize> <path q1> <path q2> ...
- Start_Demo_Applet_DE: This program starts the LUPOSDATE GUI.
- SparqlEndpoint: Start of the HTTP SPARQL-Endpoint
- QuadToN3Converter: Transforms Quads into N3 triples

## Configuration / Installation
### Luposdate Cloud Software:
There are some parameters in the code, which are important for execution. In future releases, these parameters may be put into a configuration file:

- HBaseConnection.java: 
 - deleteTableOnCreation -> true/false for new creation of the tables at program start
- CloudManagement.java: 
 - PRINT_PIGLATIN_PROGRAMM -> true/false prints out the generated Pig Latin program
 - TESTING_MODE -> true/false for activating the test modus, which does not connect to the cloud and just generates the Pig Latin program
 - bloomfilter_active -> true/false bloomfilter active or not

### Cloudera/Hadoop:
We recommend to use [Cloudera](http://www.cloudera.com/) for the installation of Hadoop.
Furthermore, the installation of LZO as described [here](http://www.cloudera.com/content/cloudera-content/cloudera-docs/CM4Ent/4.6.2/Cloudera-Manager-Installation-Guide/cmig_install_LZO_Compression.html) is necessary.