/**
 * Copyright (c) 2013, Institute of Information Systems (Sven Groppe, Thomas Kiencke and contributors of P-LUPOSDATE), University of Luebeck
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 	- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * 	  disclaimer.
 * 	- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * 	  following disclaimer in the documentation and/or other materials provided with the distribution.
 * 	- Neither the name of the University of Luebeck nor the names of its contributors may be used to endorse or promote
 * 	  products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package lupos.cloud.hbase.bulkLoad;

import java.io.IOException;

import lupos.cloud.hbase.HBaseConnection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Für das Übertragen der Tripel wird für jede Tabelle ein eigener Thread
 * gestartet.
 */
public class BulkLoad extends Thread {

	/** Name der Tabelle. */
	private String tablename;

	/** Job Referenz. */
	private Job job;

	/** Job Status. */
	private boolean finished = false;

	/**
	 * Instantiates a new bulk load.
	 * 
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public BulkLoad(String tablename) throws IOException {
		this.tablename = tablename;
		createJob();
	}

	/**
	 * Erzeugt den Job und übergibt alle nötigen Informationen.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private void createJob() throws IOException {
		System.out.println(tablename + " wird uebertragen!");
		// init job
		HBaseConnection.getConfiguration().set("hbase.table.name", tablename);

		job = new Job(HBaseConnection.getConfiguration(),
				"HBase Bulk Import for " + tablename);
		job.setJarByClass(HBaseKVMapper.class);

		job.setMapperClass(HBaseKVMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);

		// TableMapReduceUtil.addDependencyJars(job);

	}

	/**
	 * Lädt eine Tabelle per Map Reduce Bulkload.
	 * 
	 */

	public void run() {
		try {
			// generiere HFiles auf dem verteilten Dateisystem
			HTable hTable = new HTable(HBaseConnection.getConfiguration(),
					tablename);

			HFileOutputFormat.configureIncrementalLoad(job, hTable);

			FileInputFormat.addInputPath(job, new Path("/tmp/"
					+ HBaseConnection.WORKING_DIR + "/" + tablename + "_"
					+ HBaseConnection.BUFFER_FILE_NAME + ".csv"));
			FileOutputFormat.setOutputPath(job, new Path("/tmp/"
					+ HBaseConnection.WORKING_DIR + "/" + tablename + "_"
					+ HBaseConnection.BUFFER_HFILE_NAME));

			job.waitForCompletion(true);

			// Lade generierte HFiles in HBase
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
					HBaseConnection.getConfiguration());
			loader.doBulkLoad(
					new Path("/tmp/" + HBaseConnection.WORKING_DIR + "/"
							+ tablename + "_"
							+ HBaseConnection.BUFFER_HFILE_NAME), hTable);

			this.finished = true;

		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gets the job.
	 * 
	 * @return the job
	 */
	public Job getJob() {
		return job;
	}

	/**
	 * Checks if is finished.
	 * 
	 * @return true, if is finished
	 */
	public boolean isFinished() {
		return finished;
	}
}
