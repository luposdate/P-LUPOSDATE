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
package lupos.cloud.bloomfilter.mapreduce;

import java.io.IOException;

import lupos.cloud.bloomfilter.BitvectorManager;
import lupos.cloud.hbase.HBaseConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

/**
 * Für jede Tabelle wird ein eigener Thread erzeugt der für die
 * Byte-Bitvektorgeneierung zuständig ist.
 */
public class BVJobThread extends Thread {

	/** Tabellenname. */
	private String tablename;

	/** MapReduce-Job Referenz. */
	private Job job;

	/** Status des Jobs. */
	private boolean finished = false;

	/**
	 * Instantiates a new bV job thread.
	 * 
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public BVJobThread(String tablename) throws IOException {
		this.tablename = tablename;
		createJob();
	}

	/**
	 * Creates the job.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private void createJob() throws IOException {
		Configuration config = HBaseConnection.getConfiguration();
		job = new Job(config, "MR_BV_ " + tablename);

		Scan scan = new Scan();
		int caching = BloomfilterGeneratorMR.CACHING;
		if (tablename.equals("P_SO")) {
			caching = 2;
		}
		scan.setCaching(caching);
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		scan.setBatch(BloomfilterGeneratorMR.BATCH);
		scan.setFilter(new QualifierFilter(CompareOp.NOT_EQUAL,
				new BinaryComparator(Bytes.toBytes("bloomfilter"))));
		scan.addFamily(BitvectorManager.bloomfilter1ColumnFamily);
		scan.addFamily(BitvectorManager.bloomfilter2ColumnFamily);
		scan.setMaxVersions(1);

		TableMapReduceUtil.initTableMapperJob(tablename, // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				null, // mapper output key
				null, // mapper output value
				job);

		TableMapReduceUtil.initTableReducerJob(tablename, // output table
				null, // reducer class
				job);
		job.setNumReduceTasks(0); // kein Reduce Task notwendig
	}

	/**
	 * Thread run() Method.
	 * 
	 */

	public void run() {
		try {
			this.job.waitForCompletion(true);
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

	/**
	 * Gets the tablename.
	 * 
	 * @return the tablename
	 */
	public String getTablename() {
		return tablename;
	}
}
