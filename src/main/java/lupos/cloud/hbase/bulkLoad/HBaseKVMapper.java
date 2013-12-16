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
import java.nio.ByteBuffer;

import lupos.cloud.bloomfilter.BitvectorManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;

/**
 * Mapper Klasse zum Laden großer Datenmengen in HBase. Dabei wird eine
 * CSV-Datei auslgesen und jede Zeile anschließend in das passende
 * Tabellenformat gebracht.
 */
public class HBaseKVMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

	/** CSVParser Referenz. */
	CSVParser csvParser = new CSVParser('\t');

	/** Tabellenname. */
	String tableName = "";;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration c = context.getConfiguration();
		tableName = c.get("hbase.table.name");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = null; // {"a", "b", "c", "d"};

		try {
			fields = csvParser.parseLine(value.toString());
		} catch (Exception ex) {
			context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
			return;
		}

		if (fields.length < 4) {
			context.getCounter("HBaseKVMapper", "TRIPLE_ERROR").increment(1);
			return;
		}

		ImmutableBytesWritable ibKey = new ImmutableBytesWritable(
				Bytes.toBytes(fields[1]));

		// S P O Content
		KeyValue kv1 = new KeyValue(ibKey.get(), fields[0].getBytes(),
				fields[2].getBytes(), fields[3].getBytes());

		context.write(ibKey, kv1);

		String toSplit = fields[2];
		String elem1 = null;
		String elem2 = null;
		if (toSplit.contains(",")) {
			elem1 = toSplit.substring(0, toSplit.indexOf(","));
			elem2 = toSplit.substring(toSplit.indexOf(",") + 1,
					toSplit.length());
		} else {
			elem1 = toSplit.substring(0, toSplit.length());
		}
		// Bloomfilter
		if (!(elem1 == null)) {
			Integer position = BitvectorManager.hash(elem1.getBytes());
			KeyValue kv2 = new KeyValue(ibKey.get(),
					BitvectorManager.bloomfilter1ColumnFamily,
					IntegerToByteArray(4, position), "".getBytes());
			context.write(ibKey, kv2);
		}

		if (!(elem2 == null)) {
			Integer position = BitvectorManager.hash(elem2.getBytes());
			KeyValue kv2 = new KeyValue(ibKey.get(),
					BitvectorManager.bloomfilter2ColumnFamily,
					IntegerToByteArray(4, position), "".getBytes());
			context.write(ibKey, kv2);
		}
		// }
		context.getCounter("HBaseKVMapper", "TRIPLE_IMPORTED").increment(1);

	}

	public static byte[] IntegerToByteArray(int allocate, Integer pos) {
		return ByteBuffer.allocate(allocate).putInt(pos).array();
	}
}
