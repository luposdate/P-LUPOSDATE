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
package lupos.cloud.pig.udfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.WritableComparable;

/**
 * *** PATCH ***
 * This class allow Pig to order TableSplits. A table split corresponds to a key
 * range (low, high).
 * 
 * @since Pig 0.9.3
 */
public class TableSplitComparable implements WritableComparable<TableSplit> {

	TableSplit tsplit;

	// need a default constructor to be able to de-serialize using just the
	// Writable interface
	public TableSplitComparable() {
		tsplit = new TableSplit();
	}

	public TableSplitComparable(TableSplit tsplit) {
		this.tsplit = tsplit;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tsplit.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		tsplit.write(out);
	}

	@Override
	public int compareTo(TableSplit o) {
		return tsplit.compareTo(o);
	}

	@Override
	public String toString() {
		return "TableSplitComparable : " + tsplit.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return ((tsplit == null) ? 0 : tsplit.hashCode());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TableSplitComparable other = (TableSplitComparable) obj;
		if (tsplit == null) {
			if (other.tsplit != null)
				return false;
		} else if (!tsplit.equals(other.tsplit)) {
			return false;
		}
		return true;
	}
}
