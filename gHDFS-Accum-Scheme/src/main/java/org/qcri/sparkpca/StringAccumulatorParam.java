/**
 * QCRI, sPCA LICENSE
 * sPCA is a scalable implementation of Principal Component Analysis (PCA) on of Spark and MapReduce
 *
 * Copyright (c) 2015, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
*/

package org.qcri.sparkpca;

import org.apache.spark.AccumulatorParam;

/**
 * This class supports Accumulator of type String. It implements an element-by-element add operation for
 * two String
 * 
 * @author Tarek Elgamal
 *
 */

public class StringAccumulatorParam implements AccumulatorParam<String> {


	public String addInPlace(String arg0, String arg1) {
		arg0=arg0.concat(arg1);
		return arg0;
	}

	public String zero(String arg0) {
		return arg0;
	}

	public String addAccumulator(String arg0, String arg1) {
		return addInPlace(arg0,arg1);
	}
	

}
