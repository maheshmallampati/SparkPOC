package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.test.daas.driver.CashlessData;

public class CashlessDataReducer extends Reducer<Text, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> mos;

	private String keyValue;
	private String posBusnDt;
	private String fileName;
	
	private BigDecimal totCashAm;
	private BigDecimal totCashlessAm;
	private int trnCashQty;
	private int trnCashlessQty;
	private int strCnt;
	
	private String[] parts;
	
	private Text outputValue = new Text();

	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

	}
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		keyValue = key.toString();
		
		if ( keyValue.startsWith("A") ) {
			posBusnDt = keyValue.substring(2);
			fileName = "Sales";

			totCashAm = CashlessData.DEC_ZERO;
			totCashlessAm = CashlessData.DEC_ZERO;
			trnCashQty = 0;
			trnCashlessQty = 0;
			strCnt = 0;
			
		} else {
			fileName = "Cashless";
		}
		
		for (Text value : values ) {
			if ( keyValue.startsWith("A") ) {
				parts = value.toString().split("\t");
				totCashAm = totCashAm.add(new BigDecimal(parts[1])); 
				trnCashQty += Integer.parseInt(parts[2]);
				totCashlessAm = totCashlessAm.add(new BigDecimal(parts[3])); 
				trnCashlessQty += Integer.parseInt(parts[4]);
				strCnt++;
			} else {
				mos.write(NullWritable.get(), value, fileName);
			}
		}
		
		if ( keyValue.startsWith("A") ) {
			outputValue.clear();
			outputValue.set(posBusnDt + "\t" + strCnt + "\t" + totCashAm.toString() + "\t" + trnCashQty + "\t" + totCashlessAm.toString() + "\t" + trnCashlessQty);
		
			mos.write(NullWritable.get(), outputValue, fileName);
		}

	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
	
}
