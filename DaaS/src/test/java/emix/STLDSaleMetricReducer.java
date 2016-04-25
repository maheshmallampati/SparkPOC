package emix;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class STLDSaleMetricReducer extends
		Reducer<Text, Text, NullWritable, Text> {

	private static final BigDecimal DECIMAL_ZERO = new BigDecimal("0.00");

	private MultipleOutputs<NullWritable, Text> multipleOutputs;

	public void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		BigDecimal counterAmount = DECIMAL_ZERO;
		BigDecimal driveThruAmount = DECIMAL_ZERO;
		BigDecimal couponRedeemed = DECIMAL_ZERO;
		BigDecimal giftRedeemedAmount = DECIMAL_ZERO;
		BigDecimal nonProductAmount = DECIMAL_ZERO;
		BigDecimal giftSoldAmount = DECIMAL_ZERO;
		BigDecimal totalAmount = DECIMAL_ZERO;
		BigDecimal hourlyDriveThruAmount = DECIMAL_ZERO;
		BigDecimal hourlyTotalAmount = DECIMAL_ZERO;
		BigDecimal hourlyCounterAmount = DECIMAL_ZERO;

		int counterCount = 0;
		int driveThruCount = 0;
		int couponCount = 0;
		int totalCount = 0;
		int giftRedeemedCount = 0;
		int giftSoldQuantity = 0;
		int hourlyCounterCount = 0;
		int hourlyDriveThruCount = 0;
		int hourlyTotalCount = 0;
		int total_key_qty = 0;
		int pos_key_qt = 0;
		int comboCount = 0;

		String timeStamp = "";
		String businessDate = "";
		String hourFormat = "";
		String storerecord[] = null;

		Date current_timeStamp = new Date();

		boolean isDaily = false;
		boolean isHourly = false;
		boolean isPmix = false;
		boolean isError = false;

		BigDecimal DLY_PMIX_PRC_AM = DECIMAL_ZERO;
		BigDecimal POS_KEY_PRC = DECIMAL_ZERO;

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.00");

		timeStamp = format.format(current_timeStamp);

		for (Text val : values) {

			storerecord = val.toString().split("\\|");
			if (storerecord[storerecord.length - 1].equals(STLDConstants.DAILY))
				isDaily = true;
			else if (storerecord[storerecord.length - 1].equals(STLDConstants.HOURLY))
				isHourly = true;
			else if (storerecord[storerecord.length - 1].equals(STLDConstants.PMIX))
				isPmix = true;
			else if (storerecord[storerecord.length - 1].equals(STLDConstants.Errors))
				isError = true;

			if (isDaily) {
				// Daily Sales Reducer Aggregation
				totalAmount = totalAmount.add(new BigDecimal(storerecord[4]));
				totalCount = totalCount + Integer.parseInt(storerecord[5]);
				nonProductAmount = nonProductAmount.add(new BigDecimal(
						storerecord[8]));
				giftRedeemedAmount = giftRedeemedAmount.add(new BigDecimal(
						storerecord[6]));
				giftRedeemedCount = giftRedeemedCount
						+ Integer.parseInt(storerecord[7]);
				giftSoldAmount = giftSoldAmount.add(new BigDecimal(
						storerecord[9]));
				giftSoldQuantity = giftSoldQuantity
						+ Integer.parseInt(storerecord[10]);
				counterAmount = counterAmount.add(new BigDecimal(
						storerecord[11]));
				counterCount = counterCount + Integer.parseInt(storerecord[12]);
				driveThruAmount = driveThruAmount.add(new BigDecimal(
						storerecord[13]));
				driveThruCount = driveThruCount
						+ Integer.parseInt(storerecord[14]);
				couponRedeemed = couponRedeemed.add(new BigDecimal(
						storerecord[15]));
				couponCount = couponCount + Integer.parseInt(storerecord[16]);
			} else if (isHourly) {
				// Half Hourly Sales Reducer Aggregation

				hourlyTotalAmount = hourlyTotalAmount.add(new BigDecimal(
						storerecord[5]));
				hourlyTotalCount = hourlyTotalCount
						+ Integer.parseInt(storerecord[6]);
				hourlyDriveThruAmount = hourlyDriveThruAmount
						.add(new BigDecimal(storerecord[9]));
				hourlyDriveThruCount = hourlyDriveThruCount
						+ Integer.parseInt(storerecord[10]);
				hourlyCounterAmount = hourlyCounterAmount.add(new BigDecimal(
						storerecord[7]));
				hourlyCounterCount = hourlyCounterCount
						+ Integer.parseInt(storerecord[8]);
				hourFormat = storerecord[4];
			}
			if (isPmix) {
				// storerecord[0]=businessDate
				// storerecord[1]=terrCd
				// storerecord[2]=currencyCode
				// storerecord[3]=lgcyLclRfrDefCd
				// storerecord[4]=menuItem
				// storerecord[5]=alacarte/combo
				// storerecord[6]=pos_key_qt1
				// storerecord[7]=total_key_qty
				// storerecord[8]=total_combo_count
				// storerecord[9]=DLY_PMIX_PRC_AM1
				// storerecord[10]=POS_KEY_PRC1
				// storerecord[11]= PMix

				DLY_PMIX_PRC_AM = new BigDecimal(storerecord[9]);
				POS_KEY_PRC = new BigDecimal(storerecord[10]);
				pos_key_qt = pos_key_qt + Integer.parseInt(storerecord[6]);
				total_key_qty = total_key_qty
						+ Integer.parseInt(storerecord[7]);
				comboCount = comboCount + Integer.parseInt(storerecord[8]);

			}

		}
		businessDate = storerecord[0].substring(0, 4) + "-"
				+ storerecord[0].substring(4, 6) + "-"
				+ storerecord[0].substring(6, 8);

		if (isDaily) {

			// Total Amount
			multipleOutputs.write(
					STLDConstants.DAILY,
					NullWritable.get(),
					new Text((new StringBuffer(storerecord[1])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[3])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.STORE_DESCRIPTION)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(businessDate)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[2])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.TOTAL)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(totalAmount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(totalCount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.RSTMT_FLAG)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(timeStamp)).toString()),
					STLDConstants.DAILY);

			// Non Product Amount
			multipleOutputs.write(
					STLDConstants.DAILY,
					NullWritable.get(),
					new Text((new StringBuffer(storerecord[1])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[3])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.STORE_DESCRIPTION)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(businessDate)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[2])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.NON_PRODUCT)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(nonProductAmount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.RSTMT_FLAG)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(timeStamp)).toString()),
					STLDConstants.DAILY);

			// Drive Thru
			multipleOutputs.write(
					STLDConstants.DAILY,
					NullWritable.get(),
					new Text((new StringBuffer(storerecord[1])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[3])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.STORE_DESCRIPTION)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(businessDate)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[2])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.Drive_THRU)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(driveThruAmount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(driveThruCount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.RSTMT_FLAG)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(timeStamp)).toString()),
					STLDConstants.DAILY);

			// Coupons
			multipleOutputs.write(
					STLDConstants.DAILY,
					NullWritable.get(),
					new Text((new StringBuffer(storerecord[1])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[3])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.STORE_DESCRIPTION)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(businessDate)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[2])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.COUPON)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(couponRedeemed)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(couponCount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.RSTMT_FLAG)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(timeStamp)).toString()),
					STLDConstants.DAILY);

			// Gift Certificates Redeemed

			multipleOutputs.write(
					STLDConstants.DAILY,
					NullWritable.get(),
					new Text((new StringBuffer(storerecord[1])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[3])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.STORE_DESCRIPTION)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(businessDate)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[2])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.GIFT_CERT_REDEEM)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(giftRedeemedAmount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(giftRedeemedCount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.RSTMT_FLAG)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(timeStamp)).toString()),
					STLDConstants.DAILY);

			// Gift Certificates Sold

			multipleOutputs.write(
					STLDConstants.DAILY,
					NullWritable.get(),
					new Text((new StringBuffer(storerecord[1])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[3])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.STORE_DESCRIPTION)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(businessDate)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[2])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.GIFT_CERT_SOLD)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(giftSoldAmount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(giftSoldQuantity)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.RSTMT_FLAG)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(timeStamp)).toString()),
					STLDConstants.DAILY);

			// Counter
			multipleOutputs.write(
					STLDConstants.DAILY,
					NullWritable.get(),
					new Text((new StringBuffer(storerecord[1])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[3])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.STORE_DESCRIPTION)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(businessDate)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[2])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.COUNTER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(counterAmount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(counterCount)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.RSTMT_FLAG)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(timeStamp)).toString()),
					STLDConstants.DAILY);

			// Product
			multipleOutputs.write(
					STLDConstants.DAILY,
					NullWritable.get(),
					new Text((new StringBuffer(storerecord[1])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[3])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.STORE_DESCRIPTION)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(businessDate)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[2])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PRODUCT)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(totalAmount.subtract(nonProductAmount))
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.RSTMT_FLAG)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(timeStamp)).toString()),
					STLDConstants.DAILY);
		} else if (isHourly) {

			if (hourlyTotalAmount.compareTo(DECIMAL_ZERO) != 0.00) {

				multipleOutputs.write(
						STLDConstants.HOURLY,
						NullWritable.get(),
						new Text((new StringBuffer(storerecord[1])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(storerecord[3])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.STORE_DESCRIPTION)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(businessDate)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(storerecord[2])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.TOTAL)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.HALF_HOURLY)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourFormat)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyTotalAmount)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyTotalCount)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.RSTMT_FLAG)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(timeStamp)).toString()),
						STLDConstants.HOURLY);
				// Half-Hourly-Drive Thru
				multipleOutputs.write(
						STLDConstants.HOURLY,
						NullWritable.get(),
						new Text((new StringBuffer(storerecord[1])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(storerecord[3])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.STORE_DESCRIPTION)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(businessDate)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(storerecord[2])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.Drive_THRU)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.HALF_HOURLY)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourFormat)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyDriveThruAmount)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyDriveThruCount)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.RSTMT_FLAG)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(timeStamp)).toString()),
						STLDConstants.HOURLY);
				// Half-Hourly-Counter
				multipleOutputs.write(
						STLDConstants.HOURLY,
						NullWritable.get(),
						new Text((new StringBuffer(storerecord[1])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(storerecord[3])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.STORE_DESCRIPTION)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(businessDate)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(storerecord[2])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.COUNTER)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.HALF_HOURLY)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourFormat)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyCounterAmount)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyCounterCount)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.RSTMT_FLAG)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(timeStamp)).toString()),
						STLDConstants.HOURLY);
			}
		} else if (isPmix) {

			if (DLY_PMIX_PRC_AM.compareTo(DECIMAL_ZERO) != 0.00) {
				multipleOutputs.write(
						STLDConstants.PMIX,
						NullWritable.get(),
						new Text((new StringBuffer(storerecord[1])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(storerecord[3])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.STORE_DESCRIPTION)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(businessDate)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(storerecord[2])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.TOTAL_SOLD)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.NET_PRICE)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(storerecord[4])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(DLY_PMIX_PRC_AM)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(pos_key_qt)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(comboCount)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(total_key_qty)
								.append(STLDConstants.PIPE_DELIMETER)
								.append("0")
								.append(STLDConstants.PIPE_DELIMETER)
								.append(POS_KEY_PRC)
								.append(STLDConstants.PIPE_DELIMETER)
								.append("P")
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.RSTMT_FLAG)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(timeStamp)).toString()),
						STLDConstants.PMIX);
			}

		}
		if (isError) {
			multipleOutputs.write(
					STLDConstants.Errors,
					NullWritable.get(),
					new Text((new StringBuffer(storerecord[1])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[3])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(STLDConstants.STORE_DESCRIPTION)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(businessDate)
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[2])
							.append(STLDConstants.PIPE_DELIMETER)
							.append(storerecord[4])
							.append(STLDConstants.PIPE_DELIMETER)
							.append("NO_STORE_OR_NO_MENUITEMCODES")
							.append(STLDConstants.PIPE_DELIMETER)
							.append(timeStamp)).toString()),
					STLDConstants.Errors);
		}

	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();
	}
}
