package com.mcd.gdw.daas.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.poi.POIDocument;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.POIXMLProperties;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.PrintSetup;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class DataQualityReport {

	public class DataQualityCorrectnessLine {
		
		public String busnDt;
        public String terrCd;
        public String terrNa;
        public String lcat;
        public String lcatNa;
        public double dailySalesNetAmt;
        public double dailySalesGrossAmt;
        public String netGrossFl;
        public int dailySalesTc;
        public double xmlSalesAmt;
        public int xmlTc;
        
	}
	
	public class DataQualityCompletenessSummaryLine {
	
		public String busnDt;
		public String terrCd;
		public String terrNa;
		public int expectedCount;
		public int fileCount;
		public int incompleteFileCount;
		public int startColumn;
		public ArrayList<String> owshTypes;
		
	}

	public class DataQualityCompletenessDetailLine {
		
		public String terrCd;
		public String terrNa;
		public String busnDt;
		public String lcat;
		public String lcatNa;
		public String fileRejectReason;
		public String subType;
		public String subTypeRejectReason;
		public ArrayList<String> subTypes;
		
	}

	public enum ReportFormat {HTML, XLS, XLSX}
	public enum ReportType {Completeness, Correctness}
	
	private static final int HDR_CORRECTNESS_LINE_01 = 0;
	private static final int HDR_CORRECTNESS_LINE_02 = 2;
	private static final int HDR_CORRECTNESS_LINE_03 = 3;
	private static final int HDR_CORRECTNESS_LINE_04 = 4;
	private static final int HDR_CORRECTNESS_LINE_05 = 5;
	private static final int HDR_CORRECTNESS_LINE_06 = 6;
	private static final int HDR_CORRECTNESS_LINE_07 = 7;
	private static final int HDR_CORRECTNESS_LINE_08 = 8;
	private static final int HDR_CORRECTNESS_LINE_09 = 9;
	private static final int HDR_CORRECTNESS_LINE_10 = 10;

	private static final int HDR_COMPLETENESS_SUMMARY_LINE_01 = 0;
	private static final int HDR_COMPLETENESS_SUMMARY_LINE_02 = 2;
	private static final int HDR_COMPLETENESS_SUMMARY_LINE_03 = 3;
	private static final int HDR_COMPLETENESS_SUMMARY_LINE_04 = 4;

	private static final int HDR_COMPLETENESS_DETAIL_LINE_01 = 0;
	private static final int HDR_COMPLETENESS_DETAIL_LINE_02 = 2;

	private static final int MAX_ERRORS = 3000;
	
	private static final String AUTHOR = "DaaS - Data Quality";

	private ReportFormat rptFormat;
	private ReportType rptType;
	
	private String outFileNameAndPath = "";
    private Workbook wb = null;
    private Sheet ws = null;
    private Row row;
	private Cell cell;
	
	private String lastDate = "";
	private String lastTerrCd = "";
	private String lastLcat = "";
	
	private int rowIdx = 0;
	private int lastCol = 0;
	
	private double target = 0;
	private double source = 0;
	private double pct = 0;
	
	private Map<String, CellStyle> styles;
	
	private Locale currentLocale;
	private NumberFormat numberFormatter;
	private NumberFormat amountFormatter;
	private NumberFormat percentFormatter;
	private DateFormat dateFormatter;
	
	private BufferedWriter htmOut;
	
	private String priorTerrCd = "";
	private String priorBusnDt = "";
	private String countryName = "";
	private String lcatName = "";

	private int lcatCount = 0;
	private int amtWithin1Pct = 0;
	private int amtWithin5Pct = 0;
	private int amtOver5Pct = 0;
	private int tcWithin1Pct = 0;
	private int tcWithin5Pct = 0;
	private int tcOver5Pct = 0;
	
	
	private double dsAmt = 0;
	private double amtPct = 0;
	private String amtPctTxt = "";
	private String amtPctFmt = "";
	
	private double tcPct = 0;
	private String tcPctTxt = "";
	private String tcPctFmt = "";
	
	private StringBuffer summaryRpt = new StringBuffer();

	private String summaryLastTerrCd = "";
	private String summaryLastDate = "";
	
	private String summaryTerr = "";
	private String summaryDate = "";
	private int summaryLcatCount = 0;
	private int summaryLcatWithin1Pct = 0;
	private int summaryLcatWithin5Pct = 0;
	private int summaryLcatOver5Pct = 0;
	private int summaryLcatMissing = 0;
	private boolean amtWithin1PctFl = false;
	private boolean amtWithin5PctFl = false;
	private boolean amtOver5PctFl = false;
	private boolean amtMissingFl = false;
	private boolean tcWithin1PctFl = false;
	private boolean tcWithin5PctFl = false;
	private boolean tcOver5PctFl = false;
	private boolean tcMissingFl = false;
	
	private String lastOpenWorksheet = "";
	private int errCount = 0;
	private int dispCount = 0;
	

	public DataQualityReport(ReportFormat rptFormat
			                ,ReportType rptType
                            ,String outFileNameAndPath
                            ,Locale currentLocale) throws Exception {
		
		this.rptFormat = rptFormat;
		this.rptType = rptType;
		this.outFileNameAndPath = outFileNameAndPath;
		this.currentLocale = currentLocale;
		
        if ( this.rptFormat == ReportFormat.XLSX ) {
        	wb = new XSSFWorkbook();
        } else if ( this.rptFormat == ReportFormat.XLS) {
            wb = new HSSFWorkbook();
        }
        
        initializeFormatters();
        
        if ( this.rptFormat == ReportFormat.HTML ) {
        	if ( this.rptType == ReportType.Completeness ) {
        		initializeHtml("Data Completeness");
        	} else {
        		initializeHtml("Data Correctness");
        	}
        } else {
        	styles = createStyles(wb);
        }
	
        summaryRpt.setLength(0);
    	summaryLastTerrCd = "";
    	summaryLastDate = "";
    	
	}

	public void save() throws Exception {
        
		if ( this.rptFormat == ReportFormat.HTML ) {
			if ( this.rptType == ReportType.Correctness ) {
				if ( lcatCount > 0 ) {
					processDtChangeForCorrectness("");
				}
			}
			
	        htmOut.close();
		} else {
			if ( this.rptType == ReportType.Correctness ) {
				if ( ws != null ) {
					finalizeHeaderForCorrectness();
					finalizeSheetForCorrectness();
				}
			} else {
				if ( ws != null ) {
					if ( lastOpenWorksheet.equals("SUMMARY") ) {
						finalizeSheetForCompletenessSummary();
					}
					
					if ( lastOpenWorksheet.equals("DETAIL") ) {
						finalizeSheetForCompletenessDetail();
					}
				}
			}
			
	        if( wb instanceof XSSFWorkbook ) {
	        	POIXMLProperties xmlProps = ((POIXMLDocument) wb).getProperties();    
	        	POIXMLProperties.CoreProperties coreProps =  xmlProps.getCoreProperties();
	        	coreProps.setCreator(AUTHOR);
	        } else {
	        	((POIDocument) wb).createInformationProperties();
	        	SummaryInformation summaryInfo = ((POIDocument) wb).getSummaryInformation();
	        	summaryInfo.setAuthor(AUTHOR);
	        }
	        
	        FileOutputStream out = new FileOutputStream(outFileNameAndPath);
	        wb.write(out);
	        out.close();
		}

		if ( this.rptType == ReportType.Correctness ) {
			outputSummaryForCorrectness();
		}
	}
	
	public String summaryReport() {
		
		return(summaryRpt.toString());
	}

	public void addLine(DataQualityCorrectnessLine line) throws Exception {

		if ( this.rptType == ReportType.Correctness ) {
			if ( this.rptFormat == ReportFormat.HTML ) {
				addLineHtmlForCorrectness(line);
			} else {
				addLineExcelForCorrectness(line);
			}
			
			addLineSummaryForCorrectness(line);
			
		} else {
			throw new Exception("Request for Correctness Line invalid for non correctness report type.");
		}
	}

	public void addLine(DataQualityCompletenessSummaryLine line) throws Exception {

		if ( this.rptType == ReportType.Completeness ) {
			if ( this.rptFormat == ReportFormat.HTML ) {
			} else {
				addLineExcelForCompletenessSummary(line);
			}
		} else {
			throw new Exception("Request for Completeness Line invalid for non completeness report type.");
		}
	}

	public void addLine(DataQualityCompletenessDetailLine line) throws Exception {

		if ( this.rptType == ReportType.Completeness ) {
			if ( this.rptFormat == ReportFormat.HTML ) {
			} else {
				addLineExcelForCompletenessDetail(line);
			}
		} else {
			throw new Exception("Request for Completeness Line invalid for non completeness report type.");
		}
	}

	private void addLineHtmlForCorrectness(DataQualityCorrectnessLine line) throws Exception {

		if ( !line.terrCd.equals(priorTerrCd) ) {
			if ( priorTerrCd.length() > 0 ) {
				htmOut.write("    </table>\n");
			}


			if ( line.terrNa.length() > 0 ) {
				countryName = line.terrNa + " (" + line.terrCd + ")";
			} else {
				countryName = "Country Code: " + line.terrCd;
			}

			htmOut.write("    <br>\n");
			htmOut.write("    <table border=\"0\">\n");
			htmOut.write("      <tr>\n");
			htmOut.write("        <td span\"19\" class=\"hdr1\">" + countryName + "</td>\n");
			htmOut.write("      </tr>\n");

			priorTerrCd = line.terrCd;
			priorBusnDt = "";
		}

		if ( !line.busnDt.equals(priorBusnDt) ) {

			processDtChangeForCorrectness(line.busnDt);

			priorBusnDt = line.busnDt;

			lcatCount = 0;
			amtWithin1Pct = 0;
			amtWithin5Pct = 0;
			amtOver5Pct = 0;
			tcWithin1Pct = 0;
			tcWithin5Pct = 0;
			tcOver5Pct = 0;
		}

		lcatCount++;

		if ( line.lcatNa.length() > 0 ) {
			lcatName = line.lcat + " - " + line.lcatNa;
		} else {
			lcatName = line.lcat;
		}

		if ( line.netGrossFl.equals("G") ) {
			dsAmt = line.dailySalesGrossAmt; 
		} else {
			dsAmt = line.dailySalesNetAmt;
		}

		if ( dsAmt == 0 ) {
			amtPctTxt = "n/a";
			amtPctFmt = "greytext";
			amtWithin1Pct++;
		} else {
			amtPct = (line.xmlSalesAmt - dsAmt) / dsAmt;
			amtPctTxt = percentFormatter.format(amtPct);
			if ( Math.abs(amtPct) < .01 ) {
				amtPctFmt = "greenback";
				amtWithin1Pct++;
			} else if ( Math.abs(amtPct) < .05 ) {
				amtPctFmt = "yellowback";
				amtWithin5Pct++;
			} else {
				amtPctFmt = "redback";
				amtOver5Pct++;
			}
		}

		if ( line.dailySalesTc == 0 ) {
			tcPctTxt = "n/a";
			tcPctFmt = "greytext";
			tcWithin1Pct++;
		} else {
			tcPct = ((double)line.xmlTc - (double)line.dailySalesTc) / (double)line.dailySalesTc;
			tcPctTxt = percentFormatter.format(tcPct);

			if ( Math.abs(tcPct) < .01 ) {
				tcPctFmt = "greenback";
				tcWithin1Pct++;
			} else if ( Math.abs(tcPct) < .05 ) {
				tcPctFmt = "yellowback";
				tcWithin5Pct++;
			} else {
				tcPctFmt = "redback";
				tcOver5Pct++;
			}
		}
		
		htmOut.write("      <tr>\n");
		htmOut.write("        <td colspan=\"4\">&nbsp;</td>\n");
		htmOut.write("        <td>" + lcatName + "</td>\n");
		htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
		htmOut.write("        <td class=\"cellright\">" + amountFormatter.format(dsAmt) + "</td>\n");
		htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
		htmOut.write("        <td class=\"cellright\">" + amountFormatter.format(line.xmlSalesAmt) + "</td>\n");
		htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
		htmOut.write("        <td class=\"cellcenter\">" + line.netGrossFl + "</td>\n");
		htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
		htmOut.write("        <td class=\"" + amtPctFmt + "\">" + amtPctTxt + "</td>\n");
		htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
		htmOut.write("        <td class=\"cellright\">" + numberFormatter.format((long)line.dailySalesTc) + "</td>\n");
		htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
		htmOut.write("        <td class=\"cellright\">" + numberFormatter.format((long)line.xmlTc) + "</td>\n");
		htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
		htmOut.write("        <td class=\"" + tcPctFmt + "\">" + tcPctTxt + "</td>\n");
		htmOut.write("      </tr>\n");
		
	}

	private void processDtChangeForCorrectness(String busnDt) throws Exception {

		double totalCount = lcatCount; 
		double count = 0;
		double pct = 0;
		
		if ( priorBusnDt.length() > 0 && lcatCount > 0 ) {
			htmOut.write("      <tr>\n");
			htmOut.write("        <td span\"19\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");

			htmOut.write("      <tr>\n");
			htmOut.write("        <td colspan=\"4\">&nbsp;</td>\n");
			htmOut.write("        <td class=\"cellboldright\">Location Count:</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"cellboldright\">" + numberFormatter.format(lcatCount) + "</td>\n");
			htmOut.write("        <td colspan=\"12\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");

			count = amtWithin1Pct;
			pct = (count / totalCount);
			
			htmOut.write("      <tr>\n");
			htmOut.write("        <td colspan=\"4\">&nbsp;</td>\n");
			htmOut.write("        <td class=\"greennote\">Number with sales within (+/-) " + percentFormatter.format(.01) + ":</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"greennote\">" + numberFormatter.format(amtWithin1Pct) + "</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"greennote\">" + percentFormatter.format(pct) + "</td>\n");
			htmOut.write("        <td colspan=\"10\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");

			count = amtWithin5Pct;
			pct = (count / totalCount);

			htmOut.write("      <tr>\n");
			htmOut.write("        <td colspan=\"4\">&nbsp;</td>\n");
			htmOut.write("        <td class=\"yellownote\">within (+/-) " + percentFormatter.format(.05) + ":</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"yellownote\">" + numberFormatter.format(amtWithin5Pct) + "</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"yellownote\">" + percentFormatter.format(pct) + "</td>\n");
			htmOut.write("        <td colspan=\"10\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");

			count = amtOver5Pct;
			pct = (count / totalCount);

			htmOut.write("      <tr>\n");
			htmOut.write("        <td colspan=\"4\">&nbsp;</td>\n");
			htmOut.write("        <td class=\"rednote\">over/under " + percentFormatter.format(.05) + ":</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"rednote\">" + numberFormatter.format(amtOver5Pct) + "</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"rednote\">" + percentFormatter.format(pct) + "</td>\n");
			htmOut.write("        <td colspan=\"10\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");

			htmOut.write("      <tr>\n");
			htmOut.write("        <td span\"17\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");

			count = tcWithin1Pct;
			pct = (count / totalCount);
				
			htmOut.write("      <tr>\n");
			htmOut.write("        <td colspan=\"4\">&nbsp;</td>\n");
			htmOut.write("        <td class=\"greennote\">Number with transcations within (+/-) " + percentFormatter.format(.01) + ":</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"greennote\">" + numberFormatter.format(tcWithin1Pct) + "</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"greennote\">" + percentFormatter.format(pct) + "</td>\n");
			htmOut.write("        <td colspan=\"10\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");

			count = tcWithin5Pct;
			pct = (count / totalCount);

			htmOut.write("      <tr>\n");
			htmOut.write("        <td colspan=\"4\">&nbsp;</td>\n");
			htmOut.write("        <td class=\"yellownote\">within (+/-) " + percentFormatter.format(.05) + ":</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"yellownote\">" + numberFormatter.format(tcWithin5Pct) + "</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"yellownote\">" + percentFormatter.format(pct) + "</td>\n");
			htmOut.write("        <td colspan=\"10\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");

			count = tcOver5Pct;
			pct = (count / totalCount);

			htmOut.write("      <tr>\n");
			htmOut.write("        <td colspan=\"4\">&nbsp;</td>\n");
			htmOut.write("        <td class=\"rednote\">over/under " + percentFormatter.format(.05) + ":</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"rednote\">" + numberFormatter.format(tcOver5Pct) + "</td>\n");
			htmOut.write("        <td colspan=\"2\" class=\"rednote\">" + percentFormatter.format(pct) + "</td>\n");
			htmOut.write("        <td colspan=\"10\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");

			htmOut.write("      <tr>\n");
			htmOut.write("        <td span\"19\">&nbsp;</td>\n");
			htmOut.write("      </tr>\n");
		}

		if ( busnDt.length() > 0 ) {
			Calendar dt = Calendar.getInstance();
			
			dt.set(Integer.parseInt(busnDt.substring(0, 4)), Integer.parseInt(busnDt.substring(4, 6)) - 1, Integer.parseInt(busnDt.substring(6, 8)));
				
			htmOut.write("      <tr>");
			htmOut.write("        <td colspan=\"2\">&nbsp;</td>\n");
			htmOut.write("        <td colspan=\"15\" class=\"hdr2\">Date: " + dateFormatter.format(dt.getTime()) + "</td>\n");
			htmOut.write("      </tr>\n");

			htmOut.write("      <tr>\n");
			htmOut.write("        <td colspan=\"6\">&nbsp;</td>\n");
			htmOut.write("        <td colspan=\"5\" class=\"cellboldcenter\">Sales Amount</td>\n");
			htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
			htmOut.write("        <td colspan=\"5\" class=\"cellboldcenter\">Transaction Count</td>\n");
			htmOut.write("      </tr>\n");
			htmOut.write("      <tr>\n");
			htmOut.write("        <td colspan=\"4\">&nbsp;</td>\n");
			htmOut.write("        <td class=\"cellboldleft\">Location</td>\n");
			htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
			htmOut.write("        <td class=\"cellboldcenter\">Daily Sales</td>\n");
			htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
			htmOut.write("        <td class=\"cellboldcenter\">NP6</td>\n");
			htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
			htmOut.write("        <td class=\"cellboldcenter\">N/G</td>\n");
			htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
			htmOut.write("        <td class=\"cellboldcenter\">Difference %</td>\n");
			htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
			htmOut.write("        <td class=\"cellboldcenter\">Daily Sales</td>\n");
			htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
			htmOut.write("        <td class=\"cellboldcenter\">NP6</td>\n");
			htmOut.write("        <td>&nbsp;&nbsp;&nbsp;</td>\n");
			htmOut.write("        <td class=\"cellboldcenter\">Difference %</td>\n");
			htmOut.write("      </tr>\n");
		}
	}

	private void addLineExcelForCorrectness(DataQualityCorrectnessLine line) throws Exception {

		if ( !line.busnDt.equals(lastDate) || !line.terrCd.equals(lastTerrCd) ) {
			if ( ws != null ) {
				finalizeHeaderForCorrectness();
				finalizeSheetForCorrectness();
			}

			lastDate = line.busnDt;
			lastTerrCd = line.terrCd;

			ws = wb.createSheet(line.terrCd + "-" + line.busnDt);
			ws.setDisplayGridlines(false);
			ws.setPrintGridlines(false);
			ws.setFitToPage(true);
			ws.setHorizontallyCenter(true);
			PrintSetup printSetup = ws.getPrintSetup();
			printSetup.setLandscape(true);

			//the following three statements are required only for HSSF
			ws.setAutobreaks(true);
			printSetup.setFitHeight((short)1);
			printSetup.setFitWidth((short)1);

			initializeSheetForCorrectness(line.busnDt,line.terrCd,line.terrNa);
		}

		row = ws.createRow(rowIdx);

		cell = row.createCell(0);

		if ( line.lcatNa.length() > 0 ) {
			cell.setCellValue(line.lcat + " - " + line.lcatNa);
		} else {
			cell.setCellValue(line.lcat);
		}

		cell = row.createCell(1);
		if ( line.netGrossFl.equals("G") ) {
			cell.setCellValue(line.dailySalesGrossAmt);
			target = line.dailySalesGrossAmt;
		} else {
			cell.setCellValue(line.dailySalesNetAmt);
			target = line.dailySalesNetAmt;
		}
		cell.setCellStyle(styles.get("commadecimal2"));

		cell = row.createCell(2);
		cell.setCellValue(line.xmlSalesAmt);
		cell.setCellStyle(styles.get("commadecimal2"));

		cell = row.createCell(3);
		cell.setCellValue(line.netGrossFl);
		cell.setCellStyle(styles.get("center"));

		cell = row.createCell(4);
		cell.setCellFormula("IF(B" + (rowIdx+1) + "=0,\"n/a\",(C" + (rowIdx+1) + "-B" + (rowIdx+1) + ")/B" + (rowIdx+1) +"*100)");

		source = line.xmlSalesAmt; 

		if ( target == 0 ) {
			cell.setCellStyle(styles.get("na"));
		} else { 
			pct = Math.abs((source-target)/target*100);

			if ( pct < 1 ) {
				cell.setCellStyle(styles.get("decimal2_green"));
			} else if ( pct < 5 ) {
				cell.setCellStyle(styles.get("decimal2_yellow"));
			} else {
				cell.setCellStyle(styles.get("decimal2_red"));
			}
		}

		cell = row.createCell(5);
		cell.setCellFormula("IF(E" + (rowIdx+1) + "=\"n/a\",\"T0\",IF(ABS(E" + (rowIdx+1) + ")<1,\"T1\",IF(ABS(E" + (rowIdx+1) + ")<5,\"T2\",\"T3\")))");
		cell.setCellStyle(styles.get("center_border"));

		cell = row.createCell(6);
		cell.setCellValue(line.dailySalesTc);
		cell.setCellStyle(styles.get("comma"));

		cell = row.createCell(7);
		cell.setCellValue(line.xmlTc);
		cell.setCellStyle(styles.get("comma"));

		cell = row.createCell(8);
		cell.setCellFormula("IF(G" + (rowIdx+1) + "=0,\"n/a\",(H" + (rowIdx+1) + "-G" + (rowIdx+1) + ")/G" + (rowIdx+1) +"*100)");

		source = line.xmlTc; 
		target = line.dailySalesTc;

		if ( target == 0 ) {
			cell.setCellStyle(styles.get("na"));
		} else { 
			pct = Math.abs((source-target)/target*100);

			if ( pct < 1 ) {
				cell.setCellStyle(styles.get("decimal2_green"));
			} else if ( pct < 5 ) {
				cell.setCellStyle(styles.get("decimal2_yellow"));
			} else {
				cell.setCellStyle(styles.get("decimal2_red"));
			}
		}

		cell = row.createCell(9);
		cell.setCellFormula("IF(I" + (rowIdx+1) + "=\"n/a\",\"T0\",IF(ABS(I" + (rowIdx+1) + ")<1,\"T1\",IF(ABS(I" + (rowIdx+1) + ")<5,\"T2\",\"T3\")))");
		cell.setCellStyle(styles.get("center_border"));

		cell = row.createCell(10);
		cell.setCellFormula("IF(OR(F" + (rowIdx+1) + "=\"T3\",J" + (rowIdx+1) + "=\"T3\"),\"T3\",IF(OR(F" + (rowIdx+1) + "=\"T2\",J" + (rowIdx+1) + "=\"T2\"),\"T2\",IF(OR(F" + (rowIdx+1) + "=\"T1\",J" + (rowIdx+1) + "=\"T1\"),\"T1\",\"T0\")))");
		cell.setCellStyle(styles.get("center"));

		rowIdx++;

	}

	private void addLineSummaryForCorrectness(DataQualityCorrectnessLine line) throws Exception {

		if ( !summaryLastTerrCd.equals(line.terrCd) || !summaryLastDate.equals(line.busnDt) ) {
			outputSummaryForCorrectness();

			summaryLastTerrCd = line.terrCd;
			summaryLastDate = line.busnDt;

			if ( line.terrNa.length() > 0 ) {
				summaryTerr = line.terrNa + " (" + line.terrCd + ")";
			} else {
				summaryTerr = "Country Code: " + line.terrCd;
			}

			summaryDate = line.busnDt;

			summaryLcatCount = 0;
			summaryLcatWithin1Pct = 0;
			summaryLcatWithin5Pct = 0;
			summaryLcatOver5Pct = 0;
			summaryLcatMissing = 0;

		}

		summaryLcatCount++;

		amtWithin1PctFl = false;
		amtWithin5PctFl = false;
		amtOver5PctFl = false;
		amtMissingFl = false;
		tcWithin1PctFl = false;
		tcWithin5PctFl = false;
		tcOver5PctFl = false;
		tcMissingFl = false;

		if ( line.netGrossFl.equals("G") ) {
			dsAmt = line.dailySalesGrossAmt; 
		} else {
			dsAmt = line.dailySalesNetAmt;
		}

		if ( dsAmt == 0 ) {
			amtMissingFl = true;
		} else {
			amtPct = (line.xmlSalesAmt - dsAmt) / dsAmt;
			if ( Math.abs(amtPct) < .01 ) {
				amtWithin1PctFl = true;
			} else if ( Math.abs(amtPct) < .05 ) {
				amtWithin5PctFl = true;
			} else {
				amtOver5PctFl = true;
			}
		}

		if ( line.dailySalesTc == 0 ) {
			tcMissingFl = true;
		} else {
			tcPct = ((double)line.xmlTc - (double)line.dailySalesTc) / (double)line.dailySalesTc;
			if ( Math.abs(tcPct) < .01 ) {
				tcWithin1PctFl = true;
			} else if ( Math.abs(tcPct) < .05 ) {
				tcWithin5PctFl = true;
			} else {
				tcOver5PctFl = true;
			}
		}

		if ( amtMissingFl || tcMissingFl ) {
			summaryLcatMissing++;
		} else {
			if ( amtOver5PctFl || tcOver5PctFl ) {
				summaryLcatOver5Pct++;
			} else {
				if ( amtWithin5PctFl || tcWithin5PctFl ) {
					summaryLcatWithin5Pct++;
				} else {
					if ( amtWithin1PctFl || tcWithin1PctFl ) {
						summaryLcatWithin1Pct++;
					}
				}
			}
		}
	}

	private void outputSummaryForCorrectness() {

		String num = "";
		String fill = "                                                                                                      ";
		Calendar dt = Calendar.getInstance();

		if ( summaryLastTerrCd.length() > 0 ) {
			if ( summaryRpt.length() == 0 ) {
				summaryRpt.append("                                                   -----------------  Overall  ---------------\n");
				summaryRpt.append("                                                        Sales      Sales      Sales    Missing\n");
				summaryRpt.append("                                          Location     Within     Within       Over      Daily\n");
				summaryRpt.append("Country                     Date             Count  1 Percent  5 Percent  5 Percent      Sales\n");
				summaryRpt.append("-------------------- ------------------ ---------- ---------- ---------- ---------- ----------\n");
			}

			summaryRpt.append((summaryTerr + fill).substring(0, 20));
			summaryRpt.append(" ");

			dt.set(Integer.parseInt(summaryDate.substring(0, 4)), Integer.parseInt(summaryDate.substring(4, 6)) - 1, Integer.parseInt(summaryDate.substring(6, 8)));
			num = dateFormatter.format(dt.getTime());
			if ( num.length() < 18 ) {
				num = fill.substring(0, 18 - num.length()) + num;
			}
			summaryRpt.append(num);
			summaryRpt.append(" ");

			num = numberFormatter.format(summaryLcatCount);
			if ( num.length() < 10 ) {
				num = fill.substring(0, 10 - num.length()) + num;
			}
			summaryRpt.append(num);
			summaryRpt.append(" ");

			num = numberFormatter.format(summaryLcatWithin1Pct);
			if ( num.length() < 10 ) {
				num = fill.substring(0, 10 - num.length()) + num;
			}
			summaryRpt.append(num);
			summaryRpt.append(" ");

			num = numberFormatter.format(summaryLcatWithin5Pct);
			if ( num.length() < 10 ) {
				num = fill.substring(0, 10 - num.length()) + num;
			}
			summaryRpt.append(num);
			summaryRpt.append(" ");

			num = numberFormatter.format(summaryLcatOver5Pct);
			if ( num.length() < 10 ) {
				num = fill.substring(0, 10 - num.length()) + num;
			}
			summaryRpt.append(num);
			summaryRpt.append(" ");
			
			num = numberFormatter.format(summaryLcatMissing);
			if ( num.length() < 10 ) {
				num = fill.substring(0, 10 - num.length()) + num;
			}
			summaryRpt.append(num);
			summaryRpt.append("\n");
		}
	}

	private void addLineExcelForCompletenessSummary(DataQualityCompletenessSummaryLine line) throws Exception {

		if ( !line.terrCd.equals(lastTerrCd) || !lastOpenWorksheet.equals("SUMMARY") ) {
			if ( ws != null ) {
				if ( lastOpenWorksheet.equals("SUMMARY") ) {
					finalizeSheetForCompletenessSummary();
				}

				if ( lastOpenWorksheet.equals("DETAIL") ) {
					finalizeSheetForCompletenessDetail();
				}
			}

			lastTerrCd = line.terrCd;
			lastDate = "@@";

			ws = wb.createSheet("Summary -- " + line.terrCd);
			
			ws.setDisplayGridlines(false);
			ws.setPrintGridlines(false);
			ws.setFitToPage(true);
			ws.setHorizontallyCenter(true);
			PrintSetup printSetup = ws.getPrintSetup();
			printSetup.setLandscape(true);

			//the following three statements are required only for HSSF
			ws.setAutobreaks(true);
			printSetup.setFitHeight((short)1);
			printSetup.setFitWidth((short)1);

			initializeSheetForCompletenessSummary(line.terrCd,line.terrNa,line.owshTypes);
		}

		if ( !line.busnDt.equals(lastDate) ) {
			lastDate = line.busnDt;
			rowIdx++;
			row = ws.createRow(rowIdx);

			cell = row.createCell(0);
			cell.setCellValue(line.busnDt);
		}
		
		cell = row.createCell(line.startColumn);
		cell.setCellValue(line.expectedCount);
		cell.setCellStyle(styles.get("comma"));
		
		cell = row.createCell(line.startColumn+1);
		cell.setCellValue(line.fileCount);
		cell.setCellStyle(styles.get("comma"));
		
		if ( line.expectedCount > 0 ) {
			cell = row.createCell(line.startColumn+2);
			//cell.setCellFormula("IF(B" + (rowIdx+1) + "=0,\"\",C" + (rowIdx+1) + "/B" + (rowIdx+1) + "*100)");
			cell.setCellValue((double)line.fileCount / (double)line.expectedCount * 100);
			cell.setCellStyle(styles.get("commadecimal2"));
		}

		
		cell = row.createCell(line.startColumn+3);
		cell.setCellValue(line.incompleteFileCount);
		cell.setCellStyle(styles.get("comma"));

		if ( line.fileCount > 0 ) {
			cell = row.createCell(line.startColumn+4);
			//cell.setCellFormula("IF(C" + (rowIdx+1) + "=0,\"\",E" + (rowIdx+1) + "/C" + (rowIdx+1) + "*100)");
			cell.setCellValue((double)line.incompleteFileCount / (double)line.fileCount * 100);
			cell.setCellStyle(styles.get("commadecimal2"));
		}
		
	}

	private void addLineExcelForCompletenessDetail(DataQualityCompletenessDetailLine line) throws Exception {

		if ( !line.terrCd.equals(lastTerrCd) || !lastOpenWorksheet.equals("DETAIL") ) {
			if ( ws != null ) {
				if ( lastOpenWorksheet.equals("SUMMARY") ) {
					finalizeSheetForCompletenessSummary();
				}

				if ( lastOpenWorksheet.equals("DETAIL") ) {
					finalizeSheetForCompletenessDetail();
				}
			}
			
			lastTerrCd = line.terrCd;
			lastDate = "@@";
			lastLcat = "@@";
			
			errCount = 0;
			dispCount = 0;

			ws = wb.createSheet("Detail -- " + line.terrCd);
			ws.setDisplayGridlines(false);
			ws.setPrintGridlines(false);
			ws.setFitToPage(true);
			ws.setHorizontallyCenter(true);
			PrintSetup printSetup = ws.getPrintSetup();
			printSetup.setLandscape(true);

			//the following three statements are required only for HSSF
			ws.setAutobreaks(true);
			printSetup.setFitHeight((short)1);
			printSetup.setFitWidth((short)1);

			initializeSheetForCompletenessDetail(line.terrCd,line.terrNa,line.subTypes);
		}

		if ( !line.busnDt.equals(lastDate) || !line.lcat.equals(lastLcat) ) {
			lastDate = line.busnDt;
			lastLcat = line.lcat;
			
			errCount++;
			
			if ( errCount > MAX_ERRORS ) {
				dispCount++;
			} else {
				rowIdx++;
				row = ws.createRow(rowIdx);

				cell = row.createCell(0);
				cell.setCellValue(line.busnDt);
				cell = row.createCell(1);
				cell.setCellValue(line.lcat + " - " + line.lcatNa);
				cell = row.createCell(2);
				cell.setCellValue(line.fileRejectReason);
			}
		}
		
		if ( errCount <= MAX_ERRORS ) {
			int col = 3;
			
			for ( String subType : line.subTypes ) {
				if ( subType.equals(line.subType) ) {
					cell = row.createCell(col);
					cell.setCellValue(line.subTypeRejectReason);
				}
				col++;
			}
		}
		
	}
	
	private void initializeHtml(String title) throws Exception {
		
		htmOut = new BufferedWriter(new FileWriter(outFileNameAndPath));

		htmOut.write("<html>\n");
		htmOut.write("  <head>\n");
		htmOut.write("    <meta charset=\"UTF-8\">\n");
		htmOut.write("    <title>Data Quality - " + title + "</title>\n");
		htmOut.write("    <style>\n");
		htmOut.write("      .hdr1 {font-size:x-large; font-weight:bold; text-align:left;}\n");
		htmOut.write("      .hdr2 {font-size:large; font-weight:bold; text-align:left;}\n");
		htmOut.write("      .rpthdr1 {font-size:x-large; font-weight:bold; text-align:center;}\n");
		htmOut.write("      .rpthdr2 {font-size:small; font-weight:bold; text-align:center;}\n");
		htmOut.write("      .cellboldleft {font-weight:bold; text-align:left;}\n");
		htmOut.write("      .cellboldright {font-weight:bold; text-align:right;}\n");
		htmOut.write("      .cellboldcenter {font-weight:bold; text-align:center;}\n");
		htmOut.write("      .cellright {text-align:right;}\n");
		htmOut.write("      .cellcenter {text-align:center;}\n");
		htmOut.write("      .greenback {text-align:right; background-color:#00FF00;}\n");
		htmOut.write("      .yellowback {text-align:right; background-color:#FFFF00;}\n");
		htmOut.write("      .redback {text-align:right; background-color:#FF0000;}\n");
		htmOut.write("      .greytext {text-align:center; font-weight:bold; color:#C0C0C0;}\n");
		htmOut.write("      .greennote {font-weight:bold; text-align:right; background-color:#00FF00;}\n");
		htmOut.write("      .yellownote {font-weight:bold; text-align:right; background-color:#FFFF00;}\n");
		htmOut.write("      .rednote {font-weight:bold; text-align:right; background-color:#FF0000;}\n");
		htmOut.write("    </style>\n");
		htmOut.write("  </head>\n");
		htmOut.write("  <body>\n");
		htmOut.write("    <p class=\"rpthdr1\">Data Quality - " + title + "</p>\n");
		
	}
	
	private void initializeFormatters() throws Exception {

		numberFormatter = NumberFormat.getNumberInstance(currentLocale);
		amountFormatter = NumberFormat.getNumberInstance(currentLocale);
		percentFormatter = NumberFormat.getPercentInstance(currentLocale);
		dateFormatter = DateFormat.getDateInstance(DateFormat.LONG, currentLocale);
		
		numberFormatter.setMinimumFractionDigits(0);
		numberFormatter.setMaximumFractionDigits(0);
		amountFormatter.setMinimumFractionDigits(2);
		amountFormatter.setMaximumFractionDigits(2);
		percentFormatter.setMinimumFractionDigits(2);
		percentFormatter.setMaximumFractionDigits(2);

	}

	private void initializeSheetForCorrectness(String date
                                              ,String terrCd
                                              ,String terrNa) {

		String title = "";
		Calendar titleDt = Calendar.getInstance();
		titleDt.set(Integer.parseInt(date.substring(0, 4)), Integer.parseInt(date.substring(4,6))-1, Integer.parseInt(date.substring(6, 8)));

		if ( terrNa.length() > 0 ) {
			title = terrNa + " (" + terrCd + ")";
		} else {
			title = "Country Code = " + terrCd;
		}

		row = ws.createRow(HDR_CORRECTNESS_LINE_01);
		cell = row.createCell(0);
		cell.setCellValue(title);
		cell.setCellStyle(styles.get("header_24p_center"));
		ws.addMergedRegion(new CellRangeAddress(HDR_CORRECTNESS_LINE_01,HDR_CORRECTNESS_LINE_01,0,11));

		row = ws.createRow(HDR_CORRECTNESS_LINE_01+1);
		cell = row.createCell(0);
		cell.setCellValue(titleDt.getTime());
		cell.setCellStyle(styles.get("date_20p_center"));
		ws.addMergedRegion(new CellRangeAddress(HDR_CORRECTNESS_LINE_01+1,HDR_CORRECTNESS_LINE_01+1,0,11));

		for (int i=1; i <= 11; i++) {
			cell = row.createCell(i);
			cell.setCellStyle(styles.get("date_20p_center"));
		}
		
		row = ws.createRow(HDR_CORRECTNESS_LINE_02);
		cell = row.createCell(1);
		cell.setCellValue("Sales Amount");
		cell.setCellStyle(styles.get("header_20p_center"));
		ws.addMergedRegion(new CellRangeAddress(HDR_CORRECTNESS_LINE_02,HDR_CORRECTNESS_LINE_02,1,5));
		
		cell = row.createCell(5);
		cell.setCellStyle(styles.get("right_border"));

		cell = row.createCell(6);
		cell.setCellValue("Transactions");
		cell.setCellStyle(styles.get("header_20p_center"));
		ws.addMergedRegion(new CellRangeAddress(HDR_CORRECTNESS_LINE_02,HDR_CORRECTNESS_LINE_02,6,9));

		cell = row.createCell(9);
		cell.setCellStyle(styles.get("right_border"));

		cell = row.createCell(10);
		cell.setCellValue("Overall");
		cell.setCellStyle(styles.get("header_20p_center"));
		ws.addMergedRegion(new CellRangeAddress(HDR_CORRECTNESS_LINE_02,HDR_CORRECTNESS_LINE_02,10,11));

		row = ws.createRow(HDR_CORRECTNESS_LINE_03);
		cell = row.createCell(5);
		cell.setCellStyle(styles.get("right_border"));
		cell = row.createCell(9);
		cell.setCellStyle(styles.get("right_border"));

		row = ws.createRow(HDR_CORRECTNESS_LINE_09);
		cell = row.createCell(5);
		cell.setCellStyle(styles.get("right_border"));
		cell = row.createCell(9);
		cell.setCellStyle(styles.get("right_border"));

		row = ws.createRow(HDR_CORRECTNESS_LINE_10);
		cell = row.createCell(0);
		cell.setCellValue("Location");
		cell.setCellStyle(styles.get("header_14p_left"));

		cell = row.createCell(1);
		cell.setCellValue("Daily Sales     ");
		cell.setCellStyle(styles.get("header_14p_right"));

		cell = row.createCell(2);
		cell.setCellValue("NP6     ");
		cell.setCellStyle(styles.get("header_14p_right"));

		cell = row.createCell(3);
		cell.setCellValue("N/G     ");
		cell.setCellStyle(styles.get("header_14p_right"));

		cell = row.createCell(4);
		cell.setCellValue("Difference %     ");
		cell.setCellStyle(styles.get("header_14p_right"));

		cell = row.createCell(5);
		cell.setCellValue("Type     ");
		cell.setCellStyle(styles.get("header_14p_right_border"));

		cell = row.createCell(6);
		cell.setCellValue("Daily Sales     ");
		cell.setCellStyle(styles.get("header_14p_right"));

		cell = row.createCell(7);
		cell.setCellValue("NP6     ");
		cell.setCellStyle(styles.get("header_14p_right"));

		cell = row.createCell(8);
		cell.setCellValue("Difference %     ");
		cell.setCellStyle(styles.get("header_14p_right"));

		cell = row.createCell(9);
		cell.setCellValue("Type     ");
		cell.setCellStyle(styles.get("header_14p_right_border"));

		cell = row.createCell(10);
		cell.setCellValue("Overall Type     ");
		cell.setCellStyle(styles.get("header_14p_right"));

		rowIdx = HDR_CORRECTNESS_LINE_10+1; 

	}
	
	private void finalizeHeaderForCorrectness() {

		row = ws.createRow(HDR_CORRECTNESS_LINE_03);
        cell = row.createCell(2);
        cell.setCellValue("Count");
		cell.setCellStyle(styles.get("bold_right"));

		cell = row.createCell(3);
        cell.setCellValue("% Total");
		cell.setCellStyle(styles.get("bold_right"));

		cell = row.createCell(5);
		cell.setCellStyle(styles.get("right_border"));
        cell = row.createCell(9);
		cell.setCellStyle(styles.get("right_border"));

		cell = row.createCell(7);
        cell.setCellValue("Count");
		cell.setCellStyle(styles.get("bold_right"));

		cell = row.createCell(8);
        cell.setCellValue("% Total");
		cell.setCellStyle(styles.get("bold_right"));

		cell = row.createCell(10);
        cell.setCellValue("Count");
		cell.setCellStyle(styles.get("bold_right"));

		cell = row.createCell(11);
        cell.setCellValue("% Total");
		cell.setCellStyle(styles.get("bold_right"));

        row = ws.createRow(HDR_CORRECTNESS_LINE_04);
		cell = row.createCell(1);
        cell.setCellValue("All Locations");
		cell.setCellStyle(styles.get("bold_right"));

		cell = row.createCell(2);
        cell.setCellFormula("COUNTA(A" + (HDR_CORRECTNESS_LINE_10+2) + ":A" + (rowIdx) + ")");
		cell.setCellStyle(styles.get("comma_bold"));

		cell = row.createCell(5);
		cell.setCellStyle(styles.get("right_border"));
        cell = row.createCell(9);
		cell.setCellStyle(styles.get("right_border"));

		cell = row.createCell(7);
        cell.setCellFormula("C"+(HDR_CORRECTNESS_LINE_04+1));
		cell.setCellStyle(styles.get("comma_bold"));

		cell = row.createCell(10);
        cell.setCellFormula("C"+(HDR_CORRECTNESS_LINE_04+1));
		cell.setCellStyle(styles.get("comma_bold"));

        row = ws.createRow(HDR_CORRECTNESS_LINE_05);
		cell = row.createCell(1);
        cell.setCellValue("Difference within 1% (T1)");
		cell.setCellStyle(styles.get("bold_right"));

        cell = row.createCell(2);
        cell.setCellFormula("COUNTIF(F" + (HDR_CORRECTNESS_LINE_10+2) + ":F" + (rowIdx) +",\"T1\")");
		cell.setCellStyle(styles.get("comma_green_bold"));
        cell = row.createCell(3);
        cell.setCellFormula("(C" + (HDR_CORRECTNESS_LINE_05+1) + "/C" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));

		cell = row.createCell(5);
		cell.setCellStyle(styles.get("right_border"));
        cell = row.createCell(9);
		cell.setCellStyle(styles.get("right_border"));

        cell = row.createCell(7);
        cell.setCellFormula("COUNTIF(J" + (HDR_CORRECTNESS_LINE_10+2) + ":J" + (rowIdx) +",\"T1\")");
		cell.setCellStyle(styles.get("comma_green_bold"));
        cell = row.createCell(8);
        cell.setCellFormula("(H" + (HDR_CORRECTNESS_LINE_05+1) + "/H" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));

        cell = row.createCell(10);
        cell.setCellFormula("COUNTIF(K" + (HDR_CORRECTNESS_LINE_10+2) + ":K" + (rowIdx) +",\"T1\")");
		cell.setCellStyle(styles.get("comma_green_bold"));
        cell = row.createCell(11);
        cell.setCellFormula("(K" + (HDR_CORRECTNESS_LINE_05+1) + "/K" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));

        row = ws.createRow(HDR_CORRECTNESS_LINE_06);
		cell = row.createCell(1);
        cell.setCellValue("within 5% (T2)");
		cell.setCellStyle(styles.get("bold_right"));

        cell = row.createCell(2);
        cell.setCellFormula("COUNTIF(F" + (HDR_CORRECTNESS_LINE_10+2) + ":F" + (rowIdx) +",\"T2\")");
		cell.setCellStyle(styles.get("comma_yellow_bold"));
        cell = row.createCell(3);
        cell.setCellFormula("(C" + (HDR_CORRECTNESS_LINE_06+1) + "/C" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));

		cell = row.createCell(5);
		cell.setCellStyle(styles.get("right_border"));
        cell = row.createCell(9);
		cell.setCellStyle(styles.get("right_border"));

        cell = row.createCell(7);
        cell.setCellFormula("COUNTIF(J" + (HDR_CORRECTNESS_LINE_10+2) + ":J" + (rowIdx) +",\"T2\")");
		cell.setCellStyle(styles.get("comma_yellow_bold"));
        cell = row.createCell(8);
        cell.setCellFormula("(H" + (HDR_CORRECTNESS_LINE_06+1) + "/H" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));

        cell = row.createCell(10);
        cell.setCellFormula("COUNTIF(K" + (HDR_CORRECTNESS_LINE_10+2) + ":K" + (rowIdx) +",\"T2\")");
		cell.setCellStyle(styles.get("comma_yellow_bold"));
        cell = row.createCell(11);
        cell.setCellFormula("(K" + (HDR_CORRECTNESS_LINE_06+1) + "/K" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));
		
        row = ws.createRow(HDR_CORRECTNESS_LINE_07);
		cell = row.createCell(1);
        cell.setCellValue("over 5% (T3)");
		cell.setCellStyle(styles.get("bold_right"));

        cell = row.createCell(2);
        cell.setCellFormula("COUNTIF(F" + (HDR_CORRECTNESS_LINE_10+2) + ":F" + (rowIdx) +",\"T3\")");
		cell.setCellStyle(styles.get("comma_red_bold"));
        cell = row.createCell(3);
        cell.setCellFormula("(C" + (HDR_CORRECTNESS_LINE_07+1) + "/C" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));

		cell = row.createCell(5);
		cell.setCellStyle(styles.get("right_border"));
        cell = row.createCell(9);
		cell.setCellStyle(styles.get("right_border"));

        cell = row.createCell(7);
        cell.setCellFormula("COUNTIF(J" + (HDR_CORRECTNESS_LINE_10+2) + ":J" + (rowIdx) +",\"T3\")");
		cell.setCellStyle(styles.get("comma_red_bold"));
        cell = row.createCell(8);
        cell.setCellFormula("(H" + (HDR_CORRECTNESS_LINE_07+1) + "/H" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));

        cell = row.createCell(10);
        cell.setCellFormula("COUNTIF(K" + (HDR_CORRECTNESS_LINE_10+2) + ":K" + (rowIdx) +",\"T3\")");
		cell.setCellStyle(styles.get("comma_red_bold"));
        cell = row.createCell(11);
        cell.setCellFormula("(K" + (HDR_CORRECTNESS_LINE_07+1) + "/K" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));
		
        row = ws.createRow(HDR_CORRECTNESS_LINE_08);
		cell = row.createCell(1);
        cell.setCellValue("Missing Daily Sales (T0)");
		cell.setCellStyle(styles.get("bold_right"));

        cell = row.createCell(2);
        cell.setCellFormula("COUNTIF(F" + (HDR_CORRECTNESS_LINE_10+2) + ":F" + (rowIdx) +",\"T0\")");
		cell.setCellStyle(styles.get("comma_na_bold"));
        cell = row.createCell(3);
        cell.setCellFormula("(C" + (HDR_CORRECTNESS_LINE_08+1) + "/C" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));

		cell = row.createCell(5);
		cell.setCellStyle(styles.get("right_border"));
        cell = row.createCell(9);
		cell.setCellStyle(styles.get("right_border"));

        cell = row.createCell(7);
        cell.setCellFormula("COUNTIF(J" + (HDR_CORRECTNESS_LINE_10+2) + ":J" + (rowIdx) +",\"T0\")");
		cell.setCellStyle(styles.get("comma_na_bold"));
        cell = row.createCell(8);
        cell.setCellFormula("(H" + (HDR_CORRECTNESS_LINE_08+1) + "/H" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));

        cell = row.createCell(10);
        cell.setCellFormula("COUNTIF(K" + (HDR_CORRECTNESS_LINE_10+2) + ":K" + (rowIdx) +",\"T0\")");
		cell.setCellStyle(styles.get("comma_na_bold"));
        cell = row.createCell(11);
        cell.setCellFormula("(K" + (HDR_CORRECTNESS_LINE_08+1) + "/K" + (HDR_CORRECTNESS_LINE_04+1) + ")*100");
		cell.setCellStyle(styles.get("decimal2_bold"));
		
	}
	
	private void finalizeSheetForCorrectness() {

		ws.createFreezePane(1, HDR_CORRECTNESS_LINE_10+1);
        ws.setAutoFilter(new CellRangeAddress(HDR_CORRECTNESS_LINE_10,HDR_CORRECTNESS_LINE_10,0,10));
        
        for (int col=0; col <= 10; col++ ) {
            ws.autoSizeColumn(col);
        }
	}

	private void initializeSheetForCompletenessSummary(String terrCd
                                                      ,String terrNa
                                                      ,ArrayList<String> owshTypes) {

		String title = "";

		lastOpenWorksheet = "SUMMARY";
		
		if ( terrNa.length() > 0 ) {
			title = terrNa + " (" + terrCd + ")";
		} else {
			title = "Country Code = " + terrCd;
		}

		row = ws.createRow(HDR_COMPLETENESS_SUMMARY_LINE_01);
		cell = row.createCell(0);
		cell.setCellValue(title);
		cell.setCellStyle(styles.get("header_24p_center"));

		String[] parts;
		String lastType = "@";

		row = ws.createRow(HDR_COMPLETENESS_SUMMARY_LINE_02);
		Row row2 = ws.createRow(HDR_COMPLETENESS_SUMMARY_LINE_03);
		Row row3 = ws.createRow(HDR_COMPLETENESS_SUMMARY_LINE_04);
		
		int currCol = 1;
		int endCol = -1;

		cell = row3.createCell(0);
		cell.setCellValue("Date     ");
		cell.setCellStyle(styles.get("header_14p_left"));

		for ( String owshType : owshTypes ) {
			parts = owshType.split("\\|");
			
			if ( !parts[0].equals(lastType) ) {
				lastType = parts[0];

				if ( endCol > 0 ) {
					ws.addMergedRegion(new CellRangeAddress(HDR_COMPLETENESS_SUMMARY_LINE_02,HDR_COMPLETENESS_SUMMARY_LINE_02,endCol,currCol-1));
				}

				endCol = currCol;

				cell = row.createCell(currCol);
				cell.setCellValue(parts[0]);
				cell.setCellStyle(styles.get("header_14p_center"));
			}
			
			cell = row2.createCell(currCol);
			cell.setCellValue(parts[1]);
			cell.setCellStyle(styles.get("header_14p_center_wrap"));
			row2.setHeight((short)900);
			ws.addMergedRegion(new CellRangeAddress(HDR_COMPLETENESS_SUMMARY_LINE_03,HDR_COMPLETENESS_SUMMARY_LINE_03,currCol,currCol+4));

			cell = row3.createCell(currCol);
			cell.setCellValue("Expected");
			cell.setCellStyle(styles.get("header_14p_rotate"));

			cell = row3.createCell(currCol+1);
			cell.setCellValue("File Count");
			cell.setCellStyle(styles.get("header_14p_rotate"));

			cell = row3.createCell(currCol+2);
			cell.setCellValue("% Expected");
			cell.setCellStyle(styles.get("header_14p_rotate"));

			cell = row3.createCell(currCol+3);
			cell.setCellValue("Incomplete File Count");
			cell.setCellStyle(styles.get("header_14p_rotate"));

			cell = row3.createCell(currCol+4);
			cell.setCellValue("% Incomplete");
			cell.setCellStyle(styles.get("header_14p_rotate"));
			
			currCol+=5;
		}

		lastCol=currCol;
		
		if ( endCol > 0 ) {
			ws.addMergedRegion(new CellRangeAddress(HDR_COMPLETENESS_SUMMARY_LINE_02,HDR_COMPLETENESS_SUMMARY_LINE_02,endCol,currCol-1));
		}

		ws.addMergedRegion(new CellRangeAddress(HDR_COMPLETENESS_SUMMARY_LINE_01,HDR_COMPLETENESS_SUMMARY_LINE_01,0,lastCol-1));

		rowIdx = HDR_COMPLETENESS_SUMMARY_LINE_04; 

	}

	private void finalizeSheetForCompletenessSummary() {

		ws.createFreezePane(1, HDR_COMPLETENESS_SUMMARY_LINE_04+1);
        ws.setAutoFilter(new CellRangeAddress(HDR_COMPLETENESS_SUMMARY_LINE_04,HDR_COMPLETENESS_SUMMARY_LINE_04,0,lastCol-1));

        ws.autoSizeColumn(0);
        
        for (int col=1; col <= lastCol; col++ ) {
        	ws.setColumnWidth(col, 2500);
        }
	}

	private void initializeSheetForCompletenessDetail(String terrCd
                                                     ,String terrNa
                                                     ,ArrayList<String> subTypes) {

		String title = "";

		lastOpenWorksheet = "DETAIL";
		
		if ( terrNa.length() > 0 ) {
			title = terrNa + " (" + terrCd + ")";
		} else {
			title = "Country Code = " + terrCd;
		}

		row = ws.createRow(HDR_COMPLETENESS_DETAIL_LINE_01);
		cell = row.createCell(0);
		cell.setCellValue(title);
		cell.setCellStyle(styles.get("header_24p_center"));

		row = ws.createRow(HDR_COMPLETENESS_DETAIL_LINE_02);
		
		int currCol = 3;

		cell = row.createCell(0);
		cell.setCellValue("Date     ");
		cell.setCellStyle(styles.get("header_14p_left"));

		cell = row.createCell(1);
		cell.setCellValue("Location     ");
		cell.setCellStyle(styles.get("header_14p_left"));

		cell = row.createCell(2);
		cell.setCellValue("Overall     ");
		cell.setCellStyle(styles.get("header_14p_center"));

		for ( String subType : subTypes ) {
			cell = row.createCell(currCol);
			cell.setCellValue(subType + "     ");
			cell.setCellStyle(styles.get("header_14p_center"));
			
			currCol++;
		}
		
		lastCol = currCol-1;
		
		ws.addMergedRegion(new CellRangeAddress(HDR_COMPLETENESS_DETAIL_LINE_01,HDR_COMPLETENESS_DETAIL_LINE_01,0,lastCol));

		rowIdx = HDR_COMPLETENESS_DETAIL_LINE_02; 

	}

	private void finalizeSheetForCompletenessDetail() {

		ws.createFreezePane(1, HDR_COMPLETENESS_DETAIL_LINE_02+1);
        ws.setAutoFilter(new CellRangeAddress(HDR_COMPLETENESS_DETAIL_LINE_02,HDR_COMPLETENESS_DETAIL_LINE_02,0,lastCol));
        
        for (int col=0; col <= lastCol+1; col++ ) {
            ws.autoSizeColumn(col);
        }
        
        if ( dispCount > 0 ) {
			row = ws.createRow(rowIdx+1);

			cell = row.createCell(1);
			cell.setCellValue("Plus " + dispCount + " additional errors");
			cell.setCellStyle(styles.get("red_bold"));
        }
	}
	
    private static Map<String, CellStyle> createStyles(Workbook wb) {
    	
        Map<String, CellStyle> styles = new HashMap<String, CellStyle>();
        
        DataFormat df = wb.createDataFormat();
        CellStyle style;

        Font font0 = wb.createFont();
        font0.setBoldweight(Font.BOLDWEIGHT_BOLD);
        font0.setFontHeightInPoints((short)24);

        Font font1 = wb.createFont();
        font1.setBoldweight(Font.BOLDWEIGHT_BOLD);
        font1.setFontHeightInPoints((short)20);

        Font font2 = wb.createFont();
        font2.setBoldweight(Font.BOLDWEIGHT_BOLD);
        font2.setFontHeightInPoints((short)14);

        Font font3 = wb.createFont();
        font3.setBoldweight(Font.BOLDWEIGHT_BOLD);

        style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
        style.setFillForegroundColor(IndexedColors.GREY_40_PERCENT.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setFont(font3);
        styles.put("na", style);

		style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setFont(font0);
        styles.put("header_24p_center", style);

		style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setFont(font1);
        styles.put("header_20p_center", style);

        style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setDataFormat(df.getFormat("d-mmmm-yyyy"));
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setFont(font1);
        styles.put("date_20p_center", style);

		style= wb.createCellStyle();
		style.setBorderRight(CellStyle.BORDER_THIN);
        styles.put("right_border", style);

		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setFont(font2);
        styles.put("header_14p_center", style);

		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setWrapText(true);
		style.setFont(font2);
        styles.put("header_14p_center_wrap", style);

		style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_RIGHT);
		style.setFont(font2);
        styles.put("header_14p_right", style);

		style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setRotation((short)90);
		style.setFont(font2);
        styles.put("header_14p_rotate", style);

		style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_RIGHT);
		style.setBorderRight(CellStyle.BORDER_THIN);
		style.setFont(font2);
        styles.put("header_14p_right_border", style);

		style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_RIGHT);
		style.setFont(font3);
        styles.put("bold_right", style);

		style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_LEFT);
		style.setFont(font2);
        styles.put("header_14p_left", style);

		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("0.00"));
		style.setFont(font3);
        styles.put("decimal2_bold", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("#,##0.00"));
		style.setFont(font3);
        styles.put("commadecimal2_bold", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("#,##0"));
		style.setFont(font3);
        styles.put("comma_bold", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("#,##0"));
        style.setFillForegroundColor(IndexedColors.GREEN.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setFont(font3);
        styles.put("comma_green_bold", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("#,##0"));
        style.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setFont(font3);
        styles.put("comma_yellow_bold", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("#,##0"));
        style.setFillForegroundColor(IndexedColors.RED.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setFont(font3);
        styles.put("comma_red_bold", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("#,##0"));
        style.setFillForegroundColor(IndexedColors.GREY_40_PERCENT.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setFont(font3);
        styles.put("comma_na_bold", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("0.00"));
        styles.put("decimal2", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("0.00"));
        style.setFillForegroundColor(IndexedColors.GREEN.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        styles.put("decimal2_green", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("0.00"));
        style.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        styles.put("decimal2_yellow", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("0.00"));
        style.setFillForegroundColor(IndexedColors.RED.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        styles.put("decimal2_red", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("#,##0.00"));
        styles.put("commadecimal2", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(df.getFormat("#,##0"));
        styles.put("comma", style);

		style = wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setBorderRight(CellStyle.BORDER_THIN);
        styles.put("center_border", style);

		style= wb.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
        styles.put("center", style);

		style = wb.createCellStyle();
        style.setFillForegroundColor(IndexedColors.RED.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		style.setFont(font3);
        styles.put("red_bold", style);

        return(styles);
    }
}
