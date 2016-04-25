import java.util.HashMap;

import org.json.XML;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.mortbay.util.ajax.JSON;


public class TestJSONOutput {

	public static void main(String[] args){
		JSONObject jsonObj = new JSONObject();
		
		JSONArray jarr = new JSONArray();
		JSONArray jarr1 = new JSONArray();
		
		JSONArray jarr_itemid 	   = new JSONArray();
		JSONArray jarr_itemid_vals = new JSONArray();
		
		jarr.clear();
		jarr.add("order_id");
		jarr_itemid.clear();
		jarr_itemid.add("item_id");
//		jarr.add("item_id");
		jarr.add((Object)jarr_itemid);
		jarr.add("timestamp");
		jarr.add("order_total");
		
		
		
		
		
		
//		jarr.add("1,2,2014-05-04T12:22:22Z,22.21");
//		jarr.add("1,3,2014-05-04T12:22:22Z,22.22");
//		jarr.add("1,4,2014-05-04T12:22:22Z,22.23");
		
		Integer temp = null;
		jarr1.add(1);
		jarr_itemid_vals.clear();
		jarr_itemid_vals.add(temp);
		jarr_itemid_vals.add(3);
		jarr_itemid_vals.add(4);
		jarr1.add((Object)jarr_itemid_vals);
		jarr1.add("2014-05-04T12:22:22Z");
		jarr1.add(22.2);
		
		jsonObj.put("data",(Object)jarr1);
		jsonObj.put("columns",(Object)jarr);
		
		System.out.println(jsonObj.toString());
		
		System.out.println(JSON.toString(jarr1));
		
		
//		System.out.println(XML.toString(jsonObj));
		
	}
}
