import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class MapComparison {

	
	
	public static void main(String[] args) {
		HashMap<String, ArrayList<String>> map1 = new HashMap<>();
		ArrayList<String> list = new ArrayList<>();
	    list.add("a");
	    list.add("b");
	    list.add("c");
	    map1.put("sam", list);
	    
	    HashMap<String, ArrayList<String>> map2 = new HashMap<>();
	    ArrayList<String> list2 = new ArrayList<>();
	    list2.add("a");
	    list2.add("b");
	    list2.add("c");
	    map2.put("sam", list);
	    
	    ArrayList commonValues=new ArrayList();
	    HashMap<String, List<String>> map3 = new HashMap<>();
	   for(String key:map1.keySet())
	   {
		   if(map2.containsKey(key))
		   {
			   ArrayList listofMap1=map1.get(key);
			   ArrayList listofMap2=map2.get(key);
			  ArrayList commonList=new ArrayList();
			  
			  commonList.add(listofMap1);
			  commonList.add(listofMap2);
			  map3.put(key, commonList);
			  
			   
		   }
	   }
	   
	   System.out.println("Printing values from map 3 to get common values");
	   
	   for(String key:map3.keySet())
	   {
		   String concatenateList=listToString(map3.get(key));
		   System.out.println("List of Values for"+key+":"+concatenateList);
	   }
	   
			
	}
	
	public static String listToString(List<String> list)
	{
		String str = null;
		for (String s : list)
		{
		   str = str + s + "," ;
		}
		return str;
	}
	
}
