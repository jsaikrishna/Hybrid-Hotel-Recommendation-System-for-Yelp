package social.social;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.json.*;
import com.google.gson.Gson;


public class FriendsGraph implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3741134064404538431L;
	HashMap<String, HashSet<String>> map;
//	FriendsGraph(){
//			map = new HashMap<String, HashSet<String>>();
//	}
	public void addEdge(String source, String destination) {
		if(map.containsKey(source)) {
			HashSet<String> key_present_set = map.get(source);
			key_present_set.add(destination);
			map.put(source, key_present_set);
		}
		else {
			HashSet<String> key_notpresent_set = new HashSet<String>();
			key_notpresent_set.add(destination);
			map.put(source, key_notpresent_set);
		}
		displayGraph();
	}
	
	@SuppressWarnings("rawtypes")
	public void displayGraph() {
		System.out.println("In displayGraph");
			for(Map.Entry m: map.entrySet()) {
				System.out.println("In EntrySet");
				System.out.println(m.getKey() + "->->->->" + m.getValue());
			}
		}
	
	HashMap<String, HashSet<String>> modifiedMap= new HashMap<String,HashSet<String>>();
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void levelAdd() {
		for(Map.Entry m: map.entrySet()){
			HashSet<String> modifiedSet = new HashSet<String>();
			HashSet<String> mSet = (HashSet<String>) m.getValue();
			modifiedSet.addAll(mSet);
			for(String fr: mSet) {
				if(fr != "None" && map.containsKey(fr)) {
					modifiedSet.addAll(map.get(fr));
				}
			}
			modifiedMap.put((String) m.getKey(), modifiedSet);
		}
	}
	Map<String, Object> finalMap = new HashMap<String,Object>();
	public void convertToString() {
		for(Map.Entry mm: modifiedMap.entrySet()) {
			finalMap.put((String) mm.getKey(),mm.getValue().toString());
		}
	}
	public void toJson() throws IOException {
		Gson gson = new Gson();
        String json = gson.toJson(finalMap);
        //System.out.println("json = " + json);
        JSONObject jsonObj = new JSONObject(finalMap);
        FileWriter fileWriter = new FileWriter("/Users/saikrishna/studies/datacenterscalecomputing/project/social/src/main/java/social/social/friends.json");
        
        //fileWriter.write(jsonObj.toString());
        fileWriter.write(json);
        fileWriter.close();
	}
	public static void main(String[] args) throws IOException {
		// Dummy Data for Internal Checking
//		FriendsGraph fg = new FriendsGraph();
//		fg.addEdge("ab", "cd");
//		fg.addEdge("cd", "de");
//		fg.addEdge("bc", "bcd");
//		fg.addEdge("bc", "cad");
//		fg.addEdge("cad", "cade");
//		fg.addEdge("ab", "cfgd");
//		fg.addEdge("cfgd", "cfgde");
//		fg.levelAdd();
//		fg.convertToString();
//		fg.displayGraph();
//		fg.toJson();
		
	}
	
}

