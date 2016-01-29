package mmscJob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
//import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




/**
 * Class Name: MMCSMRMigrationMapper
 * Mapper Class
 * @author harisyam_s
 * 
 */	

public class MMCSSummaryUploadsDownloadsMapper extends Mapper <LongWritable, Text, Text,LongWritable>{

	// HashMap declared as Class variable
	private static Map <String,Long>storageMap = new HashMap<String, Long>();
	private static Map <String,Long>storageMapForPut = new HashMap<String, Long>();
	private static Map <String,Long>storageMapForGet = new HashMap<String, Long>();
	private static Map <String,Long>storageMapForDelete = new HashMap<String, Long>();

	private static final String GET_EVENT_TYPE_DIM_CODE	= "GETCMP";
	private static final String PUT_EVENT_TYPE_DIM_CODE	= "PUTCMP";
	private static final String DELETE_EVENT_TYPE_DIM_CODE	= "DELETECMP";
	private static final ArrayList<String> respCodeList = new ArrayList<String>(Arrays.asList("200","330","400","401","403","412","500","502","503"));  
	private static String AUTHORIZE_PUT="AuthorizePut";
	private static String PUT_COMPLETE="putComplete";
	private static String AUTH_GET_FOR_FILES="AuthorizeGetForFiles";
	private static String GET_COMPLETE="getComplete";
	private static String DELETE_COMPLETE="deleteComplete";
	private static String MEDIA_STREAM="com.apple.Dataclass.MediaStream";
	private static String UBIQUITY="com.apple.Dataclass.Ubiquity";
	private static String SHARED_STREAMS="com.apple.Dataclass.SharedStreams";
	private static String BACKUP="com.apple.Dataclass.Backup";
	private static String CONTACTS="com.apple.Dataclass.Contacts";
	private static String MESSENGER="com.apple.Dataclass.Messenger";
	private static String CALENDARS="com.apple.Dataclass.Calendars";
	private static String CLOUDKIT="com.apple.Dataclass.CloudKit";
	//Declared as class variables to hold the tokens
	private static String event_time_hour;
	private static String prsId ;
	private static String data_Class ;
	private static String event_Type ;
	private static String http_response_code ;
	private static String no_of_containers ;
	private static String no_of_storage_hosts ;
	private static String total_hits ;;
	private static String eventTypeDimCode ;
	private static String containerList;
	private static String storageHostList;

	/**
	 * Method Name: map
	 * @author harisyam_s
	 * 
	 */	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//Initializing the variables
		String line = value.toString();
		String [] myTokens = line.split("\\t");

		// Verify whether the token length is equal to the required no of fields
		if(myTokens.length>=22){
			//Get all the required fields
			event_time_hour = myTokens[0];
			prsId = myTokens[1];
			data_Class = myTokens[6];
			event_Type = myTokens[4];
			http_response_code = myTokens[5];
			no_of_containers = myTokens[15];
			no_of_storage_hosts = myTokens[16];
			total_hits = myTokens[21];
			eventTypeDimCode = "null";
			//clientInfo = myTokens[3];
			containerList = myTokens[7];
			storageHostList = myTokens[8];

			if(event_time_hour!= null && event_time_hour.length()>0 && prsId != null && prsId.length()>0 && data_Class != null && event_Type !=null ){
				// Filter the event types -- Start
				if(event_Type.equals(AUTHORIZE_PUT)){
					eventTypeDimCode="AUTHPUT";
				}
				else if(event_Type.equals(PUT_COMPLETE)){
					eventTypeDimCode="PUTCOMP";
				}
				else if(event_Type.equals(AUTH_GET_FOR_FILES)){
					eventTypeDimCode="AUTHGETFILES";
				}
				else if(event_Type.equals(GET_COMPLETE)){
					eventTypeDimCode="GETCOMP";
				}
				else if(event_Type.equals(DELETE_COMPLETE)){
					eventTypeDimCode="DELETECOMP";
				}
				//Calling the method to generate the DimCodes
				if(eventTypeDimCode!=null && !(eventTypeDimCode.equals("DELETECOMP")))

				{  
					setDataAndResponse(data_Class,http_response_code,no_of_containers,no_of_storage_hosts,total_hits,eventTypeDimCode);

				}
				// Added for merging vendor response dim code generation part
				if(event_Type.equals(PUT_COMPLETE)){

					long totalHits=0;
					try { 
						totalHits = Long.parseLong(total_hits); 
					}  
					catch (NumberFormatException e) {

					}

					if ((event_Type != null) && (event_Type.length() > 0)	&& (event_Type.equals(PUT_COMPLETE)) && (totalHits > 0)) {
						if ((data_Class != null) && (data_Class.length() > 0) && (containerList != null) && (containerList.length() > 0)) {
							try{
								setDataForPutComplete(totalHits);
							}
							catch(Exception e){

								long hMapValueTot=0;
								if(storageMapForPut.get("MMCS.PUTCMP.BAD.RAW.RECORD.CONT.CNT") == null){
									hMapValueTot=0;
								}
								else{
									hMapValueTot=storageMapForPut.get("MMCS.PUTCMP.BAD.RAW.RECORD.CONT.CNT");
								}
								//Sum up the Values
								hMapValueTot=hMapValueTot+1;
								storageMapForPut.put("MMCS.PUTCMP.BAD.RAW.RECORD.CONT.CNT", hMapValueTot);

							}

						}
					}
				}

				if(event_Type.equals(DELETE_COMPLETE)){

					long totalHits=0;
					try { 
						totalHits = Long.parseLong(total_hits); 
					}  
					catch (NumberFormatException e) {

					}

					if ((event_Type != null) && (event_Type.length() > 0)	&& (event_Type.equals(DELETE_COMPLETE)) && (totalHits > 0)) {
						if ((data_Class != null) && (data_Class.length() > 0) && (containerList != null) && (containerList.length() > 0)) {
							try{
								setDataForDeleteComplete(totalHits);
							}
							catch(Exception e){

								long hMapValueTot=0;
								if(storageMapForDelete.get("MMCS.DELETECMP.BAD.RAW.RECORD.CONT.CNT") == null){
									hMapValueTot=0;
								}
								else{
									hMapValueTot=storageMapForDelete.get("MMCS.DELETECMP.BAD.RAW.RECORD.CONT.CNT");
								}
								//Sum up the Values
								hMapValueTot=hMapValueTot+1;
								storageMapForDelete.put("MMCS.DELETECMP.BAD.RAW.RECORD.CONT.CNT", hMapValueTot);

							}

						}
					}
				}

				if(event_Type.equals(GET_COMPLETE)){

					long totalHits=0;
					try {
						totalHits = Long.parseLong(total_hits); 
					} 
					catch (NumberFormatException e) {

					}


					if ((event_Type != null) && (event_Type.length() > 0)	&& (event_Type.equals(GET_COMPLETE) && (totalHits > 0))) {
						if ((data_Class != null) && (data_Class.length() > 0) && (storageHostList != null) && (storageHostList.length() > 0)) {							
							try{

								setDataForGetComplete(totalHits);
							}
							catch(Exception e){
								long hMapValueTot=0;
								if(storageMapForGet.get("MMCS.GETCMP.BAD.RAW.RECORD.CONT.CNT") == null){
									hMapValueTot=0;
								}
								else{
									hMapValueTot=storageMapForGet.get("MMCS.GETCMP.BAD.RAW.RECORD.CONT.CNT");
								}
								//Sum up the Values
								hMapValueTot=hMapValueTot+1;
								storageMapForGet.put("MMCS.GETCMP.BAD.RAW.RECORD.CONT.CNT", hMapValueTot);
							}

						}
					}
				}

			}

		}

	}

	/*
	 * Newly added method for storage expansion logic
	 */
	public static void setDataForGetComplete(long totalHits) throws IOException, InterruptedException
	{
		String[] storageHostListValues = storageHostList.split(":");
		for(String currentStorageHostListValue : storageHostListValues) {
			String[] currentStorageHostListArr = currentStorageHostListValue.split(",");
			int length = currentStorageHostListArr.length;
			String[] valuesInStorageHostList = new String[6];
			for(int i=0;i<6;i++) {
				String currentValue = null;
				if(i<length) {
					currentValue = currentStorageHostListArr[i];
				} else {
					currentValue = "";
				}
				valuesInStorageHostList[i] = currentValue;
			}

			String firstFieldValue = valuesInStorageHostList[0];

			/* Migrated UDF logic as it is. There was a changes in storage host list structure in between ans so two type record handling */
			if (firstFieldValue.length() > 0) {

				boolean newRecordType = true;
				Long containerSize = new Long(0);
				try {
					containerSize = Long.parseLong(firstFieldValue);
				} catch (NumberFormatException e) {
					newRecordType = false;
				}


				Long vendorResponseCode = new Long(0);


				if (newRecordType) {

					try { vendorResponseCode = Long.parseLong(valuesInStorageHostList[3]); } catch (NumberFormatException e) {}

				} else {

					try { vendorResponseCode = Long.parseLong(valuesInStorageHostList[2]); } catch (NumberFormatException e) {}

				}

				if(event_time_hour!= null && event_time_hour.length()>0 && prsId != null && prsId.length()>0 && event_Type!=null){
					setDataAndResponseForGet(data_Class,vendorResponseCode,total_hits);
				}
			}
		}
	}

	public static void setDataForDeleteComplete(long totalHits)
	{
		String[] containerListValues = containerList.split(":");
		for(String currentContainerListValue : containerListValues) {

			int counter = currentContainerListValue.replaceAll("[^,]","").length()+1;

			String[] containerListValuesTemp = currentContainerListValue.split(",");
			String[] valuesInContainerList = new String[counter];
			int currentIndex = 0;
			for(String containerListValue : containerListValuesTemp) {
				valuesInContainerList[currentIndex] = containerListValue;
				currentIndex++;
			}

			long vendorResponseCode = 0;

			try { vendorResponseCode = Long.parseLong(valuesInContainerList[6]); } catch (NumberFormatException e) { }

			if(event_time_hour!= null && event_time_hour.length()>0 && prsId != null && prsId.length()>0 && event_Type!=null){
				setDataAndResponseForDelete(data_Class,vendorResponseCode,total_hits);
			}

		}
	}

	public static void setDataAndResponseForDelete(String dataClass, long vendCode, String total_hits) {

		//Declare the required variables

		long aggrTotalHits = 0L;
		if(total_hits!=null){
			aggrTotalHits=Long.parseLong(total_hits);
		}

		//Verify the total hits field
		//Declare the required variables
		long hMapValueTot = 0L;
		String hmapKeyTot = null;
		//Use String Builder to append the required DIMCODES
		if(vendCode>0){
			if(vendCode > 201 && vendCode<300){
				hmapKeyTot = "MMCS."+DELETE_EVENT_TYPE_DIM_CODE+".SUCC.OTH.CONT.CNT";
			}
			else if(vendCode >299 && vendCode != 400 && vendCode != 500 && vendCode != 403 && vendCode != 503 && vendCode != 404){

				hmapKeyTot = "MMCS."+DELETE_EVENT_TYPE_DIM_CODE+".FAIL.OTH.CONT.CNT";
			}
			else if (vendCode == 200 || vendCode == 201 || vendCode == 400 || vendCode == 500 || vendCode == 403 || vendCode == 503 || vendCode == 404){
				hmapKeyTot = "MMCS."+DELETE_EVENT_TYPE_DIM_CODE+"."+vendCode+".CONT.CNT";
			}

			else{
				hmapKeyTot = "MMCS."+DELETE_EVENT_TYPE_DIM_CODE+".OTH.CONT.CNT";
			}

		}
		else{
			hmapKeyTot = "MMCS."+DELETE_EVENT_TYPE_DIM_CODE+".OTH.CONT.CNT";
		}
		//Verifying whether any 
		if(storageMapForDelete.get(hmapKeyTot) == null){
			hMapValueTot=0;
		}
		else{
			hMapValueTot=storageMapForDelete.get(hmapKeyTot);
		}
		//Sum up the Values
		hMapValueTot=hMapValueTot+aggrTotalHits;
		//Storing the required DimCodes for Summary Report to the hash map 
		storageMapForDelete.put(hmapKeyTot, hMapValueTot);
	}
	public static void setDataAndResponseForGet(String dataClass, long vendCode, String total_hits) {

		//Declare the required variables

		long aggrTotalHits = 0L;
		if(total_hits!=null){
			aggrTotalHits=Long.parseLong(total_hits);
		}

		//Verify the total hits field
		//Declare the required variables
		long hMapValueTot = 0L;
		String hmapKeyTot = null;

		//Use String Builder to append the required DIMCODES
		if(vendCode>0){
			if(vendCode > 201 && vendCode<300 && vendCode != 206){
				hmapKeyTot = "MMCS."+GET_EVENT_TYPE_DIM_CODE+".SUCC.OTH.CONT.CNT";
			}
			else if(vendCode >299 && vendCode != 400 && vendCode != 407 && vendCode != 500 && vendCode != 403 && vendCode != 503 && vendCode != 404){
				hmapKeyTot = "MMCS."+GET_EVENT_TYPE_DIM_CODE+".FAIL.OTH.CONT.CNT";
			}
			else if (vendCode == 200 || vendCode == 206  || vendCode == 201 || vendCode == 400 || vendCode == 407 || vendCode == 500 || vendCode == 403 || vendCode == 503 || vendCode == 404){
				hmapKeyTot = "MMCS."+GET_EVENT_TYPE_DIM_CODE+"."+vendCode+".CONT.CNT";
			}
			else
			{
				hmapKeyTot = "MMCS."+GET_EVENT_TYPE_DIM_CODE+".OTH.CONT.CNT";
			}
		}
		else{

			hmapKeyTot = "MMCS."+GET_EVENT_TYPE_DIM_CODE+".OTH.CONT.CNT";
		}
		//Verifying whether any 
		if(storageMapForGet.get(hmapKeyTot) == null){
			hMapValueTot=0;
		}
		else{
			hMapValueTot=storageMapForGet.get(hmapKeyTot);
		}
		//Sum up the Values
		hMapValueTot=hMapValueTot+aggrTotalHits;
		//Storing the required DimCodes for Summary Report to the hash map 
		storageMapForGet.put(hmapKeyTot, hMapValueTot);
	}
	/*
	 * Newly added method for container expansion logic
	 */
	public static void setDataForPutComplete(long totalHits) throws IOException, InterruptedException
	{

		String[] containerListValues = containerList.split(":");
		for(String currentContainerListValue : containerListValues) {

			int counter = currentContainerListValue.replaceAll("[^,]","").length()+1;

			String[] containerListValuesTemp = currentContainerListValue.split(",");
			String[] valuesInContainerList = new String[counter];
			int currentIndex = 0;
			for(String containerListValue : containerListValuesTemp) {
				valuesInContainerList[currentIndex] = containerListValue;
				currentIndex++;
			}

			long vendorResponseCode = 0;

			try { vendorResponseCode = Long.parseLong(valuesInContainerList[6]); } catch (NumberFormatException e) { }

			if(event_time_hour!= null && event_time_hour.length()>0 && prsId != null && prsId.length()>0 && event_Type!=null){
				setDataAndResponseForPut(data_Class,vendorResponseCode,total_hits);
			}

		}

	}
	public static void setDataAndResponse(String data_Class, String http_response_code, String no_of_containers , String no_of_storage_hosts, String total_hits,String eventTypeDim) {

		//Declare the required variables
		String dataClassKey = "null";
		String eventTypeKey = null;
		String dimEventKey = null;
		String typeStrCnt = null;
		long aggrTotalHits = 0;
		long aggrCnt =0 ;
		long aggrContainers = 0;
		long aggrStorageHosts = 0;
		//Verify the total hits field
		if(total_hits == null || total_hits.trim().equals("")){
			aggrTotalHits=0;
		}
		else{
			//Check whether the total hits field is type double 
			if(total_hits.contains(".")){
				total_hits=splitString(total_hits);
			}
			try{
				aggrTotalHits = Long.parseLong(total_hits);
			}
			catch(NumberFormatException e){
				aggrTotalHits=0;
			}
		}
		//Verify the no_of_containers field
		if(no_of_containers ==null || no_of_containers.trim().equals("")){
			aggrContainers=0;
		}
		else{
			//Check whether the no_of_containers field is type double 
			if(no_of_containers.contains(".")){
				no_of_containers=splitString(no_of_containers);
			}
			try{
				aggrContainers=Long.parseLong(no_of_containers);
			}
			catch(NumberFormatException e){
				aggrContainers=0;
			}
		}
		//Verify the no_of_storage_hosts field


		if(no_of_storage_hosts == null || no_of_storage_hosts.trim().equals("")){
			aggrStorageHosts=0;
		}
		else{
			//Check whether the no_of_storage_hosts field is type double
			if(no_of_storage_hosts.contains(".")){
				no_of_storage_hosts=splitString(no_of_storage_hosts);
			}
			try{
				aggrStorageHosts=Long.parseLong(no_of_storage_hosts);
			}
			catch(NumberFormatException e){
				aggrStorageHosts=0;
			}
		}

		/*Verifying the data class and setting up the identifier for each Event Type -- Start*/
		if(eventTypeDim.equals("AUTHPUT")){
			eventTypeKey="AUTHORIZEPUT";
			dimEventKey="AUTHPUT";
			aggrCnt=aggrContainers;
			typeStrCnt="CONT";
		}
		else if(eventTypeDim.equals("PUTCOMP")) {
			eventTypeKey="PUTCOMPLETE";
			dimEventKey="PUTCOMP";
			aggrCnt=aggrContainers;
			typeStrCnt="CONT";
		}
		else if(eventTypeDim.equals("AUTHGETFILES")){
			eventTypeKey="AUTHORIZEGETFORFILES";
			dimEventKey="AUTHGET";
			aggrCnt = aggrStorageHosts;
			typeStrCnt="STHST";
		}
		else if(eventTypeDim.equals("GETCOMP")){
			eventTypeKey="GETCOMPLETE";
			dimEventKey="GETCOMP";
			aggrCnt = aggrStorageHosts;
			typeStrCnt="STHST";
		}
		/*Verifying the data class and setting up the identifier for each Event Type -- Stop*/
		/*Verifying the data class and setting up the identifier for each  Data Class -- Start*/
		if(data_Class.equals(MEDIA_STREAM)){
			dataClassKey="MSTREAM";
		}
		else if(data_Class.equals(UBIQUITY)){
			dataClassKey="UBIQUITY";
		}
		else if(data_Class.equals(SHARED_STREAMS)){
			dataClassKey="SHAREDSTREAMS";
		}
		else if(data_Class.equals(BACKUP)){
			dataClassKey="BACKUP";
		}
		else if(data_Class.equals(CONTACTS)){
			dataClassKey="CONTACTS";
		}
		else if(data_Class.equals(MESSENGER)){
			dataClassKey="MESSENGER";
		}
		else if(data_Class.equals(CALENDARS)){
			dataClassKey="CALENDARS";
		}
		else if(data_Class.equals(CLOUDKIT)){
			dataClassKey="CLOUDKIT";
		}
		/*Verifying the data class and setting up the identifier for each  Data Class -- Stop*/
		//Calling the method to generate DimCodes for Upload and Download reports for http_response_code = 200 
		if (http_response_code.equals("200") && dataClassKey!=null){
			addValueToHashMap200(dimEventKey,dataClassKey,aggrCnt,typeStrCnt);
		}
		//Calling the method to generate DimCodes for Upload and Download reports for all http_response_codes 
		addValueToHashMap(aggrTotalHits,eventTypeKey,http_response_code,eventTypeDim);
	}

	/*
	 * Newly added method for to generate vedor reponse dimcodes
	 */
	public static void setDataAndResponseForPut(String dataClass, long vendCode, String total_hits) {

		//Declare the required variables

		long aggrTotalHits = 0L;
		if(total_hits!=null){
			aggrTotalHits=Long.parseLong(total_hits);
		}

		//Verify the total hits field
		//Declare the required variables
		long hMapValueTot = 0L;
		String hmapKeyTot = null;
		//Use String Builder to append the required DIMCODES
		if(vendCode>0){
			if(vendCode > 201 && vendCode<300){
				hmapKeyTot = "MMCS."+PUT_EVENT_TYPE_DIM_CODE+".SUCC.OTH.CONT.CNT";
			}
			else if(vendCode >299 && vendCode != 400 && vendCode != 500 && vendCode != 403 && vendCode != 503 && vendCode != 404){

				hmapKeyTot = "MMCS."+PUT_EVENT_TYPE_DIM_CODE+".FAIL.OTH.CONT.CNT";
			}
			else if (vendCode == 200 || vendCode == 201 || vendCode == 400 || vendCode == 500 || vendCode == 403 || vendCode == 503 || vendCode == 404){
				hmapKeyTot = "MMCS."+PUT_EVENT_TYPE_DIM_CODE+"."+vendCode+".CONT.CNT";
			}

			else{
				hmapKeyTot = "MMCS."+PUT_EVENT_TYPE_DIM_CODE+".OTH.CONT.CNT";
			}

		}
		else{
			hmapKeyTot = "MMCS."+PUT_EVENT_TYPE_DIM_CODE+".OTH.CONT.CNT";
		}
		//Verifying whether any 
		if(storageMapForPut.get(hmapKeyTot) == null){
			hMapValueTot=0;
		}
		else{
			hMapValueTot=storageMapForPut.get(hmapKeyTot);
		}
		//Sum up the Values
		hMapValueTot=hMapValueTot+aggrTotalHits;
		//Storing the required DimCodes for Summary Report to the hash map 
		storageMapForPut.put(hmapKeyTot, hMapValueTot);
	}
	/**
	 * addValueToHashMap
	 * This method  is called to write all the Dimcodes for event types coresponding to reports Summary   . 
	 * inputs : totCount , eventTypeKey , http_response_code,eventTypeDim
	 * @author harisyam_s
	 * 
	 */	
	public static  void addValueToHashMap(Long totCount,String eventTypeKey,String http_response_code,String eventTypeDim) {

		//Declare the required variables
		long hMapValueTot = 0L;
		long hMapValueFail = 0L;
		long hMapValueEvent = 0L;
		long hMapValueSucc =0L;
		//Use String Builder to append the required DIMCODES
		String hmapKeyTot = "MMCS."+eventTypeKey+".TOT.CNT";
		String hmapKeyEventTot = "MMCS.EVENT."+eventTypeDim+".CNT";

		if(storageMap.get(hmapKeyTot) == null){
			hMapValueTot=0;
		}
		else{
			hMapValueTot=storageMap.get(hmapKeyTot);
		}
		//Checking for http_response_code not equal to 200 
		if(!http_response_code.equals("200")){

			String hmapKeyFail = "MMCS."+eventTypeKey+".FAIL.CNT";
			//Check to verify that only the required response codes are used for generating the DIMCODES
			if(respCodeList.contains(http_response_code)){
				String hmapKeyEvent = "MMCS."+eventTypeKey+"."+http_response_code+".CNT";
				if(storageMap.get(hmapKeyEvent)==null){
					hMapValueEvent=0;
				}
				else{
					hMapValueEvent=storageMap.get(hmapKeyEvent);
				}
				hMapValueEvent=hMapValueEvent+totCount;
				storageMap.put(hmapKeyEvent, hMapValueEvent);
			}

			if(storageMap.get(hmapKeyFail)==null){
				hMapValueFail=0;
			}
			else{
				hMapValueFail=storageMap.get(hmapKeyFail);
			}
			//Calculating the total count for Summary Report for failed cases
			hMapValueTot=hMapValueTot+totCount;
			hMapValueFail=hMapValueFail+totCount;
			//Storing the required DimCodes for Summary Report to the hash map  for failed cases

			storageMap.put(hmapKeyFail, hMapValueFail);
			storageMap.put(hmapKeyTot, hMapValueTot);
			storageMap.put(hmapKeyEventTot, hMapValueTot);

		}
		else if (http_response_code.equals("200")){

			//Generating the  DimCode for SUCCESS COUNT
			String hmapKeySucc="MMCS."+eventTypeKey+".SUCC.CNT";

			if(storageMap.get(hmapKeySucc)==null){
				hMapValueSucc=0;
			}else{
				hMapValueSucc=storageMap.get(hmapKeySucc);
			}
			//Calculating the total count for Summary Report for Repsonse Code 200
			hMapValueTot=hMapValueTot+totCount;
			hMapValueSucc=hMapValueSucc+totCount;
			//Storing the required DimCodes for Summary Report to the hash map 
			storageMap.put(hmapKeyTot, hMapValueTot);
			storageMap.put(hmapKeySucc, hMapValueSucc);
			storageMap.put(hmapKeyEventTot, hMapValueTot);
		}
	}
	/**
	 * addValueToHashMap200
	 * This method  is called to write all the Dimoceds for eventtype 200 coresponding to reports Upload(Puts) and Download(Gets)   . 
	 * @author harisyam_s
	 * 
	 * 
	 */	
	public static  void addValueToHashMap200(String eventType ,String dataClass,long aggrCnt, String typeStrCnt) {

		//Declare the required fields
		long hMapValueTot = 0L;
		long hMapValueEvent = 0L;
		//Use String Builder to append the required DIMCODES
		String hmapKeyTot = "MMCS."+dataClass+"."+eventType+".200."+typeStrCnt+".CNT";
		String hmapKeyEvent = "MMCS.TOT."+eventType+".200."+typeStrCnt+".CNT";
		//Verifying the Total value in HashMap is null
		if(storageMap.get(hmapKeyTot) == null){
			hMapValueTot=0;
		}
		else{
			hMapValueTot=storageMap.get(hmapKeyTot);
		}
		if(storageMap.get(hmapKeyEvent) == null){
			hMapValueEvent=0;
		}
		else{
			hMapValueEvent=storageMap.get(hmapKeyEvent);
		}
		//Calculating the required DimCodes for Upload and DownLoad Report
		hMapValueTot=hMapValueTot+aggrCnt;
		hMapValueEvent=hMapValueEvent+aggrCnt;
		//Storing the required DimCodes for Upload and DownLoad Report to the hash map 
		storageMap.put(hmapKeyTot, hMapValueTot);
		storageMap.put(hmapKeyEvent, hMapValueEvent);
	}
	/**
	 * splitString
	 * For Splitting the Count values which are double  to long  values . 
	 * @author harisyam_s
	 * 
	 * 
	 */
	public static  String splitString(String splitString){
		//Splitting the string using '.' as the delimiter
		String[] parts = splitString.split("\\.");
		//return first part of the split 
		return parts[0];
	}
	/**
	 * cleanup
	 * CleanUp is called at the end to write all the values in hashmap to context   . 
	 * @author harisyam_s
	 * 
	 * 
	 */
	@Override
	protected void cleanup(Context context)
			throws IOException,InterruptedException{
		long currValue = 0L;
		//Iterating through the HashMap and writing the Hash Map Key Value pair to the context 
		for (Entry<String, Long> entry : storageMapForPut.entrySet()) {
			if(entry!=null)
			{
				currValue=entry.getValue();
				if(entry.getKey() !=null)
				{
					context.write(new Text(entry.getKey()), new LongWritable(currValue));
				}
			}
		}
		currValue = 0L;
		for (Entry<String, Long> entry : storageMapForDelete.entrySet()) {
			if(entry!=null)
			{
				currValue=entry.getValue();
				if(entry.getKey() !=null)
				{
					context.write(new Text(entry.getKey()), new LongWritable(currValue));
				}
			}
		}
		currValue = 0L;
		for (Entry<String, Long> entry : storageMapForGet.entrySet()) {
			if(entry!=null)
			{
				currValue=entry.getValue();
				if(entry.getKey() !=null)
				{
					context.write(new Text(entry.getKey()), new LongWritable(currValue));
				}
			}
		}
		currValue = 0L;
		for (Entry<String, Long> entry : storageMap.entrySet()) {
			if(entry!=null)
			{
				currValue=entry.getValue();
				if(entry.getKey() !=null)
				{
					context.write(new Text(entry.getKey()), new LongWritable(currValue));
				}
			}
		}
	}
	/**
	 * setup
	 * Setup is called at the start to instantiate the hashmap with all the required Dimcodes  . 
	 * @author harisyam_s
	 * 
	 * 
	 */
	protected void setup(Context context)throws IOException,InterruptedException
	{
		//Initialize the HashMap with all the DimCodes to print all the dimcodes in the reducer o/p
		storageMap.put("MMCS.UBIQUITY.PUTCOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.CALENDARS.PUTCOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.MESSENGER.PUTCOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.MSTREAM.PUTCOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.CONTACTS.PUTCOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.BACKUP.PUTCOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.SHAREDSTREAMS.PUTCOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.TOT.PUTCOMP.200.CONT.CNT",0L);
		//Newly added
		/*storageMap.put("MMCS.UBIQUITY.DELETECOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.CALENDARS.DELETECOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.MESSENGER.DELETECOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.MSTREAM.DELETECOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.CONTACTS.DELETECOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.BACKUP.DELETECOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.SHAREDSTREAMS.DELETECOMP.200.CONT.CNT",0L);
		storageMap.put("MMCS.TOT.DELETECOMP.200.CONT.CNT",0L);*/
		//End
		storageMap.put("MMCS.UBIQUITY.AUTHPUT.200.CONT.CNT",0L);
		storageMap.put("MMCS.MESSENGER.AUTHPUT.200.CONT.CNT",0L);
		storageMap.put("MMCS.CALENDARS.AUTHPUT.200.CONT.CNT",0L);
		storageMap.put("MMCS.MSTREAM.AUTHPUT.200.CONT.CNT",0L);
		storageMap.put("MMCS.CONTACTS.AUTHPUT.200.CONT.CNT",0L);
		storageMap.put("MMCS.BACKUP.AUTHPUT.200.CONT.CNT",0L);
		storageMap.put("MMCS.SHAREDSTREAMS.AUTHPUT.200.CONT.CNT",0L);
		storageMap.put("MMCS.TOT.AUTHPUT.200.CONT.CNT",0L);
		storageMap.put("MMCS.UBIQUITY.GETCOMP.200.STHST.CNT",0L);
		storageMap.put("MMCS.CALENDARS.GETCOMP.200.STHST.CNT",0L);
		storageMap.put("MMCS.MESSENGER.GETCOMP.200.STHST.CNT",0L);
		storageMap.put("MMCS.MSTREAM.GETCOMP.200.STHST.CNT",0L);
		storageMap.put("MMCS.CONTACTS.GETCOMP.200.STHST.CNT",0L);
		storageMap.put("MMCS.BACKUP.GETCOMP.200.STHST.CNT",0L);
		storageMap.put("MMCS.SHAREDSTREAMS.GETCOMP.200.STHST.CNT",0L);
		storageMap.put("MMCS.TOT.GETCOMP.200.STHST.CNT",0L);
		storageMap.put("MMCS.UBIQUITY.AUTHGET.200.STHST.CNT",0L);
		storageMap.put("MMCS.MESSENGER.AUTHGET.200.STHST.CNT",0L);
		storageMap.put("MMCS.MSTREAM.AUTHGET.200.STHST.CNT",0L);
		storageMap.put("MMCS.CONTACTS.AUTHGET.200.STHST.CNT",0L);
		storageMap.put("MMCS.BACKUP.AUTHGET.200.STHST.CNT",0L);
		storageMap.put("MMCS.SHAREDSTREAMS.AUTHGET.200.STHST.CNT",0L);
		storageMap.put("MMCS.CALENDARS.AUTHGET.200.STHST.CNT",0L);
		storageMap.put("MMCS.TOT.AUTHGET.200.STHST.CNT",0L);
		//Added new Tenant details
		storageMap.put("MMCS.CLOUDKIT.AUTHGET.200.STHST.CNT",0L);
		storageMap.put("MMCS.CLOUDKIT.GETCOMP.200.STHST.CNT",0L);
		storageMap.put("MMCS.CLOUDKIT.AUTHPUT.200.CONT.CNT",0L);
		storageMap.put("MMCS.CLOUDKIT.PUTCOMP.200.CONT.CNT",0L);
		//storageMap.put("MMCS.CLOUDKIT.DELETECOMP.200.CONT.CNT",0L);

		storageMap.put("MMCS.GETCOMPLETE.503.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.502.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.500.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.412.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.403.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.401.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.400.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.330.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.FAIL.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.SUCC.CNT",0L);
		storageMap.put("MMCS.GETCOMPLETE.TOT.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.503.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.502.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.500.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.412.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.403.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.401.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.400.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.330.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.FAIL.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.SUCC.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEGETFORFILES.TOT.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.503.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.502.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.500.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.412.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.403.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.401.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.400.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.330.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.FAIL.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.SUCC.CNT",0L);
		storageMap.put("MMCS.PUTCOMPLETE.TOT.CNT",0L);
		//Newly added
		/*storageMap.put("MMCS.DELETECOMPLETE.503.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.502.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.500.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.412.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.403.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.401.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.400.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.330.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.FAIL.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.SUCC.CNT",0L);
		storageMap.put("MMCS.DELETECOMPLETE.TOT.CNT",0L);*/
		//End
		storageMap.put("MMCS.AUTHORIZEPUT.TOT.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.SUCC.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.FAIL.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.330.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.400.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.401.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.403.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.412.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.500.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.502.CNT",0L);
		storageMap.put("MMCS.AUTHORIZEPUT.503.CNT",0L);
		storageMap.put("MMCS.EVENT.PUTCOMP.CNT",0L);
		storageMap.put("MMCS.EVENT.GETCOMP.CNT",0L);
		storageMap.put("MMCS.EVENT.AUTHPUT.CNT",0L);
		storageMap.put("MMCS.EVENT.AUTHGETFILES.CNT",0L);

		storageMapForPut.put("MMCS.PUTCMP.200.CONT.CNT",0L);
		storageMapForPut.put("MMCS.PUTCMP.201.CONT.CNT",0L);
		storageMapForPut.put("MMCS.PUTCMP.400.CONT.CNT",0L);
		storageMapForPut.put("MMCS.PUTCMP.403.CONT.CNT",0L);
		storageMapForPut.put("MMCS.PUTCMP.404.CONT.CNT",0L);
		storageMapForPut.put("MMCS.PUTCMP.500.CONT.CNT",0L);
		storageMapForPut.put("MMCS.PUTCMP.503.CONT.CNT",0L);
		storageMapForPut.put("MMCS.PUTCMP.FAIL.OTH.CONT.CNT",0L);
		storageMapForPut.put("MMCS.PUTCMP.SUCC.OTH.CONT.CNT",0L);
		storageMapForPut.put("MMCS.PUTCMP.BAD.RAW.RECORD.CONT.CNT",0L);
		storageMapForGet.put("MMCS.PUTCMP.OTH.CONT.CNT",0L);

		storageMapForGet.put("MMCS.GETCMP.200.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.201.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.206.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.400.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.403.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.404.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.407.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.503.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.500.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.FAIL.OTH.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.OTH.CONT.CNT",0L);
		storageMapForGet.put("MMCS.GETCMP.SUCC.OTH.CONT.CNT",0L);
		storageMapForPut.put("MMCS.GETCMP.BAD.RAW.RECORD.CONT.CNT",0L);

		storageMapForDelete.put("MMCS.DELETECMP.200.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.201.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.206.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.400.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.403.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.404.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.407.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.503.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.500.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.FAIL.OTH.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.OTH.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.SUCC.OTH.CONT.CNT",0L);
		storageMapForDelete.put("MMCS.DELETECMP.BAD.RAW.RECORD.CONT.CNT",0L);
	}
}
