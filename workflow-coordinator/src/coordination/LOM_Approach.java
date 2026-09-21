package coordination;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import connection.DBConnectionFactory;
import connection.MQTTConnectionFactory;
import entities.Location;
import entities.Service;
import entities.Task;
import registry_info.PeerNodes_DAL;
import util.Helper;

import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.io.StringWriter;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;


public class LOM_Approach
{
	private Connection connect = null;
	final private String host = "localhost:3306";
	final private String user = "root";
	final private String passwd = "";
	HashMap<String, String> Sensors = new HashMap<String, String>();
	HashMap<String, String> Fog_Services = new HashMap<String, String>();
	HashMap<String, String> Cloud_Services = new HashMap<String, String>();
	HashMap<String, String> Deployment_Command = new HashMap<String, String>();
	HashMap<String, String> Deployment_Source_Files = new HashMap<String, String>();
	ArrayList<HashMap<String, String>> SourceFilesData = new ArrayList<>();
	double latitudes[] =  new double[100];
	double longitudes[] =  new double[100];
    int Device_List[] = new int[100];
    int Peer_Node_List[] = new int[100];
    String Communcation_Message[] = new String[100];
    String Deployment_Message[] = new String[100];
    String Task_Message[] = new String[100];
    String Peer_Nodes[] = new String[100];
    int task_Id = 0;
    String URI = "";
    String IP_Address = "";
    String Selected_Device_ID_List = "";
    int count_device_selection_list = 0;
    int task_services_count[] = new int[100];
    String Task_ServiceList[][] = new String[100][100];
    
    String Fog_Service_Name[] = new String[100];
    String Fog_Service_IP_Address[] = new String[100];
    int Fog_Service_Availability[] = new int[100];
    String Fog_Service_Type[] = new String[100];
    int Fog_Deployable[] = new int[100];
    int Fog_Non_Deployable[] = new int[100];
    double Fog_Location_Latitude[] = new double[100];
    double Fog_Location_Longitude[] = new double[100];
    int Fog_Selected_Peer_Node[] = new int[100];
    int Fog_cell_id[] = new int[100];
    int Fog_Service_Count = 0;
    int fog_service_task_IDs[] = new int[100];
    int Selected_Peer_Node = 0;
    int cell_id = 0;
    NodeList taskNodeList;
    ArrayList<Task> taskList;
    Document doc;
  
    
    public void workflow_parser(Location location, String workflowFileName)
    {
    	taskList = new ArrayList<Task>();
    	
		for(int lmn=0; lmn<100; lmn++)
	    {
	    	SourceFilesData.add(lmn, new HashMap<String, String>());
	    }
    	
	    try 
	    {
		    File workflowFile = new File(workflowFileName);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(workflowFile);
			doc.getDocumentElement().normalize();

			//input location of the incident get number of tasks from the workflow file	
			taskNodeList = doc.getElementsByTagName("Task");
			
			if (taskNodeList.getLength() > 0)
			{
				taskParser(0, location);
			}
			
			XPath xpath = XPathFactory.newInstance().newXPath();
			NodeList taskNodeList = (NodeList)xpath.evaluate("//Task", doc, XPathConstants.NODESET);
			
			for (int taskindex = 0; taskindex < taskNodeList.getLength(); taskindex++)
			{
				Node output = taskNodeList.item(taskindex);
				String Task_detail = nodeToString(output);
				Task_detail = "<?xml version=\"1.0\"?><Workflow>" + Task_detail +"</Workflow>";
			    Task_Message[(taskindex+1)] = Task_detail;
			}
	    }
	    catch (Exception e)
	    {
			e.printStackTrace();
	    }	 
    }
    
    private static String nodeToString(Node node) throws TransformerException
	{
	    StringWriter buf = new StringWriter();
	    Transformer xform = TransformerFactory.newInstance().newTransformer();
	    xform.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
	    xform.transform(new DOMSource(node), new StreamResult(buf));
	    return(buf.toString());
	}
    
    public void taskParser(int taskIndex, Location workflowLocation)
    {
    	Task task = new Task();
    	
		try
	    {
			Node taskNode = taskNodeList.item(taskIndex);

			if (taskNode.getNodeType() == Node.ELEMENT_NODE)
			{
				Element TaskElement = (Element) taskNode;
				
				task_Id = Integer.parseInt(TaskElement.getAttribute("id"));
				task.id = task_Id;
				System.out.println("Task_id= "+task_Id);
			}

			Element taskElement = (Element)taskNode;
			Communcation_Message[task_Id] = "<?xml version=\"1.0\"?><response>";
			NodeList servicesNodeList = taskElement.getElementsByTagName("Gamma");
			System.out.println("Services Count is "+servicesNodeList.getLength());
			task_services_count[task_Id] = servicesNodeList.getLength();
			
			if (servicesNodeList.getLength() > 0) 
			{
				// peer node selection according to location
				Selected_Peer_Node = peerNodeSearch(workflowLocation);
				
				PeerNodes_DAL peer_nodes_dal = new PeerNodes_DAL();
				
				task.mapped_peer_node = peer_nodes_dal.fetchPeerNodeByID(Selected_Peer_Node);
				
				Workflow_Coordinator_Main.peerNodeMqttClient = MQTTConnectionFactory.getMQTTClient(task.mapped_peer_node, Workflow_Coordinator_Main.task_in_execution, Workflow_Coordinator_Main.qos, Workflow_Coordinator_Main.Task_Execution_Count);

				cell_id = Selected_Peer_Node;
			}
			
			for(int serviceIndex = 0; serviceIndex < servicesNodeList.getLength(); serviceIndex++)
			{
				Service service = new Service();
				
				Node nNode = servicesNodeList.item(serviceIndex);
				
				if (nNode.getNodeType() == Node.ELEMENT_NODE)
				{
					Element eElement = (Element) nNode;
					service.id = Integer.parseInt(eElement.getAttribute("id"));
					service.service_name = eElement.getElementsByTagName("Service_Name").item(0).getTextContent();
					
					double latitude = Double.parseDouble(eElement.getElementsByTagName("Latitude").item(0).getTextContent());
					double longitude = Double.parseDouble(eElement.getElementsByTagName("Longitude").item(0).getTextContent());
					
					service.location = new Location(latitude, longitude);
					
					service.radius = Integer.parseInt(eElement.getElementsByTagName("Value").item(0).getTextContent());

					String availability_tag = eElement.getElementsByTagName("Availability").item(0).getTextContent();
					service.required_availability_duration = Integer.parseInt(availability_tag.replaceAll("[\\D]", ""));
					
					System.out.println("Hardware_Requirements : " + eElement.getElementsByTagName("Hardware_Requirements").item(0).getTextContent());
					System.out.println("Software_Requirements : " + eElement.getElementsByTagName("Software_Requirements").item(0).getTextContent());
					
					System.out.println("Service_Input : " + eElement.getElementsByTagName("Service_Input").item(0).getTextContent());
					System.out.println("Service_Output : " + eElement.getElementsByTagName("Service_Output").item(0).getTextContent());
				
					service.service_type = eElement.getElementsByTagName("Service_Type").item(0).getTextContent();
					service.deployment_status = eElement.getElementsByTagName("Deployable").item(0).getTextContent();
					
					service.deployable = service.deployment_status.equalsIgnoreCase("Deployable");
					
					if (service.deployable == false)
					{
						//selectEndDeviceForNonDeployableService(service, workflowLocation, Selected_Peer_Node, cell_id);
						selectEndDeviceForNonDeployableService(service.service_name, service.required_availability_duration, service.service_type, 0, workflowLocation, Selected_Peer_Node, cell_id);
					}
			
					else if (service.deployable && service.service_type.contains("Fog Service")) // For computation services
					{
						System.out.println("I have to Handle Fog Service");
						
						Fog_Service_Name[Fog_Service_Count] = service.service_name;
						Fog_Service_Availability[Fog_Service_Count] = service.required_availability_duration;
						Fog_Service_Type[Fog_Service_Count] = service.service_type;
						Fog_Deployable[Fog_Service_Count] = (service.deployable ? 1 : 0);
						Fog_Non_Deployable[Fog_Service_Count] = (service.deployable ? 0 : 1);
						Fog_Location_Latitude[Fog_Service_Count] = workflowLocation.latitude;
						Fog_Location_Longitude[Fog_Service_Count] = workflowLocation.longitude;
						Fog_Selected_Peer_Node[Fog_Service_Count] = Selected_Peer_Node;
						Fog_cell_id[Fog_Service_Count] = cell_id;
						fog_service_task_IDs[Fog_Service_Count] = task_Id;

						Communcation_Message[task_Id] = Communcation_Message[task_Id] + "<Service name=\""+service.service_name+"\">"+"<URI>"+URI+"</URI>"+"<IP_Address>"+IP_Address+"</IP_Address><Type>"+service.service_type+"</Type><Deployment>"+ (service.deployable == true ? 1 : 0) +"</Deployment></Service>";
					}
					else if(service.deployable == false && service.service_type.contains("Cloud Service")) //For computation services
					{
						Communcation_Message[task_Id] = Communcation_Message[task_Id] + "<Service name=\""+service.service_name+"\">"+"<URI>"+URI+"</URI>"+"<IP_Address>"+IP_Address+"</IP_Address><Type>"+service.service_type+"</Type><Deployment>"+ (service.deployable == true ? 1 : 0) +"</Deployment></Service></response>";
					}
				}
				
				task.services.add(service);
			}
			
			taskList.add(task);
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    }	    
    }
    
    
	public void selectEndDeviceForNonDeployableService(String Service_Name, int Availability, String Service_Type, int Deployable, Location workflowLocation, int Selected_Peer_Node, int cell_id) throws Exception
	{
		try 
		{
		  int Device_Id = 0;
		  int count= 0;
		  int closestDevice = 0;
		  String Query_Device = "";
			
	      connect = new DBConnectionFactory().getConnection();
		      
	      String query = "SELECT * FROM services where Service_Name = '"+Service_Name+"' AND Availability >="+Availability+" AND Service_Type = '"+Service_Type+"' AND Deployable ="+Deployable;
	      System.out.println("Query = "+query);	
	      Statement st = connect.createStatement();
	      ResultSet rs = st.executeQuery(query);

	      while (rs.next())
	      {
	    	  Device_Id = rs.getInt("Device_Id");

	    	  if((Selected_Device_ID_List == "") || !(Selected_Device_ID_List.contains(Integer.toString(Device_Id))))
	    	  {	  
	    		  Query_Device = "SELECT * FROM devices where id = "+Device_Id+" and peer_node_id = "+Selected_Peer_Node+ " and cell_id = "+cell_id;
		    	  Statement st_Device = connect.createStatement();
			      ResultSet rs_Device = st_Device.executeQuery(Query_Device);

			      while (rs_Device.next())
			      { 
			    	  System.out.println("count ="+count);
			    	  latitudes[count] = rs_Device.getDouble("Latitude");
			    	  longitudes[count] = rs_Device.getDouble("Longitude");
			    	  Device_List[count] = Device_Id;
			    	  count++; 
			      }
	    	  }
		      
	      }
	      
	      st.close();

	      closestDevice = Helper.findMinimal(latitudes, longitudes, Device_List, count, workflowLocation);
	      
	      count_device_selection_list = count_device_selection_list + 1;
	      
	      if(count_device_selection_list == 1)
	      {
	    	  Selected_Device_ID_List = Integer.toString(closestDevice);
	      }
	      else
	      {
	    	  Selected_Device_ID_List = Selected_Device_ID_List + " ," + Integer.toString(closestDevice);  
	      }
	      
	      Get_Device_IP_Address(closestDevice);
	      Get_Device_URI(closestDevice, Service_Name, Service_Type, Deployable);
	      
	      SourceFilesData.add((task_Id-1), new HashMap<String, String>());
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    }
	}
	
	public int peerNodeSearch(Location workflowLocation)
	{
		int closestNode = 0;

		try
		{
			int Peer_Node_Id = 0;
			int count = 0;
	
			connect = new DBConnectionFactory().getConnection();
		
			String query_peer_nodes = "SELECT * FROM peer_nodes";
			Statement st_peer_nodes = connect.createStatement();
			ResultSet rs_peer_nodes = st_peer_nodes.executeQuery(query_peer_nodes);

			while (rs_peer_nodes.next())
			{
				Peer_Node_Id = rs_peer_nodes.getInt("id");

				latitudes[count] = rs_peer_nodes.getDouble("Latitude");
				longitudes[count] = rs_peer_nodes.getDouble("Longitude");
				Peer_Node_List[count] = Peer_Node_Id;
				
				count++;
			}
			
			st_peer_nodes.close();
			
			// FindMinimal selects the closest node to the input location
			closestNode = Helper.findMinimal(latitudes, longitudes, Peer_Node_List, count, workflowLocation);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return closestNode;
	}
	
	public void selectEndDeviceForDeployableService(String Service_Name, String Location, int Availability, String Service_Type, int Deployable, int Non_Deployable, Location workflowLocation, int Selected_Peer_Node, int cell_id, int gamma_no) throws Exception
	{
		int Device_Id = 0;
		int count= 0;
		int Closest_Device = 0;
		
		try {
	      Class.forName("com.mysql.cj.jdbc.Driver");
	      
	      // Setup the connection with the DB
	      connect = DriverManager
	          .getConnection("jdbc:mysql://" + host + "/gom?"
	              + "user=" + user + "&password=" + passwd );
	      try
	      {
	       // our SQL SELECT query. 
	      // if you only need a few columns, specify them by name instead of using "*"
	      String query = "SELECT * FROM services where Service_Name = '"+Service_Name+"' AND Availability >="+Availability+" AND Service_Type = '"+Service_Type+"' AND Deployable ="+Deployable+" AND Non_Deployable = "+Non_Deployable;
	      System.out.println("Query = "+query);	
	      // create the java statement
	      Statement st = connect.createStatement();
	      // execute the query, and get a java resultset
	      ResultSet rs = st.executeQuery(query);
	      // iterate through the java resultset
	      while (rs.next())
	      {
	    	  Device_Id = rs.getInt("Device_Id");
	    	  if(!(Selected_Device_ID_List.contains(Integer.toString(Device_Id))))
	    	  {
	    	  String Query_Device = "SELECT * FROM devices where id = "+Device_Id+" AND peer_node_id = "+Selected_Peer_Node+" AND cell_id = "+cell_id;
	    	  //System.out.println("Query_Device ="+ Query_Device);
	    	  	// create the java statement
	    	  Statement st_Device = connect.createStatement();
	    	   // execute the query, and get a java resultset
		      ResultSet rs_Device = st_Device.executeQuery(Query_Device);
		      // iterate through the java resultset
		      while (rs_Device.next())
		      {
		    	  
		    	  latitudes[count] = rs_Device.getDouble("Latitude");
		    	  longitudes[count] = rs_Device.getDouble("Longitude");
		    	  Device_List[count] = Device_Id;
		    	  //Device_URI[count] = rs_Device.getString("URI");
		    	  count++; 
		      }
	    	  }
		      
	      }
	      st.close();
	      //System.out.println("count is"+count);
	      Closest_Device = Helper.findMinimal(latitudes, longitudes, Device_List, count, workflowLocation);
	      count_device_selection_list = count_device_selection_list + 1;
	      if(count_device_selection_list == 1)
	      {
	      Selected_Device_ID_List = Integer.toString(Closest_Device);
	      }
	      else
	      {
	       Selected_Device_ID_List = Selected_Device_ID_List + " ," + Integer.toString(Closest_Device);  
	      }
	      System.out.println("Selected_Device_ID_List ="+Selected_Device_ID_List);
	      count = 0;
	      Get_Device_IP_Address(Closest_Device);
	      getComputationDeviceURI(Closest_Device, Service_Name, Service_Type, Deployable);
	      
	      /*addition of deployment */
	      SourceFilesData.add((task_Id-1), new HashMap<String, String>());
	      
	      //Communcation_Message = Communcation_Message + "</Services>\n<Deployment>";
	      Communcation_Message[task_Id] = Communcation_Message[task_Id] + "</Services><Deployment><Task>["+task_Id+"]</Task><Service_Name>"+Service_Name+"</Service_Name><IP_Address>"+IP_Address+"</IP_Address>";
	      Deployment_Message[task_Id] = "<?xml version=\"1.0\"?><Deployment><Task>["+task_Id+"]</Task><Service_Name>"+Service_Name+"</Service_Name><IP_Address>"+IP_Address+"</IP_Address>";
	      String Destination_IP_Address = "<Destination>"+IP_Address+"</Destination>";
	      Communcation_Message[task_Id] = Communcation_Message[task_Id].replace("<Destination>NA</Destination>", Destination_IP_Address);
	      Deplyoment_Files_Detail(Closest_Device, Service_Name);
	      Communcation_Message[task_Id] = Communcation_Message[task_Id] + "</Deployment></response>";
	      Deployment_Message[task_Id] = Deployment_Message[task_Id] + "</Deployment>";
	    }
	    catch (Exception e)
	    {
	      System.err.println("Got an exception! ");
	      System.err.println(e.getMessage());
	    }
	      
	    } catch (Exception e) {
	      throw e;
	    } 
	}
	
	public void getComputationDeviceURI(int Closest_Device, String Service_Name, String Service_Type, int Deployable) throws Exception
	{
		String URI = "";
		try {
		      // This will load the MySQL driver, each DB has its own driver
		      //Class.forName("com.mysql.jdbc.Driver");
		      Class.forName("com.mysql.cj.jdbc.Driver");
		      
		      // Setup the connection with the DB
		      connect = DriverManager
		          .getConnection("jdbc:mysql://" + host + "/gom?"
		              + "user=" + user + "&password=" + passwd );
		      try
		      {
		       // our SQL SELECT query. 
		      // if you only need a few columns, specify them by name instead of using "*"
		      String query = "SELECT * FROM services where Service_Name = '"+Service_Name+"' AND Device_Id ="+Closest_Device;
		      //System.out.println("Query = "+query);	
		      // create the java statement
		      Statement st = connect.createStatement(); 
		      // execute the query, and get a java resultset
		      ResultSet rs = st.executeQuery(query);
		      // iterate through the java resultset
		      while (rs.next())
		      {
		    	  URI = rs.getString("URI"); 
		      }
		      st.close();
		      Fog_Services.put(Service_Name, URI); 
		      //Communcation_Message = Communcation_Message + "Service:"+Service_Name+",URI:"+URI+"\n";
		      //Communcation_Message = Communcation_Message + "<Service name=\""+Service_Name+"\">"+"\n<URI>"+URI+"</URI>\n</Service>\n";
		      Communcation_Message[task_Id] = Communcation_Message[task_Id] + "<Service name=\""+Service_Name+"\">"+"<URI>"+URI+"</URI>"+"<IP_Address>"+IP_Address+"</IP_Address><Type>"+Service_Type+"</Type><Deployment>"+Deployable+"</Deployment></Service>";
		    }
		    catch (Exception e)
		    {
		      System.err.println("Got an exception! ");
		      System.err.println(e.getMessage());
		    }
		      
		    } catch (Exception e) {
		      throw e;
		    } 
	}
	
	
	public void Deplyoment_Files_Detail(int Closest_Device, String Service_Name) throws Exception
	{
		
		int Deployment_Service_Id = 0;
		String Doployment_Service_URI = "";
		try {
		      // This will load the MySQL driver, each DB has its own driver
		      //Class.forName("com.mysql.jdbc.Driver");
		      Class.forName("com.mysql.cj.jdbc.Driver");
		      
		      // Setup the connection with the DB
		      connect = DriverManager
		          .getConnection("jdbc:mysql://" + host + "/gom?"
		              + "user=" + user + "&password=" + passwd );
		      try
		      {
		       // our SQL SELECT query. 
		      // if you only need a few columns, specify them by name instead of using "*"
		      String query = "SELECT * FROM services where Service_Name = '"+Service_Name+"' AND Device_Id ="+Closest_Device;
		      System.out.println("Query = "+query);	
		      // create the java statement
		      Statement st = connect.createStatement(); 
		      // execute the query, and get a java resultset
		      ResultSet rs = st.executeQuery(query);
		      // iterate through the java resultset
		      while (rs.next())
		      {
		    	  Deployment_Service_Id = rs.getInt("id");
		    	  Doployment_Service_URI = rs.getString("URI");
		    	  String Query_Deployment = "SELECT * FROM deployment where Deployment_Service_Id = "+Deployment_Service_Id;
		    	  System.out.println("Query_Deployment "+Query_Deployment);
		    	  	// create the java statement
		    	  Statement st_Deployment = connect.createStatement();
		    	   // execute the query, and get a java resultset
			      ResultSet rs_Deployment = st_Deployment.executeQuery(Query_Deployment);
			      // iterate through the java resultset
			      while (rs_Deployment.next())
			      {
			    	  System.out.println("Hre Command"); 
			    	  String Command = rs_Deployment.getString("Command");
			    	  if(Command.contains("argument_1"))
			    	  {
			    		  //String arg_1 = rs_Deployment.getString("arg_1");
			    		  Command = Command.replace("argument_1", URI);
			    	  }
			    	  
			    	  if(Command.contains("argument_2"))
			    	  {
			    		  String arg_2 = rs_Deployment.getString("arg_2");
			    		  Command = Command.replace("argument_2", arg_2);
			    	  }
			    	  
			    	  if(Command.contains("argument_3"))
			    	  {
			    		  String arg_3 = rs_Deployment.getString("arg_3");
			    		  Command = Command.replace("argument_3", arg_3);
			    	  }
			    	  Deployment_Command.put("Command", Command);
			    	  //Communcation_Message = Communcation_Message + "Command:"+Command+"\n";
			    	  //Communcation_Message = Communcation_Message + "<Command>"+Command+"</Command>"+"\n";
			    	  Communcation_Message[task_Id] = Communcation_Message[task_Id] + "<Command>"+Command+"</Command>";
			    	  Deployment_Message[task_Id] = Deployment_Message[task_Id] + "<Command>"+Command+"</Command>";
			    	  for(int i = 1; i <= 15; i++)
			    	  {
			    		  //System.out.println("here put");
			    		  String Source_Variable_Name = "Source_File_Address_"+i;
			    		  String Source_File_Name = rs_Deployment.getString(Source_Variable_Name);

			    		  if((!(Source_File_Name.equals(""))) && (!(Source_File_Name.equals(null))) && (!(Source_File_Name.equals("Nil"))))
			    		  {  
			    			  Deployment_Source_Files.put((Source_Variable_Name+"_"+task_Id), rs_Deployment.getString(Source_Variable_Name));
			    			  if(rs_Deployment.getString(Source_Variable_Name).contains("config"))
			    			  {
			    				  Communcation_Message[task_Id] = Communcation_Message[task_Id] + "<Configuration_File>config.txt</Configuration_File>";  
			    				  Deployment_Message[task_Id] = Deployment_Message[task_Id] + "<Configuration_File>config.txt</Configuration_File>";
			    			  }
			    		  }
			    	  }
			    	  
			    	  System.out.println("here put"+(task_Id-1));
			    	  SourceFilesData.add((task_Id-1), Deployment_Source_Files);
			      }
		      }

		      st.close();
		    }
		    catch (Exception e)
		    {
		      System.err.println("Got an exception! ");
		      System.err.println(e.getMessage());
		    }
	    }
		catch (Exception e)
		{
	      throw e;
	    } 
	}
	
	public void Get_Device_IP_Address(int Closest_Device) throws Exception
	{

		
		try {
		      // This will load the MySQL driver, each DB has its own driver
		      //Class.forName("com.mysql.jdbc.Driver");
		      Class.forName("com.mysql.cj.jdbc.Driver");
		      
		      // Setup the connection with the DB
		      connect = DriverManager
		          .getConnection("jdbc:mysql://" + host + "/gom?"
		              + "user=" + user + "&password=" + passwd );
		      try
		      {
		       // our SQL SELECT query. 
		      // if you only need a few columns, specify them by name instead of using "*"
		      String query = "SELECT * FROM devices where id ="+Closest_Device;
		      System.out.println("Query = "+query);	
		      // create the java statement
		      Statement st = connect.createStatement(); 
		      // execute the query, and get a java resultset
		      ResultSet rs = st.executeQuery(query);
		      // iterate through the java resultset
		      while (rs.next())
		      {
		    	  //System.out.println("------------------IP_Address--------------------\n"+IP_Address);
		    	  IP_Address = rs.getString("IP_Address");
		    	  System.out.println("------------------IP_Address--------------------\n"+IP_Address);
		    	  
		    	  
		      }
		      st.close();
		      //Communcation_Message = Communcation_Message + "<Service>"+Service_Name+"</Service>"+"<URI>"+URI+"</URI>\n";
		      //Communcation_Message = Communcation_Message + "<Service name=\""+Service_Name+"\">"+"\n<URI>"+URI+"</URI>\n</Service>\n";
		      //Communcation_Message[Task_Id] = Communcation_Message[Task_Id] + "<Service name=\""+Service_Name+"\">"+"<URI>"+URI+"</URI>"+"<IP_Address>"+IP_Address+"</IP_Address></Service>";
		    }
		    catch (Exception e)
		    {
		      System.err.println("Got an exception! ");
		      System.err.println(e.getMessage());
		    }
		      
		    } catch (Exception e) {
		      throw e;
		    } 
	
	}
	
	
	public void Get_Device_URI(int Closest_Device, String Service_Name, String Service_Type, int Deployable) throws Exception
	{
		
		try {
		      // This will load the MySQL driver, each DB has its own driver
		      //Class.forName("com.mysql.jdbc.Driver");
		      Class.forName("com.mysql.cj.jdbc.Driver");
		      
		      // Setup the connection with the DB
		      connect = DriverManager
		          .getConnection("jdbc:mysql://" + host + "/gom?"
		              + "user=" + user + "&password=" + passwd );
		      try
		      {
		       // our SQL SELECT query. 
		      // if you only need a few columns, specify them by name instead of using "*"
		      String query = "SELECT * FROM services where Service_Name = '"+Service_Name+"' AND Device_Id ="+Closest_Device;
		      //System.out.println("Query = "+query);	
		      // create the java statement
		      Statement st = connect.createStatement(); 
		      // execute the query, and get a java resultset
		      ResultSet rs = st.executeQuery(query);
		      // iterate through the java resultset
		      while (rs.next())
		      {
		    	  URI = rs.getString("URI");
		    	  
		      }
		      st.close();
		      Sensors.put(Service_Name, URI); 
		      //Communcation_Message = Communcation_Message + "<Service>"+Service_Name+"</Service>"+"<URI>"+URI+"</URI>\n";
		      //Communcation_Message = Communcation_Message + "<Service name=\""+Service_Name+"\">"+"\n<URI>"+URI+"</URI>\n</Service>\n";
		      if(Service_Type.contains("sensor"))
		      {
		      Communcation_Message[task_Id] = Communcation_Message[task_Id] + "<Service name=\""+Service_Name+"\">"+"<URI>"+URI+"</URI>"+"<IP_Address><Source>"+IP_Address+"</Source><Destination>NA</Destination></IP_Address><Type>"+Service_Type+"</Type><Deployment>"+Deployable+"</Deployment></Service>";
		      }
		      if(Service_Type.contains("Fog Service"))
		      {
		      Communcation_Message[task_Id] = Communcation_Message[task_Id] + "<Service name=\""+Service_Name+"\">"+"<URI>"+URI+"</URI>"+"<IP_Address>"+IP_Address+"</IP_Address><Type>"+Service_Type+"</Type><Deployment>"+Deployable+"</Deployment></Service>";
		      }
		      if(Service_Type.contains("Cloud Service"))
		      {
		      Communcation_Message[task_Id] = Communcation_Message[task_Id] + "<Service name=\""+Service_Name+"\">"+"<URI>"+URI+"</URI>"+"<IP_Address>"+IP_Address+"</IP_Address><Type>"+Service_Type+"</Type><Deployment>"+Deployable+"</Deployment></Service></response>";
		      }
		    }
		    catch (Exception e)
		    {
		      System.err.println("Got an exception! ");
		      System.err.println(e.getMessage());
		    }
		      
		    } catch (Exception e) {
		      throw e;
		    } 
	}
}