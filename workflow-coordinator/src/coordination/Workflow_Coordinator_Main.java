package coordination;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.util.ArrayList;

import org.xml.sax.InputSource;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import org.eclipse.paho.client.mqttv3.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.Server;
import io.moquette.server.config.ClasspathConfig;
import io.moquette.server.config.IConfig;
import util.ConfigReader;
import util.Helper;
import util.Preparation;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import connection.MQTTConnectionFactory;
import entities.Location;
import entities.TaskInfo;

import java.io.BufferedReader;
import java.net.*;
import java.io.*;
import java.util.regex.*;

public class Workflow_Coordinator_Main {
	public static ConfigReader configReader;
	public static Location location;
	static MqttClient sampleClient;
	static MqttClient peerNodeMqttClient;
	static ArrayList<String> output = new ArrayList<String>();
	static int qos = 1;
	static int prev_device_no = 0;
	static String Communication_Message_Received = "";
	public static String Execution_Command = null;
	public static String task_output = "";
	public static String Current_Vehicle_Status = "";
	public static double vehicle_status_info_latittude[] = new double[100];
	public static double vehicle_status_info_longitude[] = new double[100];
	public static int vehicle_count = 3;
	public static GOM_Approach gom_approach;
	public static LOM_Approach lom_approach;
	public static int closest_peer_node[] = new int[50];
	public static int delay_between_tasks = 5000;
	public static String execution_command_List[] = new String[100];
	public static int Task_Execution_Count[] = new int[100];
	public static int task_id = 1;
	public static int multiple_gamma_execution = 0;
	public static int task_in_execution = 0;
	public static double task_start_time[] = new double[100];
	public static double task_end_time[] = new double[100];
	public static double task_elapsed_time[] = new double[100];
	public static String Executed_Task_Output[] = new String[100];
	public static int task_count = 0;
	public static int output_counter[] = new int[100];
	public static int thread_counter = 0;
	public static String Process_Info = "";

	public static TaskInfo[] tasksInfo = { new TaskInfo(), new TaskInfo(), new TaskInfo(), new TaskInfo() };

	public static String task_1_IP_Address = "";

	// for experimentation purposes (deployment time computation)
	public static int device_left_task_ID = 4;

	public static boolean device_left_flag = false;
	public static double device_leave_time = 0;
	public static String leaving_device_IP_Address = "";

	/** TO CALCULATE REDEPLOYMENT DELAY **/
	public static double redeployment_t1_stime = 0;
	public static double redeployment_t1_etime = 0;
	public static double redeployment_t1_time = 0;

	public static double redeployment_stime = 0;
	public static double redeployment_etime = 0;
	public static double redeployment_time = 0;

	public static double IoT_redeployment_stime = 0;
	public static double IoT_redeployment_etime = 0;
	public static double IoT_redployment_time = 0;

	public static double prev_redeployment_stime = 0;
	/** TO CALCULATE REDEPLOYMENT DELAY **/

	public static String approach = "";

	static class PublisherListener extends AbstractInterceptHandler {
		@Override
		public void onPublish(InterceptPublishMessage message) {
			String text_message = new String(message.getPayload().array());

			System.out.println("-------------------------------------------------");
			System.out.println("| Received ****");
			System.out.println("| Topic: " + message.getTopicName());
			System.out.println("| Message: " + text_message);
			System.out.println("| QoS: {1}" + message.getQos());
			System.out.println("-------------------------------------------------");

			if (approach.contains("GOM"))
			{
				if (text_message.contains("Done with Execution")) 
				{
					Communication_Message_Received = text_message;
				}
				else if (text_message.contains("<Task>"))
				{
					int taskID = Integer
							.parseInt(text_message.substring(text_message.indexOf("[") + 1, text_message.indexOf("]")));
	
					if (tasksInfo[taskID - 1].start_time != 0)
					{
						tasksInfo[taskID - 1].end_time = System.currentTimeMillis();
						tasksInfo[taskID - 1].redeployment_time = (tasksInfo[taskID - 1].end_time - tasksInfo[taskID - 1].start_time) / 1000;
						tasksInfo[taskID - 1].start_time = 0;
					}
	
					tasksInfo[taskID - 1].time[tasksInfo[taskID - 1].count] = System.currentTimeMillis();
	
					if (tasksInfo[taskID - 1].count > 0)
					{
						tasksInfo[taskID - 1].elapsed_time[(tasksInfo[taskID - 1].count- 1)] = (tasksInfo[taskID - 1].time[(tasksInfo[taskID - 1].count)] - tasksInfo[taskID - 1].time[(tasksInfo[taskID - 1].count - 1)]) / 1000;
					}
	
					tasksInfo[taskID - 1].count = tasksInfo[taskID - 1].count + 1;
	
					if (text_message.contains("KML File"))
					{
						location = getTaskLocation(text_message);
					}
					
					if (taskID == device_left_task_ID)
					{
						double leaveTime = (System.currentTimeMillis() - device_leave_time) / 1000;
						System.out.println("task_" + device_left_task_ID + "device_leave_time =" + leaveTime
								+ " device_leave_flag =" + device_left_flag);
	
						if (tasksInfo[taskID - 1].start_time != 0)
						{
							device_leave_time = System.currentTimeMillis();
						}
	
						if (device_left_flag)
						{
							tasksInfo[taskID - 1].start_time = System.currentTimeMillis();
							device_left_flag = false;
						}
					}
	
					output_counter[taskID - 1] = output_counter[taskID - 1] + 1;
	
					Executed_Task_Output[taskID - 1] = text_message;
	
					if (execution_command_List.length > 0 && execution_command_List[taskID + 1] != null)
					{
						try
						{
							Execution_Command = execution_command_List[taskID + 1];
							Process_Info = Execution_Command;
							
							if (output_counter[taskID - 1] > 0)
							{
								CloudServiceHandler cloudServiceHandler = new CloudServiceHandler(execution_command_List[taskID + 1], taskID);
								thread_counter = thread_counter + 1;
								
								while (thread_counter != 0)
								{
									Thread.sleep(2000);
									System.out.println("Super Processing------" + thread_counter);
								}
	
								if (cloudServiceHandler.cloudServiceThread.isAlive())
								{
									cloudServiceHandler.cloudServiceThread.interrupt();
								}
	
								output_counter[taskID - 1] = output_counter[taskID - 1] - 1;
								output_counter[taskID] = output_counter[taskID] + 1;
								taskID = taskID + 1;
							}
						}
						catch (Exception e)
						{
							System.out.println(e);
						}
					}
				}
	
				output.add(text_message);

				//.................................................
				// Code instrumentation for experimental evaluation
				if (text_message.contains("<hint>[1]</hint> Start"))
				{
					tasksInfo[0].start_time = System.currentTimeMillis();
					redeployment_t1_stime = System.currentTimeMillis();
				}

				if (text_message.contains("<hint>[1]</hint> End"))
				{
					redeployment_t1_etime = System.currentTimeMillis();
					redeployment_t1_time = (redeployment_t1_etime - redeployment_t1_stime) / 1000;
					System.out.println("-----task_1_redeployment_time------- = " + redeployment_t1_time);
					redeployment_t1_etime = 0;
					redeployment_t1_stime = 0;
				}

				if (text_message.contains("<hint>[" + device_left_task_ID + "]</hint> Start"))
				{
					device_left_flag = true;
					device_leave_time = System.currentTimeMillis();
					redeployment_stime = System.currentTimeMillis();
				}

				if (text_message.contains("<hint>[" + device_left_task_ID + "]</hint> End"))
				{
					redeployment_etime = System.currentTimeMillis();
					redeployment_time = (redeployment_etime - redeployment_stime) / 1000;
					System.out.println("-----task_4_redeployment_time------- = " + redeployment_time);
					redeployment_etime = 0;
					redeployment_stime = 0;
				}
				//.................................................

				if (text_message.contains("Device has left {"))
				{
					if (text_message.contains("["))
					{
						String Device_IP_Address = text_message.substring(text_message.indexOf("[") + 1, text_message.indexOf("]"));
						int Left_Task_ID = Integer.parseInt(text_message.substring(text_message.indexOf("{") + 1, text_message.indexOf("}")));

						for (int fogServiceIndex = 0; fogServiceIndex < gom_approach.Fog_Service_Count; fogServiceIndex++)
						{
							if (gom_approach.Fog_Service_IP_Address[fogServiceIndex].contains(Device_IP_Address)
									&& gom_approach.fog_service_task_IDs[fogServiceIndex] == Left_Task_ID)
							{
								if (gom_approach.fog_service_task_IDs[fogServiceIndex] == 1)
								{
									tasksInfo[0].start_time = System.currentTimeMillis();
								}

								if (gom_approach.fog_service_task_IDs[fogServiceIndex] == device_left_task_ID)
								{
									device_left_flag = true;
									device_leave_time = System.currentTimeMillis();
								}

								try
								{
									Location fogDeviceLocation = new Location(
											gom_approach.Fog_Location_Latitude[fogServiceIndex],
											gom_approach.Fog_Location_Longitude[fogServiceIndex]);
											gom_approach.selectAlternateDevice(gom_approach.Fog_Service_Name[fogServiceIndex],
											gom_approach.Fog_Service_Availability[fogServiceIndex],
											gom_approach.Fog_Service_Type[fogServiceIndex],
											gom_approach.Fog_Deployable[fogServiceIndex],
											gom_approach.Fog_Non_Deployable[fogServiceIndex], fogDeviceLocation,
											gom_approach.Fog_Selected_Peer_Node[fogServiceIndex],
											gom_approach.Fog_cell_id[fogServiceIndex]);

									gom_approach.Communcation_Message[gom_approach.fog_service_task_IDs[fogServiceIndex]] = gom_approach.Communcation_Message[gom_approach.fog_service_task_IDs[fogServiceIndex]].replace(gom_approach.Fog_Service_IP_Address[fogServiceIndex], gom_approach.IP_Address);
									
									int servicesCountForTaskOfFogService = gom_approach.task_services_count[gom_approach.fog_service_task_IDs[fogServiceIndex]];

									for (int serviceIndex = 0; serviceIndex < servicesCountForTaskOfFogService; serviceIndex++)
									{
										if (gom_approach.Deployment_Message_List[gom_approach.fog_service_task_IDs[fogServiceIndex]][serviceIndex] != null)
										{
											gom_approach.Deployment_Message[gom_approach.fog_service_task_IDs[fogServiceIndex]] = gom_approach.Deployment_Message_List[gom_approach.fog_service_task_IDs[fogServiceIndex]][serviceIndex];
											gom_approach.Deployment_Message[gom_approach.fog_service_task_IDs[fogServiceIndex]] = gom_approach.Deployment_Message[gom_approach.fog_service_task_IDs[fogServiceIndex]].replace(gom_approach.Fog_Service_IP_Address[fogServiceIndex],	gom_approach.IP_Address);
										}
									}

									try
									{
										// Now Publishing Services
										String xmlStr = gom_approach.Communcation_Message[gom_approach.fog_service_task_IDs[fogServiceIndex]];
										Document doc = convertStringToDocument(xmlStr);
										doc.getDocumentElement().normalize();

										String New_Device_IP_Address = "New IP_Address =" + gom_approach.IP_Address;
										
										MqttMessage New_Device_IP_Address_message = new MqttMessage(New_Device_IP_Address.getBytes());
										New_Device_IP_Address_message.setQos(qos);

										String content = "Resume Execution";
										
										MqttMessage resume_message = new MqttMessage(content.getBytes());
										resume_message.setQos(qos);
										
										System.out.println("Communication Message= " + gom_approach.Communcation_Message[gom_approach.fog_service_task_IDs[fogServiceIndex]]);
										
										MqttMessage communication_message = new MqttMessage(gom_approach.Communcation_Message[gom_approach.fog_service_task_IDs[fogServiceIndex]].getBytes());
										communication_message.setQos(qos);

										MqttMessage deployment_message = new MqttMessage(gom_approach.Deployment_Message[gom_approach.fog_service_task_IDs[fogServiceIndex]].getBytes());
										deployment_message.setQos(qos);

										if (!peerNodeMqttClient.isConnected())
										{
											peerNodeMqttClient.connect(MQTTConnectionFactory.getConnectionOptions());
										}

										peerNodeMqttClient.publish("service_execution_RP1", New_Device_IP_Address_message);
										Thread.sleep(100);
										
										peerNodeMqttClient.publish("service_execution_RP1", deployment_message);
										Thread.sleep(100);
										
										peerNodeMqttClient.publish("service_execution_RP1", communication_message);
										Thread.sleep(100);
										
										peerNodeMqttClient.publish("service_execution_RP1", resume_message);
									}
									catch (Exception e)
									{
										e.printStackTrace();
									}
								}
								catch (Exception e)
								{
									e.printStackTrace();
								}
							}
						}
					}
				}
			}
			else if (approach.contains("LOM"))
			{
				if(text_message.contains("Done with Execution"))
				{
				  Communication_Message_Received = text_message;
				}
				
				if(!(text_message.contains("</Task> Device left")) &&  text_message.contains("<Task>"))
				{
				  int id = Integer.parseInt(text_message.substring(text_message.indexOf("[") + 1, text_message.indexOf("]")));
				  
				  output_counter[id-1] = output_counter[id-1] + 1;

				  Executed_Task_Output[id-1] = text_message;

				  if(execution_command_List.length > 0 && execution_command_List[id+1] != null)
				  {
					try
					{
					  if(execution_command_List[id+1].contains("super_impose_plume"))
					  {
					    Process_Info = execution_command_List[id+1];
					    Execution_Command = execution_command_List[id+1];
						if(output_counter[id-1] > 0)
						{
							CloudServiceHandler cloudServiceHandler = new CloudServiceHandler(execution_command_List[id+1], task_id);
							thread_counter = thread_counter + 1;
							
							while(thread_counter != 0)
							{
								Thread.sleep(2000);
							}
						  
							if(cloudServiceHandler.cloudServiceThread.isAlive())
							{
								cloudServiceHandler.cloudServiceThread.interrupt();
							}
							
							output_counter[id-1] = output_counter[id-1] - 1;
							output_counter[id] = output_counter[id] + 1;

							id = id + 1;
						}
						
						if(execution_command_List[id+1].contains("gps-vehicle-simulator"))
						{
							Process_Info = execution_command_List[id+1];

							if(output_counter[id-1] > 0)
							{
							    Execution_Command = execution_command_List[id+1];
							    CloudServiceHandler cloudServiceHandler = new CloudServiceHandler(execution_command_List[id+1], task_id);
								thread_counter = thread_counter + 1;
								
								while(thread_counter != 0)
								{
									Thread.sleep(2000);
								}
							  
								if(cloudServiceHandler.cloudServiceThread.isAlive())
								{
									cloudServiceHandler.cloudServiceThread.interrupt();
								}
								
								output_counter[id-1] = output_counter[id-1] - 1;
								MqttMessage vehicle_task_output = new MqttMessage(Current_Vehicle_Status.getBytes());
								System.out.println("Executed_Task_Output[task_count] ="+Current_Vehicle_Status);
								
								if(!peerNodeMqttClient.isConnected())
								{
									peerNodeMqttClient.connect(MQTTConnectionFactory.getConnectionOptions());
								}

								try
								{
									peerNodeMqttClient.publish("service_execution_RP1", vehicle_task_output);
								}
								catch (MqttException me)
								{
									me.printStackTrace();
								}
							}
						}
					  }
					  else
					  {
					    Process_Info = execution_command_List[id+1];
					    Execution_Command = execution_command_List[id+1];
						
					    if(output_counter[id-1] > 0)
						{
							CloudServiceHandler cloudServiceHandler = new CloudServiceHandler(execution_command_List[id+1], task_id);
							thread_counter = thread_counter + 1;
							
							while(thread_counter != 0)
							{
								Thread.sleep(2000);
							}
						  
							if(cloudServiceHandler.cloudServiceThread.isAlive())
							{
								cloudServiceHandler.cloudServiceThread.interrupt();
							}
							
							output_counter[id-1] = output_counter[id-1] - 1;
							output_counter[id] = output_counter[id] + 1;

							id = id + 1;
						}  
					  }
					}
					catch(Exception e)
					{
						System.out.println(e);
					}
				  }
				
				}
				
				output.add(text_message);

				//.................................................
				// Code instrumentation for experimental evaluation
				if (text_message.contains("<Task>[1]</Task> Device left"))
				{
					System.out.println("Task [1] device left");
					tasksInfo[0].start_time = System.currentTimeMillis();
					redeployment_t1_stime = System.currentTimeMillis();
				}

				if (text_message.contains("Redeployment Done [1]"))
				{
					redeployment_t1_etime = System.currentTimeMillis();
					redeployment_t1_time = (redeployment_t1_etime - redeployment_t1_stime) / 1000;
					System.out.println("-----task_1_redeployment_time------- = " + redeployment_t1_time);
					redeployment_t1_etime = 0;
					redeployment_t1_stime = 0;

				}

				if (text_message.contains("<Task>[" + device_left_task_ID + "]</Task> Device left"))
				{
					System.out.println("Task [" + device_left_task_ID + "] device left");
					device_left_flag = true;
					device_leave_time = System.currentTimeMillis();
					redeployment_stime = System.currentTimeMillis();
					prev_redeployment_stime = redeployment_stime;
				}

				if (text_message.contains("Redeployment Done [" + device_left_task_ID + "]"))
				{
					redeployment_etime = System.currentTimeMillis();
					redeployment_time = (redeployment_etime - redeployment_stime) / 1000;
					System.out.println(
							"-----task_" + device_left_task_ID + "_redeployment_time------- = " + redeployment_time);
					redeployment_etime = 0;
					redeployment_stime = 0;
				}

				if (text_message.contains("Device has left"))
				{
					System.out.println("************** Device has left ****************");
				}
				//.................................................
			}
		}
		
		public static Location getTaskLocation(String Task_Output)
		{
			final Pattern pattern = Pattern.compile("<Point><coordinates>(.+?)</coordinates></Point>", Pattern.DOTALL);
			final Matcher matcher = pattern.matcher(Task_Output);

			matcher.find();
			
			String Location = matcher.group(1);
			String[] LocationList = Location.split(",");
			Location location = new Location(Double.parseDouble(LocationList[1]), Double.parseDouble(LocationList[0]));

			return location;
		}

		public static void file_upload(String src, String host_address, String user, String pswd)
		{
			try
			{
				String fileName = Helper.getFileNameFromFullPath(src);

				JSch jsch = new JSch();
				jsch.addIdentity(pswd);
				Session session = jsch.getSession(user, host_address);
				java.util.Properties config = new java.util.Properties();
				config.put("StrictHostKeyChecking", "no");
				session.setConfig(config);
				session.connect();

				ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
				sftpChannel.connect();
				
				String path = "";
				
				for (String dir : configReader.getDeploymentPath().split("/"))
				{
					if(!dir.isEmpty())
					{
				        path = path + "/" + dir;
				        
				        try
				        {
				            sftpChannel.mkdir(path);
				        }
				        catch (Exception ee) {}
					}
				}
				
				sftpChannel.put(src, configReader.getDeploymentPath() + fileName);
			}
			catch (Exception e)
			{
				System.out.println(e);
			}
		}

		public static void main(String[] args) throws Exception
		{
			Preparation.clean_up();

			configReader =  new ConfigReader(args[0]);
			
			approach = configReader.getApproach();

			location = configReader.getWorkflowLocation();

			if (approach.contains("GOM"))
			{
				invokeGOMApproach();
			}
			else if (approach.contains("LOM"))
			{
				invokeLOMApproach();
			}
		}
		
		public static void invokeGOMApproach() throws Exception
		{
			String content = "Hi, I am Workflow Coordinator!";
			
			gom_approach = new GOM_Approach();

			gom_approach.workflow_parser(location, configReader.getWorkflowFileName());
			
			for (int taskIndex = 1; taskIndex <= gom_approach.task_ID; taskIndex++)
			{
				Task_Execution_Count[taskIndex - 1] = 0;

				System.out.println("Communication Message is: \n" + gom_approach.Communcation_Message[taskIndex]);
			}

			// Creating an MQTT Broker using Moquette
			final IConfig classPathConfig = new ClasspathConfig();

			final Server mqttBroker = new Server();

			final List<? extends InterceptHandler> userHandlers = Arrays.asList(new PublisherListener());

			mqttBroker.startServer(classPathConfig, userHandlers);

			System.out.println("moquette mqtt broker started, press ctrl-c to shutdown..");

			Runtime.getRuntime().addShutdownHook(new Thread()
			{
				@Override
				public void run()
				{
					System.out.println("stopping moquette mqtt broker..");
					mqttBroker.stopServer();
				}
			});

			Thread.sleep(4000);

			/*****************************************
			 * PAHO CLIENT START
			 *****************************************/
			MqttMessage message = new MqttMessage(content.getBytes());
			MqttMessage communication_message = new MqttMessage(content.getBytes());
			MqttMessage deployment_message;
			
			try
			{
				peerNodeMqttClient.publish("service_execution_RP1", message);
			}
			catch (MqttException e)
			{
				e.printStackTrace();
			}

			for (int taskIndex = 0; taskIndex < gom_approach.taskNodeList.getLength(); taskIndex++)
			{
				if (taskIndex == 0)
				{
					task_start_time[taskIndex] = System.currentTimeMillis();
				} else
				{
					task_end_time[taskIndex - 1] = System.currentTimeMillis();
					task_start_time[taskIndex] = System.currentTimeMillis();
				}

				task_in_execution = taskIndex;
				
				if(taskIndex > 0)
				{
					while (!(Communication_Message_Received.contains("Done with Execution")))
					{
						Thread.sleep(1000);
						System.out.println("Listening.....");
					}
				}

				if (taskIndex == 0)
				{
					for (int serviceIndex = 0; serviceIndex < gom_approach.task_services_count[taskIndex + 1]; serviceIndex++)
					{
						// Deployment_Message_List is a 2D array having Task_ID as first index and Service_ID as second index
						if (gom_approach.Deployment_Message_List[taskIndex+1][serviceIndex] != null)
						{
							String key = Integer.toString((taskIndex)) + Integer.toString(serviceIndex);

							int map_key = Integer.parseInt(key);
							int no_of_uploads = gom_approach.SourceFilesData.get(map_key).size();
							
							for (int uploadCount = 1; uploadCount <= no_of_uploads; uploadCount++)
							{
								String source_variable_name = "Source_File_Address_" + uploadCount;

								if (gom_approach.SourceFilesData.get(map_key).get(((source_variable_name + "_" + (taskIndex + 1)))) != null)
								{
									file_upload(
											gom_approach.SourceFilesData.get(map_key).get(((source_variable_name + "_" + (taskIndex + 1)))),
											gom_approach.taskList.get(taskIndex).services.get(serviceIndex).mapped_end_device.IP_Address,
											gom_approach.taskList.get(taskIndex).services.get(serviceIndex).mapped_end_device.sftp_username,
											gom_approach.taskList.get(taskIndex).services.get(serviceIndex).mapped_end_device.sftp_password);
								}
							}
						}
					}

					content = "Deploy 1st Data Integration Service";
					message = new MqttMessage(content.getBytes());
					message.setQos(qos);

					System.out.println("Communication Message= " + gom_approach.Communcation_Message[taskIndex + 1]);

					communication_message = new MqttMessage(gom_approach.Communcation_Message[taskIndex + 1].getBytes());
					communication_message.setQos(qos);
					
					deployment_message = new MqttMessage(gom_approach.Deployment_Message[taskIndex + 1].getBytes());
					deployment_message.setQos(qos);

					task_id = task_id + 1;
					
					if (!peerNodeMqttClient.isConnected())
					{
						try
						{
							peerNodeMqttClient.connect(MQTTConnectionFactory.getConnectionOptions());
						}
						catch (MqttException me)
						{
							me.printStackTrace();
						}
					}

					if (deployment_message != null)
					{
						System.out.println("deployment_message =" + deployment_message);
						System.out.println("*****************************************************************");
						System.out.println("communication_message =" + communication_message);
						System.out.println("*****************************************************************");
						
						try
						{
							peerNodeMqttClient.publish("service_execution_RP1" , deployment_message);
							peerNodeMqttClient.publish("service_execution_RP1" , communication_message);
							peerNodeMqttClient.publish("service_execution_RP1", message);
						}
						catch (MqttException e)
						{
							e.printStackTrace();
						}
					}
				}
				else
				{
					Communication_Message_Received = "";
					task_count = task_count + 1;

					gom_approach.Task_Parser(taskIndex, location);
					
					Task_Execution_Count[taskIndex] = 0;

					System.out.println("Communication Message is: \n" + gom_approach.Communcation_Message[taskIndex]);
					System.out.println("*****************************************************************");
					System.out.println("Deployment Message is: \n" + gom_approach.Deployment_Message[taskIndex]);
					System.out.println("*****************************************************************");

					if (gom_approach.task_services_count[taskIndex + 1] > 1)
					{
						// Publishing Service Deployment Message
						int no_of_uploads = gom_approach.SourceFilesData.get(taskIndex).size();
						
						if (gom_approach.Communcation_Message[taskIndex + 1].contains("traffic_flow_service"))
						{
							task_id = task_id + 1;

							// TODO under review
							closest_peer_node[0] = 7;
							
							for (int serviceIndex = 0; serviceIndex < gom_approach.task_services_count[taskIndex + 1]; serviceIndex++)
							{
								if (gom_approach.Deployment_Message_List[taskIndex+1][serviceIndex] != null)
								{
									String key = Integer.toString((taskIndex))+ Integer.toString(serviceIndex);

									int map_key = Integer.parseInt(key);
									
									no_of_uploads = gom_approach.SourceFilesData.get(map_key).size();
									
									for (int uploadIndex = 1; uploadIndex <= no_of_uploads; uploadIndex++)
									{
										String source_variable_name = "Source_File_Address_" + uploadIndex;

										if (gom_approach.SourceFilesData.get(map_key).get(((source_variable_name + "_" + (taskIndex + 1)))) != null)
										{
											file_upload(gom_approach.SourceFilesData.get(map_key).get(((source_variable_name + "_" + (taskIndex + 1)))),
												gom_approach.taskList.get(taskIndex).services.get(serviceIndex).mapped_end_device.IP_Address,
												gom_approach.taskList.get(taskIndex).services.get(serviceIndex).mapped_end_device.sftp_username,
												gom_approach.taskList.get(taskIndex).services.get(serviceIndex).mapped_end_device.sftp_password);
										}
									}
								}
							}

							String xmlStr = gom_approach.Communcation_Message[taskIndex + 1];
							Document doc = convertStringToDocument(xmlStr);
							doc.getDocumentElement().normalize();

							content = "Deploy Data Integration Service";
							message = new MqttMessage(content.getBytes());
							message.setQos(qos);

							System.out.println("Communication Message= "+ gom_approach.Communcation_Message[taskIndex + 1]);
							
							communication_message = new MqttMessage(gom_approach.Communcation_Message[taskIndex + 1].getBytes());
							communication_message.setQos(qos);
							
							deployment_message = new MqttMessage(gom_approach.Deployment_Message[taskIndex + 1].getBytes());
							deployment_message.setQos(qos);

							MqttMessage vehicle_task_output = new MqttMessage(Current_Vehicle_Status.getBytes());
							vehicle_task_output.setQos(qos);

							try
							{
								if (!peerNodeMqttClient.isConnected())
								{
										peerNodeMqttClient.connect(MQTTConnectionFactory.getConnectionOptions());
								}

								peerNodeMqttClient.publish("service_execution_RP1", vehicle_task_output);
								peerNodeMqttClient.publish("service_execution_RP1", deployment_message);

								Thread.sleep(1000);
								
								peerNodeMqttClient.publish("service_execution_RP1", communication_message);
								peerNodeMqttClient.publish("service_execution_RP1", message);
							}
							catch (MqttException e)
							{
								e.printStackTrace();
							}
						}
						else
						{
							for (int serviceIndex = 0; serviceIndex < gom_approach.task_services_count[taskIndex + 1]; serviceIndex++)
							{
								if (gom_approach.Deployment_Message_List[taskIndex+1][serviceIndex] != null)
								{
									String key = Integer.toString((taskIndex)) + Integer.toString(serviceIndex);

									int map_key = Integer.parseInt(key);
									no_of_uploads = gom_approach.SourceFilesData.get(map_key).size();
									
									for (int uploadCount = 1; uploadCount <= no_of_uploads; uploadCount++)
									{
										String source_variable_name = "Source_File_Address_" + uploadCount;

										if (gom_approach.SourceFilesData.get(map_key).get(((source_variable_name + "_" + (taskIndex + 1)))) != null)
										{
											file_upload(
											gom_approach.SourceFilesData.get(map_key).get(((source_variable_name + "_" + (taskIndex + 1)))),
											gom_approach.taskList.get(taskIndex).services.get(serviceIndex).mapped_end_device.IP_Address,
											gom_approach.taskList.get(taskIndex).services.get(serviceIndex).mapped_end_device.sftp_username,
											gom_approach.taskList.get(taskIndex).services.get(serviceIndex).mapped_end_device.sftp_password);
										}
									}
								}
							}

							String xmlStr = gom_approach.Communcation_Message[taskIndex + 1];

							Document doc = convertStringToDocument(xmlStr);
							doc.getDocumentElement().normalize();

							content = "Deploy Data Integration Service";
							
							message = new MqttMessage(content.getBytes());
							message.setQos(qos);
							
							System.out.println("Communication Message= " + gom_approach.Communcation_Message[taskIndex + 1]);
							
							communication_message = new MqttMessage(gom_approach.Communcation_Message[taskIndex + 1].getBytes());
							communication_message.setQos(qos);

							deployment_message = new MqttMessage(gom_approach.Deployment_Message[taskIndex + 1].getBytes());
							deployment_message.setQos(qos);
							
							try
							{
								if (!peerNodeMqttClient.isConnected())
								{
										peerNodeMqttClient.connect(MQTTConnectionFactory.getConnectionOptions());
								}

								peerNodeMqttClient.publish("service_execution_RP1", deployment_message);
								peerNodeMqttClient.publish("service_execution_RP1", communication_message);
								peerNodeMqttClient.publish("service_execution_RP1", message);
							}
							catch (MqttException e)
							{
								e.printStackTrace();
							}
						}
					}
					else
					{
						if(gom_approach.Communcation_Message[taskIndex + 1].contains("Cloud Service"))
						{
							String cloudServiceURL = null;
							String messageContent = gom_approach.Communcation_Message[taskIndex + 1];
							Document messageDoc = convertStringToDocument(messageContent); 
							String sService_URI = parseXML(messageDoc);
							System.out.println("sService_URI =" + sService_URI);

							if (gom_approach.Communcation_Message[taskIndex + 1].contains("impose_plume_service"))
							{
								//cloudServiceURL = "http://203.135.63.70/CloudServices/services/PlumeModelingServices";
								cloudServiceURL = sService_URI;
								
								task_id = task_id + 1;
								
								if (output_counter[taskIndex - 1] > 0)
								{
									HttpResponse<String> response = null;
									
									try
									{
										response = Unirest.post(cloudServiceURL)
										  .header("SOAPAction", "\"\"")
										  .header("Content-Type", "text/plain")
										  .body("<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\">\r\n    <Body>\r\n        <super_impose_plume_on_google_map xmlns=\"http://incident_management\">\r\n            <kmlData>KML Data</kmlData>\r\n        </super_impose_plume_on_google_map>\r\n    </Body>\r\n</Envelope>")
										  .asString();
									}
									catch (UnirestException e)
									{
										e.printStackTrace();
									}
									
									Document doc = convertStringToDocument(response.getBody());
									
									String outputURL = doc.getElementsByTagName("super_impose_plume_on_google_mapReturn").item(0).getChildNodes().item(0).getNodeValue();
									
									Helper.openURL(outputURL);
											
									Communication_Message_Received = "Done with Execution";
									output_counter[taskIndex - 1] = output_counter[taskIndex - 1] - 1;
									output_counter[taskIndex] = output_counter[taskIndex] + 1;
								}
							}
							else if (gom_approach.Communcation_Message[taskIndex + 1].contains("vehicle_status_service"))
							{
								//cloudServiceURL = "http://203.135.63.70/CloudServices/services/GPSSimulation";
								cloudServiceURL = sService_URI;

								task_id = task_id + 1;
								
								if (output_counter[taskIndex - 1] > 0)
								{
									HttpResponse<String> response = null;
									
									try
									{
										response = Unirest.post(cloudServiceURL)
										  .header("SOAPAction", "\"\"")
										  .header("Content-Type", "text/plain")
										  .body("<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\">\r\n" + "    <Body>\r\n" + "        <getVehicleStatus xmlns=\"http://incident_management\"/>\r\n" + "    </Body>\r\n" + "</Envelope>")
										  .asString();
									}
									catch (UnirestException e)
									{
										e.printStackTrace();
									}
									
									Document doc = convertStringToDocument(response.getBody());
									
									String outputURL = doc.getElementsByTagName("url").item(0).getChildNodes().item(0).getNodeValue();
									String vehicleStatus = doc.getElementsByTagName("vehicle_status").item(0).getChildNodes().item(0).getNodeValue();

									Helper.openURL(outputURL);
									
									Workflow_Coordinator_Main.Communication_Message_Received = "Done with Execution";
									Workflow_Coordinator_Main.output_counter[taskIndex + 1] = Workflow_Coordinator_Main.output_counter[taskIndex + 1] + 1;

									String taskOutput = "<Task>[" + (Workflow_Coordinator_Main.task_count + 1) + "]</Task> Vehicle_Status_Info =" + vehicleStatus;
									Workflow_Coordinator_Main.Executed_Task_Output[Workflow_Coordinator_Main.task_count] = taskOutput;
									Workflow_Coordinator_Main.Current_Vehicle_Status = taskOutput;
									
									parse_vehicle_status(vehicleStatus);
									
									execution_command_List[taskIndex + 1] = Execution_Command;
									output_counter[taskIndex - 1] = output_counter[taskIndex - 1] - 1;
									
									Workflow_Coordinator_Main.thread_counter = 0;
								}
							}
							else if (gom_approach.Communcation_Message[taskIndex + 1].contains("re_route_vehicle_service"))
							{
								//cloudServiceURL = "http://203.135.63.70/CloudServices/services/TrafficFlowServices";
								cloudServiceURL = sService_URI;
								
								task_id = task_id + 1;
								
								if (output_counter[taskIndex - 1] > 0)
								{
									HttpResponse<String> response = null;
									
									try
									{
										response = Unirest.post(cloudServiceURL)
										  .header("SOAPAction", "\"\"")
										  .header("Content-Type", "text/plain")
										  .body("<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\">\r\n    <Body>\r\n        <re_route_vehicles xmlns=\"http://incident_management\">\r\n            <currentVehicleLocation>30.456048,-91.205240</currentVehicleLocation>\r\n        </re_route_vehicles>\r\n    </Body>\r\n</Envelope>")
										  .asString();
									}
									catch (UnirestException e)
									{
										e.printStackTrace();
									}
									
									Document doc = convertStringToDocument(response.getBody());
									
									String outputURL = doc.getElementsByTagName("re_route_vehiclesReturn").item(0).getChildNodes().item(0).getNodeValue();
									
									Helper.openURL(outputURL);
									
									output_counter[taskIndex - 1] = output_counter[taskIndex - 1] - 1;
								}
							}
						}
					}
				}

				if (taskIndex == (gom_approach.taskNodeList.getLength() - 1))
				{
					task_end_time[taskIndex] = System.currentTimeMillis();
				}
			}

			for (int taskIndex = 0; taskIndex < gom_approach.task_ID; taskIndex++)
			{
				task_elapsed_time[taskIndex] = task_end_time[taskIndex] - task_start_time[taskIndex];
				task_elapsed_time[taskIndex] = task_elapsed_time[taskIndex] / 1000;

				System.out.println("Task Elapsed Time in seconds =" + task_elapsed_time[taskIndex]);
			}
			/************************************
			 * Paho CLIENT END
			 **********************************/
		}
		
		public static void invokeLOMApproach() throws Exception
		{

			String content = "Hi, I am Workflow Coordinator!";
			
			lom_approach = new LOM_Approach();

			lom_approach.workflow_parser(location, configReader.getWorkflowFileName());
			System.out.println("read_xml_file.Task_Id =" + lom_approach.task_Id);

			// Creating a MQTT Broker using Moquette
			final IConfig classPathConfig = new ClasspathConfig();

			final Server mqttBroker = new Server();

			final List<? extends InterceptHandler> userHandlers = Arrays.asList(new PublisherListener());
			mqttBroker.startServer(classPathConfig, userHandlers);

			System.out.println("moquette mqtt broker started, press ctrl-c to shutdown..");
			Runtime.getRuntime().addShutdownHook(new Thread()
			{
				@Override
				public void run()
				{
					System.out.println("stopping moquette mqtt broker..");
					mqttBroker.stopServer();
				}
			});

			Thread.sleep(4000);

			/*****************************************
			 * PAHO CLIENT START
			 *****************************************/

			MqttMessage message = new MqttMessage(content.getBytes());
			MqttMessage Task_Message;
			
			try
			{
				peerNodeMqttClient.publish("service_execution_RP1", message);
			}
			catch (MqttException e)
			{
				e.printStackTrace();
			}
			
			for (int taskIndex = 0; taskIndex < lom_approach.taskNodeList.getLength(); taskIndex++)
			{
				if (taskIndex == 0)
				{
					task_start_time[taskIndex] = System.currentTimeMillis();
				} else
				{
					task_end_time[taskIndex - 1] = System.currentTimeMillis();
					task_start_time[taskIndex] = System.currentTimeMillis();
				}

				task_in_execution = taskIndex;

				if (taskIndex == 0)
				{
					if (lom_approach.task_services_count[taskIndex + 1] > 1)
					{
						try
						{
							content = "Task Orchestration and Management Request";
							message = new MqttMessage(content.getBytes());
							message.setQos(qos);

							System.out.println("Communication Message= " + lom_approach.Communcation_Message[taskIndex + 1]);

							Task_Message = new MqttMessage(lom_approach.Task_Message[taskIndex + 1].getBytes());
							Task_Message.setQos(qos);
							
							task_id = task_id + 1;

							if (!peerNodeMqttClient.isConnected())
							{
								peerNodeMqttClient.connect(MQTTConnectionFactory.getConnectionOptions());
							}

							peerNodeMqttClient.publish("service_execution_RP1", Task_Message);
							peerNodeMqttClient.publish("service_execution_RP1", message);
						}
						catch (Exception e)
						{
							e.printStackTrace();
						}
					}
				} 
				else
				{
					while (!(Communication_Message_Received.contains("Done with Execution")))
					{
						Thread.sleep(1000);
						System.out.println("Listening.....");
					}

					Communication_Message_Received = "";
					task_count = task_count + 1;
					
					lom_approach.taskParser(task_count, location);
					
					if (lom_approach.task_services_count[taskIndex + 1] > 1)
					{
						String peer_node_tcp_address = "tcp://" + lom_approach.taskList.get(taskIndex).mapped_peer_node.IP_Address + ":" + lom_approach.taskList.get(taskIndex).mapped_peer_node.mqtt_port;
						
						String share_output_message = "Share output with:[" + peer_node_tcp_address + "]";
						
						MqttMessage share_output = new MqttMessage(share_output_message.getBytes());
						share_output.setQos(qos);
						
						if (peerNodeMqttClient.isConnected())
						{
							try
							{
								peerNodeMqttClient.publish("service_execution_RP", share_output);
							}
							catch (MqttException e)
							{
								e.printStackTrace();
							}
						}
						
						try
						{
							if (!peerNodeMqttClient.isConnected())
							{
								peerNodeMqttClient.connect(MQTTConnectionFactory.getConnectionOptions());
							}
							
							if (lom_approach.Communcation_Message[taskIndex + 1].contains("traffic_flow_service"))
							{
								// TODO under review
								task_id = task_id + 1;
								closest_peer_node[0] = 7;

								content = "Deploy Data Integration Service";

								message = new MqttMessage(content.getBytes());
								message.setQos(qos);
								
								MqttMessage vehicle_task_output = new MqttMessage(Current_Vehicle_Status.getBytes());
								vehicle_task_output.setQos(qos);

								peerNodeMqttClient.publish("service_execution_RP1",	vehicle_task_output);
							}
							else
							{
								content = "Deploy Data Integration Service";

								message = new MqttMessage(content.getBytes());
								message.setQos(qos);
							}
							
							System.out.println("Communication Message= " + lom_approach.Communcation_Message[taskIndex + 1]);

							Task_Message = new MqttMessage(lom_approach.Task_Message[taskIndex + 1].getBytes());
							Task_Message.setQos(qos);

							peerNodeMqttClient.publish("service_execution_RP1", Task_Message);
							peerNodeMqttClient.publish("service_execution_RP1", message);
						}
						catch (Exception e) 
						{
							e.printStackTrace();
						}
					}
					else
					{
						if(lom_approach.Communcation_Message[taskIndex + 1].contains("Cloud Service"))
						{
							String cloudServiceURL = null;
							String messageContent = lom_approach.Communcation_Message[taskIndex + 1];
							System.out.println("messageContent ="+messageContent);
							Document messageDoc = convertStringToDocument(messageContent); 
							String sService_URI = parseXML(messageDoc);
							System.out.println("sService_URI =" + sService_URI);
	
							if (lom_approach.Communcation_Message[taskIndex + 1].contains("impose_plume_service"))
							{
								//cloudServiceURL = "http://203.135.63.70/CloudServices/services/PlumeModelingServices";
								cloudServiceURL = sService_URI;
								
								task_id = task_id + 1;
								
								if (output_counter[taskIndex - 1] > 0)
								{
									HttpResponse<String> response = null;
									
									try
									{
										response = Unirest.post(cloudServiceURL)
										  .header("SOAPAction", "\"\"")
										  .header("Content-Type", "text/plain")
										  .body("<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\">\r\n    <Body>\r\n        <super_impose_plume_on_google_map xmlns=\"http://incident_management\">\r\n            <kmlData>KML Data</kmlData>\r\n        </super_impose_plume_on_google_map>\r\n    </Body>\r\n</Envelope>")
										  .asString();
									}
									catch (UnirestException e)
									{
										e.printStackTrace();
									}
									
									Document doc = convertStringToDocument(response.getBody());
									
									String outputURL = doc.getElementsByTagName("super_impose_plume_on_google_mapReturn").item(0).getChildNodes().item(0).getNodeValue();
									
									Helper.openURL(outputURL);
											
									Communication_Message_Received = "Done with Execution";
									output_counter[taskIndex - 1] = output_counter[taskIndex - 1] - 1;
									output_counter[taskIndex] = output_counter[taskIndex] + 1;
								}
							}
							else if(lom_approach.Communcation_Message[taskIndex + 1].contains("vehicle_status_service"))
							{
								//cloudServiceURL = "http://203.135.63.70/CloudServices/services/GPSSimulation";
								cloudServiceURL = sService_URI;

								task_id = task_id + 1;
								
								if (output_counter[taskIndex - 1] > 0)
								{
									HttpResponse<String> response = null;
									
									try
									{
										response = Unirest.post(cloudServiceURL)
										  .header("SOAPAction", "\"\"")
										  .header("Content-Type", "text/plain")
										  .body("<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\">\r\n" + "    <Body>\r\n" + "        <getVehicleStatus xmlns=\"http://incident_management\"/>\r\n" + "    </Body>\r\n" + "</Envelope>")
										  .asString();
									}
									catch (UnirestException e)
									{
										e.printStackTrace();
									}
									
									Document doc = convertStringToDocument(response.getBody());
									
									String outputURL = doc.getElementsByTagName("url").item(0).getChildNodes().item(0).getNodeValue();
									String vehicleStatus = doc.getElementsByTagName("vehicle_status").item(0).getChildNodes().item(0).getNodeValue();

									Helper.openURL(outputURL);
									
									Workflow_Coordinator_Main.Communication_Message_Received = "Done with Execution";
									Workflow_Coordinator_Main.output_counter[taskIndex + 1] = Workflow_Coordinator_Main.output_counter[taskIndex + 1] + 1;

									String taskOutput = "<Task>[" + (Workflow_Coordinator_Main.task_count + 1) + "]</Task> Vehicle_Status_Info =" + vehicleStatus;
									Workflow_Coordinator_Main.Executed_Task_Output[Workflow_Coordinator_Main.task_count] = taskOutput;
									Workflow_Coordinator_Main.Current_Vehicle_Status = taskOutput;
									
									parse_vehicle_status(vehicleStatus);
									
									execution_command_List[taskIndex + 1] = Execution_Command;
									output_counter[taskIndex - 1] = output_counter[taskIndex - 1] - 1;
								
									Workflow_Coordinator_Main.thread_counter = 0;
								}
							}
							else if(lom_approach.Communcation_Message[taskIndex + 1].contains("re_route_vehicle_service"))
							{
								//cloudServiceURL = "http://203.135.63.70/CloudServices/services/TrafficFlowServices";
								cloudServiceURL = sService_URI;
								
								task_id = task_id + 1;
								
								if (output_counter[taskIndex - 1] > 0)
								{
									HttpResponse<String> response = null;
									
									try
									{
										response = Unirest.post(cloudServiceURL)
										  .header("SOAPAction", "\"\"")
										  .header("Content-Type", "text/plain")
										  .body("<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\">\r\n    <Body>\r\n        <re_route_vehicles xmlns=\"http://incident_management\">\r\n            <currentVehicleLocation>30.456048,-91.205240</currentVehicleLocation>\r\n        </re_route_vehicles>\r\n    </Body>\r\n</Envelope>")
										  .asString();
									}
									catch (UnirestException e)
									{
										e.printStackTrace();
									}
									
									Document doc = convertStringToDocument(response.getBody());
									
									String outputURL = doc.getElementsByTagName("re_route_vehiclesReturn").item(0).getChildNodes().item(0).getNodeValue();
									
									Helper.openURL(outputURL);
									
									output_counter[taskIndex - 1] = output_counter[taskIndex - 1] - 1;
								}
							}
						}
					}
				}

				if (taskIndex == (lom_approach.task_Id - 1))
				{
					task_end_time[taskIndex] = System.currentTimeMillis();
				}
			}
			
			for (int taskIndex = 0; taskIndex < lom_approach.task_Id; taskIndex++)
			{
				task_elapsed_time[taskIndex] = task_end_time[taskIndex] - task_start_time[taskIndex];
				task_elapsed_time[taskIndex] = task_elapsed_time[taskIndex] / 1000;
				System.out.println("Task Elapsed Time in seconds =" + task_elapsed_time[taskIndex]);
			}

			/************************************
			 * Paho CLIENT END
			 ***********************************/
		}

		public static Document convertStringToDocument(String xmlStr)
		{
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder;
			try
			{
				builder = factory.newDocumentBuilder();
				Document doc = builder.parse(new InputSource(new StringReader(xmlStr)));
			
				return doc;
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			
			return null;
		}
		
		public static String parseXML(Document doc)
	    {
			doc.getDocumentElement().normalize();	
			NodeList nServiceList = doc.getElementsByTagName("Service"); 
			Node nNode = nServiceList.item(0); 
			Element eElement = (Element) nNode; 
			NodeList childList = eElement.getChildNodes(); 
			Node childNode = childList.item(0);
			String sService_URI = new String() ; 
			
			if(childNode.getNodeName().contains("URI"))
				sService_URI = childNode.getTextContent();
			
			return sService_URI;
	    
	    }
	}
	
	public static void Get_Status(URL vehicle_status_url)
	{
		try
		{
			URLConnection vehicleConnection = vehicle_status_url.openConnection();
			DataInputStream dis = new DataInputStream(vehicleConnection.getInputStream());
			String inputLine;
			String vehicle_status = "";

			while ((inputLine = dis.readLine()) != null)
			{
				vehicle_status = vehicle_status + inputLine;
			}
			
			dis.close();
		}
		catch (MalformedURLException me)
		{
			System.out.println("MalformedURLException: " + me);
		}
		catch (IOException ioe)
		{
			System.out.println("IOException: " + ioe);
		}
	}

	public static String URLConnectionReader(URL vehicle_status_url)
	{
		String inputLine = "";
		String output = "";

		try
		{
			URLConnection yc = vehicle_status_url.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));

			while ((inputLine = in.readLine()) != null)
			{
				output = output + inputLine;
			}
			
			in.close();
		}
		catch (MalformedURLException me)
		{
			System.out.println("MalformedURLException: " + me);
		}
		catch (IOException ioe)
		{
			System.out.println("IOException: " + ioe);
		}
		
		return output;
	}

	public static void parse_vehicle_status(String vehicle_status)
	{
		try
		{
			JSONArray vehicleStatusesJSONArray = new JSONArray(vehicle_status);

			vehicle_count = vehicleStatusesJSONArray.length() + 1;

			for (int i = 0; i < vehicleStatusesJSONArray.length(); i++)
			{
				JSONObject jsonObject = vehicleStatusesJSONArray.getJSONObject(i);

				double latitude = jsonObject.getJSONObject("gpsSimulator").getJSONObject("currentPosition")
						.getJSONObject("position").getDouble("latitude");
				double longitude = jsonObject.getJSONObject("gpsSimulator").getJSONObject("currentPosition")
						.getJSONObject("position").getDouble("longitude");

				vehicle_status_info_latittude[i] = latitude;
				vehicle_status_info_longitude[i] = longitude;
				
				location = new Location(latitude, longitude);

				if (approach.contains("GOM"))
				{
					closest_peer_node[i] = gom_approach.peerNodeSearch(location);
				}
				else
				{
					closest_peer_node[i] = lom_approach.peerNodeSearch(location);
				}
			}

		} catch (JSONException pe)
		{
			System.out.println(pe);
		}
	}

	public static void Stop_Vehicles(URL vehicle_stop_url)
	{
		System.out.println("Stop Vehicles");
		try
		{
			URLConnection vehicleConnection = vehicle_stop_url.openConnection();
			DataInputStream dis = new DataInputStream(vehicleConnection.getInputStream());
			dis.close();
		}
		catch (MalformedURLException me)
		{
			System.out.println("MalformedURLException: " + me);
		}
		catch (IOException ioe)
		{
			System.out.println("IOException: " + ioe);
		}
	}
}

class LogStreamReader_Combine implements Runnable
{
	private BufferedReader reader;
	int taskID;
	public static String vehicle_status_output = "";
	int flag = 0;

	public LogStreamReader_Combine(InputStream is, int taskID)
	{
		this.reader = new BufferedReader(new InputStreamReader(is));
		this.taskID = taskID;
	}

	public void run()
	{
		try
		{
			String line = reader.readLine();
			String output = "";
			output = output + line;
			
			while (line != null)
			{
				output = output + line;

				Workflow_Coordinator_Main.task_output = output;
				
				if (line.contains("Started GpsSimulatorApplication"))
				{
					try
					{
						Thread.sleep(1000);
						Helper.openURL("http://localhost:8080/api/dc");
						Thread.sleep(5000);
						Helper.openURL("plume_display.html");
						Thread.sleep(5000);
						Workflow_Coordinator_Main.Communication_Message_Received = "Done with Execution";
						Workflow_Coordinator_Main.output_counter[taskID] = Workflow_Coordinator_Main.output_counter[taskID] + 1;
					}
					catch (InterruptedException e)
					{
						System.out.println("Error occured while executing Linux command. Error Description: " + e.getMessage());
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}

				if (line.contains("GenericMessage [payload=PositionInfo") && flag == 0)
				{
					try
					{
						URL vehicle_status_url = new URL("http://localhost:8080/api/status");
						String vehicle_status = Workflow_Coordinator_Main.URLConnectionReader(vehicle_status_url);
						String taskOutput = "<Task>[" + (Workflow_Coordinator_Main.task_count + 1) + "]</Task> Vehicle_Status_Info =" + vehicle_status;
						Workflow_Coordinator_Main.Executed_Task_Output[Workflow_Coordinator_Main.task_count] = taskOutput;
						Workflow_Coordinator_Main.Current_Vehicle_Status = taskOutput;
						flag = 1;
						URL vehicle_stop_url = new URL("http://localhost:8080/api/cancel");
						Workflow_Coordinator_Main.Stop_Vehicles(vehicle_stop_url);
						Thread.sleep(30000);
						Workflow_Coordinator_Main.parse_vehicle_status(vehicle_status);
					}
					catch (MalformedURLException me)
					{
						System.out.println("MalformedURLException: " + me);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}

				line = reader.readLine();
			}
			
			Workflow_Coordinator_Main.thread_counter = 0;

			Workflow_Coordinator_Main.Executed_Task_Output[Workflow_Coordinator_Main.task_count] = output;
			reader.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}

class CloudServiceHandler implements Runnable
{
	String jarFilePath;
	Thread cloudServiceThread;
	int taskID;

	CloudServiceHandler(String jarFilePath, int taskID)
	{
		this.taskID = taskID;
		this.jarFilePath = jarFilePath;
		cloudServiceThread = new Thread(this, jarFilePath);
		cloudServiceThread.start();
	}

	public void run()
	{
		String command = Workflow_Coordinator_Main.Execution_Command;

		boolean waitForResponse = false;

		ProcessBuilder pb = new ProcessBuilder(command);
		pb.redirectErrorStream(true);

		try
		{
			Process shell = pb.start();
			InputStream shellIn = shell.getInputStream();
			LogStreamReader_Combine lsr = new LogStreamReader_Combine(shellIn, taskID);
			Thread thread = new Thread(lsr, "LogStreamReader_Combine");
			thread.start();

			if (waitForResponse)
			{
				// Wait for the shell to finish and get the return code
				shell.waitFor();

				shellIn.close();
			}
		}
		catch (IOException e)
		{
			System.out.println("Error occured while executing Linux command. Error Description: " + e.getMessage());
		}
		catch (InterruptedException e)
		{
			System.out.println("Error occured while executing Linux command. Error Description: " + e.getMessage());
		}

		System.out.println(jarFilePath + " exiting.");
	}
}