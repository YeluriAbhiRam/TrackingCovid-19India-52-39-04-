# TrackingCovid-19India_52-39-04_Hadoop
This Repository consists of an analysis done on the spread of COVID-19 in India.  

CovidStats.java is the algorithm applied on the input datasets to generate various outputs.  

Installation and Running Process:  
1.Install Virtual Box on your computer.  
2.Once the Virtual Box is Installed,the Virtual machine can be downloaded from the following link-  
https://drive.google.com/open?id=0B2vqFbCIJR_USXBzNVZYZGloOVU  
3.Name the downloaded image as 'Hadoop.vdi'.  
4.Create a New Virtual Machine in VirtualBox using the uncompressed VDI file as the Hard Drive.  
-Run VirtualBox  
-Click the "New" button  
-Enter any name  
-Select "Linux" in the OS type dropdown  
-Select "Next"  
-On the "Memory" panel choose aroung 4 gb memory and click "Next".  
-On the Virtual Hard Disk” panel select “Existing” – this opens the VirtualBox Virtual Disk Manager”  
-Select the “Add” button.  
-Select the “harddisk which you have downloaded” ( in this case it should be Hadoop.vdi) file.  
-Click "Select"  
-Click "Next"    
-Click "Finished"  
-Click RUN to Start the VM (you should see Ubuntu running)  
-Use username as woir and password as abcd1234 whenever required.  
5.Now Setup the Java classpath  
6.Open Terminal  
7.Start hadoop by start_hadoop.sh command.  
8.Now place the CSVReader jar file in any place and edit the classpath and soureme file  
9.Now add the CovidStats.java in some file in  home/examples/  
10.Place the input dataset in the same file.  
  
Following are the commands to be entered:(if NewAlgo is the file containing all the java code and inputs)  
  
source /home/woir/sourceme  
  
/home/woir/stop_all.sh

/home/woir/start_hadoop.sh

jps

cd /home/woir/example/NewAlgo/

dos2unix *.csv

hadoop dfs -rm /user/woir_hadoop/input/*

hadoop dfs -copyFromLocal ~/example/NewAlgo/newdatatset.csv  /user/woir_hadoop/input

hadoop dfs -rm -r /user/woir_hadoop/output/*

rm -rf build

mkdir build

javac -d build CovidStats.java

jar -cvf covid.jar -C build/ .

hadoop jar covid.jar org.myorg.CovidStats /user/woir_hadoop/input/ /user/woir_hadoop/output/

hadoop dfs -copyToLocal /user/woir_hadoop/ /home/woir/Downloads/CovidStats

You can check the HDFS using the below link-
http://localhost:50070/explorer.html#/user/woir_hadoop/output

CovidStats folder consists of all the java code,class files,input datasets and the outpts generated  
Documentation folder consists of the documentation of the project  
Graphs folder consists of all the grpahs generated on application of output using matplotlib  
Implementation Videos folders consists of all the videos of the implementation  
jarFiles folder consists of all the required jar files for the project  
Output screenshots are present in the output screenshots folder  
