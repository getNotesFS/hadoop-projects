

# ---------------------- COPIAR ARCHIVOS DE LOCAL A HDFS CON PUT -----------------------


hdfs dfs -mkdir /input_ec 
hdfs dfs -put /mnt/shared_folder/electriccar/input/*.csv /input_ec


# ---------------------- Compilar los archivos java, ejecutar trabajo en Hadoop y leer salida -----------------------
# ElectricCar.java

hadoop com.sun.tools.javac.Main ElectricCar.java
jar cf ElectricCar.jar ElectricCar*.class

hadoop jar ElectricCar.jar ElectricCar /input_ec/Electric_Vehicle_Population_Data.csv /output_ec/outputEC

hdfs dfs -cat /output_ec/outputEC/part-r-00000 


# ElectricCarTrend.java

hadoop com.sun.tools.javac.Main ElectricCarTrend.java

jar cf ElectricCarTrend.jar ElectricCarTrend*.class

hadoop jar ElectricCarTrend.jar ElectricCarTrend /input_ec/Electric_Vehicle_Population_Data.csv /output_ec/outputECTrend

hdfs dfs -cat /output_ec/outputECT/part-r-00000


# ElectricCarTypeAnalysis.java

hadoop com.sun.tools.javac.Main ElectricCarTypeAnalysis.java

jar cf ElectricCarTypeAnalysis.jar ElectricCarTypeAnalysis*.class

hadoop jar ElectricCarTypeAnalysis.jar ElectricCarTypeAnalysis /input_ec/Electric_Vehicle_Population_Data.csv /output_ec/outputECTA

hdfs dfs -cat /output_ec/outputECTA/part-r-00000



# Popular Model By Year

hadoop com.sun.tools.javac.Main PopularModelByYear.java

jar cf PopularModelByYear.jar PopularModelByYear*.class

hadoop jar PopularModelByYear.jar PopularModelByYear /input_ec/Electric_Vehicle_Population_Data.csv /output_ec/outputPMBY

hdfs dfs -cat /output_ec/outputPMBY/part-r-00000


# EVCharginStationAnalysis.java

hadoop com.sun.tools.javac.Main EVChargingStationAnalysis.java

jar cf EVChargingStationAnalysis.jar EVChargingStationAnalysis*.class

hadoop jar EVChargingStationAnalysis.jar EVChargingStationAnalysis /input_ec/electric-vehicle-charging-stations.csv /output_ec/outputEVCSA

hdfs dfs -cat /output_ec/outputEVCSA/part-r-00000





# ---------------------- VARIABLES GLOBALES en hduser -----------------------

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-arm64
export HADOOP_HOME=/home/hduser/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin

export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar



# Pig export

export PIG_INSTALL=/usr/local/pig
export PATH=$PATH:/usr/local/pig/bin 

