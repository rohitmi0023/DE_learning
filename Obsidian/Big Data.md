
# Overview
Which one to choose?
goal of data analysis pe depend karta hei- 
	agar ML algo yaa large scale data hei then Spark.
	Agar Giant data ko store and process karna hei then Hadoop. Cost effective, scalable bhi hei
	Obviously, combination bhi use kar sakte hei. Jaise ki agar bhut large data hei tab for batch processing, Hadoop use karenge phir aage ke real-time or graph analytics ke liye Spark.

3V's - Volume, Variety, Velocity
Cluster - bhut saare machines rakhenge jisme data batches/parts mei rahenge. Yeh collection of machines ko cluster kehte hei.
Hive - SQL wrapper of MapReduce
Pig - Any programming language of MapReduce
Impala - SQL directly HDFS mei stored data pe run karke result deta hei
Sqoop - Data transfer from RDBMS to HDFS karta hei
Flume - CDC data ingestion to HDFS
HBase- HDFS ke upar RDBMS
Hue- Cluster ke insights ko fronted mei dekhte hei
Oozie - workflow management tool
Mahout - ML Library

# HDFS and MapReduce
HDFS mei files store hota hei. Jab hum log ek file upload karte hei to HDFS tab har ek file ke block ek node of a cluster mei store hota hei. Har ek node mei ek software process running hota hei called Daemon/DataNode.
Machine/NameNode - metadata of files jaise ki har ek node mei kiske blocks hei.
DataNode Data Redundancy- to remove strong dependency on data storing node, each block gets stored in 3 different NameNodes.
NameNode High Availability - Two ways- 1. NFS mei replica of NameNode contents. 2. NameNode Standby.

MapReduce -  Ek big file ko smaller chunks mei convert karta hei jinka parallel processing hota hei.
Daemon of MR- Jab hum MR run karte hei tab job ko JobTracker pe submit kiya jaata hei, jo ki job ko mappers and reducers mei distribute karta hei. Yeh mappers and reducers will run on other cluster nodes.
TaskTracker- Running the actual mappers and reducers. Yeh har ek nodes pe run hota hei. Mappers and Reducers mostly usi nodes ke data ko handle karta hei.

MR Design Patterns:
1. Filter Patterns
	1. a. Simple Filter
	2. Bloom filter
	3. Sampling
2. Summarization Patterns
	1. Inverted Index
	2. Numerical Summarizations
3. Structural Patterns
	1. RDBMS -> Haddoop  

# Hadoop v/s Spark
Both are distributed systems matlab data aur processing alag-alag computers(nodes) pe distribute hoti hei, ek saath kaam karte hei.
1. **Hadoop (Batch Processing):**
       - **Data Storage (HDFS):** Data ko chunks mein divide karke multiple machines pe store karta hai.    
    - **Processing (MapReduce):** Ek baar mein bada data process karta hai, lekin slow (disk-based).
    - Example: Raat bhar chalta hai, reports generate karta hai.
2. **Spark (Fast Processing):**
    - **In-Memory Processing:** Data ko RAM mein rakhta hai, isliye Hadoop se 100x faster.
    - **Real-time/Batch Dono:** Live data (e.g., Twitter trends) aur bada data dono handle karta hai.
    - Example: Instant results chahiye (jo Hadoop slow karega).
**Simple Analogy:**
- **Hadoop = Bullock Cart** (Bada load, slow but reliable).    
- **Spark = Bullet Train** (Fast, expensive RAM use karta hai).
# Apache Spark
* yeh RDD system se data process karta hei
* Yeh apne hee software mei with the help of RAM apna data process and temporarily store karke immediatedly information access karta hei.
* Components iske:
	* Spark Core
	* Spark SQL
	* Spark Streaming
	* Structured Streaming
	* ML lib
	* GraphX