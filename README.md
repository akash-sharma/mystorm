# mystorm
This is my new project on apache storm

// build jar

mvn clean install

// deploy storm topology jar

storm jar <jar_file_name> com.mystorm.topology.SimpleConsumerTopology

// kill topology

storm kill SIMPLE_CONSUMER_TOPOLOGY -w 50


### Written two storm topologies : 

(1) SimpleConsumerTopology.java

This is simple topology having 1 kafka spout and 1 kafka bolt

(2) FieldGroupKafkaOutputTopology.java

This is a topology in which I have taken field grouping for a particular type of key (customerId) in kafka packet, and then pushing processed output to another kafka topic.


### Important Notes

=> Grouping Criteria :
Shuffle Grouping - Tuples are randomly distributed across the bolt's tasks in a way such that each bolt is guaranteed to get an equal number of tuples.
Local or shuffle grouping: If the target bolt has one or more tasks in the same worker process, tuples will be shuffled to just those in-process tasks. Otherwise, this acts like a normal shuffle grouping.

Shuffle Grouping randomly distributed tuples across the bolts, If topology is running on multiple workers then it will shuffle tuples across the worker which will increase I/O Operations which will tend to high CPU (concluded by observation) but In case of Local or Shuffle Grouping tuples are shuffled within worker which will reduce I/O operation and CPU utilization.Shuffle Grouping can be useful in case when your are getting large traffic, tuples are highly non-uniformed, spout/bolt task counts are not in proportion.
   
   
=> Number of Ackers :
    A Storm topology has a set of special "acker" tasks that track the DAG of tuples for every spout tuple. When an acker sees that a DAG is complete, it sends a message to the spout task that created the spout tuple to ack the message. You can set the number of acker tasks for a topology in the topology configuration using Config.TOPOLOGY_ACKERS. Storm defaults TOPOLOGY_ACKERS to one task per worker.
    

=> Topology Message Timeout :
    Topology message timeout property defined the maximum time a tuple can take to process If it is not processed in that much time then it will fail the tuple.Storm by default providing 60 seconds time out. If message timeout configured is less then it will lead to high number of fail packets which results topology is having high traffic/rebalancing. We can use default values also.
    

=> Spout/Bolts task count :
    An Important strategy to make topology balanced and proper utilized. Spout/Bolt task count must be in proportion with number of workers. we should keeps these number in such way that load is distributed and balanced.

If we are running topology which is having 2 workers and 1 spout then only 1 worker is having that spout which means one worker is getting all packets, parallalization is not achieved in this case and workers are also not balanced so better spout task count 2 which means each worker is having 1 spout and it will be balanced.
    

=> Wait Strategy :
 There are few topologies where there is high CPU Utilization even during a low traffic time.Each spout/bot polls queue for tuples in regular interval to check if there is any packets to process. In Idle situation CPU consumption is same due to polling in that case wait strategy will come into picture to optimization CPU. wait strategy will increase polling time when there is no packet to process in topology. 
 
 
=> Capacity :   measure how much a spout/bolt task is utilized. If capacity of spout/bolt is greater then 1 means it is over utilized, if it is very less then it is under utilized. In idle case it must be 1. Better keep this value between 0.5 to 0.7.

=> offsets.retention.minutes : Offsets older than this retention period will be discarded

### Good articles with problems : 

http://mail-archives.apache.org/mod_mbox/storm-user/201804.mbox/%3CMMXP123MB11171DAF227A8567A18D862FA4B00@MMXP123MB1117.GBRP123.PROD.OUTLOOK.COM%3E

https://www.programmersought.com/article/43831135194/

https://dev.storm.apache.narkive.com/jBE2Jk6V/jira-created-storm-1017-if-ignorezkoffsets-set-true-kafkaspout-will-reset-zk-offset-when-recover


### Doc links :

https://storm.apache.org/releases/current/Concepts.html

https://storm.apache.org/releases/current/Performance.html

http://storm.apache.org/releases/current/Guaranteeing-message-processing.html


### How to write Storm UTs

https://stackoverflow.com/questions/16549265/testing-storm-bolts-and-spouts

