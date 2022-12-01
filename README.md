# Yet-Another-Kafka-Big-Data_UE20CS322
## A mini-Kafka System complete with Brokers, producers, consumers and zookeeper
#### Zookeeper managing 3 Brokers (JSON Files), dynamic number of producers (TXT) and consumers (JSON keys)
#### Topics are stored as subdirectories of a file system, along with JSON files containing timestamps on when producers start producing 
## Running Instructions / Description of Files

### zookeeper.y
**Usage-** <br>
"python broker.py" <br>

-Contains the **heartbeat** function that checks on the status of all brokers in the network.
-It is also responsible for selecting the the new lead broker in the event of a failure of the current leader
-Logs all action to zookeeper log

### broker.py
**Usage-** <br>
"python broker.py" <br>

-Calls the functions from zookeeper.py to ensure that all brokers are working <br>
-Displays the status of the current lead broker <br>
-Handles replication of the lead broker across the other 2 brokers <br>
-Contains functions to handle registering and deregistering of producers <br>
-Manages topics and adds new topics as they come through
-Logs all actions to broker log

### producer.py
**Usage-** <br>
"python producer.py <topicName> <fileName>" to registers a new producer to a topic and the content of the message as the file content <br>
"python producer.py -deregister <producerName>" to deregister a producer<br> 
-Contains functions to convert the data into the correct kind of files usable in our progam, and appending them to the producers/ sub-directory <br>
-Also call the deregister function to deregister a producer <br>

### consumer.py
**Usage-** <br>
"python consumer.py" <br>
Opens an interactive interface that allows the user to enter commands <br>
**Commands Accepted** - List present in the main function of consumer.py <br>
-Can Login, logout, register new consumers subscribing to existing and new topics. <br>
-Can view logs of both broker and zookeeper <br>
-Can consume the messages produced for a given topic, before and after (with --from-beginning flag) consumer subscribed to that topic
### killerBroker.py
**Usage-** <br>
"python consumer.py" <br>
-Made to simulate failure of any of the brokers, to showcase our fault tolerance and leader selection program of zookeeper
-Opens an interface where user can enter the name of the broker he wishes to "kill" <br>


