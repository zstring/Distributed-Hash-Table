#Distributed Hash Table 

This Application support all DHT functionalities and support insert and query operations. Thus, if we run multiple instances of the app, all content provider instances form a Chord ring and serve insert/query requests in a distributed fashion according to the Chord protocol.

Three Main Functionalities are implemented<br/>
 1) ID space partitioning/re­partitioning,<br/> 
 2) Ring­based routing, and <br/>
 3) Node joins. <br/>
 
Used the "SHA­1" hash function to partition the space based on the emulator-ID.

References:<br/>
<a href="http://www.cse.buffalo.edu/~stevko/courses/cse486/spring15/files/chord_sigcomm.pdf">Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications</a><br/>
<a href="http://www.cse.buffalo.edu/~stevko/courses/cse486/spring15/lectures/14-dht.pdf">Lecture Notes by Prof Steve Ko</a>
