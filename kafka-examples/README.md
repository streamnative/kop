This directory contains examples of client code that uses kafka.

To run the demo:

   1. Start kafka brokers.
   2. Set parameters in file `KafkaProperties`,  build this sub-project by `mvn package`.
   3. For unlimited producer run, `bin/java-producer-demo.sh`.
   4. For unlimited consumer run, `bin/java-consumer-demo.sh`.
   5. Or run producer and consumer together, for unlimited producer-consumer run, `bin/java-producer-consumer-demo.sh`.
