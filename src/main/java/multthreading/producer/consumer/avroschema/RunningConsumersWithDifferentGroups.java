package multthreading.producer.consumer.avroschema;

public class RunningConsumersWithDifferentGroups {
  public static void main(String[] args) throws Exception {
    // INITIALIZE STRING ARRAY OF 3 CONSUMER GROUPS
    String[] consumerGroups = new String[3];
    // LOOP THROUGH AND INCREMENT i VALUE TO CREATE THREE DISTINCT CONSUMER GROUPS
    for (int i = 0; i < consumerGroups.length; i++) {
      consumerGroups[i] ="test-consumer-group-"+i;
    }
    // CALL THE RUN METHOD TO CREATE MULITHREADED CONSUMERS
    RunProducerAndConsumers.run(3, consumerGroups);
  }
}
