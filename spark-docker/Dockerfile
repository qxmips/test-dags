FROM bitnami/spark
RUN curl -L http://maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.1/spark-sql-kafka-0-10_2.12-3.1.1.jar --output /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.1.1.jar
RUN curl -L http://maven.org/maven2/org/apache/kafka/kafka-clients/2.7.0/kafka-clients-2.7.0.jar --output /opt/bitnami/spark/jars/kafka-clients-2.7.0.jar
RUN curl -L http://maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.1.1/spark-streaming-kafka-0-10-assembly_2.12-3.1.1.jar --output /opt/bitnami/spark/jars/spark-streaming-kafka-0-10-assembly_2.12-3.1.1.jar
RUN curl -L http://maven.org/maven2/org/apache/kafka/kafka_2.12/2.7.0/kafka_2.12-2.7.0.jar  --output /opt/bitnami/spark/jars/kafka_2.12-2.7.0.jar
RUN curl -L http://maven.org/maven2/org/apache/commons/commons-pool2/2.7.0/commons-pool2-2.7.0.jar --output /opt/bitnami/spark/jars/commons-pool2-2.7.0.jar 