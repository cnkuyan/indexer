# Financial Indexer

This app provides an API, for the clients to query real-time price statistics of financial instruments received within the last 60 seconds (sliding time interval).

There are three API endpoints:

• POST /ticks  :  This one is called by the clients to register a Tick. It is also the sole input of this rest API
   
• GET /statistics : This one returns the statistics based on the ticks of all instruments of the last 60 seconds (sliding time interval)

• GET /statistics/{instrument_id} : This one returns the statistics based on the ticks of the given instrument of the last 60 seconds (sliding time interval)


### Configuring the Sliding Time Window Period:
 
 The application has been configured with a default value of 60 seconds for the sliding time interval.
 In order to change it to another value,  please update the constant value SLIDINGWINDOW_DURATION_SECS defined in IndexerAppllication.java
 
 

### Prerequisites:

You need Maven, Git and  Java SE Runtime Environment 11 (minimum)  to run this project. 

```
https://www.oracle.com/java/technologies/javase-jdk11-downloads.html
https://maven.apache.org/install.html
https://git-scm.com
```

### Installing and Running the Tests:

Clone repository to your local machine

```
git clone git@github.com:cnkuyan/indexer.git
```

Go to project folder

```
cd indexer
```

Run maven to create jar ( Without the last parameter to run the tests at this point)

```
mvn clean install package -DskipTests
```


### Running the app:
```
java -jar target/indexer-0.0.1-SNAPSHOT.jar
```


### Interacting with the App:

The following will be carried using the `curl` tool.

#### Sending data with curl
```
curl http://localhost:8080/ticks --header "Content-Type: application/json"  --request POST  --data '{ "instrument": "IBM", "timestamp": 1617473494140, "price": 130.1 }'
```

#### Querying the Statistics of all instruments:
```
curl http://localhost:8080/statistics
```

#### Querying the Statistics of a Specific instrument:
```
curl http://localhost:8080/statistics/{instrument_id}
```

### Built With

* [SpringBoot](https://projects.spring.io/spring-boot/) - The web framework used for creating APIs
* [Maven](https://maven.apache.org/) - Dependency Management
 

### Author

* **Cenk Uyan** - [github/cnkuyan](https://github.com/cnkuyan)


