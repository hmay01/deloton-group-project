# Deloton Exercise Bike Data Integration

<p align="center">
  <img width="1300" alt="image" src="https://user-images.githubusercontent.com/106311108/195805226-08ac28a3-ce2a-4c62-9756-f73b9ede569f.png">
</p>

# Summary of Products

This repository holds the code for **four new products** developed in collaboration with Deloton Exercise Bikes as part of their **recent data integration program**:


## 1. **Heart Reate Alerts**
- Takes advantage of sensors on the bike's handlebars which report heart rate every 0.5 seconds.
- Users receive an alert **email notification within 15 seconds** of an abnormal heart rate.
- The "normal" heart rate range is adaptable for each user, based on their age and weight.
<p align="center">
  <img width="1048" alt="image" src="https://user-images.githubusercontent.com/106311108/195821611-5990de67-c57e-4f23-b31c-fb52627cd8b0.png">
</p>





## 2. **Live Dashboard**
- Available at: http://35.176.74.92:8080/
- Data visualisation of the last 12 hours worth of data for business stakeholders.
- *Current Ride:* **User and ride metrics** which update every second, including **live heart rate screening**.
<p align="center">
  <img width="1419" alt="image" src="https://user-images.githubusercontent.com/106311108/195811651-81a361e1-4bfc-4c59-b2a8-5023a02480f1.png">
</p>

- *Recent Rides:* Recent behaviour of riders at a glance. Including **gender and age distribution** . Updates at the end of each ride. 
<p align="center">
  <img width="1335" alt="image" src="https://user-images.githubusercontent.com/106311108/195813319-ff596fd1-a7fc-4890-809e-49d64e3e7499.png">
</p>

## 3. **Daily Report**
- Consolidates the last 24 hours worth of ride data into a digestible PDF delivered to Deloton's C-suite at the end of every working day.
- Includes number of rides, gender split of riders, ages of riders of the past day, average power usage, and average heart rate of riders.
<p align="left">
  <img width="379" alt="image" src="https://user-images.githubusercontent.com/106311108/195823490-f9921882-304c-46c7-b768-d06ae78e971f.png">
</p>

## 4. **Resful API**
### Overview

Deloton Exercise Bike's API generally follows REST conventions and is a JSON-based API. All requests are made to endpoints beginning:

```bash
http://3.9.134.104:5000
```

(subject to change when pushed to EC2 instance)

The Deloton Exercise Bike's API can be used by staff members at Deloton to perform queries on Snowflake Databases and standardises this process across the company. This allows the Deloton staff to use this API to fetch some data in a secure way.

### HTTP Methods

The Deloton Exercise Bike's API currently accepts the standard HTTP methods: GET and DELETE

Make a  request to retrieve a representation of the specified resource

Make a  request to delete a specified resource.

### Path parameters

Path parameters are variable parts of a url path. This will be represented using  in the documentation. An example of this is:

```bash
http://3.9.134.104:5000/rider/{user_id}/rides
```

A request url can contain several path parameters and the  are replace with the desired value

### Query parameters

Query parameters are appended to a given request url following a  and have a  pair structure. An example of how a query parameter can be used is seen below:
```bash
http://3.9.134.104:5000/daily?date={specified_date}
```

### Endpoints

#### Daily rides

This endpoint returns a JSON object containing all the rides that take place in the current day. An example request looks like this:
```bash
GET http://3.9.134.104:5000/daily
```

The endpoint also has an optional query parameter for the date. Here the  is date and the  is the specified date in the DD-MM-YYYY format. The query parameter returns a JSON object containing all the rides for the specified date. An example request using the query parameter looks like this:
```bash
GET http://3.9.134.104:5000/daily?date=01-01-2020
```

#### Ride based on ID

This endpoint accepts two HTTP methods: DELETE and GET. The endpoint also uses a path parameter for the ride ID. The outcome of the request url will change depending on the HTTP method specified.

- **GET**

This method returns a JSON object containing a ride with the ID specified in the path parameter. An example request looks like this:
```bash
GET http://3.9.134.104:5000/ride/1
```

- **DELETE**

This method deletes a ride with an ID matching the ID specified in the path parameter. This method returns a JSON object to relay if the ride deletion has been successful. An example request looks like this:
```bash
DELETE http://3.9.134.104:5000/ride/1
```

#### Rider based on ID

This endpoint returns a JSON object of the rider information based on the user ID specified in the path parameter. An example of some of the rider information returned includes: name, gender, age, avg. heart rate and number of rides. An example request looks like this:
```bash
GET http://3.9.134.104:5000/rider/5
```

#### Rides for specified rider

This endpoint returns a JSON object containing all the rides for a rider based on the user ID specified in the path parameter. An example request looks like this:
```bash
GET http://3.9.134.104:5000/rider/5/rides
```

#### Running API locally

Run the following commands from the terminal:
```bash
export FLASK_DEBUG=1
export FLASK_APP="app.py"
flask run
```


## Product Deployment

The depoyment of all products relies on **AWS (Amazon's Cloud Computing Ecosystem)**, with all services run out of the **eu-west-2 (London)** region to ensure the lowest possible latency. 

### Other integral technologies:
- **Apache Kafka**: real-time data streaming, as provisioned by Deloton
- **Docker**: elastic container service to enable hassle free, reliable cloud deployment

#### Live program deployment:
E.g. Data Streaming, Heart Rate Alerts, and the Live Dashboard

1. Dockerize programs into images
2. Upload these images to Amazons Elastic Container Registry
3. Run these images on EC2 virtual machines

#### Trigger-based program deployment:
E.g. Daily Report and Database dumps

1. Dockerize programs into images
2. Upload these images to Amazons Elastic Container Registry
3. Assign these images to Lambda functions
4. Assign an SNS (Simple Notication System) topic and trigger to the Lambda functions

### Architectural Diagram
<p align="center">
  <img width="1412" alt="image" src="https://user-images.githubusercontent.com/106311108/195806501-039a22a6-1f25-4c15-b07a-7876588da67f.png">
</p>
#Links to products

this is how it works but not too much detail



# API 

embed

https://github.com/hmay01/deloton-group-project/edit/main/README.md

