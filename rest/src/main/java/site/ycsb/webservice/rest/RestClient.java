/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.webservice.rest;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.ThreadLocalRandom;

import javax.ws.rs.HttpMethod;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Class responsible for making web service requests for benchmarking purpose.
 * Using Apache HttpClient over standard Java HTTP API as this is more flexible
 * and provides better functionality. For example HttpClient can automatically
 * handle redirects and proxy authentication which the standard Java API can't.
 */
public class RestClient extends DB {

  private static final String URL_PREFIX = "url.prefix";
  private static final String URL_PREFIX_LOCAL = "url.prefix_local";
  private static final String CON_TIMEOUT = "timeout.con";
  private static final String READ_TIMEOUT = "timeout.read";
  private static final String EXEC_TIMEOUT = "timeout.exec";
  private static final String LOG_ENABLED = "log.enable";
  private static final String HEADERS = "headers";
  private static final String USERS = "users";
  private static final String USERS_LOCAL = "users_local";
  private static final String IS_APPSERVICE = "isappservice";
  private static final String IS_MICROSERVICE = "ismicroservice";
  private static final String COMPRESSED_RESPONSE = "response.compression";
  private boolean compressedResponse;
  private boolean logEnabled;
  private boolean isAppService;
  private boolean isMicroservice;
  private String urlPrefix;
  private Properties props;
  private String[] headers;
  private String[] users;
  private CloseableHttpClient client;
  private int conTimeout = 10000;
  private int readTimeout = 10000;
  private int execTimeout = 10000;
  private volatile Criteria requestTimedout = new Criteria(false);

  @Override
  public void init() throws DBException {
    props = getProperties();
    isAppService = Boolean.valueOf(props.getProperty(IS_APPSERVICE, "false").trim());
    isMicroservice = Boolean.valueOf(props.getProperty(IS_MICROSERVICE, "false").trim());
    urlPrefix = isAppService ? props.getProperty(URL_PREFIX_LOCAL, "http://127.0.0.1:8080")
        : props.getProperty(URL_PREFIX, "");
    conTimeout = Integer.valueOf(props.getProperty(CON_TIMEOUT, "10")) * 1000;
    readTimeout = Integer.valueOf(props.getProperty(READ_TIMEOUT, "10")) * 1000;
    execTimeout = Integer.valueOf(props.getProperty(EXEC_TIMEOUT, "10")) * 1000;
    logEnabled = Boolean.valueOf(props.getProperty(LOG_ENABLED, "false").trim());
    compressedResponse = Boolean.valueOf(props.getProperty(COMPRESSED_RESPONSE, "false").trim());
    headers = props.getProperty(HEADERS, "Accept */* Content-Type application/xml user-agent Mozilla/5.0 ").trim()
        .split(" ");
    users = props.getProperty(USERS, "[]").trim().split(",");
    // for (String u : users) {
    // System.err.println("User: " + u);
    // }
    setupClient();
  }

  private void setupClient() {
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder = requestBuilder.setConnectTimeout(conTimeout);
    requestBuilder = requestBuilder.setConnectionRequestTimeout(readTimeout);
    requestBuilder = requestBuilder.setSocketTimeout(readTimeout);
    HttpClientBuilder clientBuilder = HttpClientBuilder.create().setDefaultRequestConfig(requestBuilder.build());
    this.client = clientBuilder.setConnectionManagerShared(true).build();
  }

  @Override
  public Status read(String table, String endpoint, Set<String> fields, Map<String, ByteIterator> result) {
    int responseCode;
    if (isMicroservice) {
      // Add a random userId to the endpoint if it is a request that requires it
      if (endpoint.startsWith("dashboard")) {
        endpoint += "&userId=" + users[ThreadLocalRandom.current().nextInt(0, users.length)];
      } else if (endpoint.equals("meals-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Meals")
          || endpoint.equals("workouts-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Workouts")
          || endpoint
              .equals("measurements-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Measurements")) {
        endpoint += "?userId=" + users[ThreadLocalRandom.current().nextInt(0, users.length)];
      }
    } else {
      // Add a random userId to the endpoint if it is a request that requires it
      if (endpoint.startsWith("Dashboard")) {
        endpoint += "&userId=" + users[ThreadLocalRandom.current().nextInt(0, users.length)];
      } else if (endpoint.equals("Meals")
          || endpoint.equals("Workouts")
          || endpoint.equals("Measurements")) {
        endpoint += "?userId=" + users[ThreadLocalRandom.current().nextInt(0, users.length)];
      }
    }
    try {
      responseCode = httpGet(urlPrefix + endpoint, result);
    } catch (Exception e) {
      responseCode = handleExceptions(e, urlPrefix + endpoint, HttpMethod.GET);
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("GET Request: ").append(urlPrefix).append(endpoint)
          .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status insert(String table, String endpoint, Map<String, ByteIterator> values) {
    String data = "";
    if (endpoint.equals("Exercises")
        || endpoint.equals("workouts-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Exercises")) {
      int workoutId = ThreadLocalRandom.current().nextInt(1, 34);
      data = "{\"name\": \"YCSB Exercise (INSERTED)\", \"workoutId\": " + workoutId
          + "," +
          "\"defaultNumberOfSets\": 6, \"targetMuscle\": \"YCSBMuscle (INSERTED)\"}";

    } else if (endpoint.equals("Meals")
        || endpoint.equals("meals-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Meals")) {

      data = "{\"category\": \"Lunch\", \"totalCalories\": 1000," +
          "\"protein\": 100, \"carbohydrates\": 100, \"fats\": 20, \"date\": \"2023-03-26T22:56:53.217Z\", \"applicationUserId\": \""
          + users[ThreadLocalRandom.current().nextInt(0, users.length)] + "\"}";

    } else if (endpoint.equals("Measurements") || endpoint
        .equals("measurements-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Measurements")) {

      data = "{\"type\": \"Waist\", \"value\": 90, \"unit\": \"cm\", \"date\": \"2023-03-26T22:56:53.217Z\", \"applicationUserId\": \""
          + users[ThreadLocalRandom.current().nextInt(0, users.length)] + "\"}";

    } else if (endpoint.equals("Workouts") || endpoint
        .equals("workouts-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Workouts")) {

      data = "{\"name\": \"YCSB Workout (INSERTED)\", \"dayOfWeek\": \"Saturday\", \"dateLastCompleted\": \"2023-03-26T22:56:53.217Z\", \"applicationUserId\": \""
          + users[ThreadLocalRandom.current().nextInt(0, users.length)]
          + "\", \"exercises\": [], \"trackedWorkouts\": []}";

    } else if (endpoint.equals("TrackedWorkouts") || endpoint
        .equals("workouts-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/TrackedWorkouts")) {
      int workoutId = ThreadLocalRandom.current().nextInt(1, 34);
      data = "{\"notes\": \"This tracked workout has been INSERTED by YCSB running tests\", \"startTime\": \"2023-03-26T20:56:53.217Z\", \"endTime\": \"2023-03-26T22:56:53.217Z\", \"totalVolume\": 1234, \"applicationUserId\": \""
          + users[ThreadLocalRandom.current().nextInt(0, users.length)]
          + "\", \"workoutId\": " + workoutId
          + ", \"exerciseSetsCompleted\": [], \"isCompleted\": false}";

    } else if (endpoint.equals("ExerciseSets/range") || endpoint
        .equals("workouts-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/ExerciseSets/range")) {
      int trackedWorkoutId = ThreadLocalRandom.current().nextInt(1, 34);
      int exerciseId = ThreadLocalRandom.current().nextInt(1, 100);
      data = "[{\"exerciseId\": " + exerciseId
          + ", \"exerciseName\": \"YCSB Exercise (INSERTED)\", \"reps\": 20, \"weight\": 100, \"isWarmup\": true, \"trackedWorkoutId\": "
          + trackedWorkoutId + ", \"isComplete\": false},"
          +
          "{\"exerciseId\": " + exerciseId
          + ", \"exerciseName\": \"YCSB Exercise (INSERTED)\", \"reps\": 20, \"weight\": 100, \"isWarmup\": true, \"trackedWorkoutId\": "
          + trackedWorkoutId + ", \"isComplete\": false}]";
    } else {
      throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }

    int responseCode;
    try {
      responseCode = httpExecute(new HttpPost(urlPrefix + endpoint), data);
    } catch (Exception e) {
      responseCode = handleExceptions(e, urlPrefix + endpoint, HttpMethod.POST);
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("POST Request: ").append(urlPrefix).append(endpoint)
          .append(" | Response Code: ").append(responseCode).toString());
    }
    return

    getStatus(responseCode);
  }

  @Override
  public Status delete(String table, String endpoint) {
    int responseCode;
    try {
      responseCode = httpDelete(urlPrefix + endpoint);
    } catch (Exception e) {
      responseCode = handleExceptions(e, urlPrefix + endpoint, HttpMethod.DELETE);
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("DELETE Request: ").append(urlPrefix).append(endpoint)
          .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status update(String table, String endpoint, Map<String, ByteIterator> values) {
    int responseCode;

    String data = "";
    if (endpoint.startsWith("Exercises")
        || endpoint.startsWith("workouts-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Exercises")) {
      int workoutId = ThreadLocalRandom.current().nextInt(1, 34);
      data = "{\"name\": \"YCSB Exercise (UPDATED)\", \"workoutId\": " + workoutId
          + "," +
          "\"defaultNumberOfSets\": 6, \"targetMuscle\": \"YCSBMuscle (UPDATED)\"}";
    } else if (endpoint.startsWith("Meals") || endpoint
        .startsWith("meals-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Meals")) {
      data = "{\"category\": \"Lunch\", \"totalCalories\": 1000," +
          "\"protein\": 100, \"carbohydrates\": 100, \"fats\": 20, \"date\": \"2023-03-26T22:56:53.217Z\"}";
    } else if (endpoint.startsWith("Measurements") || endpoint
        .startsWith("measurements-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Measurements")) {
      data = "{\"type\": \"Body fat\", \"value\": 22, \"unit\": \"%\", \"date\": \"2023-03-26T22:56:53.217Z\"}";
    } else if (endpoint.startsWith("Workouts") || endpoint
        .startsWith("workouts-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/Workouts")) {
      data = "{\"name\": \"YCSB Workout (UPDATED)\", \"dayOfWeek\": \"Sunday (UPDATED)\", \"dateLastCompleted\": \"2023-03-26T22:56:53.217Z\"}";
    } else if (endpoint.startsWith("TrackedWorkouts") || endpoint
        .startsWith("workouts-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/TrackedWorkouts")) {
      int workoutId = ThreadLocalRandom.current().nextInt(1, 34);
      data = "{\"notes\": \"This tracked workout has been UPDATED by YCSB running tests\", \"startTime\": \"2023-03-26T20:56:53.217Z\", \"endTime\": \"2023-03-26T22:56:53.217Z\", \"totalVolume\": 1234, \"workoutId\": "
          + workoutId + "}";
    } else if (endpoint.startsWith("ExerciseSets") || endpoint
        .startsWith("workouts-api.salmonisland-f0d5c65e.northeurope.azurecontainerapps.io/api/ExerciseSets")) {
      int trackedWorkoutId = ThreadLocalRandom.current().nextInt(1, 34);
      int exerciseId = ThreadLocalRandom.current().nextInt(1, 100);
      data = "{\"exerciseId\": " + exerciseId
          + ", \"exerciseName\": \"YCSB Exercise (UPDATED)\", \"reps\": 20, \"weight\": 100, \"isWarmup\": true, \"trackedWorkoutId\": "
          + trackedWorkoutId + ", \"isComplete\": false}";
    } else {
      throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }
    try {
      responseCode = httpExecute(new HttpPut(urlPrefix + endpoint), data);
    } catch (Exception e) {
      responseCode = handleExceptions(e, urlPrefix + endpoint, HttpMethod.PUT);
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("PUT Request: ").append(urlPrefix).append(endpoint)
          .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  // Maps HTTP status codes to YCSB status codes.
  private Status getStatus(int responseCode) {
    int rc = responseCode / 100;
    if (responseCode == 400) {
      return Status.BAD_REQUEST;
    } else if (responseCode == 403) {
      return Status.FORBIDDEN;
    } else if (responseCode == 404) {
      return Status.NOT_FOUND;
    } else if (responseCode == 501) {
      return Status.NOT_IMPLEMENTED;
    } else if (responseCode == 503) {
      return Status.SERVICE_UNAVAILABLE;
    } else if (rc == 5) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  private int handleExceptions(Exception e, String url, String method) {
    if (logEnabled) {
      System.err.println(new StringBuilder(method).append(" Request: ").append(url).append(" | ")
          .append(e.getClass().getName()).append(" occured | Error message: ")
          .append(e.getMessage()).toString());
    }

    if (e instanceof ClientProtocolException) {
      return 400;
    }
    return 500;
  }

  // Connection is automatically released back in case of an exception.
  private int httpGet(String endpoint, Map<String, ByteIterator> result) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    HttpGet request = new HttpGet(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    HttpEntity responseEntity = response.getEntity();
    // If null entity don't bother about connection release.
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      /*
       * TODO: Gzip Compression must be supported in the future. Header[]
       * header = response.getAllHeaders();
       * if(response.getHeaders("Content-Encoding")[0].getValue().contains
       * ("gzip")) stream = new GZIPInputStream(stream);
       */
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      while ((line = reader.readLine()) != null) {
        if (requestTimedout.isSatisfied()) {
          // Must avoid memory leak.
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          client.close();
          throw new TimeoutException();
        }
        responseContent.append(line);
      }
      timer.interrupt();
      result.put("response", new StringByteIterator(responseContent.toString()));
      // Closing the input stream will trigger connection release.
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    client.close();
    return responseCode;
  }

  private int httpExecute(HttpEntityEnclosingRequestBase request, String data) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data.getBytes()),
        ContentType.APPLICATION_FORM_URLENCODED);
    reqEntity.setChunked(true);
    request.setEntity(reqEntity);
    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    HttpEntity responseEntity = response.getEntity();
    // If null entity don't bother about connection release.
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      if (compressedResponse) {
        stream = new GZIPInputStream(stream);
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      while ((line = reader.readLine()) != null) {
        if (requestTimedout.isSatisfied()) {
          // Must avoid memory leak.
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          client.close();
          throw new TimeoutException();
        }
        responseContent.append(line);
      }
      timer.interrupt();
      // Closing the input stream will trigger connection release.
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    client.close();
    return responseCode;
  }

  private int httpDelete(String endpoint) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    HttpDelete request = new HttpDelete(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    response.close();
    client.close();
    return responseCode;
  }

  /**
   * Marks the input {@link Criteria} as satisfied when the input time has
   * elapsed.
   */
  class Timer implements Runnable {

    private long timeout;
    private Criteria timedout;

    public Timer(long timeout, Criteria timedout) {
      this.timedout = timedout;
      this.timeout = timeout;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(timeout);
        this.timedout.setIsSatisfied(true);
      } catch (InterruptedException e) {
        // Do nothing.
      }
    }

  }

  /**
   * Sets the flag when a criteria is fulfilled.
   */
  class Criteria {

    private boolean isSatisfied;

    public Criteria(boolean isSatisfied) {
      this.isSatisfied = isSatisfied;
    }

    public boolean isSatisfied() {
      return isSatisfied;
    }

    public void setIsSatisfied(boolean satisfied) {
      this.isSatisfied = satisfied;
    }

  }

  /**
   * Private exception class for execution timeout.
   */
  class TimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TimeoutException() {
      super("HTTP Request exceeded execution time limit.");
    }

  }

}
