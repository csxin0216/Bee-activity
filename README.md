# Bee Activity

要求用 scala 和 kafka 实现以下 4 个蜜蜂活动，过程中需要用到 docker 和 Postgres 数据库。<br>

假设中央接收器发布着陆Kafka主题“events”上的事件数据。每一个发表事件具有蜜蜂id，着陆时间戳(舍入到最接近的秒)，以及着陆点的坐标。<br>
* 任务1: 模拟蜜蜂的着陆事件，并在Kafka的“事件”主题上发布它们。<br>
* 任务2: 使用 KStreams DSL，实现计算管道每个时间窗口中每个方格中蜜蜂降落的数量。<br>
* 任务3: 使用 KStreams DSL，实现计算长途旅行的蜜蜂<br>
* 任务4: 把所有的长途旅行者保存到 Postgres 数据库的“长途旅行者”表中<br>

该项目中，我写了 scala 文件和 test 文件。方便起见，我将4个测试的结果导出为**"Test results - TestSolution.html"**并上传。在下载并解压缩整个文件后，在web浏览器中打开HTML文件，单击右上角的“Expand”按钮，即可查看4个测试的详细输出结果。<br>


# How to run it?

## Run the 4 tests

(1) In this project, I have written 4 tests in TestSolution.scala located in src/test/scala. Among them, tests 1-3 can be run directly, while test4 requires opening centos/postgresql-12-centos8 in Docker Desktop, with the PostgreSQL username, password, and database set to "scalauser", "pwd123", and "population", respectively. The configuration for test4 is the same as that of assignment4. <br>

(2) For convenience, I have exported the results of the 4 tests as **"Test Results - TestSolution.html"** as uploaded. After Downloading and unzipping the whole file, open the HTML file in your web browser. Then, click on the "Expand" button in the upper right corner to view the detailed output results for the 4 tests.

## Run the 4 scala files

(1) Also, you can run the 4 scala files by openning the aforementioned PostgreSQL as well as ZooKeeper and Kafka in the Docker Desktop, and then sequentially running the 4 scala files under src/ main/scala. <br>
<br>
<br>

# IT5100B 2023 - Project 2

## Problem Statement

Consider the following experiment. A bee population (10 - 100 individuals) is fitted
with sensors and confined in a rectangular area of size `W x H` length units. The bees fly from
(say) flower to flower (we consider the flowers to have zero height), 
alternating between periods of flight and periods of being
at rest. The sensors are able to sense when the bee has landed and
is at rest, and sends a wireless signal to a central receiver. The sensors cannot
sense the resumption of flight, since that event is not of interest to us.

The sensor signal emitted when a bee lands
is converted by the central receiver into a piece of data with the
following attributes:
  * bee's id (as a string -- preferably a GUID)
  * the current timestamp, expressed as *epoch* time (i.e. number of seconds since
     January 1, 1970, 00:00 GMT)
  * `x` and `y` coordinates of the landing spot

To define the objectives of this problem, we will introduce a few conventions:

  * The rectangular area of confinement is divided into squares of `1 x 1` each. 
    Each square is identified by the pair of coordinates corresponding to its
    bottom-left corner.

  * Moreover, time is divided into `T`-second intervals (or *windows*); 
    each window ends on a timestamp that is a multiple of `T`.
    For instance, if `T` is 900 (i.e. 15 min), then there would be a window ending
    at timestamp 1648054800, another one ending at 1648055700 (= 1648054800 + 900), 
    and so on.

  * A bee that has landed on more than `K` squares is a *long-distance traveller*.


Considering `W`, `H`, `T` and `K` as inputs to our problem, the objectives of
this project are as follows:

  * Compute the number of bees that have landed in each square during each time interval.
    At the end of each time window, publish the number of bees for each square on a given
    topic (see the implementation section).
    The computation has to appear to be performed in *real-time*, that is, the output 
    corresponding to each time window has to happen as soon as possible after the window's 
    expiration time.

  * Detect the long-distance travellers and publish their IDs on a topic. Also save these
    IDs into a table in a database.

## Implementation

Assume that the central receiver publishes the landing
event data on a Kafka topic called `events`. Each published
event has the bee id, the landing timestamp (rounded down to the nearest second), 
and the coordinates of the landing point, all in CSV format.

  * **Task 1:** simulate the bees' landing events and publish them on the `events` topic on Kafka.

    * Start with a timestamp close/equal to the current time.
    * Generate random bee ID, `x` and `y` coordinates, within the given limits.
    * Publish the event (ID, timestamp, `x`, `y`) on the `events` topic.
    * Advance the timestamp by a random amount of time (possibly 0).
    * Repeat from second step.
    * Let this process run forever.
    * Place the code into the `GenerateBeeFlight.scala` file.

  * **Task 2:** using the `KStreams` DSL, implement a pipeline that computes
    the number of bee landings in each square, for each time window.

    * You may find the concept of *window* and *window aggregation* useful here.
    * You may define auxiliary topics if you feel they would be helpful.
    * Publish your output counts to the topic `bee-counts`.
    * Place your code into the `CountBeeLandings.scala`.

  * **Task 3:** using the `KStreams` DSL, implement a pipeline that detects
    long-distance travellers.

    * Publish long-distance travellers on the `long-distance-travellers` topic.
    * You must publish such travellers exactly once, as soon as they are detected.
    * Place your code into the `LongDistanceFlyers.scala` file.

  * **Task 4:** save all the long-distance travellers into the table `longdistancetravellers`
    in a Postgres DB.

    * Place your code into the `SaveLongDistanceFlyers.scala` file.

Do not forget to write tests that prove that your code is working properly.
