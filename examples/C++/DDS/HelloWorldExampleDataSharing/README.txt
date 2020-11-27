# Overview

To test DataSharing with more cases.

# Usage

Please use "./DDSHelloWorldExample -h" to show the usage.

# Basic Test cases

- Multiple writers and readers in different processes

    ```
    $ ./DDSHelloWorldExampleDataSharing -t publisher -c 10000
    ```

    ```
    $ ./DDSHelloWorldExampleDataSharing -t subscriber
    ```

    ```
    $ ./DDSHelloWorldExampleDataSharing -t publisher -c 10000
    ```

    ```
    $ ./DDSHelloWorldExampleDataSharing -t subscriber
    ```

- Multiple participants in the same process space to use shared memory.

    e.x) Process-A (participant-A Writer-A, Participant-B Reader-B)

    ```
    $ ./DDSHelloWorldExampleDataSharing -t all -c 10000
    ```

- Multiple participants in the different process space to use shared memory on the same topic

    e.x) Process-A (participant-A Writer-A, Participant-B Writer-B) and Process-B (participant-A Reader-A, Participant-B Reader-B) with Topic A only.

    ```
    $ ./DDSHelloWorldExampleDataSharing -t publisher -c 10000 --pubside_participant_count 2 --pubside_writer_count 2
    ```

    ```
    $ ./DDSHelloWorldExampleDataSharing -t subscriber --subside_participant_count 2 --subside_reader_count 2
    ```

- Multiple participants in the different process space to use shared memory on different topic.

    e.x) Process-A (participant-A Writer-A, Participant-B Writer-B) and Process-B (participant-A Reader-A, Participant-B Reader-B) with Topic A and B.

    ```
    $ ./DDSHelloWorldExampleDataSharing -t publisher -c 10000 --pubside_participant_count 2 --pubside_topic_count 2 --pubside_writer_count 2
    ```

    ```
    $ ./DDSHelloWorldExampleDataSharing -t subscriber --subside_participant_count 2 --subside_topic_count 2 --subside_reader_count 2
    ```

# Other more test cases

We can mix the parameter to test more cases, such as

- One participant with (one publisher/multiple writers) and One participant with (one subscriber/multiple readers) in the different process space to use shared memory on the different topic

    ```
    $ ./DDSHelloWorldExampleDataSharing -t publisher -c 10000 --pubside_participant_count 1 --pubside_publisher_count 1 --pubside_topic_count 10 --pubside_writer_count 10
    ```

    ```
    $ ./DDSHelloWorldExampleDataSharing -t subscriber --subside_participant_count 1 --subside_subscriber_count 1 --pubside_topic_count 10 --subside_reader_count 10
    ```

- One participant with (multiple publishers/writers) and One participant with (multiple subscirbers/readers) in the different process space to use shared memory on the different topic

    ```
    $ ./DDSHelloWorldExampleDataSharing -t publisher -c 10000 --pubside_participant_count 10 --pubside_publisher_count 10 --pubside_topic_count 10 --pubside_writer_count 10
    ```

    ```
    $ ./DDSHelloWorldExampleDataSharing -t subscriber --subside_participant_count 10 --subside_subscriber_count 10 --pubside_topic_count 10 --subside_reader_count 10
    ```

NOTE: There are lots of test cases if using arguments options with different values. (Please try yourself if you want.)
