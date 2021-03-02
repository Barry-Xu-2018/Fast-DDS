
# Overview

In order to improve the fault tolerant of FastDDS, this document describes scenario to test FastDDS and consideration for test codes.

To increase the fault tolerant of FastDDS, consider to add some test cases into 'FastDDS/test'. But wonder if we can test with FastDDS for these scenario without updating FastDDS library.


## test scenario for fault tolerant and expected behavior

|ID|Test Scenario|Expected Result|Remark|
|---|---|---|---|
|1| publisher(s):subscriber(s)=1:1<br>Publisher application is terminated (such as crash) during writing data | - Subscriber doesn't crash<br>- Subscriber can read the data which is notified that it is ready <br>- Run a new publisher to make the communication work again  | Question: Can the resources allocated by the crashed application be recycled? |
|2| publisher(s):subscriber(s)=1:1<br>Subscriber is terminated (such as crash) during reading data| - Publisher doesn't crash<br> - Run a new subscriber to make the communication work again | |
|3| publisher(s):subscriber(s)=m:n<br>Multiple publishers are sending data. Multiple subscribers are receiving data. One publisher is terminated suddenly (such as crashed) | - Other publisher/subscriber applications don't crash<br>- Subscriber can read the data which is notified that it is ready<br>- The communication are still working. | |
|4| publisher(s):subscriber(s)=m:n<br>Multiple publishers are sending data. Multiple subscribers are receiving data. One subscriber is terminated (e.g. crashed) suddenly. | - Other publisher/subscriber applications don't crash<br> - The communication are still working | |
|5| publisher(s):subscriber(s)=1:1<br>Signal(such as INT) comes in during writing data and application has handled this signal. | - Writing data isn't interrupted<br> - Communication is still working | |
|6| publisher and subscriber are located at different container. And execute 1~5 case. | |  |
|7| publisher(s):subscriber(s)=m:n<br> Publisher and subscriber are located at different container. While communication is in progress, restart container and run test again | - The communication work well without error.<br> - No need to clear environment by manual. ||
|9| publisher(s):subscriber(s)=m:n<br> While communication is in progress, reboot system (simulate system crash and reboot). Run test | - The communication work well without error.<br> - No need to clear environment by manual. | |
|10| publisher(s):subscriber(s)=m:n<br> Shared memory exhausted at some time | - The exist running publisher/subscriber applications are still working | |
|11| publisher(s):subscriber(s)=m:n<br> Run publisher and subscriber for long time (One day or serval days) | - publisher/subscriber work well.<br>- No memory leak occurs (include shm) ||

Potential issue:  
If not modify code of FastDDS, it is hard to make sure time point such as `during writing`. 
Instead, use large the number of execution times (such as 10000, 100000) and use big message to catch this time point. Of cause, this cannot make sure time point is caught.

## Consideration for test codes.

In FastDDS, existing test framework is unsuitable for above test.  
Put test codes to Fast-DDS/test/fault_tolerant.  
Use C++ language to implement two example applications, such as publisher and subscriber. And python script to control execution, check the output data and display test result.  


