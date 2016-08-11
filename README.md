# Dyno Queues
 
 Dyno Queues is a recipe that provides task queues utilizing [Dynomite](https://github.com/Netflix/dynomite).
 

 


## Dyno Queues Features

+ Time based queues.  Each queue element has a timestamp associated and is only polled out after that time.
+ Priority queues
+ No strict FIFO semantics.  However, within a shard, the elements are delivered in FIFO (depending upon the priority) 
