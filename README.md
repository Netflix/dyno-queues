## DISCLAIMER: THIS PROJECT IS NO LONGER ACTIVELY MAINTAINED



# Dyno Queues
[![Build Status](https://travis-ci.org/Netflix/dyno-queues.svg)](https://travis-ci.org/Netflix/dyno-queues)
[![Dev chat at https://gitter.im/Netflix/dynomite](https://badges.gitter.im/Netflix/dynomite.svg)](https://gitter.im/Netflix/dynomite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
 
Dyno Queues is a recipe that provides task queues utilizing [Dynomite](https://github.com/Netflix/dynomite).
 

## Dyno Queues Features

+ Time based queues.  Each queue element has a timestamp associated and is only polled out after that time.
+ Priority queues
+ No strict FIFO semantics.  However, within a shard, the elements are delivered in FIFO (depending upon the priority) 
