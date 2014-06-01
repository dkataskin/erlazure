##erlazure

Erlazure is a library for accessing Windows Azure Storage Services. The API is subject to change.

Service APIs implemented:
* Queue storage service (API ver. "2012-02-12")
* Blob storage service (API ver. "2012-02-12")

##Requirements

Erlazure requires OTP version R16+.

##Implemented API functions
* Queue storage service
  * List queues
  * Get queue acl
  * Create queue
  * Delete queue
  * Put message
  * Get messages
  * Peek messages
  * Delete message
  * Clear messages
  * Update message
  
* Blob storage service
  * List containers
  * Create container
  * Delete container
  * Put block blob
  * Put page blob
  * List blobs
  * Get blob
  * Snapshot blob
  * Copy blob
  * Delete blob
  * Put block
  * Put block list
  * Get block list
  * Lease container
  
##Getting started

Start an instance of erlazure by calling erlazure:start/2 where **Account** is Storage account name and **Key** is Storage account key.
```erlang
erlazure:start(Account, Key)
```