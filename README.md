# java-rc-sai-castelec
Java implementation of the reverse connector API of Click2Sync for SAI ERP from Castelec

## Running

1. Load in eclipse as eclipse project
2. Java 1.8 or later
3. Missing libraries (check .classpath file to understand which libraries to include)

## Notices

- This is the example of a SAI reverse connector implementation for:
	- Products readonly
	- Orders read/write

- But can be implemented also for products read/write if needed, just by calling the right DBF methods

## C2S Reverse Connector Protocol & API Reference

https://www.click2sync.com/developers/reverse.html