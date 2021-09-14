# File-arrays

* Flask was configured in order to run the application.
* Files_arrays.csv is used as a dynamically source of the arrays - the app asks for a full path in order to continue.
* process partially works - there were some challenges sending requests to the server along with the server sending a heartbeat to the db:
  although the relevant port was opened, connection was refused to be established.

The process does read the files, corrupt the arrays with a 10% chance and cleans the corrupted array +
trying to send the jsons with iteration of 4 requests, each containing 1 array.
Eventually, methods use each other and class works ok.
