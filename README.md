# FreeTDS-Lib
A Convenient library to communicate with FreeTDS

This is a LIB module which gives you option to get your SQL tasks done by just providing necessary data to provided static functions.
The LIB takes care of communicating with DB through FreeTDS and does all operations and returns you the success status.

No Compilation required, just include the header file in your project and start using it.

Dependencies 
freeTds, clone and compile FreeTDS module and map the path to the project.
https://github.com/FreeTDS/freetds

====================== Static Function 1 ======================
Below static functions can be used to get your script executed.
Returns 0 in case of success and 1 in case of Fail

TDSLib::TDSLibClient::tdsExecuteQuery(host, user, pass, appname, database, script);

====================== Static Function 2 ======================
Below static functions can be used to get your script executed and fetch records.
Pass two variables as shown in below example, in success state two variables will be packed with data.
Returns 0 in case of success and 1 in case of Fail

vector<string> fieldName;
vector<vector<string>> fieldValue;

TDSLib::TDSLibClient::tdsExecuteQueryandFetch(host, user, pass, appname, database, script, [&](vector<string> fName, vector<vector<string>> fValue) {fieldName = fName; fieldValue = fValue;});

====================== Static Function 3 ======================
Below static function can be used for BulkCopy
Input Parameter "loglist" is optional and should hold the fields which needs to be logged.
Input parameter "Bindings" should contain "Column Name" and its "type".
Input Parameter "file" should hold the actual Bulk of records.

vector<std::pair<string, int>> Bindings = { { "ColunmName1", 127 },{ "ColunmName2", 39 },{ "ColunmName3", 39 },{ "ColunmName4", 39 } };
vector<string> loglist = { "Field1", "Field3" };
vector<vector<string>> file;

TDSLib::TDSLibClient::tdsBulkCopy(host, user, pass, appname, database, schemaName, tableName, file, Bindings, loglist);


==================================================
Note :-
Lamdas TdsLogerr() and TdsLogwarn() can be updated upto your project flavour, like storing into a log file.

