/****************************** File Header ******************************
File Name       : Tds-Client-Lib.h
Class           : TDSLib::TDSLibClient
Description     : This is a Lib module
1) Receives request to communicate with DB
2) Creates TDS object for Execute SQL / Fetch Data / Bulk Copy
--------------------------------------------------------------------------
History of the File
Date                                        Description
10/24/2016                                  File Created
--------------------------------------------------------------------------
******************************* End of File Header ***********************/

#pragma once
#include <sstream>
#include <vector>
#include <functional>
#include <algorithm>
#include <sybfront.h>
#include "spdlog/spdlog.h"

using namespace std;

namespace TDSLib {

	auto spdLogerr = [](auto message) { spdlog::get("logger")->error() << message; };
	auto spdLogwarn = [](auto message) { spdlog::get("logger")->warn() << message; };
	typedef vector<vector<string>> CsvFile;

	class TDSLibClient {
	public:

		/****************************** Function Header ******************************
		Class           : TDSLib::TDSLibClient
		Function Name   : tdsBulkCopy
		Function Scope  : Static
		Input Parameter : const char* _host                       -> Host Information
		const char* _user                       -> User Id
		const char* pass                        -> Pass
		const char* appname                     -> Application Name
		const char* database                    -> DataBase
		const char* schema                      -> Schema
		const char* table                       -> Table name
		const char* file                        -> File, which will be Bulk Copied
		const char* bindings                    -> Bindings
		const char* loglist                     -> List of field names which needs to be logged.
		Return Value    : int -> zero in case of success, 1 in case of fail
		Description     : Creates object and do Bulk Copy of the data availabe in File
		******************************* End of Function Header ***********************/
		static int tdsBulkCopy(const string& host,
								const string& user,
								const string& pass,
								const string& appname,
								const string& database,
								const string& schema,
								const string& table,
								CsvFile& file,
								vector<pair<string, int>>& bindings,
								vector<string>& loglist = vector<string>()) {
			TDSLibClient client(host, user, pass, appname, 1);
			if (!client.IsInitialized) {
				if (!client.prepareBulkcopy(database, schema, table)) {
					if (!client.bulkCopy(file, bindings, loglist)) {
						stringstream msg; msg << __func__ << " : Success";
						//spdLogerr(msg.str());
						return 0;
					}
				}
			}
			stringstream msg; msg << __func__ << " : Failed";
			spdLogerr(msg.str());
			return 1;
		}

		/****************************** Function Header ******************************
		Class           : TDSLib::TDSLibClient
		Function Name   : tdsExecuteQuery
		Function Scope  : Static
		Input Parameter : const char* _host                      -> Host Information
		const char* _user                      -> User Id
		const char* pass                       -> Pass
		const char* appname                    -> Application Name
		const char* database                   -> DataBase
		const char* sql                        -> SQL statement
		Return Value    : int -> zero in case of success, 1 in case of fail
		Description     : Creates object and get the script executed
		******************************* End of Function Header ***********************/
		static int tdsExecuteQuery(const string& host,
									const string& user,
									const string& pass,
									const string& appname,
									const string& database,
									const string& sql) {
			TDSLibClient client(host, user, pass, appname);
			if (!client.IsInitialized) {
				if (!client.executeCommand(database, sql)) {
					stringstream msg; msg << __func__ << " : Success";
					//spdLogerr(msg.str());
					return 0;
				}
			}
			stringstream msg; msg << __func__ << " : Failed : " << sql;
			spdLogerr(msg.str());
			return 1;
		}

		/****************************** Function Header ******************************
		Class           : TDSLib::TDSLibClient
		Function Name   : tdsExecuteQueryandFetch
		Function Scope  : Static
		Input Parameter : const char* _host                                      -> Host Information
		const char* _user                                      -> User Id
		const char* pass                                       -> Pass
		const char* appname                                    -> Application Name
		const char* database                                   -> DataBase
		const char* sql                                        -> SQL statement
		function<void(vector<string>&, CsvFile&)> OnTable      -> CallBack function to store retrived values
		Return Value    : int -> zero in case of success, 1 in case of fail
		Description     : Creates object and get the script executed
		******************************* End of Function Header ***********************/
		static int tdsExecuteQueryandFetch(const string& host,
											const string& user,
											const string& pass,
											const string& appname,
											const string& database,
											const string& sql,
											function<void(vector<string>&, CsvFile&)> OnTable) {
			TDSLibClient client(host, user, pass, appname);
			if (!client.IsInitialized) {
				if (!client.executeCommand(database, sql)) {
					if (!client.fetchResult(OnTable)) {
						stringstream msg; msg << __func__ << " : Success";
						//spdLogerr(msg.str());
						return 0;
					}
				}
			}
			stringstream msg; msg << __func__ << " : Failed : " << sql;
			spdLogerr(msg.str());
			return 1;
		}

		TDSLibClient(const string& host, const string& user, const string& pass, const string& appname, const bool bcopy = 0);
		int initAndConnect(const string& host, const string& user, const string& password, const string& appname, const bool bcopy = 0);
		int executeCommand(const string &database, const string &sql);
		int prepareBulkcopy(const string& databaseName, const string& schemaName, const string& tableName);
		int bulkCopy(CsvFile& file, vector<pair<string, int>>& bindings, vector<string>& loglist);
		string ToTrimmedString(char *begin, char *end);
		int fetchResult(function<void(vector<string>&, CsvFile&)> OnRow);

		TDSLibClient() = default;
		TDSLibClient(const TDSLibClient&) = delete;
		TDSLibClient& operator=(const TDSLibClient&) = delete;
		~TDSLibClient();

		int IsInitialized = 0;
		int IsBulkCopy = 0;
	private:
		DBPROCESS *dbproc = NULL;
	};

	/* Function Definitions */
	int Tds_Error_handler(DBPROCESS* dbproc, int severity, int dberr, int oserr, char* dberrstr, char* oserrstr) {
		stringstream msg; msg << (__func__) << " : ";

		if ((NULL == dbproc) || DBDEAD(dbproc)) {
			msg << " dbproc is NULL error : " << dberrstr;
		}
		else if ((oserr != DBNOERR)) {
			msg << " Operating-system error : " << oserr << dberrstr;
		}
		else {
			msg << " DB-Library error : " << dberr << dberrstr;
		}

		spdLogerr(msg.str());
		dbexit();
		return(INT_CANCEL);
	}

	int Tds_Msg_handler(DBPROCESS* dbproc, DBINT msgno, int msgstate, int severity, char* msgtext, char* srvname, char* procname, int line) {
		stringstream msg; msg << (__func__) << " : ";
		enum { changed_database = 5701, changed_language = 5703 };

		if (msgno != changed_database && msgno != changed_language) {
			msg << "msgno " << msgno << " severity " << severity << " msgstate " << msgstate;

			if (srvname) { msg << ", server: '" << srvname << "'"; }
			if (procname) { msg << ", procedure: '" << procname << "'"; }
			if (0 < line) { msg << ", line: '" << line << "'"; }
			if (msgtext) { msg << ", message text: '" << msgtext << "'"; }

			spdLogwarn(msg.str());
		}
		return(0);
	}

	/****************************** Function Header ******************************
	Class           : TDSLib::TDSLibClient
	Function Name   : initAndConnect
	Function Scope  : Public
	Input Parameter : const char* _host                       -> Host Information
	const char* _user                       -> User Id
	const char* pass                        -> Pass
	const char* appname                     -> Application Name
	const bool bcopy                        -> should be enabled only in case of Bulk copy
	Return Value    : int                                     -> zero in case of success, 1 in case of fail
	Description     : Performs Initialization operation
	******************************* End of Function Header ***********************/
	int TDSLib::TDSLibClient::initAndConnect(const string& host, const string& user, const string& password, const string& appname, const bool bcopy) {
		stringstream msg; msg << __func__;

		IsBulkCopy = bcopy;
		if (FAIL == dbinit()) {
			IsInitialized = 1;
			msg << " : failed to initialize the driver";
			spdLogerr(msg.str());
			return 1;
		}
		dberrhandle((EHANDLEFUNC)Tds_Error_handler);
		dbmsghandle((MHANDLEFUNC)Tds_Msg_handler);

		auto *login = dblogin();
		if (NULL == login) {
			IsInitialized = 1;
			msg << " : connect() unable to allocate login structure";
			spdLogerr(msg.str());
			return 1;
		}

		DBSETLUSER(login, user.c_str());
		DBSETLPWD(login, password.c_str());
		DBSETLAPP(login, appname.c_str());

		/* Settings for Bulk Copy*/
		if (bcopy) {
			dbsetversion(DBVERSION_100);
			BCP_SETL(login, TRUE);
		}

		if ((DBPROCESS *)NULL == (dbproc = dbopen(login, host.c_str()))) {
			IsInitialized = 1;
			msg << " : Can't connect to server : " << host;
			spdLogerr(msg.str());
			return 1;
		}

		return 0;
	};

	/****************************** Function Header ******************************
	Class           : TDSLib::TDSLibClient
	Function Name   : executeCommand
	Function Scope  : Public
	Input Parameter : const char* database                  -> Database Name
	const char* sql                       -> SQL Query
	Return Value    : int -> zero in case of success, 1 in case of fail
	Description     : Executes the given SQL
	******************************* End of Function Header ***********************/
	int TDSLib::TDSLibClient::executeCommand(const string &database, const string &sql) {
		stringstream msg; msg << __func__;

		if (FAIL == dbuse(dbproc, database.c_str())) {
			msg << " : failed to use a database " << database;
			spdLogerr(msg.str());
			return 1;
		}

		if (FAIL == dbcmd(dbproc, sql.c_str())) {
			msg << " : failed to process a query " << sql;
			spdLogerr(msg.str());
			return 1;
		}

		if (FAIL == dbsqlexec(dbproc)) {
			msg << " : failed to execute a query " << sql;
			spdLogerr(msg.str());
			return 1;
		}
		return 0;
	}

	/****************************** Function Header ******************************
	Class           : TDSLib::TDSLibClient
	Function Name   : prepareBulkcopy
	Function Scope  : Public
	Input Parameter : const char* database                    -> Database Name
	const char* schemaName                  -> Schema
	const char* tableName                   -> Table Name
	Return Value    : int                                     -> zero in case of success, 1 in case of fail
	Description     : Prepare for Bulk Copy
	******************************* End of Function Header ***********************/
	int TDSLib::TDSLibClient::prepareBulkcopy(const string& database, const string& schemaName, const string& tableName) {
		stringstream msg; msg << __func__;

		/* Turn on option to allow bulk copy */
		string sql = "execute sp_dboption " + database + ", 'bulk', true";
		if (executeCommand("master", sql)) {
			return 1;
		}
		while (NO_MORE_RESULTS != dbresults(dbproc))
			continue;

		if (executeCommand(database, "checkpoint")) {
			return 1;
		}
		while (NO_MORE_RESULTS != dbresults(dbproc))
			continue;

		string fullNamespace = database + "." + schemaName + "." + tableName;
		if (FAIL == bcp_init(dbproc, fullNamespace.c_str(), NULL, "bcp.errors", DB_IN)) {
			msg << " : Failed to prepare bulkcopy ";
			spdLogerr(msg.str());
			return 1;
		}
		return 0;
	}

	/****************************** Function Header ******************************
	Class           : TDSLib::TDSLibClient
	Function Name   : bulkCopy
	Function Scope  : Public
	Input Parameter : CsvFile& file                          -> File to be Bulk Copied
	vector<pair<string, int>>& binding     -> Binding
	vector<string>& loglist                -> List of fields to be logged
	Return Value    : int                                    -> zero in case of success, 1 in case of fail
	Description     : Perform Bulk Copy
	******************************* End of Function Header ***********************/
	int TDSLib::TDSLibClient::bulkCopy(CsvFile& file, vector<pair<string, int>>& bindings, vector<string>& loglist) {
		stringstream msg; msg << __func__ << "[";
		for (auto& e : file) {
			int counter = 1;
			for (auto& field : e) {
				auto binding = bindings.at(counter - 1);
				if (FAIL == (bcp_bind(dbproc, (unsigned char*)field.c_str(), 0, binding.second == 52 || binding.second == 56 || binding.second == 127 ? -1 : field.size(), NULL, 0, binding.second, counter++)))
					return 1;
				find_if(loglist.begin(), loglist.end(), [&](string str) { if (binding.first == str) { msg << " " << binding.first << " : " << field; } return 0;});
			}
			bcp_sendrow(dbproc);
		}

		bcp_done(dbproc);
		msg << "]" << " Bulkcopy sent";
		spdLogerr(msg.str());
		return 0;
	}

	/****************************** Function Header ******************************
	Class           : TDSLib::TDSLibClient
	Function Name   : ToTrimmedString
	Function Scope  : Public
	Input Parameter : char *begin       -> Starting point of the string
	char *end         -> Ending point of the string
	Return Value    : string            -> returns the trimmed string
	Description     : Trim the provided string from begin to end and returns
	******************************* End of Function Header ***********************/
	string TDSLib::TDSLibClient::ToTrimmedString(char *begin, char *end) {
		for (; begin < end && *begin == ' '; ++begin);
		for (; begin < end && *(end - 1) == ' '; --end);
		return string(begin, end);
	}

	/****************************** Function Header ******************************
	Class           : TDSLib::TDSLibClient
	Function Name   : fetchResult
	Function Scope  : Public
	Input Parameter : function<void(vector<string>&, CsvFile&)> OnTable       -> Callback function
	Return Value    : int                                                     -> zero in case of success, 1 in case of fail
	Description     : Execute SQL and Fetch back the fields
	******************************* End of Function Header ***********************/
	int TDSLib::TDSLibClient::fetchResult(function<void(vector<string>&, CsvFile&)> OnTable) {
		stringstream msg; msg << __func__ << "[";
		CsvFile fieldValues;
		vector<string> fieldName;

		while (true)
		{
			auto status = dbresults(dbproc);

			if (status == NO_MORE_RESULTS) {
				break;
			}

			if (status == FAIL) {
				msg << "failed to fetch a result";
				spdLogerr(msg.str());
				break;
			}

			auto columnCount = dbnumcols(dbproc);

			if (columnCount == 0) {
				continue;
			}

			for (auto i = 0; i < columnCount; ++i) {
				auto name = dbcolname(dbproc, i + 1);
				fieldName.emplace_back(name);
			}
			while (true)
			{
				auto rowCode = dbnextrow(dbproc);
				if (rowCode == NO_MORE_ROWS) {
					break;
				}
				vector<string> row;
				switch (rowCode) {
				case REG_ROW:
					for (auto i = 0; i < columnCount; ++i) {
						auto data = dbdata(dbproc, i + 1);
						if (data == NULL) {
							row.push_back("NULL");
						}
						else {
							auto type = dbcoltype(dbproc, i + 1);
							auto length = dbdatlen(dbproc, i + 1);
							vector<BYTE> buffer(max(32, 2 * length) + 2, 0);
							auto count = dbconvert(dbproc, type, data, length, SYBCHAR, &buffer[0], buffer.size() - 1);
							if (count == -1) {
								msg << " failed to fetch column data, insufficient buffer space";
								spdLogerr(msg.str());
								break;
							}
							row.push_back(ToTrimmedString((char*)&buffer[0], (char*)&buffer[count]));
						}
					}
					fieldValues.push_back(row);
					break;

				case BUF_FULL:
					msg << " failed to fetch a row, the buffer is full";
					spdLogerr(msg.str());
					break;

				case FAIL:
					msg << " failed to fetch a row";
					spdLogerr(msg.str());
					break;

				default:
					break;
				}
			}
		}
		OnTable(fieldName, fieldValues);
		return 0;
	}

	/****************************** Function Header ******************************
	Class           : TDSLib::TDSLibClient
	Function Name   : TDSLibClient (Constructor)
	Function Scope  : Public
	Input Parameter : const char* _host                       -> Host Information
	const char* _user                       -> User Id
	const char* pass                        -> Pass
	const char* appname                     -> Application Name
	const bool bcopy                        -> should be enabled only in case of Bulk copy
	Description     : Init function is executed
	******************************* End of Function Header ***********************/
	TDSLibClient::TDSLibClient(const string& host, const string& user, const string& password, const string& appname, const bool bcopy) {
		if (initAndConnect(host, user, password, appname, bcopy)) {
			spdLogerr("initialization  was not successfull");
		}
	}

	/****************************** Function Header ******************************
	Class           : TDSLib::TDSLibClient
	Function Name   : TDSLibClient (Destructor)
	Function Scope  : Public
	Description     : Init function is executed
	******************************* End of Function Header ***********************/
	TDSLibClient::~TDSLibClient() {
		if (!IsBulkCopy) {
			if (dbproc != NULL) {
				dbclose(dbproc);
			}
		}
		dbexit();
	}
}
