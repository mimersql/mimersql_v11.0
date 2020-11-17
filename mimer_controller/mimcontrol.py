# Copyright (c) 2017 Mimer Information Technology

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
"""
Implementation of the Mimer SQL administration funcationallity used in the Mimer SQL Controller web service.

All methods that return JSON data will have some common fields:
{"return_code":"success", ...} 
{"return_code":"failure", "error_message":".....", ...}

"""
import sys
import os
import re
import subprocess
import json
import mimerpy
import mimerpy.mimPyExceptions as mimexception
from datetime import datetime
import tempfile
import shutil

def clean_sql_input(sqlParam: str):
    tmpStr = sqlParam
    tmpStr = tmpStr.replace('\'', '')
    tmpStr = tmpStr.replace(';', '')
    return tmpStr

def escape_sql_input(sqlParam: str):
    tmpStr = sqlParam
    tmpStr = tmpStr.replace('\'', '\'\'')
    return tmpStr

def check_params(parameters):
    """Check that required parameters are set.

    Args:
        parameters(dict): The parameter list to check

    Returns:
        {"return_status":"success"}
        or
        {"return_status":"failure","error_code":"<error code>","error_message":"<error message>"}        

    """
    for key in parameters:
        if parameters[key] is None:
            json_response = {'return_status':'failure','error_code':"12872",'error_message':"Invalid parameters: Missing parameter \'" + key + "\'"}
            return json_response
    json_response = {'return_status':'success'}
    return json_response

def get_mimer_error_code(e):
    """Helper function to get Mimer SQL error codes that works with different versions of MimerPy.

    Args:
        e(mimexception.Error): The Mimer SQL Error

    Returns:
        The Mimer SQL error code
    """
    err = -1
    if mimerpy.version > '1.0.20':
        err = e.message[0]
    else:
        if e.message == "Login failure":
            err = 90
        else:
            e_tup = e.message.split()
            e_err = e_tup[-1]
            try:
                err = int(e_err)
            except ValueError:
                err = -1
    return err

def get_mimer_error_text(e):
    """Helper function to get Mimer SQL error text that works with different versions of MimerPy.

    Args:
        e(mimexception.Error): The Mimer SQL Error

    Returns:
        The Mimer SQL error text
    """
    if mimerpy.version > '1.0.20':
        if e.message[0] == 90:
            return 'Login failure'
        else:
            return e.message[1]
    else:
        return e.message

def check_status(database_name:str):
    """Check status of a Mimer SQL Database.

    Args: 
        database_name(str): The database name

    Returns: 
        {
            "return_code":"success", 
            "status": <server status>, 
            "login": <login enabled>, 
            "database_path": <path of database>,
            "connected_users": <connected users>,
            "start_time": <start time>,
            "bufferpool_size": <buffer pool size>
        }
        or
        {
            "return_code":"failure", 
            "error_code": <error code>, 
            "error_message":"<error message>"
        }
    """
    parameterStatus = check_params({"database_name":database_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    result = subprocess.run(['mimcontrol', '-bc', database_name], stdout=subprocess.PIPE)
    response = result.stdout.decode('utf-8').replace('\n', '')
    if result.returncode == 0:
        parsed_respone = response.split(',')
        if parsed_respone[0] == 'Running':
            json_response = {
                "return_code": "success",
                "status": parsed_respone[0],
                "login": parsed_respone[1],
                "database_path": parsed_respone[2],
                "connected_users": int(parsed_respone[3]),
                "start_time": parsed_respone[5],
                "bufferpool_size": int(parsed_respone[6])
            }
        else:
            json_response = {
                "return_code": "success",
                "status": parsed_respone[0],
                "login": parsed_respone[1],
                "database_path": parsed_respone[2],
                "connected_users": 0,
                "start_time": 0,
                "bufferpool_size": 0
            }
    else:
        json_response = {
                "return_code": "failure",
                "error_code": result.returncode,
                "error_message": response
            }
    return json_response


def stop_database(database_name:str, kill:bool=False):
    """Stop the specified Mimer SQL Database.

    Args:
        database_name(str): The database name
        kill(bool): If true kill the database otherwise stop it gracefully

    Returns: 
            {"return_code":"success"}
            or
            {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}
    """
    parameterStatus = check_params({"database_name":database_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    if kill:
        stop_flag = '-k'
    else:
        stop_flag = '-t'
    result = subprocess.run(['mimcontrol', stop_flag, database_name], stdout=subprocess.PIPE)
    response = result.stdout.decode('utf-8').replace('\n', '')
    if result.returncode == 0:
        json_response = {
            "return_code": "success"
        }
    else:
        json_response = {
            "return_code": "failure",
            "error_code": result.returncode,
            "error_message": response
        }
    return json_response


def start_database(database_name:str):
    """Start a Mimer SQL Database.

    Args:
        database_name(str): The database name

    Returns: 
            {"return_code":"success"}
            or
            {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}
    """
    parameterStatus = check_params({"database_name":database_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    result = subprocess.run(['mimcontrol', '-s', database_name], stdout=subprocess.PIPE)
    response = result.stdout.decode('utf-8').replace('\n', '')
    if result.returncode == 0:
        json_response = {
            "return_code": "success"
        }
    else:
        json_response = {
            "return_code": "failure",
            "error_code": result.returncode,
            "error_message": response
        }
    return json_response

def show_db_perf(database_name:str):
    """Show performance measurements of a Mimer SQL database.

    Show information like connected users, transactions, background threads and so on.

    Args:
        database_name(str): The database name

    Returns: JSON document with performance data, for example
        {
            "aborted_transactions": "0",
            "active_users": [
                "SYSADM",
                "SYSADM"
            ],
            "bg_threads": "3",
            "committed_read": "6",
            "committed_transactions": "0",
            "cpu": "0.1",
            "io_threads": "7",
            "memory": "0.2",
            "pending_bg_requests": "0",
            "pending_transaction_restarts": "0",
            "request_threads": "8",
            "return_code": "success",
            "sqlpool_memory": "1245"
        }

    """  
    parameterStatus = check_params({"database_name":database_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    result = subprocess.run(['miminfo', '-p', database_name], stdout=subprocess.PIPE)
    json_response = {}
    if result is not None:
        response = result.stdout.decode('utf-8')
    if result.returncode != 0 or 'Mimer SQL fatal error' in response:
        if result.stderr is not None:
            output = result.stderr.decode('utf-8').replace('\n', '')
        elif response is not None:
            output = response.replace('\n', '')
        else:
            output = "Mimer SQL unkown error"
        json_response['return_code'] = "failure"
        json_response['error_code'] = result.returncode
        json_response['error_message'] = output
        return json_response
    
    m = re.search(r"transaction commits[\s|\t]*:[\s|\t]*(\d+)", response)
    if m:
        commits = m.group(1)
        json_response['committed_transactions'] = commits
    m = re.search(r"read commits[\s|\t]*:[\s|\t]*(\d+)", response)
    if m:
        read_commits = m.group(1)
        json_response['committed_read'] = read_commits
    m = re.search(r"transaction aborts[\s|\t]*:[\s|\t]*(\d+)", response)
    if m:
        aborts = m.group(1)
        json_response['aborted_transactions'] = aborts
    m = re.search(r"pending restarts[\s|\t]*:[\s|\t]*(\d+)", response)
    if m:
        pending_restart = m.group(1)
        json_response['pending_transaction_restarts'] = pending_restart
    m = re.search(r"request threads[\s|\t]*:[\s|\t]*(\d+)", response)
    if m:
        request_threads = m.group(1)
        json_response['request_threads'] = request_threads
    m = re.search(r"background threads[\s|\t]*:[\s|\t]*(\d+)", response)
    if m:
        bg_threads = m.group(1)
        json_response['bg_threads'] = bg_threads
    m = re.search(r"I/O threads[\s|\t]*:[\s|\t]*(\d+)", response)
    if m:
        io_threads = m.group(1)
        json_response['io_threads'] = io_threads
    m = re.search(r"Pending background thread requests[\s|\t]*:[\s|\t]*(\d+)", response)
    if m:
        pending_requests = m.group(1)
        json_response['pending_bg_requests'] = pending_requests
    

    result = subprocess.run(['miminfo', '-s', database_name], stdout=subprocess.PIPE)
    response = result.stdout.decode('utf-8')
    m = re.search(r"SQL pool memory used      \(KB\):[\s|\t]*(\d+)", response)
    if m:
        sql_pool = m.group(1)
        json_response['sqlpool_memory'] = sql_pool   
    active_users = []
    for active_user in re.findall(r"(\S+)[\s|\t]+\d+[\s|\t]+\d+[\s|\t]+\d+", response):
        active_users.append(active_user)
    json_response['active_users'] = active_users

    result = None
    with subprocess.Popen(['ps', '-o', '%cpu,%mem,command', 'ax'], stdout=subprocess.PIPE) as p1:
        with subprocess.Popen(['grep', 'mimexper ' + database_name], stdin=p1.stdout, stdout=subprocess.PIPE) as p2:
            result = p2.communicate()
    response = repr(result[0])
    m = re.search(r"(\d+\.\d+)[\s|\t]+(\d+\.\d+)[\s|\t]+mimexper", response)
    if m:
        cpu = m.group(1)
        mem = m.group(2)
        json_response['cpu'] = cpu 
        json_response['memory'] = mem 
    json_response['return_code'] = "success"

    return json_response


def show_sql_log(database_name:str, sysadm_pass:str):
    """Show SQL execution log.

    Show executed SQL with performance information

    Args:
        database_name(str): The database name
        sysadm_pass(str): SYSADM password

    Returns: JSON document with the SQL log, for example:
        {
            "return_code":"success",
            "logs" [
                {
                    "total_table_operation_count": table operations,
                    "total_statement_execution_count":execution count,
                    "total_transaction_records_count":transaction records,
                    "total_elapsed_time":elaps_time": total execution time,
                    "statement": the SQL statement
                },
                .
                .
                .
            ]
        }
        or
        {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}
    """  
    parameterStatus = check_params({"database_name":database_name,"password":sysadm_pass})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    result = subprocess.run(['sqlmonitor', '-d3', database_name, '-uSYSADM', '-p' + sysadm_pass], stdout=subprocess.PIPE)
    json_response = {}
    response = result.stdout.decode('utf-8')
    if result.returncode != 0:
        if result.stderr is not None:
            output = result.stderr.decode('utf-8').replace('\n', '')
        elif result.stdout is not None:
            output = response.replace('\n', '')
        else:
            output = "Unknown Mimer SQL error"
        json_response['return_code'] = "failure"
        json_response['error_code'] = result.returncode
        json_response['error_message'] = output
        return json_response
    
    logs = []
    json_response["return_code"] = "success"
    for (op_cnt, exec_cnt, trans_record_cnt, elaps_time, statement) in re.findall(r"(\d+)[\s|\t]+\d+[\s|\t]+(\d+)[\s|\t]+\d+[\s|\t]+(\d+)[\s|\t]+(\d+\.\d+)[\s|\t]+\d+[\s|\t]+(\w+.*)", response):
        if exec_cnt!='0':
            log = {
                "total_table_operation_count":op_cnt,
                "total_statement_execution_count":exec_cnt,
                "total_transaction_records_count":trans_record_cnt,
                "total_elapsed_time":elaps_time,
                "statement":statement 
            }
            logs.append(log)
    json_response["logs"] = logs
    return json_response


def change_config(database_name: str, jsonVal: dict):
    """Set one more Mimer SQL configuration parameters

    The parameters are speficied in the JSON input.

    Args:
        database_name(str): The database name
        jsonVal(dict): The configuraiton parameters to change in the following format:
            {
                'config': [
                    {
                        'config_name': 'name',
                        'config_value': 'value'
                    },
                    ...
                ]
            }

    Returns:
            {
                'return_code': 'success',
                'config': [
                    {
                        'config_name': 'name',
                        'return_code': 'success'
                    },
                    ...
                ]
            }
            or when failure
            {
                'return_code': 'failure',
                'error_code': error_code,
                'error_message': 'error message, for example that the config file could not be found'
                'config': [
                    {
                        'config_name': 'name',
                        'return_code': 'value'
                        'error_message: 'message'
                    },
                    ...
                ]
            }

            return_code != 'success' mean we can not set configration at all.
            config.return_code != 'success' mean we could not change that particular setting
            It's possible that only some of the config-items have errors
    """
    parameterStatus = check_params({"database_name":database_name,"jsonVal":jsonVal})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    #Get path to multidefs
    result = subprocess.run(['mimpath', database_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        return {'return_code': 'failure', 'error_code': result.returncode, 'error_message': 'Could not get path to database ' + database_name }
    db_paths = result.stdout.decode('utf-8').replace('\n', '').split(':')
    db_path = db_paths[0]
    multidefs_file = db_path + '/multidefs'
    configuration = jsonVal
    return_val = {
        'return_code': 'success',
        'config': []
    }
    for config in configuration['config']:
        result = subprocess.run(['mimchval', multidefs_file + ' ' + config['config_name'] + ' ' + str(config['config_value'])], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        r_val = {'config_name': config['config_name']}
        if result.returncode != 0:
            output = result.stderr.decode('utf-8').replace('\n', '')
            r_val['return_code'] = 'failure'
            r_val['error_code'] = result.returncode
            r_val['error_message'] = output
        else:
            r_val['return_code'] = 'success'
        return_val['config'].append(r_val)
    return return_val


def get_config(database_name: str, jsonVal: dict):
    """Get one more Mimer SQL configuration parameters. 
    
    If the input 'config'-list is empty all parameters are returned 

    Args:
        database_name(str): The name of the database
        jsonVal(dict): The parameters that should be returned in JSON format, or empty for all parameters
        
        jsonVal format:
        {
            'config': [
                {
                    'config_name': 'name',
                },
                {
                    'config_name': 'name',
                }
            ]
        }


    Returns: 
            When successfull
            {
                'return_code': 'success',
                'config': [
                    {
                        'config_name': 'name',
                        'config_value': 'value',
                        'config_comment': 'comment'
                    },
                    ...
                ]
            }
            or if failure:
            {
                'return_code': 'failure',
                'error_code': error_code
                'error_message': 'error message, for example that the config file could not be found' #Only present if there is an error
            }
    """
    parameterStatus = check_params({"database_name":database_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    #Get path to multidefs
    result = subprocess.run(['mimpath', database_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        return {'return_code': 'failure', 'error_code': result.returncode, 'error_message': 'Could not get path to database ' + database_name }
    db_paths = result.stdout.decode('utf-8').replace('\n', '').split(':')
    db_path = db_paths[0]
    multidefs_file = db_path + '/multidefs'
    with open(multidefs_file) as f:
        multidefs_content = [line.rstrip() for line in f]
    if(jsonVal != None):
        configuration = jsonVal
    else:
        configuration = None
    return_val = {
        'config': []
    }
    
    conf_arr = []
    for conf_line in multidefs_content:
        if conf_line.startswith('--') or conf_line.startswith('#'):
            continue
        parsed_cur_conf = re.split(r'\s+',conf_line)
        conf_arr.append([parsed_cur_conf[0], parsed_cur_conf[1], ' '.join(parsed_cur_conf[2:])])


    if configuration == None or configuration.get('config') == None:
        for conf_arr_line in conf_arr:
            return_val['config'].append({'config_name': conf_arr_line[0], 'config_value': conf_arr_line[1], 'config_comment': conf_arr_line[2]})
    else:
        for config in configuration['config']:
            conf_found = False
            for conf_arr_line in conf_arr:
                if config.get('config_name') == None or config.get('config_name').upper() == conf_arr_line[0].upper():
                    return_val['config'].append({'config_name': conf_arr_line[0], 'config_value': conf_arr_line[1], 'config_comment': conf_arr_line[2]})
                    conf_found = True
            if conf_found == False:
                return_val['config'].append({'config_name': config.get('config_name'), 'config_value': None, 'config_comment': 'Configuration parameter not found'})
    return_val['return_code'] = 'success'
    return return_val


def __get_mimer_data_dir(database_name: str):
    """Get Mimer Database directory (or directories).

    Get the path to the Mimer SQL databank files for the specified database

    Args:
        database_name(str): The name of the database

    Returns:
        A list of paths
    """
    mimpaths = []
    result = subprocess.run(['mimpath', database_name], stdout=subprocess.PIPE)
    response = result.stdout.decode('utf-8').replace('\n', '')
    if result.returncode == 0:
        parsed_respone = response.split(':')
        for mim_path in parsed_respone:
            mimpaths.append(mim_path)

    return mimpaths


def __get_mimer_version():
    """Get Mimer SQL version.


    Returns: List with version info.
        [0] Short version, for example 110
        [1] Full version information, for example '11.0.3C May 13 2020 Rev 32192'
    """
    version = ['','']
    result = subprocess.run(['mimversion'], stdout=subprocess.PIPE)
    response = result.stdout.decode('utf-8').replace('\n', '')
    if result.returncode == 0:
        version[1] = response
        parsed_respone = response.split('.')
        version[0] = parsed_respone[0] + parsed_respone[1]

    return version

def __verify_databank(databank_file: str , sysdb_file: str):
    """Verify a databank file.

    Args:
        databank_file(str): The databank to verify
        sysdb_file(str): The SYSDB that the databank belongs to

    Returns:
        List with status
        [0] = OK|WARNING|ERROR
        [1] = ''|Output from databank check
    """
    res = ('UNKOWN', '')
    with tempfile.NamedTemporaryFile(mode="w+") as tmpFile:
        result = subprocess.run(['dbc', databank_file, tmpFile.name, sysdb_file ],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            res = ('OK', '')
        else:
            dbc_out = tmpFile.read()
            if result.returncode == 1:
                res = ('WARNING', dbc_out)
            else:
                res = ('ERROR', dbc_out)
    return res


def create_backup(database_name: str, user_name: str, password: str, backup_location: str, backup_name: str = None, backup_comment: str = None):
    """Create backup.

    A folder with todays date will be created in the backup location.

    Args:
        database_name(str): The database name
        user_name(str): The Mimer SQL user that will create the backup,
        password(str): Password for the Mimer SQL backup user,
        backup_location)(str): The base location for the backup, for example /data/backup,
        backup_name(str): Optional name of backup. If empty the generated backup_id will be used,
        backup_comment(str): Optional comment for backup to make it easier to find

    Returns:
    {
        "return_code": "success",
        "backup_id": "id of backup",
        "databanks": [
            {
                "databank": "sysdb110.dbf",
                "databank_check": "OK"
            },
            {
                "databank": "transdb.dbf",
                "databank_check": "OK"
            },
            {
                "databank": "logdb.dbf",
                "databank_check": "OK"
            },
            {
                "databank": "dblog.dbf",
                "databank_check": "OK"
            }
        ]
    }
    or if the backup operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }
    or if the databank check of the backup operation fails:
    {
        "return_code": "failure",
        "databanks": [
            {
                "databank": "sysdb110.dbf",
                "databank_check": "OK"
            },
            {
                "databank": "transdb.dbf",
                "databank_check": "OK"
            },
            {
                "databank": "logdb.dbf",
                "databank_check": "OK"
            },
            {
                "databank": "dblog.dbf",
                "databank_check": "ERROR"|"WARNING",
                "databank_check_message": "output from dbcheck"
            }
        ]
    }
    """    
    return_val = {
    }
    sysdb_file_name = None
    sqldb_file_name = None

    parameterStatus = check_params({"database_name":database_name,"backup_location":backup_location,"user_name":user_name,"password":password})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    
    (mimversion, _) = __get_mimer_version()
    ts = datetime.now()
    backup_id = ts.isoformat()
    backup_date = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    backup_path = backup_location + '/' + backup_id
    if backup_name:
        __backup_name = backup_name
    else:
        __backup_name = backup_id

    try:
        os.mkdir(backup_path)
    except OSError as e:
        return_val['return_code'] = 'failure'
        return_val['error_code'] = e.errno
        return_val['error_message'] = e.strerror
        return return_val
    con = None
    try:
        con = mimerpy.connect(dsn = database_name, user = user_name, password = password)
        con.autocommit(True)
        with con.execute("select databank_name, file_name, backup_date from information_schema.ext_databanks") as databank_cursor:
            databanks = databank_cursor.fetchall()
        con.execute("start backup")
        backup_info = { 'backup_id': backup_id, 'backup_name': __backup_name, "backup_date": backup_date,
                    'backup_comment': backup_comment, 'databanks': []}
        for databank in databanks:
            backup_file_name = os.path.basename(databank[1])
            if databank[0] == 'SYSDB':
                sysdb_file_name = backup_file_name
            con.execute("create backup in '{0}/{1}' for databank {2}".format(backup_path, backup_file_name, databank[0]))
            #print("create backup in '{0}/{1}' for databank {2}".format(backup_path, backup_file_name, databank[0]))
        
        con.execute("commit backup")
        return_val['return_code'] = 'success'
        return_val['backup_id'] = backup_id
        return_val['databanks'] = []

        for databank in databanks:
            dbcheck_ret = __verify_databank(os.path.join(backup_path, os.path.basename(databank[1])), os.path.join(backup_path, os.path.basename(sysdb_file_name)))
            if dbcheck_ret[0] == 'OK':
                return_val['databanks'].append({'databank': databank[0], 'databank_check': dbcheck_ret[0]})
            else:
                return_val['return_code'] = 'failure'
                return_val['databanks'].append({'databank': databank[0], 'databank_check': dbcheck_ret[0], 'databank_check_message': dbcheck_ret[1]})
            backup_info['databanks'].append(
                {'databank': databank[0], 'file_name': databank[1], 'verification': dbcheck_ret[0], 'last_backup': databank[2] if databank[2] else ''}
                )
        backup_info['backup_status'] = return_val['return_code']
        with open(backup_path + '/backup_info.json', 'w') as f:
            f.write(json.dumps(backup_info, indent=4))
    except  mimexception.Error as e:
        return_val['return_code'] = 'failure'
        return_val['error_code'] = get_mimer_error_code(e)
        return_val['error_message'] = get_mimer_error_text(e)
        if con != None:
            con.execute("rollback backup")
    finally:
        if con != None:
            con.close()
    return return_val



def restore_backup(database_name: str, user_name:str, password:str, backup_location:str, backup_id:str = None, restore_log:bool = False, keep_transdb:bool = True):
    """Restore from backup.

    Args:
        database_name(str): The database name. Mandatory.
        user_name(str): The Mimer SQL user that will create the backup. Mandatory.
        password(str): Password for the Mimer SQL backup user. Mandatory.
        backup_location)(str): The base location for the backup, for example /data/backup. Mandatory
        backup_id(str): Optional backup_id of backup to be restored.
        restore_log(bool): Should the log be restored. Default false
        keep_transdb(bool): When restoreing using log, should we keep transdb. Default true. 
        

        For a full restore of any backup, specify backup_id.

        For a full restore of the latest backup without restoring the log,
        specify no backup_id and restore_log=False.

        For a full resore of the latest backup and then re-applying the log file to get to the current state,
        specify no backup_id and restore_log=True. This only work if the LOGDB is intact. keep_transdb = true mean that the TRANSDB from the live system is kept.
        This way, all transactions that was made when the system stopped is kept, even if they haven't been flushed to LOGDB.
        If the live TRANSDB have been corrupted or there, for some reason, are problems to restart the system with the live TRANSDB,
        keep_transdb=false can be used to take TRANSDB from the backup instead. If restore_log=false, transdb is always copied from the backup.


    Returns:
        {
            "return_code": "success",
        }
        or if the restore operation fails:
        {
            "return_code":"failure", 
            "error_code": <error code>, 
            "error_message":"<error message>"
        }
    """
    transdb_file_name=None
    logdb_file_name=None
    sysdb_file_name=None
    sqldb_file_name=None
    backup_info = { 'databanks': []}
    return_val = {
        'return_code': 'failure'
    }
    db_user_name = user_name
    db_password = password

    parameterStatus = check_params({"database_name":database_name,"backup_location":backup_location,"user_name":db_user_name,"password":db_password})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    
    (mimversion, _) = __get_mimer_version()

    #Find the correct backup directory and verify all databanks in the backup folder. Return an error if they are not ok
    try:
        if backup_id == None or restore_log:
            (_, all_backups, _) = next(os.walk(backup_location))
            #all_backups = os.listdir(backup_location)
            all_backups.sort()
            tmp_backup_id = all_backups[-1]
            if backup_id == None:
                backup_id = tmp_backup_id
            else:
                if restore_log:
                    #Verify that the named backup is actually the latest
                    if backup_id != tmp_backup_id:
                        return {'return_code': 'failure', 'error_code': -1,
                            'error_message': 'The named backup ' + backup_id + ' is not the latest. The lates backup is ' + tmp_backup_id + '. When restoring from log, only the last backup can be used. Do not give backup_name or specify the latest one'}
                else:
                    backup_id = tmp_backup_id
        backup_path = os.path.join(backup_location, backup_id)
        #Get backup_info.json
        if not os.path.isfile(os.path.join(backup_path, 'backup_info.json')):
            return {'return_code': 'failure', 'error_code': -1,
                    'error_message': 'backup_info.json not found'}

        with open(os.path.join(backup_path, 'backup_info.json')) as f:
            backup_info = json.loads(f.read())
            backup_files = backup_info['databanks']
            for backup_db_info in backup_files:
                if backup_db_info.get('databank') == 'TRANSDB':
                    transdb_file_name = backup_db_info.get('file_name')
                elif backup_db_info.get('databank') == 'SYSDB':
                    sysdb_file_name = backup_db_info.get('file_name')
                elif backup_db_info.get('databank') == 'LOGDB':
                    logdb_file_name = backup_db_info.get('file_name')
                elif backup_db_info.get('databank') == 'SQLDB':
                    sqldb_file_name = backup_db_info.get('file_name')
        #Check that we have all system databanks
        if not transdb_file_name or not sysdb_file_name or not logdb_file_name or  not sqldb_file_name:
            return_val['return_code'] = 'failure'
            return_val['error_code'] = -1
            return_val['error_message'] = 'The backup does not contain all system databanks'
            return return_val
        for db_file in backup_files:
            db_ver = __verify_databank(os.path.join(backup_path, os.path.basename(db_file['file_name'])), os.path.join(backup_path, os.path.basename(sysdb_file_name)))
            if db_ver[0] != 'OK':
                return_val['return_code'] = 'failure'
                return_val['error_code'] = -2
                return_val['error_message'] = 'Could not verify all databanks. First error was: ' + db_ver[1]
                return return_val
        #Ok, we have a valid backup to restore from
        #Check the current status of the server and stop it if running
        check_res = check_status(database_name)
        if check_res['return_code'] == 'success':
            #If we will not restore the log, kill the server
            if not restore_log:
                stop_res = stop_database(database_name, kill=True)
            else:
                if check_res['status'] == "Running":
                    stop_res = stop_database(database_name, kill=False)
                    if stop_res['return_code'] != 'success':
                        #We could not stop the server, kill it
                        stop_res = stop_database(database_name, kill=True)
                        if stop_res['return_code'] != 'success':
                            #We could not stop or kill the server, abort
                            return {'return_code': 'failure', 'error_code': -3,
                                    'error_message': 'Could not start or stop database: ' + stop_res['error_message']}
                elif check_res['status'] != "Running":
                    #Unknown state, don't continue the restore operation
                    return {'return_code': 'failure', 'error_code': -4,
                            'error_message': 'Unknown response when stopping database: ' + check_res['status']}
        else:
            return {'return_code': 'failure', 'error_code': -5,
                    'error_message': 'Unknown state of the database: ' + check_res['error_message']}

        #Copy all backup files back into the correct place
        m_dir = __get_mimer_data_dir(database_name)
        if m_dir == None or len(m_dir) == 0 or len(m_dir[0]) == 0:
            #Could not get Mimer data directory
            return {'return_code': 'failure', 'error_code': -6,
                    'error_message': 'Could not find Mimer database directory'}
        mimer_data_dir = m_dir[0]
        #If we want to keep LOGDB or TRANSDB from the live system, verify that they are ok first. Otherwise abort
        if restore_log:
            live_sysdb_file_name = os.path.join(mimer_data_dir, os.path.basename(sysdb_file_name))
            (live_logdb_path, live_logdb_file_name) = os.path.split(logdb_file_name)
            if not live_logdb_path:
                 live_logdb_file_name = os.path.join(mimer_data_dir, live_logdb_file_name)
            else:
                 live_logdb_file_name = logdb_file_name
            logdb_ver = __verify_databank(live_logdb_file_name, live_sysdb_file_name)
            if logdb_ver[0] != 'OK':
                return_val['return_code'] = 'failure'
                return_val['error_code'] = -2
                return_val['error_message'] = 'Live LOGDB is corrupt. You have to restore the backup using restore_log=false. Error: ' + logdb_ver[1]
                return return_val
            if keep_transdb == True:
                (live_transdb_path, live_transdb_file_name) = os.path.split(transdb_file_name)
                if not live_transdb_path:
                    live_transdb_file_name = os.path.join(mimer_data_dir, live_transdb_file_name)
                else:
                    live_transdb_file_name = transdb_file_name
                transdb_ver = __verify_databank(live_transdb_file_name, live_sysdb_file_name)
                if transdb_ver[0] != 'OK':
                    return_val['return_code'] = 'failure'
                    return_val['error_code'] = -2
                    return_val['error_message'] = 'Live TRANSDB is corrupt. You have to restore the backup using keep_transdb=false. Error: ' + transdb_ver[1]
                    return return_val
        for backup_db_info in backup_info['databanks']:
            backup_db = backup_db_info.get('databank')
            (backup_db_path, backup_db_file) = os.path.split(backup_db_info.get('file_name'))
            if restore_log and backup_db == 'LOGDB':
                continue
            if keep_transdb and backup_db == 'TRANSDB':
                continue
            src = os.path.join(backup_path, os.path.basename(backup_db_file))
            if len(backup_db_path) == 0:
                dst = mimer_data_dir
            else:
                dst = os.path.join(backup_db_path, backup_db_file)
            shutil.copy2(src, dst)

        if restore_log:
            #Restore SYSDB from the log to be able to start
            with subprocess.Popen(['echo', 'Y'], stdout=subprocess.PIPE) as p1:
                result = subprocess.run(['bsql', '-s', '-u' + db_user_name, '-p' + db_password, database_name ], stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                bsql_out = result.stdout.decode('utf-8').replace('\n', ' ').strip()
            #Even if we don't restore SYSDB, the returncode is 0 so we need to parse the output
            if result.returncode != 0 or not bsql_out.endswith('Databank SYSDB has been restored from log SQL>'):
                return_val['return_code'] = 'failure'
                return_val['error_code'] = result.returncode if result.returncode != 0 else -7
                if bsql_out == 'SQL>Y SQL&':
                    bsql_err = 'Illegal restore sequence, no log to apply.'
                    return_val['error_code'] = -8
                else:
                    bsql_err = bsql_out
                return_val['error_message'] = 'Could not restore SYSDB from LOG. You need to manually fix this problem using BSQL or '
                return_val['error_message'] += 'restore with restore_log=false and backup_id=' + backup_id + ' to fix the problem. Error: ' + bsql_err
                return return_val
        #Done, start the database server again
        start_res = start_database(database_name)
        if start_res['return_code'] != 'success':
            return_val['return_code'] = 'failure'
            return_val['error_code'] = start_res['error_code']
            return_val['error_message'] = 'Could not start database after restore: ' + start_res['error_message']
            return return_val
        
        #Restore user databanks if restore_log=true
        if restore_log:
            con = None
            try:
                con = mimerpy.connect(dsn = database_name, user = db_user_name, password = db_password)
                con.autocommit(True)
                databank_cursor = con.execute("select databank_name, file_name from information_schema.ext_databanks")
                databanks = databank_cursor.fetchall()
                databank_cursor.close()
                for databank in databanks:
                    if databank[0] not in ['SYSDB', 'LOGDB', 'TRANSDB', 'SQLDB']:
                        con.execute("alter databank {0} restore using log".format(databank[0]))
                          
            except  mimexception.Error as e:
                return_val['return_code'] = 'failure'
                return_val['error_code'] = get_mimer_error_code(e)
                return_val['error_message'] = get_mimer_error_text(e)
            finally:
                if con != None:
                    con.close()
        #Done
        return_val['return_code'] = 'success'
        return return_val  

    except OSError as e:
        return_val['return_code'] = 'failure'
        return_val['error_code'] = e.errno
        return_val['error_message'] = e.strerror
        return return_val
    except Exception as ex:
        return_val['return_code'] = 'failure'
        return_val['error_code'] = -1
        return_val['error_message'] = str(ex)
        return return_val

    return return_val

def __get_backup_info(backup_path: str, backup_id: str):
        #Get backup_info.json
        backup_path = os.path.join(backup_path, backup_id)
        if not os.path.isdir(backup_path):
            return {'return_code': 'failure', 'error_code': -1,
                    'error_message': 'Backup not found'}
        if not os.path.isfile(os.path.join(backup_path, 'backup_info.json')):
            return {'return_code': 'failure', 'error_code': -1,
                    'error_message': 'Invalid backup, backup_info.json not found'}

        with open(os.path.join(backup_path, 'backup_info.json')) as f:
            backup_info = json.loads(f.read())

        return {'return_code': 'success', **backup_info}


def show_backup(backup_location:str, backup_id:str):
    """Show information about a backup.

    Args:
        backup_location(str): The path to where the backup is located
        backup_id(str): The id of the backup

    Returns:
    {
        "return_code": "success",
        "backup_id": "id of backup",
        "backup_name": "A user supplied backup",
        "backup_comment": "A user supplied comment about the backup",
        "backup_date": "Date of backup",
        "databanks": [
            {
                "databank": "Databank name",
                "file_name": "Databank file",
                "verification": "Verification status",
                "last_backup": "Last time this databank was backed up"
            },
            ...
        ],
        "backup_status": "Status of the backup"
    }

    or if the operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }
    """
    parameterStatus = check_params({"backup_location":backup_location,"backup_id":backup_id})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus

    return __get_backup_info(backup_location, backup_id)


def list_backup(backup_path: str):
    """List backups with backup information.
    
    Args: 
        backup_location(str): Path to backups

    Returns:
    {
        "return_code": "success",
        "backups": [
            {
                "backup_id": "id of backup",
                "backup_name": "A user supplied backup",
                "backup_comment": "A user supplied comment about the backup",
                "backup_date": "Date of backup",
                "databanks": [
                    {
                        "databank": "Databank name",
                        "file_name": "Databank file",
                        "verification": "Verification status",
                        "last_backup": "Last time this databank was backed up"
                    },
                    ...
                ],
                "backup_status": "Status of the backup"
            },
            ...
        ]
    }

    or if the operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }
    """
    parameterStatus = check_params({"backup_location":backup_path})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus

    if not os.path.isdir(backup_path):
        return {'return_code': 'failure', 'error_code': -1,
                'error_message': 'Invalid backup_location'}  

    (_, all_backups, _) = next(os.walk(backup_path))
    all_backups.sort()
    return_val = { 'return_code': 'success', 'backups': []}
    for backup in all_backups:
        res = __get_backup_info(backup_path, backup)
        return_val['backups'].append(res)

    return return_val



def delete_backup(backup_location: str, backup_id: str):
    """Delete a backup.

    Args:
        backup_location(str): Location of the backup
        backup_id(str): ID (folder name) of backup to delete
    }

    Returns:
    {
        "return_code": "success",
    }
    or if the operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }
    """
    parameterStatus = check_params({"backup_location":backup_location,"backup_id":backup_id})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus

    backup_path = os.path.join(backup_location, backup_id)
    if not os.path.isdir(backup_path):
        return {'return_code': 'failure', 'error_code': -1,
                'error_message': 'Backup not found'}
    if not os.path.isfile(os.path.join(backup_path, 'backup_info.json')):
        return {'return_code': 'failure', 'error_code': -1,
                'error_message': 'Invalid backup, backup_info.json not found'}
    try:
        shutil.rmtree(backup_path)
    except OSError as e:
        return_val = {}
        return_val['return_code'] = 'failure'
        return_val['error_code'] = e.errno
        return_val['error_message'] = e.strerror
        return return_val
    return { 'return_code': 'success'}


 


def listUsers(database_name: str, creator_name: str, creator_pass: str):
    """List all users owned by the specified user

    Args:
        database_name(str): The database name
        creator_name(str): The creator name
        creator_pass(str): Creator password

    Returns: 
        {
            "return_code": "success",
            "user_list": [
                {
                    "user": "SYSADM"
                }
            ]
        }        

    """
    parameterStatus = check_params({"database_name":database_name,"creator_name": "creator_name", "creator_pass":creator_pass})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    list_users_sql = "select ident_name from information_schema.ext_idents where ident_type='USER' and ident_name <> ?"
    
    try:
        with mimerpy.connect(dsn=database_name, user=creator_name, password=creator_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(list_users_sql, (creator_name,))
                users = cursor.fetchall()
                user_list = []
                for user in users:
                    userinfo = {
                    'user':user[0]
                    }
                    user_list.append(userinfo)
        json_response['user_list'] = user_list
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def createUser(database_name: str, creator_name: str, creator_pass: str, user_name: str, user_pass: str, can_backup:bool = False, can_databank:bool = False, can_schema:bool = False, can_ident:bool = False):
    """Create a new user.

    Args:
        database_name(str): The database name
        creator_name(str): The user creating the new user
        creator_pass(str): Password for the user creating a new user
        user_name(str): The new user name
        user_pass(str): The new user password
        can_backup(str): Should the user have privilege to make a backup [true|false]
        can_databank(str): Should the user have privilege to create databanks [true|false]
        can_schema(str): Should the user have privilege to create schemas [true|false]
        can_ident(str): Should the user have privilege to create idents [true|false]

    Returns: 
    {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"creator_name":creator_name,"creator_password":creator_pass,"user_name":user_name,"user_password":user_pass})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    user_name = clean_sql_input(user_name)
    user_pass = escape_sql_input(user_pass)

    create_user_sql = "create ident " + user_name + " as user using '" + user_pass + "'"
    
    try:
        with mimerpy.connect(dsn=database_name, user=creator_name, password=creator_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(create_user_sql)
                if can_backup:
                    grant_sql = "grant backup to " + user_name
                    cursor.execute(grant_sql)
                if can_databank:
                    grant_sql = "grant databank to " + user_name
                    cursor.execute(grant_sql)
                if can_schema:
                    grant_sql = "grant schema to " + user_name
                    cursor.execute(grant_sql)
                if can_schema:
                    grant_sql = "grant ident to " + user_name
                    cursor.execute(grant_sql)

        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def deleteUser(database_name: str, creator_name: str, creator_pass: str, user_name: str, is_cascade: bool = False):
    """Delete user.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name
        creator_name: User that created the user to delete
        creator_pass: Password for the creator
        user_name: User to delete
        is_cascade: [true|false]. If true, the revoke has cascade effect


    Returns: 
    {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}
    See mimcontrol.createUser() for more details

    """
    parameterStatus = check_params({"database_name":database_name,"creator_name":creator_name,"creator_password":creator_pass,"user_name":user_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    user_name = clean_sql_input(user_name)
    drop_user_sql = "drop ident " + user_name
    if is_cascade:
        drop_user_sql += " cascade"
    
    
    try:
        with mimerpy.connect(dsn=database_name, user=creator_name, password=creator_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(drop_user_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def changeUserPass(database_name: str, login_name: str, login_pass: str, user_name: str,  new_pass: str):
    """Reset the password by the user's creator

    Args:
        database_name(str): The database name
        login_name: The user that will perform the password change
        login_pass: Password for the user that will perform the password change
        user_name: The user to change password for
        new_password: New password
    }


    Returns: 
    {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"creator_name":login_name,"creator_password":login_pass,"user_name":user_name,"user_password":new_pass})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    user_name = clean_sql_input(user_name)
    new_pass = escape_sql_input(new_pass)
    update_user_pass_sql = "alter ident " + user_name + " set password '" + new_pass + "'"
    
    try:
        with mimerpy.connect(dsn=database_name, user=login_name, password=login_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(update_user_pass_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def userLookup(database_name: str, user_name: str, user_pass: str):
    """Look up for user information.

    Args:
        database_name(str): The database name
        user_name: User name to lookup
        user_pass: Password of the user

    Returns: 
        {
            "return_code": "success",
            "creator": "SYSTEM",
            "function_privileges": [
                {
                    "function_name": "BUILTIN.BINARY",
                    "grantor": "EXECUTE",
                    "privilege": "BUILTIN.BINARY"
                },
                ...
            ],
            "own_schemas": [
                {
                    "schema_name": "SYSADM"
                },
                ...
            ],
            "procedure_privileges": [
                {
                    "procedure_name": "SYSADM.p1",
                    "privilege": "EXECUTE",
                    "grantor": "SYSTEM",
                    "is_grantable":"YES"
                },
                ...
            ],

            "sub_users": [
                {
                    "sub_user": "test"
                },
                ...
            ],
            "groups": [
                {
                    "group_name": "SYSADM.testgroup"
                },
                ...
            ],
            "system_privileges": [
                {
                    "grantor": "_SYSTEM",
                    "privilege": "BACKUP"
                },
                ...
            ],
            "table_privileges": [
                {
                    "table_name": "T1", 
                    "privilege": "INSERT",
                    "grantor": "SYSTEM",
                    "is_grantable":"YES",
                    "table_type"
                }
            ]
        }
        or
        {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"user_name":user_name,"password":user_pass})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    schema_sql = "select schema_name from information_schema.SCHEMATA where schema_owner = ? and schema_name not in ('SYSTEM','INFORMATION_SCHEMA','MIMER','ODBC')"
    system_privilege_sql = "select privilege_type, grantor, is_grantable from information_schema.ext_system_privileges where grantee = ?"
    table_sql = "select tp.table_schema||'.'||tp.table_name, privilege_type, grantor, is_grantable, table_type from information_schema.table_privileges tp join "
    table_sql += " information_schema.tables t on t.table_schema = tp.table_schema and t.table_name = tp.table_name  where tp.table_schema not in ('SYSTEM','INFORMATION_SCHEMA','MIMER')"
    function_sql = "select object_schema||'.'||object_name, privilege_type, grantor, is_grantable from information_schema.ext_object_privileges where object_type='function'"
    procedure_sql = "select object_schema||'.'||object_name, privilege_type, grantor, is_grantable from information_schema.ext_object_privileges where object_type='procedure'"
    creator_sql = "select ident_creator from information_schema.ext_idents where ident_name = ?"
    subuser_sql = "select ident_name from information_schema.ext_idents where ident_creator = ? and ident_type='USER'"
    group_sql = "select object_schema||'.'||object_name, privilege_type, grantor, is_grantable from information_schema.ext_object_privileges where privilege_type='member' and grantee = ?"

    try:
        with mimerpy.connect(dsn=database_name, user=user_name, password=user_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(schema_sql, (user_name,))
                schemas = cursor.fetchall()
                schema_list = []
                for schema in schemas:
                    schema_list.append({'schema_name':schema[0]})
                
                cursor.execute(system_privilege_sql, (user_name,))
                system_privileges = cursor.fetchall()
                sp_list = []
                for sp in system_privileges:
                    sp_list.append({'privilege':sp[0],'grantor':sp[1], 'is_grantable': sp[2]})
                
                cursor.execute(table_sql)
                table_privileges = cursor.fetchall()
                tp_list = []
                for tp in table_privileges:
                    tp_list.append({'table_name':tp[0], 'privilege':tp[1],'grantor':tp[2], 'is_grantable': tp[3], 'table_type': tp[4]})

                cursor.execute(function_sql)
                function_privileges = cursor.fetchall()
                fp_list = []
                for fp in function_privileges:
                    fp_list.append({'function_name':fp[0], 'privilege':fp[1],'grantor':fp[2], 'is_grantable': fp[3]})

                cursor.execute(procedure_sql)
                procedure_privileges = cursor.fetchall()
                pp_list = []
                for pp in procedure_privileges:
                    pp_list.append({'procedure_name':pp[0], 'privilege':pp[1],'grantor':pp[2], 'is_grantable': pp[3]})

                cursor.execute(creator_sql, (user_name,))
                creator = cursor.fetchone()
                json_response['creator'] = creator[0] 
                    
                cursor.execute(subuser_sql, (user_name,))
                subs = cursor.fetchall()
                sub_list = []
                for sub in subs:
                    sub_list.append({'sub_user':sub[0]})

                cursor.execute(group_sql, (user_name,))
                groups = cursor.fetchall()
                group_list = []
                for group in groups:
                    group_list.append({'group_name':group[0]})

        json_response['sub_users'] = sub_list
        json_response['own_schemas'] = schema_list
        json_response['system_privileges'] = sp_list
        json_response['table_privileges'] = tp_list
        json_response['function_privileges'] = fp_list
        json_response['procedure_privileges'] = pp_list
        json_response['groups'] = group_list
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response




def grantAccessPrivilege(database_name: str, grantor_name: str, grantor_pass: str, grantee: str, table_name: str, privilege: str, is_grantable: bool = False):
    """Grant access privilege (select, insert, update, delete, references, all) of a table 
    to a grantee (user, group) by the grantor.

    Args:
        database_name(str): The database name
        grantor_name(str): grantor name
        grantor_pass(str): password of the grantor 
        grantee(str): grantee name 
        table_name(str): name of the table to be accessed
        privilege(str): type of privilege (can be [select|inser|update|delete|references|all])
        is_grantable(str): [true|false]. If true, the grantee can grant the privilege to other users/groups

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"grantor_name":grantor_name,"grantor_password":grantor_pass,"grantee_name":grantee,"table_name":table_name,"privilege":privilege})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    grant_access_sql = "grant " + clean_sql_input(privilege) + " on " + clean_sql_input(table_name) + " to " + clean_sql_input(grantee)
    if is_grantable:
        grant_access_sql += " with grant option"
    
    try:
        with mimerpy.connect(dsn=database_name, user=grantor_name, password=grantor_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(grant_access_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def grantExecutePrivilege(database_name: str, grantor_name: str, grantor_pass: str, grantee: str, routine_name: str, routine_type: str, is_grantable: bool = False):
    """Grant execution privilege of a function or procedure 
    to a grantee (user, group) by the grantor.

    Args:
        database_name(str): The database name
        grantor_name(str): grantor name
        grantor_pass(str): password of the grantor 
        grantee(str): grantee name 
        routine_name(str): name of the function or procedure
        routine_type(str): [function|procedure]
        is_grantable(str): [true|false]. If true, the grantee can grant the privilege to other users/groups

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"grantor_name":grantor_name,"grantor_password":grantor_pass,"grantee_name":grantee,"routine_name":routine_name,"routine_type":routine_type})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    if(routine_type not in ['function', 'procedure']):
        return {"return_code":"failure", "error_code": -1, "error_message":"Unkown routine_type, valid values are function and procedure"}
    json_response = {}

    grant_function_sql = clean_sql_input("grant execute on function " + routine_name + " to " + grantee)
    grant_procedure_sql = clean_sql_input("grant execute on procedure " + routine_name + " to " + grantee)
    if is_grantable:
        grant_function_sql += " with grant option"
        grant_procedure_sql += " with grant option"
    
    try:
        with mimerpy.connect(dsn=database_name, user=grantor_name, password=grantor_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                if routine_type=="function":
                    cursor.execute(grant_function_sql)
                else:
                    cursor.execute(grant_procedure_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def grantSystemPrivilege(database_name: str, grantor_name: str, grantor_pass: str, grantee: str, privilege_type: str, is_grantable: bool = False):
    """Grant system privilege (backup, schema, databank, ident, shadow, statistics) to a grantee by the grantor.

    Args:
        database_name(str): The database name
        grantor_name(str): grantor name
        grantor_pass(str): password of the grantor 
        grantee(str): grantee name 
        privilege(str): [backup|schema|databank|ident|shadow|statistics]
        is_grantable(str): [true|false]. If true, the grantee can grant the privilege to other users/groups
 
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"grantor_name":grantor_name,"grantor_password":grantor_pass,"grantee_name":grantee,"privilege":privilege_type})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    if(privilege_type.lower() not in ['schema', 'backup', 'ident', 'databank', 'shadow', 'statistics']):
        return {"return_code":"failure", "error_code": -1, "error_message":"Unkown privilege type"}
    json_response = {}
    grant_system_sql = clean_sql_input("grant " + privilege_type + " to " + grantee)
    if is_grantable:
        grant_system_sql += " with grant option"
    
    try:
        with mimerpy.connect(dsn=database_name, user=grantor_name, password=grantor_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(grant_system_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def grantMembership(database_name: str, grantor_name: str, grantor_pass: str, grantee: str, group_name: str, is_grantable: bool = False):
    """Grant membership of a group to a grantee by the group owner.

    Args:
        database_name(str): The database name
        grantor_name(str): grantor name
        grantor_pass(str): password of the grantor 
        grantee(str): grantee name 
        group_name(str): name of the group
        is_grantable(str): [true|false]. If true, the grantee can grant the privilege to other users
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"grantor_name":grantor_name,"grantor_password":grantor_pass,"grantee_name":grantee,"group_name":group_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    grant_member_sql = clean_sql_input("grant member on group " + group_name + " to " + grantee)
    if is_grantable:
        grant_member_sql += " with grant option"
    
    try:
        with mimerpy.connect(dsn=database_name, user=grantor_name, password=grantor_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(grant_member_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response

def revokeAccessPrivilege(database_name: str, grantor_name: str, grantor_pass: str, grantee: str, table_name: str, privilege: str, is_cascade: bool = False):
    """Revoke access privilege (select, insert, update, delete, references, all) of a table 
    from a grantee (user, group) by the grantor.

    Args:
        database_name(str): The database name
        grantor_name(str): grantor name
        grantor_pass(str): password of the grantor 
        grantee(str): grantee name 
        table_name(str): name of the table to be accessed
        privilege(str): type of privilege (can be [select|insert|update|delete|references|all])
        is_cascade(str): [true|false]. If true, the revoke has cascade effect
    }

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"grantor_name":grantor_name,"grantor_password":grantor_pass,"grantee_name":grantee,"table_name":table_name,"privilege":privilege})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    grant_access_sql = clean_sql_input("revoke " + privilege + " on " + table_name + " from " + grantee)
    if is_cascade:
        grant_access_sql += " cascade"
    
    try:
        with mimerpy.connect(dsn=database_name, user=grantor_name, password=grantor_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(grant_access_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def revokeExecutePrivilege(database_name: str, grantor_name: str, grantor_pass: str, grantee: str, routine_name: str, routine_type: str, is_cascade: bool = False):
    """Revoke execution privilege of a function or procedure from a grantee (user, group) by the grantor.

    Args:
        database_name(str): The database name
        grantor_name(str): grantor name
        grantor_pass(str): password of the grantor 
        grantee(str): grantee name 
        routine_name(str): name of the function or procedure
        routine_type(str): [function|procedure]
        is_cascade(str): [true|false]. If true, the revoke has cascade effect

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"grantor_name":grantor_name,"grantor_password":grantor_pass,"grantee_name":grantee,"routine_name":routine_name,"routine_type":routine_type})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    if(routine_type not in ['function', 'procedure']):
        return {"return_code":"failure", "error_code": -1, "error_message":"Unkown routine_type, valid values are function and procedure"}
    json_response = {}

    grant_function_sql = clean_sql_input("revoke execute on function " + routine_name + " from " + grantee)
    grant_procedure_sql = clean_sql_input("revoke execute on procedure " + routine_name + " from " + grantee)
    if is_cascade:
        grant_function_sql += " cascade"
        grant_procedure_sql += " cascade"
    
    try:
        with mimerpy.connect(dsn=database_name, user=grantor_name, password=grantor_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                if routine_type=="function":
                    cursor.execute(grant_function_sql)
                else:
                    cursor.execute(grant_procedure_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def revokeSystemPrivilege(database_name: str, grantor_name: str, grantor_pass: str, grantee: str, privilege_type: str, is_cascade: bool = False):
    """Revoke system privilege (backup, schema, databank, ident, shadow, statistics) from a grantee by the grantor.

    Args:
        database_name(str): The database name
        grantor_name(str): grantor name
        grantor_pass(str): password of the grantor 
        grantee_name(str): grantee name 
        privilege(str): [backup|schema|databank|ident|shadow|statistics]
        is_cascade(str): [true|false]. If true, the revoke has cascade effect

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"grantor_name":grantor_name,"grantor_password":grantor_pass,"grantee_name":grantee,"privilege":privilege_type})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    if(privilege_type.lower() not in ['schema', 'backup', 'ident', 'databank', 'shadow', 'statistics']):
        return {"return_code":"failure", "error_code": -1, "error_message":"Unkown privilege type"}
    json_response = {}
    grant_system_sql = clean_sql_input("revoke " + privilege_type + " from " + grantee)
    if is_cascade:
        grant_system_sql += " cascade"
    
    try:
        with mimerpy.connect(dsn=database_name, user=grantor_name, password=grantor_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(grant_system_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def revokeMembership(database_name: str, grantor_name: str, grantor_pass: str, grantee: str, group_name: str, is_cascade: bool = False):
    """Revoke membership of a group from a grantee by the group owner.

    Args:
        database_name(str): The database name
        grantor_name(str): grantor name
        grantor_pass(str): password of the grantor 
        grantee(str): grantee name 
        group_name(str): name of the group
        is_cascade(str): [true|false]. If true, the revoke has cascade effect
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"grantor_name":grantor_name,"grantor_password":grantor_pass,"grantee_name":grantee,"group_name":group_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    grant_member_sql = clean_sql_input("revoke member on group " + group_name + " from " + grantee)
    if is_cascade:
        grant_member_sql += " cascade"
    
    try:
        with mimerpy.connect(dsn=database_name, user=grantor_name, password=grantor_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(grant_member_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def createGroup(database_name: str, creator_name: str, creator_pass: str, group_name: str):
    """Create a new group.

    Args:
        database_name(str): The database name
        creator_name(str): grantor name
        creator_pass(str): password of the creator 
        group_name(str): group name 
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"creator_name":creator_name,"creator_password":creator_pass,"group_name":group_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    create_group_sql = clean_sql_input("create ident " + group_name + " as group")
    
    try:
        with mimerpy.connect(dsn=database_name, user=creator_name, password=creator_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(create_group_sql)
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def deleteGroup(database_name: str, creator_name: str, creator_pass: str, group_name: str, is_cascade: bool = False):
    """Delete a group.

    Args:
        database_name(str): The database name
        creator_name(str): grantor name
        creator_pass(str): password of the creator 
        group_name(str): group name 
        is_cascade(str): [true|false]. If true, the revoke has cascade effect
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"creator_name":creator_name,"creator_password":creator_pass,"group_name":group_name})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    drop_group_sql = clean_sql_input("drop ident " + group_name)
    if is_cascade:
        drop_group_sql += " cascade"
    
    try:
        with mimerpy.connect(dsn=database_name, user=creator_name, password=creator_pass) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(drop_group_sql)
                json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response


def groupLookup(database_name: str, creator_name: str, creator_pass: str, group_name: str):
    """Look up for group information.

    Args:
        database_name(str): The database name
        creator_name: creator name
        creator_password: password of the creator
        group_name: name of the group

    Returns: 
        {
            "return_code":"success",
            "creator":"SYSADM",
            "function_privileges":[],
            "members":[{"member":"test"}],
            "procedure_privileges":[{"grantor":"SYSADM","is_grantable":"YES","privilege":"EXECUTE","procedure_name":"SYSADM.p1"}],
            "system_privileges":[{"grantor":"SYSADM","is_grantable":"YES","privilege":"BACKUP"}],
            "table_privileges":[{"grantor":"SYSADM","is_grantable":"YES","privilege":"DELETE","table_name":"SYSADM.t"},{"grantor":"SYSADM","is_grantable":"YES","privilege":"INSERT","table_name":"SYSADM.t"},{"grantor":"SYSADM","is_grantable":"YES","privilege":"LOAD","table_name":"SYSADM.t"},{"grantor":"SYSADM","is_grantable":"YES","privilege":"REFERENCES","table_name":"SYSADM.t"},{"grantor":"SYSADM","is_grantable":"YES","privilege":"SELECT","table_name":"SYSADM.t"},{"grantor":"SYSADM","is_grantable":"YES","privilege":"UPDATE","table_name":"SYSADM.t"}]
        } 
        or
        {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    parameterStatus = check_params({"database_name":database_name,"creator_name":creator_name,"group_name":group_name,"creator_password":creator_pass})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    
    json_response = {}
    system_privilege_sql = "select privilege_type, grantor, is_grantable from information_schema.ext_system_privileges where grantee = ?"
    table_sql = "select table_schema||'.'||table_name, privilege_type, grantor, is_grantable from information_schema.table_privileges where grantee = ? and table_schema not in ('SYSTEM','INFORMATION_SCHEMA','MIMER')"
    function_sql = "select object_schema||'.'||object_name, privilege_type, grantor, is_grantable from information_schema.ext_object_privileges where grantee = ? and object_type='function'"
    procedure_sql = "select object_schema||'.'||object_name, privilege_type, grantor, is_grantable from information_schema.ext_object_privileges where grantee = ? and object_type='procedure'"
    creator_sql = "select ident_creator from information_schema.ext_idents where ident_name = ?"
    member_sql = "select grantee from information_schema.ext_object_privileges where object_name = ? and grantor = ? and privilege_type='MEMBER'"

    try:
        with mimerpy.connect(dsn=database_name, user=creator_name, password=creator_pass) as mimer_con:
            with mimer_con.cursor() as cursor:            
                cursor.execute(system_privilege_sql, (group_name,))
                system_privileges = cursor.fetchall()
                sp_list = []
                for sp in system_privileges:
                    sp_list.append({'privilege':sp[0],'grantor':sp[1], 'is_grantable': sp[2]})
                
                cursor.execute(table_sql, (group_name,))
                table_privileges = cursor.fetchall()
                tp_list = []
                for tp in table_privileges:
                    tp_list.append({'table_name':tp[0], 'privilege':tp[1],'grantor':tp[2], 'is_grantable': tp[3]})

                cursor.execute(function_sql, (group_name,))
                function_privileges = cursor.fetchall()
                fp_list = []
                for fp in function_privileges:
                    fp_list.append({'function_name':fp[0], 'privilege':fp[1],'grantor':fp[2], 'is_grantable': fp[3]})

                cursor.execute(procedure_sql, (group_name,))
                procedure_privileges = cursor.fetchall()
                pp_list = []
                for pp in procedure_privileges:
                    pp_list.append({'procedure_name':pp[0], 'privilege':pp[1],'grantor':pp[2], 'is_grantable': pp[3]})

                cursor.execute(creator_sql, (group_name,))
                creator = cursor.fetchone()
                if creator:
                    json_response['creator'] = creator[0] 
                    
                cursor.execute(member_sql, (group_name,creator_name))
                members = cursor.fetchall()
                member_list = []
                for member in members:
                    member_list.append({'member':member[0]})

        json_response['system_privileges'] = sp_list
        json_response['table_privileges'] = tp_list
        json_response['function_privileges'] = fp_list
        json_response['procedure_privileges'] = pp_list
        json_response['members'] = member_list
        json_response['return_code'] = 'success'
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    return json_response



def createSchema(database_name: str, user:str, password: str, schema_name: str):
    """Create new database schema owned by the specified user.

    Args:
        database_name(str): The database name
        user(str): Owner and creator of the schema (used to login)
        password(str): Password for the owner and creator of the schema
        schema_name(str): Name of the new schema

    Returns:
    {
        "return_code": "success",
    }
    or if the operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }

    """
    parameterStatus = check_params({"database_name":database_name,"schema_name":schema_name,"user":user,"password":password})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    try:
        with mimerpy.connect(dsn=database_name, user=user, password=password, autocommit=False) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(clean_sql_input("CREATE SCHEMA " + schema_name))
                mimer_con.commit()
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    json_response['return_code'] = 'success'
    return json_response

def deleteSchema(database_name: str, user:str, password: str, schema_name: str):
    """Delete a database schema.

    Args:
        database_name(str): The database name
        user(str): Mimer SQL user with permissions to delete the schema
        password(str): Password for the owner and creator of the schema
        schema_name(str): Name of the new schema

    Returns:
    {
        "return_code": "success",
    }
    or if the operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }

    """
    parameterStatus = check_params({"database_name":database_name,"schema_name":schema_name,"user":user,"password":password})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    try:
        with mimerpy.connect(dsn=database_name, user=user, password=password, autocommit=False) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(clean_sql_input("DROP SCHEMA " + schema_name))
                mimer_con.commit()
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    json_response['return_code'] = 'success'
    return json_response

def listSchema(database_name: str, user:str, password: str):
    """List database schema.

    List the schemas visisible for the specified user.

    Args:
        database_name(str): The database name
        user(str): Mimer SQL user to list schemas for
        password(str): Password for the user to list schemas for

    Returns:
    {
        "return_code": "success",
        "schema_list [
            {"schema_name": <schema name>},
            ...
        ]
    }
    or if the operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }

    """
    parameterStatus = check_params({"database_name":database_name,"user":user,"password":password})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    try:
        with mimerpy.connect(dsn=database_name, user=user, password=password, autocommit=True) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA")
                schemas = cursor.fetchall()
                schema_list = []
                for schema in schemas:
                    schema_list.append({'schema_name': schema[0]})
                json_response['schema_list'] = schema_list
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    json_response['return_code'] = 'success'
    return json_response

def schemaLookup(database_name: str, schema_name: str, user:str, password: str):
    """Lookup database schema.

    Describe a databasee schema. Show all tables, views, indexes,
    procedures, functions, and schema owner.

    Args:
        database_name(str): The database name
        schema_name(str): The schema to desribe
        user(str): Mimer SQL user to list schemas for
        password(str): Password for the user to list schemas for

    Returns:
    {
        "return_code": "success",
        "schema_owner": "SYSADM",
        "tables": [
            {
                "table_name": "<table name>"
            },
            ...
        ],
        "views": [
            {
                "view_name": "<view name>"
            },
            ...
        ],
        "functions": [
            {
                "function_name": "<function name>"
            },
            ...
        ],
        "indexes": [
            {
                "index_name": "tab1_name_idx"
            },
            ...
        ],
        "procedures": [
            {
                "procedure_name": "<procedure name>"
            },
            ...
        ]
    }
    or if the operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }
    """
    parameterStatus = check_params({"database_name":database_name,"schema_name":schema_name, "user":user,"password":password})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    try:
        with mimerpy.connect(dsn=database_name, user=user, password=password, autocommit=True) as mimer_con:
            with mimer_con.cursor() as cursor:
                #Start by checking if the schema actually exist
                cursor.execute("select * from INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", schema_name)
                result = cursor.fetchall()
                if len(result) <= 0:
                    json_response['return_code'] = 'failure'
                    json_response['error_code'] = -23006
                    json_response['error_message'] = 'Schema not found'
                    return json_response

                cursor.execute("SELECT TABLE_NAME from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'", schema_name)
                result = cursor.fetchall()
                table_name = []
                for column_name in result:
                    table_name.append({"table_name":column_name[0]})
                json_response['tables'] = table_name

                cursor.execute("select TABLE_NAME from INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = ?", schema_name)
                result = cursor.fetchall()
                table_name = []
                for column_name in result:
                    table_name.append({"view_name":column_name[0]})
                json_response['views'] = table_name

                cursor.execute("select ROUTINE_NAME from information_schema.routines where routine_type='procedure' and routine_schema = ?;", schema_name)
                result = cursor.fetchall()
                table_name = []
                for column_name in result:
                    table_name.append({"procedure_name":column_name[0]})
                json_response['procedures'] = table_name

                cursor.execute("select ROUTINE_NAME from information_schema.routines where routine_type='FUNCTION' and routine_schema = ?;", schema_name)
                result = cursor.fetchall()
                table_name = []
                for column_name in result:
                    table_name.append({"function_name":column_name[0]})
                json_response['functions'] = table_name

                cursor.execute("select INDEX_NAME from INFORMATION_SCHEMA.EXT_INDEX_COLUMN_USAGE where INDEX_SCHEMA = ?", schema_name)
                result = cursor.fetchall()
                table_name = []
                for column_name in result:
                    table_name.append({"index_name":column_name[0]})
                json_response['indexes'] = table_name

                cursor.execute("select SCHEMA_OWNER from INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", schema_name)
                result = cursor.fetchone()
                json_response['schema_owner'] = result[0]
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    json_response['return_code'] = 'success'
    return json_response

def tableLookup(database_name: str, schema_name: str, table_name: str, user:str, password: str):
    """Lookup table.

    Describe a table
        database_name(str): The database name
        user(str): Login username
        password(str): Login password
        schema_name(str): Schema of the table to describe
        table_name(str): The table to describe

    Returns:
    {
        "columns": [
            {
                "column_name": "c1",
                "column_type": "INTEGER",
                "primary_key": true
            },
            {
                "column_name": "c2",
                "column_type": "CHARACTER VARYING",
                "primary_key": false
            }
        ],
        "number_of_rows": 10,
        "return_code": "success"
    }
    or if the operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }
    """
    parameterStatus = check_params({"database_name":database_name,"schema_name":schema_name,"table_name":table_name,"user":user,"password":password})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    sql = "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?"
    sql2 = "select constraint_name from information_schema.table_constraints where constraint_type='primary key' and table_Schema = ? and table_name = ?"
    sql3 = "select column_name from information_schema.key_column_usage where constraint_name = ?"
    sqlCount = "select count(*) from " + schema_name + "." + table_name
    try:
        with mimerpy.connect(dsn=database_name, user=user, password=password, autocommit=True) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(sql, (schema_name, table_name))
                result = cursor.fetchall()
                columns = []
                cursor.execute(sql2, (schema_name, table_name))
                primaryKeys = cursor.fetchall()
                primaryColumns = []
                for pKey in primaryKeys:
                    cursor.execute(sql3, pKey)
                    re = cursor.fetchall()
                    primaryColumns.append(re)
                for column_name,column_type in result:
                    appendValue = {"column_name":column_name, "column_type":column_type, "primary_key":False}
                    if(len(primaryColumns) > 0):
                        for columnsNames in primaryColumns[0]:
                            if column_name in columnsNames:
                                appendValue["primary_key"] = True
                    columns.append(appendValue)
                json_response['columns'] = columns
                cursor.execute(sqlCount)
                json_response['number_of_rows'] = cursor.fetchone()[0]
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    json_response['return_code'] = 'success'
    return json_response

def viewLookup(database_name: str, schema_name: str, view_name: str, user:str, password: str):
    """Lookup view.

    Describe a view

    Args:
        database_name(str): The database name
        user(str): Login username
        password(str): Login password
        schema_name(str): Schema of the view to describe
        view_name(str): The view to describe

    Returns:
    {
        "columns": [
            {
                "column_name": "c1"
            },
            ...
        ],
        "number_of_rows": 10,
        "return_code": "success"
    }
    or if the operation fails:
    {
        "return_code":"failure", 
        "error_code": <error code>, 
        "error_message":"<error message>"
    }
    """
    parameterStatus = check_params({"database_name":database_name,"schema_name":schema_name,"view_name":view_name,"user":user,"password":password})
    if(parameterStatus['return_status'] == 'failure'):
        return parameterStatus
    json_response = {}
    sql = "select COLUMN_NAME from INFORMATION_SCHEMA.VIEW_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND VIEW_NAME = ?"
    sqlCount = "select count(*) from " + schema_name + "." + view_name
    try:
        with mimerpy.connect(dsn=database_name, user=user, password=password, autocommit=True) as mimer_con:
            with mimer_con.cursor() as cursor:
                cursor.execute(sql, (schema_name, view_name))
                result = cursor.fetchall()
                columns = []
                for column_name in result:
                    columns.append({"column_name":column_name[0]})   
                json_response['columns'] = columns
                cursor.execute(sqlCount)
                json_response['number_of_rows'] = cursor.fetchone()[0]
    except mimexception.Error as e:
        json_response['return_code'] = 'failure'
        json_response['error_code'] = get_mimer_error_code(e)
        json_response['error_message'] = get_mimer_error_text(e)
        return json_response
    json_response['return_code'] = 'success'
    return json_response

if __name__ == "__main__":
    print("Mimer SQL REST controller")


