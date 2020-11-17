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
"""The Mimer SQL REST controller.

The mimer_controller.py handles all the HTTP(S) requests. The different Mimer SQL administration/monitoring is
implemented in mimcontrol.py.

The different web service calls can be testd using curl. If a proper OpenSSL certificate haven't be created
it might be necesary to ignore security problems. Here is an example:
    curl --insecure -u mimadmin:pass -H "Content-Type: application/json" 
    -X POST -d '{"old_password":"25bG517VOa","new_password":"SYSADM"}'  
    https://localhost:5001/update_sysadm_pass/mimerdb

"""
import sys
import os
import subprocess
from flask import Flask, request, jsonify
from flask_htpasswd import HtPasswdAuth
import json
import mimcontrol

app = Flask(__name__)


authFile=os.environ.get('MIMER_REST_CONTROLLER_AUTH_FILE', '.htpasswd')
app.config['FLASK_HTPASSWD_PATH'] = authFile
app.config['FLASK_SECRET'] = 'Mimer SQL REST Controller'
htpasswd = HtPasswdAuth(app)


@app.route('/')
@htpasswd.required
def index(user):
    return 'Mimer SQL REST controller'

@app.route('/gettoken')
@htpasswd.required
def gettoken(user):
    """Return a security token that can be used instead of username:password from different clients.

    Args:
        user(str): The HttpAuth user

    Returns:
        The security token

    """
    return jsonify({'token': htpasswd.generate_token(user)})

@app.route('/status/<database_name>', methods=['POST', 'GET'])
@htpasswd.required
def check_status(user, database_name):
    """Check status of a Mimer SQL Database.

    Args: 
        user(str): The HttpAuth user
        database_name(str): The database name

    Returns: 
        JSON document. See mimcontrol.check_status(database_name)
    """
    return mimcontrol.check_status(database_name)


@app.route('/stopdatabase/<database_name>', methods=['POST', 'GET'])
@htpasswd.required
def stop_database(user, database_name):
    """Stop a Mimer SQL Database.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Returns: 
        JSON document. See mimcontrol.stop_database(database_name)
    """
    return mimcontrol.stop_database(database_name)


@app.route('/startdatabase/<database_name>', methods=['POST', 'GET'])
@htpasswd.required
def start_database(user, database_name):
    """Start a Mimer SQL Database.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Returns: 
        JSON document. See mimcontrol.start_database(database_name)
    """
    return mimcontrol.start_database(database_name)


@app.route('/setconfig/<database_name>', methods=['POST'])
@htpasswd.required
def set_config(user, database_name):
    """Set configuration parameters.

    The parameters to be set is specified in the input JSON document

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Returns: 
        JSON document. See mimcontrol.change_config(database_name)
    """
    return mimcontrol.change_config(database_name, request.get_json())

@app.route('/getconfig/<database_name>', methods=['POST', 'GET'])
@htpasswd.required
def get_config(user, database_name):
    """Get one or more configuration parameters.

    With a GET, or a POST without an input JSON all configuration parameters are returned.

    See mimcontrol.get_config(database_name, dict) for JSON specification

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Returns: 
        JSON document. See mimcontrol.change_config(database_name)
    """
    if(request.method == 'GET'):
        return mimcontrol.get_config(database_name, None)
    else:
        return mimcontrol.get_config(database_name, request.get_json())


@app.route('/createbackup/<database_name>', methods=['POST'])
@htpasswd.required
def create_backup(user, database_name):
    """Create backup.

    A folder with todays date will be created in the backup location.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    The input parameters for the backup are specified in the request using JSON
    with the following format:
    {
        'user_name': 'name',
        'password': 'password',
        'backup_location': 'path to backup location',
        'backup_name': 'optional name of backup',
        'backup_comment': 'optional comment for backup'
    }

    Returns: JSON reponse. See mimcontrol.create_backup() for details.

    """
    user_name = request.get_json().get('user_name')
    password = request.get_json().get('password')
    backup_location = request.get_json().get('backup_location')
    backup_name = request.get_json().get('backup_name')
    backup_comment = request.get_json().get('backup_comment')
    return mimcontrol.create_backup(database_name=database_name, user_name=user_name, password=password, backup_location=backup_location, backup_name=backup_name, backup_comment=backup_comment)

@app.route('/restorebackup/<database_name>', methods=['POST'])
@htpasswd.required
def restore_backup(user, database_name):
    """Restore from backup.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON for a full restore of any backup
    {
        'user_name': 'name',
        'password': 'password',
        'backup_location': 'path to backup location',
        'backup_id': 'name of the folder containing the backup'
    }
    Input JSON for a full restore of the latest backup
    {
        'user_name': 'name',
        'password': 'password',
        'backup_location': 'path to backup location',
        'restore_log': 'false'
    }
    Input JSON for a full restore of the latest backup and then re-applying the log file to get to the current state.
    This only work if the LOGDB is intact. keep_transdb = true mean that the TRANSDB from the live system is kept.
    This way, all transactions that was made when the system stopped is kept, even if they haven't been flushed to LOGDB.
    If the live TRANSDB have been corrupted or there, for some reason, are problems to restart the system with the live TRANSDB,
    keep_transdb=false can be used to take TRANSDB from the backup instead. If restore_log=false, transdb is always copied from the backup.
    {
        'user_name': 'name',
        'password': 'password',
        'backup_location': 'path to backup location',
        'restore_log': 'true',
        'keep_transdb': 'true'|'false' #Default true. 
    }
    Returns: JSON reponse. See mimcontrol.restore_backup() for details.

    """
    user_name = request.get_json().get('user_name')
    password = request.get_json().get('password')
    backup_location = request.get_json().get('backup_location')
    backup_id = request.get_json().get('backup_id')
    restore_log = request.get_json().get('restore_log', 'false') == 'true'
    keep_transdb = request.get_json().get('keep_transdb', 'true') == 'true'
    return mimcontrol.restore_backup(database_name=database_name, user_name=user_name, password=password, backup_location=backup_location, backup_id=backup_id, restore_log=restore_log, keep_transdb=keep_transdb)


@app.route('/showbackup', methods=['POST'])
@htpasswd.required
def show_backup(user):
    """Show information about a backup.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        'backup_location': 'path to backup location',
        'backup_id': 'name of the folder containing the backup'
    }

    Returns: JSON Document. See mimcontrol.show_backup() for details

    """
    backup_location = request.get_json().get('backup_location')
    backup_id = request.get_json().get('backup_id')
    return mimcontrol.show_backup(backup_location=backup_location, backup_id=backup_id)


@app.route('/listbackups', methods=['POST'])
@htpasswd.required
def list_backup(user):
    """List backups with backup information.

    Args:
        user(str): The HttpAuth user

    Input JSON parameter:
    {
        'backup_location': 'path to backup location'
    }
    Returns: JSON document,see mimcontrol.list_backup() for details.

    """
    return mimcontrol.list_backup(request.get_json().get('backup_location'))



@app.route('/deletebackup', methods=['POST'])
@htpasswd.required
def deletebackup(user):
    """Delete a backup.

    Args:
        user(str): The HttpAuth user

    Input JSON paramater:
    {
        'backup_location': 'path to backup location',
        'backup_id': 'ID of backup to delete'
    }
    Returns: JSON document, see mimcontrol.delete_backup() for details

    """
    b_location = request.get_json().get('backup_location')
    b_id = request.get_json().get('backup_id')
    return mimcontrol.delete_backup(b_location, b_id)

@app.route('/show_perf/<database_name>', methods=['POST', 'GET'])
@htpasswd.required
def show_db_perf(user, database_name):
    """Show performance measurements of a Mimer SQL database.

    Show information like connected users, transactions, background threads and so on.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name


    Returns: JSON document, see mimcontrol.show_db_perf() for details

    """  
    return mimcontrol.show_db_perf(database_name)


@app.route('/log_sql/<database_name>', methods=['POST'])
@htpasswd.required
def show_sql_log(user, database_name):
    """Show SQL execution log.

    Show executed SQL with performance information

    Input JSON paramater:
    {
        'password': '<SYSADM password>'
    }

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Returns: JSON document, see mimcontrol.show_sql_log() for details
    """
    pswd = request.get_json().get('password')
    return mimcontrol.show_sql_log(database_name, pswd)


@app.route('/update_sysadm_pass/<database_name>', methods=['POST'])
@htpasswd.required
def update_sysadm_pass(user, database_name):
    """Change the password of SYSADM.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Method: POST

    Input JSON paramater:
    {
        'old_password': '<original password>',
        'new_password': '<new password>'
    }

    Return:
    {"return_code":"success"}
    {"return_code":"failure", "error_message":"....."}

    """
    old_pswd = request.get_json().get('old_password')
    new_pswd = request.get_json().get('new_password')
    return mimcontrol.changeUserPass(database_name, 'SYSADM', old_pswd, 'SYSADM', new_pswd)


@app.route('/create_schema/<database_name>', methods=['POST'])
@htpasswd.required
def create_schema(user, database_name):
    """Create new databae schema.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Method: POST

    Input JSON paramater:
    {
        'user': '<creator and owner of the new schema>',
        'password': '<password>',
        'schema_name':'<schema name>'
    }

    Returns: JSON document, see mimcontrol.createSchema() for details
    """
    user = request.get_json().get('user')
    pswd = request.get_json().get('password')
    schema_name = request.get_json().get('schema_name')
    return mimcontrol.createSchema(database_name, user, pswd, schema_name)

@app.route('/delete_schema/<database_name>', methods=['POST'])
@htpasswd.required
def delete_schema(user, database_name):
    """Delete database schema.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Method: POST

    Input JSON paramater:
    {
        'user': '<user with permissions to delete the schema>',
        'password': '<password>',
        'schema_name':'<schema name>'
    }

    Returns: JSON document, see mimcontrol.deleteSchema() for details
    """
    user = request.get_json().get('user')
    pswd = request.get_json().get('password')
    schema_name = request.get_json().get('schema_name')
    return mimcontrol.deleteSchema(database_name, user, pswd, schema_name)


@app.route('/list_schemas/<database_name>', methods=['POST'])
@htpasswd.required
def list_schema(user, database_name):
    """List database schema.

    List the schemas visisible for the specified user.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Method: POST

    Input JSON paramater:
    {
        'user': '<use to show schemas for>',
        'password': '<password>'
    }

    Returns: JSON document, see mimcontrol.listSchema() for details
    """
    user = request.get_json().get('user')
    pswd = request.get_json().get('password')
    return mimcontrol.listSchema(database_name, user, pswd)


@app.route('/schema_lookup/<database_name>', methods=['POST'])
@htpasswd.required
def schema_lookup(user, database_name):
    """Lookup database schema.

    Describe a databasee schema. Show all tables, views, indexes,
    procedures, functions, and schema owner.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Method: POST

    Input JSON paramater:
    {
        'user': '<use to show schemas for>',
        'password': '<password>'
    }

    Returns: JSON document, see mimcontrol.schemaLookup() for details
    """
    user = request.get_json().get('user')
    pswd = request.get_json().get('password')
    schema_name = request.get_json().get('schema_name')
    return mimcontrol.schemaLookup(database_name, schema_name, user, pswd)

@app.route('/table_lookup/<database_name>', methods=['POST'])
@htpasswd.required
def table_lookup(user, database_name):
    """Lookup table.

    Describe a table

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Method: POST

    Input JSON paramater:
    {
        'user': '<use to show schemas for>',
        'password': '<password>',
        'schema_name':'<schema name>',
        'table_name':'<table name>'
    }

    Returns: JSON document, see mimcontrol.tableLookup() for details
    """
    user = request.get_json().get('user')
    pswd = request.get_json().get('password')
    schema_name = request.get_json().get('schema_name')
    table_name = request.get_json().get('table_name')
    return mimcontrol.tableLookup(database_name, schema_name, table_name, user, pswd)


@app.route('/view_lookup/<database_name>', methods=['POST'])
@htpasswd.required
def view_lookup(user, database_name):
    """Lookup view.

    Describe a view

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Method: POST

    Input JSON paramater:
    {
        'user': '<use to show schemas for>',
        'password': '<password>',
        'schema_name':'<schema name>',
        'view_name':'<view name>'
    }

    Returns: JSON document, see mimcontrol.viewLookup() for details
    """
    user = request.get_json().get('user')
    pswd = request.get_json().get('password')
    schema_name = request.get_json().get('schema_name')
    view_name = request.get_json().get('view_name')
    return mimcontrol.viewLookup(database_name, schema_name, view_name, user, pswd)


@app.route('/list_users/<database_name>', methods=['POST'])
@htpasswd.required
def list_users(user, database_name):
    """List all users created by a specific user in the database, together with their creators.

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Method: POST

    Input JSON paramater:
    {
        'creator_name': Owner/Creator
        'creator_password': Owner/Creator password
    }

    Returns: 
        {"return_code":"success","user_list":[{"user":"SYSADM"}]}
        or
        {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}
        See mimcontrol.listUsers() for more details

    """
    pswd = request.get_json().get('creator_password')
    user = request.get_json().get('creator_name')
    return mimcontrol.listUsers(database_name, user, pswd)


@app.route('/create_user/<database_name>', methods=['POST'])
@htpasswd.required
def create_user(user, database_name):
    """Create a new user.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        creator_name: The user creating the new user
        creator_password: Password for the user creating a new user
        user_name: The new user name
        user_password: The new user password
        can_backup: Should the user have privilege to make a backup [true|false]
        can_databank: Should the user have privilege to create databanks [true|false]
        can_schema: Should the user have privilege to create schemas [true|false]
        can_ident: Should the user have privilege to create idents [true|false]
    }


    Returns: 
    {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}
    See mimcontrol.createUser() for more details

    """
    creator_name = request.get_json().get('creator_name')
    creator_password = request.get_json().get('creator_password')
    user_name = request.get_json().get('user_name')
    user_pswd = request.get_json().get('user_password')
    can_backup = request.get_json().get('can_backup')  == 'true'
    can_databank = request.get_json().get('can_databank')  == 'true'
    can_schema = request.get_json().get('can_schema')  == 'true'
    can_ident = request.get_json().get('can_ident')  == 'true'
    return mimcontrol.createUser(database_name, creator_name, creator_password, user_name, user_pswd, can_backup, can_databank, can_schema, can_ident)

@app.route('/delete_user/<database_name>', methods=['POST'])
@htpasswd.required
def delete_user(user, database_name):
    """Delete user.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        creator_name: User that created the user to delete
        creator_password: Password for the creator
        user_name: User to delete
        is_cascade: [true|false]. If true, the revoke has cascade effect
    }


    Returns: 
    {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}
    See mimcontrol.deleteUser() for more details

    """
    creator_name = request.get_json().get('creator_name')
    creator_password = request.get_json().get('creator_password')
    user_name = request.get_json().get('user_name')
    is_cascade = request.get_json().get('is_cascade')  == 'true'
    return mimcontrol.deleteUser(database_name, creator_name, creator_password, user_name, is_cascade)



@app.route('/update_user_pass/<database_name>', methods=['POST'])
@htpasswd.required
def update_user_pass(user, database_name):
    """Update the password by the user him/her self.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        user_name: User name
        old_password: Old password
        new_password: New password
    }


    Returns: 
    {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}
    See mimcontrol.update_user_pass() for more details

    """
    user_name = request.get_json().get('user_name')
    old_pswd = request.get_json().get('old_password')
    new_pswd = request.get_json().get('new_password')
    return mimcontrol.changeUserPass(database_name, user_name, old_pswd, user_name, new_pswd)


@app.route('/reset_user_pass/<database_name>', methods=['POST'])
@htpasswd.required
def reset_user_pass(user, database_name):
    """Reset the password by the user's creator

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        creator_name: creator of the user
        creator_password: old password of the user
        user_name: User name
        new_password: New password
    }


    Returns: 
    {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}
    See mimcontrol.changeUserPass() for more details

    """
    creator_name = request.get_json().get('creator_name')
    creator_pswd = request.get_json().get('creator_password')
    user_name = request.get_json().get('user_name')
    user_password = request.get_json().get('user_password')
    return mimcontrol.changeUserPass(database_name, creator_name, creator_pswd, user_name, user_password)


@app.route('/user_lookup/<database_name>', methods=['POST'])
@htpasswd.required
def user_lookup(user, database_name):
    """Look up user information.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        user_name: User name to lookup
        password: Password of the user
    }

    """
    user_name = request.get_json().get('user_name')
    pswd = request.get_json().get('password')
    return mimcontrol.userLookup(database_name, user_name, pswd)


@app.route('/grant_access/<database_name>', methods=['POST'])
@htpasswd.required
def grant_access(user, database_name):
    """Grant access privilege (select, insert, update, delete, references, all) of a table to a grantee (user, group) by the grantor.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        grantor_name: grantor name
        grantor_password: password of the grantor 
        grantee_name: grantee name 
        table_name: name of the table to be accessed
        privilege: type of privilege (can be [select|inser|update|delete|references|all])
        is_grantable: [true|false]. If true, the grantee can grant the privilege to other users/groups
    }

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    grantor_name = request.get_json().get('grantor_name')
    grantor_pass = request.get_json().get('grantor_password')
    grantee_name = request.get_json().get('grantee_name')
    table_name = request.get_json().get('table_name')
    privilege = request.get_json().get('privilege')
    is_grantable = request.get_json().get('is_grantable')  == 'true'
    return mimcontrol.grantAccessPrivilege(database_name, grantor_name, grantor_pass, grantee_name, table_name, privilege, is_grantable)


@app.route('/revoke_access/<database_name>', methods=['POST'])
@htpasswd.required
def revoke_access(user, database_name):
    """Revoke access privilege (select, insert, update, delete, references, all) of a table from a grantee (user, group) by the grantor.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        grantor_name: grantor name
        grantor_password: password of the grantor 
        grantee_name: grantee name 
        table_name: name of the table to be accessed
        privilege: type of privilege (can be [select|inser|update|delete|references|all])
        is_cascade: [true|false]. If true, the revoke has cascade effect
    }

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    grantor_name = request.get_json().get('grantor_name')
    grantor_pass = request.get_json().get('grantor_password')
    grantee_name = request.get_json().get('grantee_name')
    table_name = request.get_json().get('table_name')
    privilege = request.get_json().get('privilege')
    is_cascade = request.get_json().get('is_cascade')  == 'true'
    return mimcontrol.revokeAccessPrivilege(database_name, grantor_name, grantor_pass, grantee_name, table_name, privilege, is_cascade)


@app.route('/grant_execute/<database_name>', methods=['POST'])
@htpasswd.required
def grant_execute(user, database_name):
    """Grant execution privilege of a function or procedure to a grantee (user, group) by the grantor.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        grantor_name: grantor name
        grantor_password: password of the grantor 
        grantee_name: grantee name 
        routine_name: name of the function or procedure
        routine_type: [function|procedure]
        is_grantable: [true|false]. If true, the grantee can grant the privilege to other users/groups
    }

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    grantor_name = request.get_json().get('grantor_name')
    grantor_pass = request.get_json().get('grantor_password')
    grantee_name = request.get_json().get('grantee_name')
    routine_name = request.get_json().get('routine_name')
    routine_type = request.get_json().get('routine_type')
    is_grantable = request.get_json().get('is_grantable')  == 'true'
    return mimcontrol.grantExecutePrivilege(database_name, grantor_name, grantor_pass, grantee_name, routine_name, routine_type, is_grantable)

@app.route('/revoke_execute/<database_name>', methods=['POST'])
@htpasswd.required
def revoke_execute(user, database_name):
    """Revoke execution privilege of a function or procedure from a grantee (user, group) by the grantor.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        grantor_name: grantor name
        grantor_password: password of the grantor 
        grantee_name: grantee name 
        routine_name: name of the function or procedure
        routine_type: [function|procedure]
        is_cascade: [true|false]. If true, the revoke has cascade effect
    }

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    grantor_name = request.get_json().get('grantor_name')
    grantor_pass = request.get_json().get('grantor_password')
    grantee_name = request.get_json().get('grantee_name')
    routine_name = request.get_json().get('routine_name')
    routine_type = request.get_json().get('routine_type')
    is_cascade = request.get_json().get('is_cascade')  == 'true'
    return mimcontrol.revokeExecutePrivilege(database_name, grantor_name, grantor_pass, grantee_name, routine_name, routine_type, is_cascade)


@app.route('/grant_system_privilege/<database_name>', methods=['POST'])
@htpasswd.required
def grant_system_privilege(user, database_name):
    """Grant system privilege (backup, schema, databank, ident, shadow, statistics) to a grantee by the grantor.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        grantor_name: grantor name
        grantor_password: password of the grantor 
        grantee_name: grantee name 
        privilege: [backup|schema|databank|ident|shadow|statistics]
        is_grantable: [true|false]. If true, the grantee can grant the privilege to other users/groups
    }

    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    grantor_name = request.get_json().get('grantor_name')
    grantor_pass = request.get_json().get('grantor_password')
    grantee_name = request.get_json().get('grantee_name')
    privilege = request.get_json().get('privilege')
    is_grantable = request.get_json().get('is_grantable')  == 'true'
    return mimcontrol.grantSystemPrivilege(database_name, grantor_name, grantor_pass, grantee_name, privilege, is_grantable)


@app.route('/revoke_system_privilege/<database_name>', methods=['POST'])
@htpasswd.required
def revoke_system_privilege(user, database_name):
    """Revoke system privilege (backup, schema, databank, ident, shadow, statistics) from a grantee by the grantor.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        grantor_name: grantor name
        grantor_password: password of the grantor 
        grantee_name: grantee name 
        privilege: [backup|schema|databank|ident|shadow|statistics]
        is_cascade: [true|false]. If true, the revoke has cascade effect
    }
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    grantor_name = request.get_json().get('grantor_name')
    grantor_pass = request.get_json().get('grantor_password')
    grantee_name = request.get_json().get('grantee_name')
    privilege = request.get_json().get('privilege')
    is_cascade = request.get_json().get('is_cascade')  == 'true'
    return mimcontrol.revokeSystemPrivilege(database_name, grantor_name, grantor_pass, grantee_name, privilege, is_cascade)


@app.route('/create_group/<database_name>', methods=['POST'])
@htpasswd.required
def create_group(user, database_name):
    """Create a new group.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        creator_name: grantor name
        creator_password: password of the creator 
        group_name: group name 
    }
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    creator_name = request.get_json().get('creator_name')
    creator_pass = request.get_json().get('creator_password')
    group_name = request.get_json().get('group_name')
    return mimcontrol.createGroup(database_name, creator_name, creator_pass, group_name)


@app.route('/delete_group/<database_name>', methods=['POST'])
@htpasswd.required
def delete_group(user, database_name):
    """Delete a group.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        creator_name: grantor name
        creator_password: password of the creator 
        group_name: group name 
        is_cascade: [true|false]. If true, the revoke has cascade effect
    }
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    creator_name = request.get_json().get('creator_name')
    creator_pass = request.get_json().get('creator_password')
    group_name = request.get_json().get('group_name')
    is_cascade = request.get_json().get('is_cascade')  == 'true'
    return mimcontrol.deleteGroup(database_name, creator_name, creator_pass, group_name, is_cascade)


@app.route('/grant_member/<database_name>', methods=['POST'])
@htpasswd.required
def grant_member(user, database_name):
    """Grant membership of a group to a grantee by the group owner.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        grantor_name: grantor name
        grantor_password: password of the grantor 
        grantee_name: grantee name 
        group_name: name of the group
        is_grantable: [true|false]. If true, the grantee can grant the privilege to other users
    }
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    grantor_name = request.get_json().get('grantor_name')
    grantor_pass = request.get_json().get('grantor_password')
    grantee_name = request.get_json().get('grantee_name')
    group_name = request.get_json().get('group_name')
    is_grantable = request.get_json().get('is_grantable')  == 'true'
    return mimcontrol.grantMembership(database_name, grantor_name, grantor_pass, grantee_name, group_name, is_grantable)


@app.route('/revoke_member/<database_name>', methods=['POST'])
@htpasswd.required
def revoke_member(user, database_name):
    """Revoke membership of a group from a grantee by the group owner.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        grantor_name: grantor name
        grantor_password: password of the grantor 
        grantee_name: grantee name 
        group_name: name of the group
        is_cascade: [true|false]. If true, the revoke has cascade effect
    }
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    grantor_name = request.get_json().get('grantor_name')
    grantor_pass = request.get_json().get('grantor_password')
    grantee_name = request.get_json().get('grantee_name')
    group_name = request.get_json().get('group_name')
    is_cascade = request.get_json().get('is_cascade')  == 'true'
    return mimcontrol.revokeMembership(database_name, grantor_name, grantor_pass, grantee_name, group_name, is_cascade)


@app.route('/group_lookup/<database_name>', methods=['POST'])
@htpasswd.required
def group_lookup(user, database_name):
    """Look up for group information.

    Method: POST

    Args:
        user(str): The HttpAuth user
        database_name(str): The database name

    Input JSON paramater:
    {
        creator_name: creator name
        creator_password: password of the creator
        group_name: name of the group
    }
    
    Returns: 
        {"return_code":"success"} or {"return_code":"failure", "error_code": <error code>, "error_message":"<error message>"}

    """
    creator_name = request.get_json().get('creator_name')
    creator_password = request.get_json().get('creator_password')
    group_name = request.get_json().get('group_name')
    return mimcontrol.groupLookup(database_name, creator_name, creator_password, group_name)


if __name__ == "__main__":
    app.run(host= '0.0.0.0', port=5001, ssl_context='adhoc')
