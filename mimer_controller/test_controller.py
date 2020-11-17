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
'''
To use the test as is:
Setup the http user and password to mimadmin:mimadmin. That way https://localhost:5001/gettoken will be used to get the security token
When running the test locally and the REST service in Docker, define a SQL hosts setting with mimerdb that points to the database in the container

To run:
python3 -m unittest -v test_controller.py
'''
import requests
import json
import mimcontrol
import mimerpy
import os
import shutil
from requests.auth import HTTPBasicAuth
import unittest
from mimerpy.mimPyExceptions import *

backup_path = '/data/backup' #If running outside of Docker this path should exist on the machine
skip_slow_tests = os.environ.get('SKIP_SLOW_TESTS', 'false') == 'true'


class TestController(unittest.TestCase):
    database_name=os.environ.get('MIMER_REST_DATABASE', 'mimerdb')
    invalid_database_name='db44444x'
    database_name_direct_connect=os.environ.get('MIMER_REST_DIRECT_DATABASE', 'mimerdb')
    sysadm_pwd='SYSADM'
    test_group = 'PY_GROUP'
    test_user = 'PY_TEST'
    test_user_pwd = 'PY_TEST_PWD'
    test_user_two = 'PY_TEST_TWO'
    test_user_two_pwd = 'PY_TEST_PWD'
    test_user_three = 'PY_TEST_THREE'
    test_user_three_pwd = 'PY_TEST_THREE_PWD'
    test_http = os.environ.get('MIMER_REST_TEST_USE_HTTP', 'false') == 'true'
    debug_output = False
    base_url = os.environ.get('MIMER_REST_BASE_URL', 'https://localhost:5001')
    
    def startIfNeeded(self):
        if self.test_http:
            response = requests.get(self.base_url + '/status/' + self.database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
            if jsonResponse.get("status") != 'Running':
                response = requests.get(self.base_url + '/startdatabase/' + self.database_name, verify=False, headers=self.json_header)
                jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            response = mimcontrol.check_status(self.database_name)
            if response.get("status") != 'Running':
                response = mimcontrol.start_database(self.database_name)

    def clearEnv(self):
        #Clean backup directory
        if os.path.isdir(backup_path):
            shutil.rmtree(backup_path)
        os.mkdir(backup_path)

        #Try to clean the database
        try:
            with mimerpy.connect(dsn = self.database_name_direct_connect, user = 'SYSADM', password = self.sysadm_pwd) as con:
                with con.cursor() as cur:
                    con.autocommitmode=True

                    #Idents
                    cur.execute("select * from information_schema.ext_idents where ident_creator <> 'SYSTEM'")
                    idents = cur.fetchall()
                    for ident in idents:
                        try:
                            con.execute("drop ident " + ident[1] + ' cascade')
                        except DatabaseError as dbe:
                            print(dbe)

                    #Databanks
                    cur.execute("select * from information_schema.ext_databanks where databank_creator <> 'SYSTEM'")
                    databanks = cur.fetchall()
                    for databank in databanks:
                        try:
                            con.execute("drop databank " + databank[1] + ' cascade')
                        except DatabaseError as dbe:
                            print(dbe)

                    #Sequences
                    cur.execute("select * from information_schema.sequences")
                    seqs = cur.fetchall()
                    for seq in seqs:
                        try:
                            con.execute("drop databank " + seq[2] + ' cascade')
                        except DatabaseError as dbe:
                            print(dbe)

                    #Schemas
                    cur.execute("select * from information_schema.ext_schemas where schema_name not in ('MIMER', 'ODBC', 'SYSADM', 'SYSTEM', 'INFORMATION_SCHEMA', 'BUILTIN')")
                    schemas = cur.fetchall()
                    for schema in schemas:
                        try:
                            con.execute("drop schema " + schema[0] + ' cascade')
                        except DatabaseError as dbe:
                            print(dbe)
                    
            #Remove the mydb file if it exist
            if os.path.exists('/tmp/mydb.dbf'):
                try:
                    os.remove('/tmp/mydb.dbf')
                except Exception:
                    print("Cannot delete /tmp/mydb.dbf")

        except Exception as e:
            print(e.message)

    def setupEnv(self):
        #Setup idents and stuff
        try:
            with mimerpy.connect(dsn = self.database_name_direct_connect, user = 'SYSADM', password = self.sysadm_pwd) as con:
                with con.cursor() as cur:
                    con.autocommitmode=True


                con.execute("create databank mydb with log option in '/tmp/mydb.dbf'")
                con.execute("create table t1(id integer primary key) in mydb")
                con.execute("insert into t1 values(1)")
                con.execute("create ident " + TestController.test_user + " as user using '" + TestController.test_user_pwd + "'")
                con.execute("create ident " + TestController.test_user_two + " as user using '" + TestController.test_user_two_pwd + "'")
                con.execute("create ident " + TestController.test_user_three + " as user using '" + TestController.test_user_three_pwd + "'")
                con.execute("grant databank to " + TestController.test_user)
                con.execute("grant databank to " + TestController.test_user_two)
                con.execute("create ident " + TestController.test_group + " as group")
                con.execute("grant member on " + TestController.test_group + ' to ' + TestController.test_user)
                con.execute("grant member on " + TestController.test_group + ' to ' + TestController.test_user_two)


            #As PY_TEST
            with mimerpy.connect(dsn = self.database_name_direct_connect, user = self.test_user, password = self.test_user_pwd) as py_con:
                py_con.execute("create databank py_test_db")
                py_con.execute("create table py_one(id integer primary key, c1 varchar(20)) in py_test_db")
                py_con.execute("create index py_one_idx on py_one(c1)")
                py_con.execute("create view py_one_view as select id, c1 from py_one")
                f1 = """create function func1(in i1 integer)
                        returns int
                        begin
                                return i1 *2;
                        end """
                py_con.execute(f1)
                p1 = """create procedure del_py_one(ic1 nvarchar(256))
                    modifies sql data
                    begin
                        delete from py_one where c1 = ic1;
                    end"""
                py_con.execute(p1)
                p2 = """create procedure ins_py_one(ic1 nvarchar(256))
                    modifies sql data
                    begin
                        insert into py_one(c1) values(ic1);
                    end"""
                py_con.execute(p2)
        except Exception as e:
            print(e.message)

    @classmethod
    def setUpClass(cls):
        if cls.test_http:
            response = requests.get(cls.base_url + '/gettoken', verify=False, auth=HTTPBasicAuth('mimadmin', 'mimadmin'))
            jsonResponse = json.loads(response.content.decode('utf-8'))
            cls.security_token = jsonResponse['token']
            cls.header = {'Authorization': 'token ' + cls.security_token}
            cls.json_header = {
                        'Content-Type': 'application/json',
                        'Authorization': 'token ' + cls.security_token
            }
        else:
            cls.security_token='xxx'
            cls.header='xxx'
            cls.json_header='xxx'

    def setUp(self):
        self.backup_id_one = ''
        self.backup_id_two = ''
        self.startIfNeeded()
        self.clearEnv()
        self.setupEnv()


    def get_priv(self, type, user_name, password, schema_name, object_name):
        input = {
            'user_name': user_name,
            'password': password
        }

        if object_name is not None:
            object = object_name.upper()
        schema = schema_name.upper()

        if type.upper() == "TABLE":
            list_name = 'table_privileges'
            priv_name = 'table_name'
        elif type.upper() == "FUNCTION":
            list_name = 'function_privileges'
            priv_name = 'function_name'
        elif type.upper() == "PROCEDURE":
            list_name = 'procedure_privileges'
            priv_name = 'procedure_name'
        elif type.upper() == "GROUP":
            list_name = 'groups'
            priv_name = 'group_name'

        if self.test_http:
            response = requests.post(self.base_url + '/user_lookup/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.userLookup(database_name=self.database_name, user_name=input['user_name'], user_pass=input['password'])

        object_privs = jsonResponse.get(list_name)
        t = []
        if object_name is None:
            return object_privs
        for priv in object_privs:
            if priv[priv_name].upper() == schema + '.' + object:
                t.append(priv)

        return t

    def check_priv(self, type, priv_list, schema_name, object_name, privilege, grantable):
        schema = schema_name.upper()
        object = object_name.upper()
        upriv = privilege.upper()
        ugrant = grantable.upper()

        if type.upper() == "TABLE":
            priv_name = 'table_name'
        elif type.upper() == "FUNCTION":
            priv_name = 'function_name'
        elif type.upper() == "PROCEDURE":
            priv_name = 'procedure_name'
        for priv in priv_list:
            if priv[priv_name].upper() == schema + '.' + object and priv['privilege'] == upriv and priv['is_grantable'] == ugrant:
                return True
        return False

    def check_system_priv(self, user_name, password, privilege, grantable):
        input = {
            'user_name': user_name,
            'password': password
        }

        if self.test_http:
            response = requests.post(self.base_url + '/user_lookup/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.userLookup(database_name=self.database_name, user_name=input['user_name'], user_pass=input['password'])

        system_privs = jsonResponse.get('system_privileges')
        for priv in system_privs:
            if priv['privilege'] == privilege.upper() and priv['is_grantable'] == grantable.upper():
                return True
        return False

    def test_url(self):
        if self.test_http:
            url = self.base_url
            #Testing URL
            r = requests.get(url, verify=False, headers=self.header)
            
            self.assertEqual(r.status_code, 200)
            if self.debug_output:
                print("Status: " + str(r.status_code))
                print("Result: " + r.content.decode('utf-8'))
            # With CURL:
            # curl --insecure -I  -H "Authorization: token eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6Im1pbWFkbWluIiwiaGFzaGhhc2giOiIxODYxZTI5NzkyMTg3MWI0YTA4MTcxNzU1ZmNmNTAyZGEzZjA0YWRhMTMzYmFmMDBiYjk1Nzg5OTgxYzgwOGE3In0.Xss7gQkdleerTgYRpmqUMyQ3H-VlDVEw0izoME-YGc4n3k_oTQ9_Q_pzJIKm2X5_A1KvrdovZIVfipw6sRjrUg" https://localhost:5001

    #Test config
    def test_config(self):
        config_set = {
            'config': [
                {
                    'config_name': 'Pages4K',
                    'config_value': 25000
                },
                {
                    'config_name': 'Pages32K',
                    'config_value': 8000
                },
                {
                    'config_name': 'Pages999K',
                    'config_value': 800
                }
            ]
        }

        if self.test_http:
            response = requests.post(self.base_url + '/setconfig/' + self.database_name, verify=False, headers=self.json_header, json=config_set)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.change_config(database_name=self.database_name, jsonVal=config_set)

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Change config should work')
        
        if self.debug_output:
            print("Setconfig response:")
            print(json.dumps(jsonResponse, indent=4))

        config_get = {
            'config': [
                {
                    'config_name': 'Pages4K'
                },
                {
                    'config_name': 'pages32k'
                },
                {
                    'config_name': 'Pages999K'
                }
            ]
        }
        if self.test_http:
            response = requests.post(self.base_url + '/getconfig/' + self.database_name, verify=False, headers=self.json_header, json=config_get)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.get_config(database_name=self.database_name, jsonVal=config_get)

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Get config should work')
        for config in jsonResponse['config']:
            if config.get('config_name') == 'Pages4K':
                self.assertEqual(config.get('config_value'), '25000', 'Config Pages4K should be 25000')

        if self.debug_output:
            print("Getconfig response, POST without input param:")
            print(json.dumps(jsonResponse, indent=4))
        
        if self.test_http:
            response = requests.get(self.base_url + '/getconfig/' + self.database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.get_config(database_name=self.database_name, jsonVal=None)

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Get all config should work')
        
        if self.debug_output:
            print("Getconfig response, GET (ALL):")
            print(json.dumps(jsonResponse, indent=4))

    @unittest.skipIf(skip_slow_tests == True, "Skipping slow test")
    def test_server(self):
        if self.test_http:
            #Testing stop_database followed by check_status
            response = requests.get(self.base_url + '/stopdatabase/' + self.database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
            self.assertEqual(jsonResponse.get('return_code'), 'success', 'Stopping server should work')
            if self.debug_output:
                print("Response, stop: " + json.dumps(jsonResponse, indent=4))
            response = requests.get(self.base_url + '/status/' + self.database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
            self.assertEqual(jsonResponse.get('return_code'), 'success', 'Checking newly stopped server should work')
            if self.debug_output:
                print("Response, check: " + json.dumps(jsonResponse, indent=4))

            #Testing start_database followed by check_status
            response = requests.get(self.base_url + '/startdatabase/' + self.database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
            self.assertEqual(jsonResponse.get('return_code'), 'success', 'Starting database should work')
            if self.debug_output:
                print("Response, start: " + json.dumps(jsonResponse, indent=4))
            response = requests.get(self.base_url + '/status/' + self.database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
            self.assertEqual(jsonResponse.get('return_code'), 'success', 'Checking newly started server should work')
            if self.debug_output:
                print("Response, check: " + json.dumps(jsonResponse, indent=4))

            #Trying to stop server that doens't exist
            response = requests.get(self.base_url + '/stopdatabase/' + self.invalid_database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
            self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Stopping a database that does not exist should fail')
            if self.debug_output:
                print("Response, stop failure: " + json.dumps(jsonResponse, indent=4))

            #Trying to start server that doens't exist
            response = requests.get(self.base_url + '/startdatabase/' + self.invalid_database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
            self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Starting a database that does not exist should fail')
            if self.debug_output:
                print("Response, start failure: " + json.dumps(jsonResponse, indent=4))

            #Trying to check status on a server that doesn't exist
            response = requests.get(self.base_url + '/status/' + self.invalid_database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
            self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Checking status on database that does not exist should fail')
            if self.debug_output:
                print("Response, check failure: " + json.dumps(jsonResponse, indent=4))
        else:
            #Testing stop_database followed by check_status
            response = mimcontrol.stop_database(self.database_name)
            self.assertEqual(response.get('return_code'), 'success', 'Stopping server should work')
            if self.debug_output:
                print("Response, stop: " + json.dumps(response, indent=4))
            response = mimcontrol.check_status(self.database_name)
            self.assertEqual(response.get('return_code'), 'success', 'Checking newly stopped server should work')
            if self.debug_output:
                print("Response, check: " + json.dumps(response, indent=4))

            #Testing start_database followed by check_status
            response = mimcontrol.start_database(self.database_name)
            self.assertEqual(response.get('return_code'), 'success', 'Starting database should work')
            if self.debug_output:
                print("Response, start: " + json.dumps(response, indent=4))
            response = mimcontrol.check_status(self.database_name)
            self.assertEqual(response.get('return_code'), 'success', 'Checking newly started server should work')
            if self.debug_output:
                print("Response, check: " + json.dumps(response, indent=4))

            #Trying to stop server that doens't exist
            response = mimcontrol.stop_database(self.invalid_database_name)
            self.assertEqual(response.get('return_code'), 'failure', 'Stopping a database that does not exist should fail')
            if self.debug_output:
                print("Response, stop failure: " + json.dumps(response, indent=4))

            #Trying to start server that doens't exist
            response = mimcontrol.start_database(self.invalid_database_name)
            self.assertEqual(response.get('return_code'), 'failure', 'Starting a database that does not exist should fail')
            if self.debug_output:
                print("Response, start failure: " + json.dumps(response, indent=4))

            #Trying to check status on a server that doesn't exist
            response = mimcontrol.check_status(self.invalid_database_name)
            self.assertEqual(response.get('return_code'), 'failure', 'Checking status on database that does not exist should fail')
            if self.debug_output:
                print("Response, check_status fail: " + json.dumps(response, indent=4))


    def test_backup(self):
        backup_json = {
            'user_name': 'SYSADM',
            'password': 'SYSADM',
            'backup_location': backup_path
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/createbackup/' + self.database_name, verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.create_backup(database_name=self.database_name, user_name=backup_json['user_name'], password=backup_json['password'], backup_location=backup_json['backup_location'])
        self.backup_id_one = jsonResponse['backup_id']

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Creating backup 1 should work')
        self.assertIsNotNone(jsonResponse.get('backup_id'), 'Backup_id should not be None')
        self.assertNotEqual(jsonResponse.get('backup_id'), '', 'Backup_id should not be empty')
        self.assertIsNotNone(jsonResponse.get('databanks'), 'Databank info missing')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        con = mimerpy.connect(dsn = self.database_name_direct_connect, user = 'SYSADM', password = self.sysadm_pwd)
        con.execute("insert into t1 values(2)")
        con.commit()
        con.close()

        #Testing backup with name and comment
        backup_json = {
            'user_name': 'SYSADM',
            'password': 'SYSADM',
            'backup_name': 'Before upgrade',
            'backup_comment': 'Backup before upgraing the system just in case something goes wrong',
            'backup_location': backup_path
        }

        if(self.test_http):
            response = requests.post(self.base_url + '/createbackup/' + self.database_name, verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.create_backup(database_name=self.database_name, user_name=backup_json['user_name'], password=backup_json['password'], backup_location=backup_json['backup_location'], backup_name=backup_json.get('backup_name'),backup_comment=backup_json.get('backup_comment'))
        self.backup_id_two = jsonResponse['backup_id']
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Create backup with name and comment')
        self.assertIsNotNone(jsonResponse.get('backup_id'), 'No backup_id')
        self.assertNotEqual(jsonResponse.get('backup_id'), self.backup_id_one, 'Backup id re-used')
        self.assertNotEqual(jsonResponse.get('backup_id'), '', 'Backup_id empty')
        self.assertIsNotNone(jsonResponse.get('databanks'), 'Databank info missing in backup')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))
        con = mimerpy.connect(dsn = self.database_name_direct_connect, user = 'SYSADM', password = self.sysadm_pwd)
        con.execute("insert into t1 values(3)")
        con.commit()
        con.close()


    def test_manage_backup(self):
        #Create some backups
        self.test_backup()
        empty_backup = '1999-01-01'
        if os.path.isdir(os.path.join(backup_path, empty_backup)):
            shutil.rmtree(os.path.join(backup_path, empty_backup))
        if os.path.isdir(os.path.join(backup_path, 'xxx')):
            shutil.rmtree(os.path.join(backup_path, 'xxx'))
        os.mkdir(os.path.join(backup_path, empty_backup))

        #Getting backup info 1
        backup_json = {
            'backup_id': self.backup_id_one,
            'backup_location': backup_path
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/showbackup', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_backup(backup_json.get('backup_location'), backup_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Get backup info 1')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Getting backup info 2
        backup_json = {
            'backup_id': self.backup_id_two,
            'backup_location': backup_path
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/showbackup', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_backup(backup_json.get('backup_location'), backup_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Get backup info 2')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Getting backup info xxx, should fail
        backup_json = {
            'backup_id': 'xxx',
            'backup_location': backup_path
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/showbackup', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_backup(backup_json.get('backup_location'), backup_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Getting backup info using invalid backup_id should fail')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Getting backup info from existing dir without backup_info.json, should fail:
        backup_json = {
            'backup_id': empty_backup,
            'backup_location': backup_path
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/showbackup', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_backup(backup_json.get('backup_location'), backup_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Getting backup info from valid dir but without backup_info.json should fail')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Getting backup info 2 from wrong path, should fail
        backup_json = {
            'backup_id': self.backup_id_two,
            'backup_location': backup_path +'upxxx'
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/showbackup', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_backup(backup_json.get('backup_location'), backup_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Getting backup info from invalid path should fail')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Listing backups
        backup_json = {
            'backup_location': backup_path
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/listbackups', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.list_backup(backup_json.get('backup_location'))
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Listing backups')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Deleting backups 2
        backup_json = {
            'backup_location': backup_path,
            'backup_id': self.backup_id_two
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/deletebackup', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.delete_backup(backup_location=backup_json.get('backup_location'), backup_id=backup_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Deleting backup 2')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Deleting backups with invalid id, should fail
        backup_json = {
            'backup_location': backup_path,
            'backup_id': self.backup_id_two
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/deletebackup', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.delete_backup(backup_location=backup_json.get('backup_location'), backup_id=backup_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Deleting backup with invalid id should fail')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Deleting backups with invalid path, should fail
        backup_json = {
            'backup_location': backup_path + 'xxx',
            'backup_id': self.backup_id_two
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/deletebackup', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.delete_backup(backup_location=backup_json.get('backup_location'), backup_id=backup_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Deleting a backup with invalid path should fail')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Deleting backups with invalid backup, should fail 
        backup_json = {
            'backup_location': backup_path,
            'backup_id': empty_backup
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/deletebackup', verify=False, headers=self.json_header, json=backup_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.delete_backup(backup_location=backup_json.get('backup_location'), backup_id=backup_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Deleting backup with invalid backup_id should fail')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

    @unittest.skipIf(skip_slow_tests == True, "Skipping slow test")
    def test_restore(self):
        #Create a couple of backups to have something to work with
        self.test_backup()
        #Should work. Testing restore, restore log, keep transdb
        restore_json = {
            'user_name': 'SYSADM',
            'password': 'SYSADM',
            'backup_location': backup_path,
            'restore_log': 'true',
            'keep_transdb': 'true'
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/restorebackup/' + self.database_name, verify=False, headers=self.json_header, json=restore_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.restore_backup(database_name=self.database_name, user_name=restore_json['user_name'], password=restore_json['password'], backup_location=restore_json['backup_location'], restore_log=restore_json.get('restore_log')=='true', keep_transdb=restore_json.get('keep_transdb')=='true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Restore should work. Restore log, keep transdb')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Should work. Testing restore, restore log, restore transdb
        restore_json = {
            'user_name': 'SYSADM',
            'password': 'SYSADM',
            'backup_location': backup_path,
            'restore_log': 'true',
            'keep_transdb': 'false'
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/restorebackup/' + self.database_name, verify=False, headers=self.json_header, json=restore_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.restore_backup(database_name=self.database_name, user_name=restore_json['user_name'], password=restore_json['password'], backup_location=restore_json['backup_location'], restore_log=restore_json.get('restore_log')=='true', keep_transdb=restore_json.get('keep_transdb')=='true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Restore should work. Restore log, restore transdb')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Should fail. Testing restore, restore log, restore transdb, not the latest backup_id
        restore_json = {
            'user_name': 'SYSADM',
            'password': 'SYSADM',
            'backup_location': backup_path,
            'restore_log': 'true',
            'keep_transdb': 'true',
            'backup_id': self.backup_id_one #The second latest...
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/restorebackup/' + self.database_name, verify=False, headers=self.json_header, json=restore_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.restore_backup(database_name=self.database_name, user_name=restore_json['user_name'], password=restore_json['password'], backup_location=restore_json['backup_location'], restore_log=restore_json.get('restore_log')=='true', keep_transdb=restore_json.get('keep_transdb')=='true', backup_id=restore_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Restore should fail, not the latest backup_id and restore log and transdb')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Should work. Testing restore, restore log=false, not the latest backup_id
        restore_json = {
            'user_name': 'SYSADM',
            'password': 'SYSADM',
            'backup_location': backup_path,
            'restore_log': 'false',
            'backup_id': self.backup_id_one
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/restorebackup/' + self.database_name, verify=False, headers=self.json_header, json=restore_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.restore_backup(database_name=self.database_name, user_name=restore_json['user_name'], password=restore_json['password'], backup_location=restore_json['backup_location'], restore_log=restore_json.get('restore_log')=='true', backup_id=restore_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Restore should work, not the latest backup_id but restore_log=false')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Should fail since there is no log to apply any longer. Since we took logdb from the backup we have lost data
        #restore log=true, id for latest backup_id
        restore_json = {
            'user_name': 'SYSADM',
            'password': 'SYSADM',
            'backup_location': backup_path,
            'restore_log': 'true',
            'backup_id': self.backup_id_two
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/restorebackup/' + self.database_name, verify=False, headers=self.json_header, json=restore_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.restore_backup(database_name=self.database_name, user_name=restore_json['user_name'], password=restore_json['password'], backup_location=restore_json['backup_location'], restore_log=restore_json.get('restore_log')=='true', backup_id=restore_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Should fail since there is no log to apply any longer')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Should work, fix problem above, but we have lost one row
        #Should work. Testing restore, restore log=false, id for latest backup_id
        restore_json = {
            'user_name': 'SYSADM',
            'password': 'SYSADM',
            'backup_location': backup_path,
            'restore_log': 'false',
            'backup_id': self.backup_id_two
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/restorebackup/' + self.database_name, verify=False, headers=self.json_header, json=restore_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.restore_backup(database_name=self.database_name, user_name=restore_json['user_name'], password=restore_json['password'], backup_location=restore_json['backup_location'], restore_log=restore_json.get('restore_log')=='true', backup_id=restore_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Should work, fix problem above, but we have lost one row')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        #Should work
        #Create a new backup first, otherwise no restore is needed
        self.clearEnv()
        self.setupEnv()
        self.test_backup()
        #Should work. Testing restore, restore log=true, id for latest backup_id HTTP
        restore_json = {
            'user_name': 'SYSADM',
            'password': 'SYSADM',
        'backup_location': backup_path,
            'restore_log': 'true',
            'backup_id': self.backup_id_two
        }
        if(self.test_http):
            response = requests.post(self.base_url + '/restorebackup/' + self.database_name, verify=False, headers=self.json_header, json=restore_json)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.restore_backup(database_name=self.database_name, user_name=restore_json['user_name'], password=restore_json['password'], backup_location=restore_json['backup_location'], restore_log=restore_json.get('restore_log')=='true', backup_id=restore_json.get('backup_id'))
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Should work, new backup again')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

    def test_db_perf(self):
        #Getting perforamnce figures
        if(self.test_http):
            response = requests.get(self.base_url + '/show_perf/' + self.database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_db_perf(self.database_name)
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Show DB perf should work')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

        if(self.test_http):
            response = requests.get(self.base_url + '/show_perf/' + self.invalid_database_name, verify=False, headers=self.json_header)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_db_perf(self.invalid_database_name)
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Show DB perf with invalid database_name should not work')
        if self.debug_output:
            print(json.dumps(jsonResponse, indent = 4))

    def test_sql_log(self):
        #Test SQL log
        if(self.test_http):
            response = requests.post(self.base_url + '/log_sql/' + self.database_name, verify=False, headers=self.json_header, json={'password':self.sysadm_pwd })
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_sql_log(self.database_name, self.sysadm_pwd)
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Show SQL log should work')

        if(self.test_http):
            response = requests.post(self.base_url + '/log_sql/' + self.invalid_database_name, verify=False, headers=self.json_header, json={'password':self.sysadm_pwd })
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_sql_log(self.invalid_database_name, self.sysadm_pwd)
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Show SQL log with invalid database name should fail')

        if(self.test_http):
            response = requests.post(self.base_url + '/log_sql/' + self.database_name, verify=False, headers=self.json_header, json={'password':self.sysadm_pwd + 'xxx' })
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.show_sql_log(self.database_name, self.sysadm_pwd + 'xxx')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Show SQL log with invalid SYSADM password name should fail')


    #Test user management
    def test_change_sysadm_password(self):
        #Change SYSADM password
        input = {
            'old_password': self.sysadm_pwd,
            'new_password': 'xyz'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/update_sysadm_pass/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.changeUserPass(database_name=self.database_name, login_name='SYSADM', login_pass=input['old_password'], user_name='SYSADM', new_pass=input['new_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Changing SYSADM password should work')
        
        #Login failure
        if self.test_http:
            response = requests.post(self.base_url + '/update_sysadm_pass/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.changeUserPass(database_name=self.database_name, login_name='SYSADM', login_pass=input['old_password'], user_name='SYSADM', new_pass=input['new_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'The SYSADM password have changed, we should get login failure')
        self.assertEqual(jsonResponse.get('error_message'), 'Login failure', 'Login failure, should fail')
        self.assertEqual(jsonResponse.get('error_code'), 90, 'Login failure, should fail')        

        #Now change it back...
        input = {
            'old_password': 'xyz',
            'new_password': self.sysadm_pwd
        }

        if self.test_http:
            response = requests.post(self.base_url + '/update_sysadm_pass/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.changeUserPass(self.database_name, 'SYSADM', input['old_password'], 'SYSADM', input['new_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Changing back SYSADM password should work')


    def test_create_user(self):
        #Test create user
        input =     {
            'creator_name': 'SYSADM',
            'creator_password': self.sysadm_pwd,
            'user_name': 'PY_TEST_XXX',
            'user_password': 'PY_TEST_XXX',
            'can_backup': 'true',
            'can_databank': 'true',
            'can_schema': 'true',
            'can_ident': 'true'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/create_user/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createUser(self.database_name, input.get('creator_name'),
                                                input.get('creator_password'), input.get('user_name'), 
                                                input.get('user_password'), input.get('can_backup') == 'true', 
                                                input.get('can_databank') == 'true', input.get('can_schema') == 'true', input.get('can_ident') == 'true')

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Create user should work')

        #Try to create user with invalid creator
        input['creator_password']='adsf'
        if self.test_http:
            response = requests.post(self.base_url + '/create_user/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createUser(self.database_name, input.get('creator_name'),
                                                input.get('creator_password'), input.get('user_name'), 
                                                input.get('user_password'), input.get('can_backup'), 
                                                input.get('can_databank') == 'true', input.get('can_schema') == 'true', input.get('can_ident') == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'We should get login failure')

        #Let the newly created user create a user
        input =     {
            'creator_name': 'PY_TEST_XXX',
            'creator_password': 'PY_TEST_XXX',
            'user_name': 'PY_TEST_XXX2',
            'user_password': 'PY_TEST_XXX2',
            'can_backup': 'true',
            'can_databank': 'true',
            'can_schema': 'true',
            'can_ident': 'false'
        }
        if self.test_http:
            response = requests.post(self.base_url + '/create_user/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createUser(self.database_name, input.get('creator_name'),
                                                input.get('creator_password'), input.get('user_name'), 
                                                input.get('user_password'), input.get('can_backup') == 'true', 
                                                input.get('can_databank') == 'true', input.get('can_schema') == 'true', input.get('can_ident') == 'true')

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Create sub-user user should work')

        #Try to create a third user using an ident that doesn't have ident privileges
        input =     {
            'creator_name': 'PY_TEST_XXX2',
            'creator_password': 'PY_TEST_XXX2',
            'user_name': 'PY_TEST_XXX3',
            'user_password': 'PY_TEST_XXX3',
            'can_backup': 'true',
            'can_databank': 'true',
            'can_schema': 'true',
            'can_ident': 'false'
        }
        if self.test_http:
            response = requests.post(self.base_url + '/create_user/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createUser(self.database_name, input.get('creator_name'),
                                                input.get('creator_password'), input.get('user_name'), 
                                                input.get('user_password'), input.get('can_backup') == 'true', 
                                                input.get('can_databank') == 'true', input.get('can_schema') == 'true', input.get('can_ident') == 'true')

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Create ident using ident without privileges')

    def test_update_user_password(self):
        #Update user passwd
        input = {
            'user_name': self.test_user,
            'old_password': self.test_user_pwd,
            'new_password': 'xyz'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/update_user_pass/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.changeUserPass(database_name=self.database_name, login_name=input['user_name'], login_pass=input['old_password'], user_name=input['user_name'], new_pass=input['new_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Updating user password should work')
        
        #Now change it back...
        input = {
            'user_name': self.test_user,
            'old_password': 'xyz',
            'new_password': self.test_user_pwd
        }

        if self.test_http:
            response = requests.post(self.base_url + '/update_user_pass/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.changeUserPass(database_name=self.database_name, login_name=input['user_name'], login_pass=input['old_password'], user_name=input['user_name'], new_pass=input['new_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Changing back user password should work')

    def test_reset_user_password(self):
        #Reset a users passwd
        input = {
            'creator_name': 'SYSADM',
            'creator_password': self.sysadm_pwd,
            'user_name': self.test_user,
            'user_password': 'xyz',
        }

        if self.test_http:
            response = requests.post(self.base_url + '/reset_user_pass/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.changeUserPass(database_name=self.database_name, login_name=input['creator_name'], login_pass=input['creator_password'], user_name=input['user_name'], new_pass=input['user_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Resetting user password should work')
        
        #Check that the password have been changed by trying to change it
        input = {
            'user_name': self.test_user,
            'old_password': 'xyz',
            'new_password': 'qqq'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/update_user_pass/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.changeUserPass(database_name=self.database_name, login_name=input['user_name'], login_pass=input['old_password'], user_name=input['user_name'], new_pass=input['new_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Updating user password should work, reset password did not work')

        #Now change it back...
        input = {
            'creator_name': 'SYSADM',
            'creator_password': self.sysadm_pwd,
            'user_name': self.test_user,
            'user_password': self.test_user_pwd
        }

        if self.test_http:
            response = requests.post(self.base_url + '/reset_user_pass/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.changeUserPass(database_name=self.database_name, login_name=input['creator_name'], login_pass=input['creator_password'], user_name=input['user_name'], new_pass=input['user_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Resetting back user password should work')


    def test_list_users(self):
        input = {
            'creator_name': 'SYSADM',
            'creator_password': self.sysadm_pwd
        }

        if self.test_http:
            response = requests.post(self.base_url + '/list_users/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.listUsers(database_name=self.database_name, creator_name=input['creator_name'], creator_pass=input['creator_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'List users should work')
        self.assertEqual(jsonResponse.get('user_list'), [{'user': 'PY_TEST'}, {'user': 'PY_TEST_THREE'}, {'user': 'PY_TEST_TWO'}], 'Wrong user info')

        #Invalid creator
        input = {
            'creator_name': 'SYSADMx',
            'creator_password': self.sysadm_pwd
        }

        if self.test_http:
            response = requests.post(self.base_url + '/list_users/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.listUsers(database_name=self.database_name, creator_name=input['creator_name'], creator_pass=input['creator_password'])

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Should give login failure')
        self.assertEqual(jsonResponse.get('error_code'), 90, 'Wrong error_code for login failure')      
    

    def test_user_lookup(self):
        input = {
            'user_name': self.test_user,
            'password': self.test_user_pwd 
        }

        if self.test_http:
            response = requests.post(self.base_url + '/user_lookup/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.userLookup(database_name=self.database_name, user_name=input['user_name'], user_pass=input['password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'User lookup for should work')
        self.assertEqual(jsonResponse.get('creator'), 'SYSADM', 'Wrong group info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('groups'), [{'group_name': 'SYSADM.PY_GROUP'}], 'Wrong group info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('own_schemas'), [{'schema_name': 'PY_TEST'}], 'Wrong schema info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('system_privileges'), [{'grantor': 'SYSADM', 'is_grantable': 'NO', 'privilege': 'DATABANK'}], 'Wrong system privileges info for user ' + TestController.test_user)
        self.assertEqual(len(jsonResponse.get('function_privileges')), 21, 'Wrong function privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('procedure_privileges'), [{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'EXECUTE', 'procedure_name': 'PY_TEST.del_py_one'}, {'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'EXECUTE', 'procedure_name': 'PY_TEST.ins_py_one'}], 'Wrong procedure privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[0],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'DELETE', 'table_name': 'PY_TEST.py_one', 'table_type': 'BASE TABLE'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[1],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'INSERT', 'table_name': 'PY_TEST.py_one', 'table_type': 'BASE TABLE'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[2],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'LOAD', 'table_name': 'PY_TEST.py_one', 'table_type': 'BASE TABLE'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[3],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'REFERENCES', 'table_name': 'PY_TEST.py_one', 'table_type': 'BASE TABLE'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[4],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'SELECT', 'table_name': 'PY_TEST.py_one', 'table_type': 'BASE TABLE'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[5],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'UPDATE', 'table_name': 'PY_TEST.py_one', 'table_type': 'BASE TABLE'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[6],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'DELETE', 'table_name': 'PY_TEST.py_one_view', 'table_type': 'VIEW'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[7],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'INSERT', 'table_name': 'PY_TEST.py_one_view', 'table_type': 'VIEW'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[8],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'REFERENCES', 'table_name': 'PY_TEST.py_one_view', 'table_type': 'VIEW'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[9],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'SELECT', 'table_name': 'PY_TEST.py_one_view', 'table_type': 'VIEW'}, 'Wrong table privileges info info for user ' + TestController.test_user)
        self.assertEqual(jsonResponse.get('table_privileges')[10],{'grantor': '_SYSTEM', 'is_grantable': 'YES', 'privilege': 'UPDATE', 'table_name': 'PY_TEST.py_one_view', 'table_type': 'VIEW'}, 'Wrong table privileges info info for user ' + TestController.test_user)

        input['user_name'] = 'xxxx'
        if self.test_http:
            response = requests.post(self.base_url + '/user_lookup/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.userLookup(database_name=self.database_name, user_name=input['user_name'], user_pass=input['password'])

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'User does not exist')
        self.assertEqual(jsonResponse.get('error_code'), 90, 'Wrong error code for  login failure')

    def test_delete_user(self):
        input = {
            'creator_name': 'SYSADM',
            'creator_password': self.sysadm_pwd,
            'user_name': self.test_user,
            'is_cascade': 'false'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/delete_user/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.deleteUser(database_name=self.database_name, creator_name=input['creator_name'], creator_pass=input['creator_password'], user_name=input['user_name'], is_cascade=input['is_cascade'] == 'true')

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Delete user should fail without cascade')
        self.assertEqual(jsonResponse.get('error_code'), -12592, 'Wrong error_code when deleting witout cascade')

        input['is_cascade'] = 'true'
        if self.test_http:
            response = requests.post(self.base_url + '/delete_user/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.deleteUser(database_name=self.database_name, creator_name=input['creator_name'], creator_pass=input['creator_password'], user_name=input['user_name'], is_cascade=input['is_cascade'] == 'true')

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Delete user should work when using cascade')

        #Try again, now it should fail since we have already deleted the user
        if self.test_http:
            response = requests.post(self.base_url + '/delete_user/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.deleteUser(database_name=self.database_name, creator_name=input['creator_name'], creator_pass=input['creator_password'], user_name=input['user_name'], is_cascade=input['is_cascade'] == 'true')

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'User already deleted')
        self.assertEqual(jsonResponse.get('error_code'), -12517, 'Wrong error_code user not found')


    def test_schema(self):
        input = {
            'user': 'SYSADM',
            'password': self.sysadm_pwd,
            'schema_name': 'tst_schema'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/create_schema/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createSchema(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'SYSADM should be able to create a schema')

        #List schemas, there should be one plus the system schemas
        if self.test_http:
            response = requests.post(self.base_url + '/list_schemas/' + self.database_name, verify=False, headers=self.json_header, json=input)
            r = response.content.decode('utf-8')
            jsonResponse = json.loads(r)
        else:
            jsonResponse = mimcontrol.listSchema(database_name=self.database_name, user=input['user'], password=input['password'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'SYSADM should be able to create a schema')
        self.assertEqual(jsonResponse.get('schema_list'), [{'schema_name': 'MIMER'}, {'schema_name': 'ODBC'}, {'schema_name': 'SYSADM'}, {'schema_name': 'tst_schema'}], "Unexpected schemas")


        #Create the same schema again, it should fail
        if self.test_http:
            response = requests.post(self.base_url + '/create_schema/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createSchema(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Schema should already exist')

        #Delete the schema
        if self.test_http:
            response = requests.post(self.base_url + '/delete_schema/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.deleteSchema(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Should work to delete schema')

        #Delete the schema again, should fail
        if self.test_http:
            response = requests.post(self.base_url + '/delete_schema/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.deleteSchema(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'The schema is already deleted, this should fail')

        #Create the same schema again, now it should work
        if self.test_http:
            response = requests.post(self.base_url + '/create_schema/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createSchema(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'])


        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Create schema should work')

        #Schema lookup, login SYSADM, schema SYSADM
        lookup_input = {
            'user': 'SYSADM',
            'password': self.sysadm_pwd,
            'schema_name': 'SYSADM'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/schema_lookup/' + self.database_name, verify=False, headers=self.json_header, json=lookup_input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.schemaLookup(database_name=self.database_name, user=lookup_input['user'], password=lookup_input['password'], schema_name=lookup_input['schema_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Schema lookup should work')
        self.assertEqual(jsonResponse.get('schema_owner'), 'SYSADM', 'Wrong schema owner')
        self.assertEqual(jsonResponse.get('tables'), [{'table_name': 't1'}], 'Wrong table info')

        #Schema lookup, login SYSADM, schema tst_schema
        lookup_input = {
            'user': 'SYSADM',
            'password': self.sysadm_pwd,
            'schema_name': 'tst_schema'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/schema_lookup/' + self.database_name, verify=False, headers=self.json_header, json=lookup_input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.schemaLookup(database_name=self.database_name, user=lookup_input['user'], password=lookup_input['password'], schema_name=lookup_input['schema_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Schema lookup should work')
        self.assertEqual(jsonResponse.get('schema_owner'), 'SYSADM', 'Wrong schema owner')

        #Schema lookup, login SYSADM, schema PY_TEST
        lookup_input = {
            'user': 'SYSADM',
            'password': self.sysadm_pwd,
            'schema_name': self.test_user
        }
        if self.test_http:
            response = requests.post(self.base_url + '/schema_lookup/' + self.database_name, verify=False, headers=self.json_header, json=lookup_input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.schemaLookup(database_name=self.database_name, user=lookup_input['user'], password=lookup_input['password'], schema_name=lookup_input['schema_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Schema should not be found')
        self.assertEqual(jsonResponse.get('error_code'), -23006, 'Schema should not be found')

        #Schema lookup, login TEST_USER, schema TEST_USER
        lookup_input = {
            'user': self.test_user,
            'password': self.test_user_pwd,
            'schema_name': self.test_user
        }
        if self.test_http:
            response = requests.post(self.base_url + '/schema_lookup/' + self.database_name, verify=False, headers=self.json_header, json=lookup_input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.schemaLookup(database_name=self.database_name, user=lookup_input['user'], password=lookup_input['password'], schema_name=lookup_input['schema_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Schema lookup should work')
        self.assertEqual(jsonResponse.get('schema_owner'), self.test_user, 'Wrong schema owner')
        self.assertEqual(jsonResponse.get('tables'), [{'table_name': 'py_one'}], 'Wrong table info')
        self.assertEqual(jsonResponse.get('views'), [{'view_name': 'py_one_view'}], 'Wrong view info')
        self.assertEqual(jsonResponse.get('indexes'), [{'index_name': 'py_one_idx'}], 'Wrong index info')
        self.assertEqual(jsonResponse.get('procedures'), [{'procedure_name': 'del_py_one'}, {'procedure_name': 'ins_py_one'}], 'Wrong table info')
        self.assertEqual(jsonResponse.get('functions'), [{'function_name': 'func1'}], 'Wrong table info')


        input = {
            'user': self.test_user,
            'password': self.test_user_pwd,
            'schema_name': 'tst_schema2'
        }
        #Try to create schema with user without privilegies
        if self.test_http:
            response = requests.post(self.base_url + '/create_schema/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createSchema(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'No privilegies')


    def test_table(self):
        input = {
            'user': self.test_user,
            'password': self.test_user_pwd,
            'schema_name': self.test_user,
            'table_name': 'py_one' 
        }

        if self.test_http:
            response = requests.post(self.base_url + '/table_lookup/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.tableLookup(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'], table_name=input['table_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Table lookup for table py_one should work')
        self.assertEqual(jsonResponse.get('columns'), [{'column_name': 'id', 'column_type': 'INTEGER', 'primary_key': True}, {'column_name': 'c1', 'column_type': 'CHARACTER VARYING', 'primary_key': False}], 'Wrong table info for py_one')

        input['table_name'] = 'pxxx_one'
        if self.test_http:
            response = requests.post(self.base_url + '/table_lookup/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.tableLookup(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'], table_name=input['table_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Table lookup for table pxxx_one should fail')
        self.assertEqual(jsonResponse.get('error_code'), -12200, 'Wrong error_code for table not found')
    
    def test_view(self):
        input = {
            'user': self.test_user,
            'password': self.test_user_pwd,
            'schema_name': self.test_user,
            'view_name': 'py_one_view' 
        }

        if self.test_http:
            response = requests.post(self.base_url + '/view_lookup/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.viewLookup(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'], view_name=input['view_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'success', 'View lookup for view py_one should work')
        self.assertEqual(jsonResponse.get('columns'), [{'column_name': 'id'}, {'column_name': 'c1'}], 'Wrong view info for py_one')
        self.assertEqual(jsonResponse.get('number_of_rows'), 0, "Wrow row count")
        input['view_name'] = 'pxxx_one'
        if self.test_http:
            response = requests.post(self.base_url + '/view_lookup/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.viewLookup(database_name=self.database_name, user=input['user'], password=input['password'], schema_name=input['schema_name'], view_name=input['view_name'])

        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'View lookup for view pxxx_one should fail')
        self.assertEqual(jsonResponse.get('error_code'), -12200, 'Wrong error_code for view not found')


    def test_grant_access(self):
        #[select|insert|update|delete|references|all]
        input = {
            'grantor_name': TestController.test_user,
            'grantor_password': TestController.test_user_pwd, 
            'grantee_name': TestController.test_user_two,
            'table_name': 'py_one',
            'privilege': 'select',
            'is_grantable': 'false'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges not found")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges found")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges found")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges found")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges found")

        input['privilege']='insert'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges not found")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges found")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges found")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges found")

        input['privilege']='delete'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges not found")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges found")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges found")

        input['privilege']='references'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges not ound")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges found")

        input['privilege']='update'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges not ound")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges not found")

        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, 'SYSADM', 't1')
        self.assertEqual(tt, [], TestController.test_user_two + " should have not privileges on SYSADM.t1")
        input = {
            'grantor_name': 'SYSADM',
            'grantor_password': TestController.sysadm_pwd, 
            'grantee_name': TestController.test_user_two,
            'table_name': 't1',
            'privilege': 'all',
            'is_grantable': 'false'
        }
        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, 'SYSADM', input['table_name'])
        self.assertTrue(self.check_priv('table', tt, 'SYSADM', input['table_name'], 'SELECT', 'NO'), "SELECT privileges not found")
        self.assertTrue(self.check_priv('table', tt, 'SYSADM', input['table_name'], 'INSERT', 'NO'), "INSERT privileges not found")
        self.assertTrue(self.check_priv('table', tt, 'SYSADM', input['table_name'], 'DELETE', 'NO'), "DELETE privileges not found")
        self.assertTrue(self.check_priv('table', tt, 'SYSADM', input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges not ound")
        self.assertTrue(self.check_priv('table', tt, 'SYSADM', input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges not found")

        input['table_name'] = 'nonexistent'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', "Granting access to a non existing table should fail")
        self.assertEqual(jsonResponse.get('error_code'), -12501, "Wrong error code granting access to a non existing table")
        

        #We do not have grant option on the table. This should not give an error, but the privilege should not be granted.
        input = {
            'grantor_name': TestController.test_user_two,
            'grantor_password': TestController.test_user_two_pwd, 
            'grantee_name': TestController.test_user,
            'table_name': 'SYSADM.t1',
            'privilege': 'insert',
            'is_grantable': 'false'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', "Granting access to a table without grantable permissions should not give error")
        tt = self.get_priv('table', TestController.test_user, TestController.test_user_pwd, 'SYSADM', 't1')
        self.assertListEqual(tt, [], "INSERT privileges found")


    def test_revoke_access(self):
        grant_input = {
            'grantor_name': TestController.test_user,
            'grantor_password': TestController.test_user_pwd, 
            'grantee_name': TestController.test_user_two,
            'table_name': 'py_one',
            'privilege': 'all',
            'is_grantable': 'false'
        }

        #Grant access to py_one so that we have something to use for revoke tests
        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=grant_input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, grant_input['grantor_name'], grant_input['grantor_password'], 
                                                            grant_input['grantee_name'], grant_input['table_name'], grant_input['privilege'], 
                                                            grant_input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, grant_input['table_name'])
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'SELECT', 'NO'), "SELECT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'INSERT', 'NO'), "INSERT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'DELETE', 'NO'), "DELETE privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges not found")

        input = {
            'grantor_name': TestController.test_user,
            'grantor_password': TestController.test_user_pwd, 
            'grantee_name': TestController.test_user_two,
            'table_name': 'py_one',
            'privilege': 'select',
            'is_cascade': 'false'
        }
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_cascade'] == 'true')        
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges found after revoke")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges not found")

        input['privilege'] = 'insert'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_cascade'] == 'true')        
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges found after revoke")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges not found")

        input['privilege'] = 'delete'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_cascade'] == 'true')        
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges found after revoke")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges not found")

        input['privilege'] = 'references'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_cascade'] == 'true')        
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges found after revoke")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges not found")

        input['privilege'] = 'update'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_cascade'] == 'true')        
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'SELECT', 'NO'), "SELECT privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'INSERT', 'NO'), "INSERT privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'DELETE', 'NO'), "DELETE privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges found after revoke")
        self.assertFalse(self.check_priv('table', tt, TestController.test_user, input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges found after revoke")


        #Set back all permissions so we can test revoke all
        if self.test_http:
            response = requests.post(self.base_url + '/grant_access/' + self.database_name, verify=False, headers=self.json_header, json=grant_input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantAccessPrivilege(self.database_name, grant_input['grantor_name'], grant_input['grantor_password'], 
                                                            grant_input['grantee_name'], grant_input['table_name'], grant_input['privilege'], 
                                                            grant_input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, grant_input['table_name'])
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'SELECT', 'NO'), "SELECT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'INSERT', 'NO'), "INSERT privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'DELETE', 'NO'), "DELETE privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'REFERENCES', 'NO'), "REFERENCES privileges not found")
        self.assertTrue(self.check_priv('table', tt, TestController.test_user, grant_input['table_name'], 'UPDATE', 'NO'), "UPDATE privileges not found")

        input['privilege'] = 'all'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_access/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeAccessPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['table_name'], input['privilege'], input['is_cascade'] == 'true')        
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke all access should work')
        tt = self.get_priv('table', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['table_name'])
        self.assertEqual(tt, [], "No privileges should exist after revoke all")


    def test_grant_execute(self):
        input = {
            'grantor_name': TestController.test_user,
            'grantor_password': TestController.test_user_pwd, 
            'grantee_name': TestController.test_user_two,
            'routine_name': 'func1',
            'routine_type': 'function',
            'is_grantable': 'false'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/grant_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant execute should work')
        tt = self.get_priv('function', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['routine_name'])
        self.assertTrue(self.check_priv('function', tt, TestController.test_user, input['routine_name'], 'EXECUTE', 'NO'), "Execute privileges not found")

        input['routine_type'] = 'procedure'
        input['routine_name'] = 'ins_py_one'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant execute should work')
        tt = self.get_priv('procedure', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['routine_name'])
        self.assertTrue(self.check_priv('procedure', tt, TestController.test_user, input['routine_name'], 'EXECUTE', 'NO'), "Execute privileges not found")


        input['routine_type'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Unknown routine type should fail')
        
        input['routine_type'] = 'function'
        input['grantor_name'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Login failure')
        self.assertEqual(jsonResponse.get('error_code'), 90, 'Wrong error code for login failure')

        input['grantor_name'] = TestController.test_user
        input['grantee_name'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Unknown grantee')
        self.assertEqual(jsonResponse.get('error_code'), -12564, 'Wrong error code for uknown grantee')

        input['grantee_name'] = TestController.test_user_two
        input['routine_type'] = 'function'
        input['routine_name'] = 'ins_py_one'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Wrong type, ins_py_one exists but it is a function')
        self.assertEqual(jsonResponse.get('error_code'), -12517, 'Object does not exist')


    def test_revoke_execute(self):
        #Start by granting access
        input = {
            'grantor_name': TestController.test_user,
            'grantor_password': TestController.test_user_pwd, 
            'grantee_name': TestController.test_user_two,
            'routine_name': 'func1',
            'routine_type': 'function',
            'is_grantable': 'false'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/grant_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant execute should work')
        tt = self.get_priv('function', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['routine_name'])
        self.assertTrue(self.check_priv('function', tt, TestController.test_user, input['routine_name'], 'EXECUTE', 'NO'), "Execute privileges not found")

        input['routine_type'] = 'procedure'
        input['routine_name'] = 'ins_py_one'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant execute should work')
        tt = self.get_priv('procedure', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['routine_name'])
        self.assertTrue(self.check_priv('procedure', tt, TestController.test_user, input['routine_name'], 'EXECUTE', 'NO'), "Execute privileges not found")

        #Now start testing revoke
        input = {
            'grantor_name': TestController.test_user,
            'grantor_password': TestController.test_user_pwd, 
            'grantee_name': TestController.test_user_two,
            'routine_name': 'func1',
            'routine_type': 'function',
            'is_cascade': 'false'
        }



        if self.test_http:
            response = requests.post(self.base_url + '/revoke_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_cascade'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke execute should work')
        tt = self.get_priv('function', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['routine_name'])
        self.assertFalse(self.check_priv('function', tt, TestController.test_user, input['routine_name'], 'EXECUTE', 'NO'), "Execute privileges found after revoke")

        input['routine_type'] = 'procedure'
        input['routine_name'] = 'ins_py_one'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_cascade'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke execute should work')
        tt = self.get_priv('procedure', TestController.test_user_two, TestController.test_user_two_pwd, TestController.test_user, input['routine_name'])
        self.assertFalse(self.check_priv('procedure', tt, TestController.test_user, input['routine_name'], 'EXECUTE', 'NO'), "SELECT privileges found after revoke")


        input['routine_type'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_cascade'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Unknown routine type should fail')
        
        input['routine_type'] = 'function'
        input['grantor_name'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_cascade'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Login failure')
        self.assertEqual(jsonResponse.get('error_code'), 90, 'Wrong error code for login failure')

        input['grantor_name'] = TestController.test_user
        input['grantee_name'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_cascade'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Unknown grantee')
        self.assertEqual(jsonResponse.get('error_code'), -12564, 'Wrong error code for uknown grantee')

        input['grantee_name'] = TestController.test_user_two
        input['routine_type'] = 'function'
        input['routine_name'] = 'ins_py_one'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_execute/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeExecutePrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['routine_name'], input['routine_type'], input['is_cascade'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Wrong type, ins_py_one exists but it is a function')
        self.assertEqual(jsonResponse.get('error_code'), -12517, 'Object does not exist')

    def test_grant_system_privileges(self):
        input = {
            'grantor_name': 'SYSADM',
            'grantor_password': TestController.sysadm_pwd, 
            'grantee_name': TestController.test_user_three,
            'privilege': 'backup',
            'is_grantable': 'false'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/grant_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant backup should work')
        self.assertTrue(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'BACKUP', 'NO'), "Backup privileges not found")

        input['privilege'] = 'databank'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant backup should work')
        self.assertTrue(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'databank', 'NO'), "Databank privileges not found")

        input['privilege'] = 'schema'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant backup should work')
        self.assertTrue(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'SCHEMA', 'NO'), "Schema privileges not found")

        input['privilege'] = 'statistics'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant backup should work')
        self.assertTrue(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'statistics', 'NO'), "Statistics privileges not found")

        input['privilege'] = 'ident'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant backup should work')
        self.assertTrue(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'ident', 'NO'), "Ident privileges not found")


        input['privilege'] = 'shadow'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grant backup should work')
        self.assertTrue(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'shadow', 'NO'), "Shadow privileges not found")

        input['privilege'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Uknown system privilege should fail')

        input['privilege'] = 'backup'
        input['grantee_name'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Grantee name does not exist')
        self.assertEqual(jsonResponse.get('error_code'), -12564, 'Wrong error code for grantee not found')

        input['grantor_password'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Login failure')
        self.assertEqual(jsonResponse.get('error_code'), 90, 'Wrong error code for login failure')

    def test_revoke_system_privileges(self):
        #Start by granting all privileges by executing test_grant_system_privileges
        self.test_grant_system_privileges()
        input = {
            'grantor_name': 'SYSADM',
            'grantor_password': TestController.sysadm_pwd, 
            'grantee_name': TestController.test_user_three,
            'privilege': 'backup',
            'is_grantable': 'false'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/revoke_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke backup should work')
        self.assertFalse(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'BACKUP', 'NO'), "Backup privileges found")

        input['privilege'] = 'databank'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke backup should work')
        self.assertFalse(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'databank', 'NO'), "Databank privileges found")

        input['privilege'] = 'schema'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke backup should work')
        self.assertFalse(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'SCHEMA', 'NO'), "Schema privileges found")

        input['privilege'] = 'statistics'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke backup should work')
        self.assertFalse(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'statistics', 'NO'), "Statistics privileges found")

        input['privilege'] = 'ident'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke backup should work')
        self.assertFalse(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'ident', 'NO'), "Ident privileges found")


        input['privilege'] = 'shadow'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Revoke backup should work')
        self.assertFalse(self.check_system_priv(TestController.test_user_three, TestController.test_user_three_pwd, 'shadow', 'NO'), "Shadow privileges found")

        input['privilege'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Uknown system privilege should fail')

        input['privilege'] = 'backup'
        input['grantee_name'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Grantee name does not exist')
        self.assertEqual(jsonResponse.get('error_code'), -12564, 'Wrong error code for grantee not found')

        input['grantor_password'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_system_privilege/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeSystemPrivilege(self.database_name, input['grantor_name'], input['grantor_password'], 
                                                            input['grantee_name'], input['privilege'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Login failure')
        self.assertEqual(jsonResponse.get('error_code'), 90, 'Wrong error code for login failure')


    def test_create_group(self):
        #Test create group
        input = {
            'creator_name': 'SYSADM',
            'creator_password': self.sysadm_pwd,
            'group_name': 'PY_GROUP_XXX'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/create_group/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createGroup(self.database_name, input.get('creator_name'),
                                                input.get('creator_password'), input.get('group_name'))
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Create group should work')

        #Try again, should fail
        if self.test_http:
            response = requests.post(self.base_url + '/create_group/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createGroup(self.database_name, input.get('creator_name'),
                                                input.get('creator_password'), input.get('group_name'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Create group should fail')
        self.assertEqual(jsonResponse.get('error_code'), -12558, 'Wrong error code when group already exist')

        #Used do not have privileges to create group, should fail
        input = {
            'creator_name': self.test_user_three,
            'creator_password': self.test_user_three_pwd,
            'group_name': 'PY_GROUP_ZZZ'
        }
        if self.test_http:
            response = requests.post(self.base_url + '/create_group/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.createGroup(self.database_name, input.get('creator_name'),
                                                input.get('creator_password'), input.get('group_name'))
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Create group should fail')
        self.assertEqual(jsonResponse.get('error_code'), -12502, 'Wrong error code when user do not have permissions')

    def test_delete_group(self):
        #Test delete group
        input = {
            'creator_name': 'SYSADM',
            'creator_password': self.sysadm_pwd,
            'group_name': self.test_group,
            'is_cascade': 'false'
        }

        if self.test_http:
            response = requests.post(self.base_url + '/delete_group/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.deleteGroup(self.database_name, input.get('creator_name'),
                                                input.get('creator_password'), input.get('group_name'), input.get('is_cascade') == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Drop group should work')

        #Try again, should fail
        if self.test_http:
            response = requests.post(self.base_url + '/delete_group/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.deleteGroup(self.database_name, input.get('creator_name'),
                                                input.get('creator_password'), input.get('group_name'), input.get('is_cascade') == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Drop group should fail')
        self.assertEqual(jsonResponse.get('error_code'), -12517, 'Wrong error code when group do not exist')

    def test_grant_member(self):
        #Test grant member
        input = {
            'grantor_name': 'SYSADM',
            'grantor_password': self.sysadm_pwd,
            'grantee_name': self.test_user_three,
            'group_name': self.test_group,
            'is_grantable': 'false'
        }

        #Verify that the user is not member of the group:
        tt = self.get_priv('group', TestController.test_user_three, TestController.test_user_three_pwd, TestController.test_user_three, None)
        self.assertEqual(tt, [], "Should not be member of the group")
        if self.test_http:
            response = requests.post(self.base_url + '/grant_member/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantMembership(self.database_name, input.get('grantor_name'),
                                                input['grantor_password'], input['grantee_name'],
                                                input['group_name'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Create group should work')
        tt = self.get_priv('group', TestController.test_user_three, TestController.test_user_three_pwd, TestController.test_user_three, None)
        self.assertEqual(tt, [{'group_name': 'SYSADM.PY_GROUP'}], "Should be member of the group")

        #Grantee doesn't exist
        input['grantee'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_member/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantMembership(self.database_name, input.get('grantor_name'),
                                                input['grantor_password'], input['grantee_name'],
                                                input['group_name'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Grantee does not exist, should not give error anyway')

        #Login failure
        input['grantor_password'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/grant_member/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.grantMembership(self.database_name, input.get('grantor_name'),
                                                input['grantor_password'], input['grantee_name'],
                                                input['group_name'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Should give login failure')


    def test_revoke_member(self):
        #Test revoke member
        input = {
            'grantor_name': 'SYSADM',
            'grantor_password': self.sysadm_pwd,
            'grantee_name': self.test_user_two,
            'group_name': self.test_group,
            'is_grantable': 'false'
        }

        #Verify that the user is not member of the group:
        tt = self.get_priv('group', TestController.test_user_two, TestController.test_user_two_pwd, '', None)
        self.assertEqual(tt, [{'group_name': 'SYSADM.PY_GROUP'}], "Should be member of the group")
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_member/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeMembership(self.database_name, input.get('grantor_name'),
                                                input['grantor_password'], input['grantee_name'],
                                                input['group_name'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Create group should work')
        tt = self.get_priv('group', TestController.test_user_two, TestController.test_user_two_pwd, '', None)
        self.assertEqual(tt, [], "Should not be member of the group")
        #Login failure
        input['grantor_password'] = 'xxx'
        if self.test_http:
            response = requests.post(self.base_url + '/revoke_member/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.revokeMembership(self.database_name, input.get('grantor_name'),
                                                input['grantor_password'], input['grantee_name'],
                                                input['group_name'], input['is_grantable'] == 'true')
        self.assertEqual(jsonResponse.get('return_code'), 'failure', 'Should give login failure')
    
    def test_group_lookup(self):
        input = {
            'creator_name': 'SYSADM',
            'creator_password': self.sysadm_pwd,
            'group_name': self.test_group
        }

        if self.test_http:
            response = requests.post(self.base_url + '/group_lookup/' + self.database_name, verify=False, headers=self.json_header, json=input)
            jsonResponse = json.loads(response.content.decode('utf-8'))
        else:
            jsonResponse = mimcontrol.groupLookup(self.database_name, input.get('creator_name'),
                                                input['creator_password'], input['group_name'])
        self.assertEqual(jsonResponse.get('return_code'), 'success', 'Lookup group should work')
        self.assertEqual(jsonResponse.get('members'), [{'member': 'PY_TEST'}, {'member': 'PY_TEST_TWO'}], "Wrong group members")

if __name__ == "__main__":
    unittest.main()