# Mimer SQL version 11.0

This is a docker build of Mimer SQL version 11.0. It comes with a ten user license, the same that all our free downloads come with; see https://developer.mimer.com/product-overview/downloads/

The docker image include a webservice that let you monitor and administer the Mimer SQL database using standard REST calls.

## Running Mimer
Run the container with

```docker run -p 1360:1360 -d mimersql/mimersql_v11.0:latest```
or use a specic tag, for example the V11.0.4A releas:
```docker run -p 1360:1360 -d mimersql/mimersql_v11.0:v11.0.4a```

This launches a Mimer SQL database server that is accessible on port 1360, the standard port for Mimer SQL.

A SYSADM password can be specified with -e MIMER_SYSADM_PASSWORD=\<password\>. If not, a new password is generated and printed. Remember this password, it cannot be recovered.

A Mimer SQL license can be specified with -e MIMER_KEY=<Hex key value>. If a persistent storage is used the Mimer SQL license is saved in the Mimer data directory, MIMER_DATA_DIR (see below), for future use, i.e when the container is started again. An alternative way is to copy your Mimer SQL license file directly to MIMER_DATA_DIR/my_mimerkey.mcfg.

## Configuration of the Mimer SQL database
It's possible to configure the Mimer SQL database, for example how much memory it will use and what TCP port to use with Docker environment variables. To configure the TCP Port for example, use -e MIMER_TCP_PORT=\<port number\> when starting the container. The following environment variables with the corresponding Mimer SQL multidefs configuration parameter are available:

- MIMER_TCP_PORT = TCPPort
- MIMER_MAX_DBFILES = Databanks
- MIMER_MAX_USERS = Users
- MIMER_MAX_TABLES = Tables
- MIMER_MAX_TRANS = ActTrans
- MIMER_PAGES_4K = Pages4K
- MIMER_PAGES_32K = Pages32K
- MIMER_PAGES_32K = Pages128K
- MIMER_REQUEST_THREADS = RequestThreads
- MIMER_BACKGROUND_THREADS = BackgroundThreads
- MIMER_TC_FLUSH_THREADS = TcFlushThreads
- MIMER_BG_PRIORITY = BackgroundPriority
- MIMER_INIT_SQLPOOL = SQLPool
- MIMER_MAX_SQLPOOL = MaxSQLPool
- MIMER_DELAYED_COMMIT = DelayedCommit
- MIMER_DELAYED_COMMIT_TIMEOUT = DelayedCommitTimeout
- MIMER_GROUP_COMMIT_TIMEOUT = GroupCommitTimeout
- MIMER_NETWORK_ENCRYPTION = NetworkEncryption

See the Mimer SQL documenation for information about the different multidefs configuration parameters.

## Connecting to the database
Access Mimer using for instance DBVisualizer (https://www.dbvis.com) which comes with a JDBC driver and support for Mimer. With the example above is the host `localhost` and the port 1360. Login as "SYSADM", password "SYSADM".

The JDBC connection string would then be
```jdbc:mimer://localhost:1360/mimerdb```

## Saving data between containers
Since the container is a separate entity, the above solution will lose all data when the container is killed. 

There are several solutions to this but the easiest is to use ´bind mounts´ where a directory on the host system is bound to the container's MIMER_DATA_DIR, thus causing all file writes to happen in the host file system which is persistent. This is commonly used for testing.

An alternative solutin is to use Docker volumes. This way the storage is managed by Docker. Just as with `bind mounts` the MIMER_DATA_DIR is mapped to the volume.

The default MIMER_DATA_DIR in the container is `/data`. This can be changed with the environment variable MIMER_DATA_DIR. This is where the database, license files and configurations are stored. Everything is stored in the database home directory located in MIMER_DATA_DIR, by default `mimerdb`.

### Using bind mounts
To use `bind mounts`, create a directory on the host that will act as the MIMER_DATA_DIR. When the directoy is created, mount it when starting the container:

```docker run -v /my_data:/data -p 1360:1360 -d mimersql/mimersql_v11.0:latest```

The Mimer SQL database and it's configuration will now be stored in /my_data/mimerdb on the host.

### Using Docker volumes
To use Docker volumes, create the volume using `docker volume create`, for example:
```docker volume create mimer_data````

When starting the container, map the volume to the container:

```docker run -v mimer_data:/data -p 1360:1360 -d mimersql/mimersql_v11.0:latest```
or
```docker run --mount source=mimer_data, target=/data -p 1360:1360 -d mimersql/mimersql_v11.0:latest```

The Mimer SQL database and it's configuration will now be stored in the Docker volume.

## Using the webservice to monitor and administer Mimer SQL
This version of the the Docker image comes with an integrated webservice that let you administer and monitor Mimer SQL using REST calls. The request and respone are JSON based. See the API specification in the doc folder for details on all the calls.

Here is an example that show the SQL execution log:
```curl --insecure -u mimadmin:cgujdUy639B -H "Content-Type: application/json" -X POST -d '{"password":"x7#xx93"}'  https://localhost:5001/log_sql/mimerdb```

You don't have to specify login information with all calls. You can also generate a security token that can be used instead:

```curl --insecure -u mimadmin:cgujdUy639B -H "Content-Type: application/json" -X GET  https://localhost:5001/gettoken```. This will give you a token to use in future calls:

```curl --insecure -u mimadmin:cgujdUy639B -H "Content-Type: application/json, Authorization: token <your security token>" -X POST -d '{"password":"x7#xx93"}'  https://localhost:5001/log_sql/mimerdb```

To enable the webservice, specify `-e MIMER_REST_CONTROLLER=true` when starting the container.

The following parameters can be used to control the webservice:
- MIMER_REST_CONTROLLER: Enable or disable the webservice. Valid values are true and false.
- MIMER_REST_CONTROLLER_USER: The username used in the HTTP authentication. If not specified, `mimadmin` is used.
- MIMER_REST_CONTROLLER_PASSWORD: The password used in the HTTP authentication. If not specified a password is generated and printed at the first start.
- MIMER_REST_CONTROLLER_PORT: The portnumber used by the webservice. Default port number if not specified is 5001.
- MIMER_REST_CONTROLLER_USE_HTTP: If true, HTTP is used instead of HTTPS. This is NOT recomended since passwords will be sent in clear text. Valid values are true and false.
