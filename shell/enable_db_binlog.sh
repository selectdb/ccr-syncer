#!/bin/bash

# ./cli --host $host --port $port --user $user --password $password --db $db
usage() {
    echo "Usage: $0 [-h host] [-p port] [-u user] [-P password] [-d db]"
    exit 1
}

# parse cli args
host="127.0.0.1"
port="9030"
user="root"
password=""
db=""
while getopts ":h:p:u:P:d:" opt; do
    case $opt in
        h) host="$OPTARG"
        ;;
        p) port="$OPTARG"
        ;;
        u) user="$OPTARG"
        ;;
        P) password="$OPTARG"
        ;;
        d) db="$OPTARG"
        ;;
        \?) echo "Invalid option -$OPTARG" >&2
        usage
        ;;
    esac
done

if [ -z "$db" ]; then
    echo "db is empty"
    exit 1
fi

mysql_client="mysql -h ${host} -P ${port} -u${user} --password=${password}"

# check db binlog is enable
db_binlog_enable=$($mysql_client -e "show create database ${db}" 2>/dev/null | grep '"binlog.enable" = "true"')
# remove empty line
db_binlog_enable=$(echo $db_binlog_enable | sed 's/^[ \t]*//g')
if ! [ -z "$db_binlog_enable" ]; then
    echo "db ${db} binlog is enable"
    exit 0
fi

echo "enable db ${db} binlog"
# use mysql client list all tables in db
tables=$(${mysql_client} -e "use ${db};show tables;" 2>/dev/null | sed '1d') 
view_or_external_tables=$(${mysql_client} -e "select table_name from information_schema.tables where table_schema=\"${db}\" and table_type in ('VIEW','EXTERNAL TABLE')" 2>/dev/null | sed '1d')
for table in $tables; do
    echo "table: $table"

    # skip view
    is_view_or_external_table="false"
    for view_or_external_table in $view_or_external_tables; do
      if [ "$view_or_external_table" == "$table" ]; then
        is_view_or_external_table="true"
        break
      fi
    done
    if [ "$is_view_or_external_table" == "true" ]; then
      continue
    fi

    # check table binlog is enable
    table_binlog_enable=$($mysql_client -e "show create table ${db}.${table}" 2>/dev/null | grep '"binlog.enable" = "true"')
    # remove empty line
    table_binlog_enable=$(echo $table_binlog_enable | sed 's/^[ \t]*//g')
    if ! [ -z "$table_binlog_enable" ]; then
        echo "table ${table} binlog is enable"
    else
        echo "enable table ${table} binlog"
        ${mysql_client} -e "ALTER TABLE $db.$table SET (\"binlog.enable\" = \"true\", \"binlog.ttl_seconds\"=\"86400\");" || exit 1
    fi
done
${mysql_client} -e "ALTER DATABASE $db SET properties (\"binlog.enable\" = \"true\", \"binlog.ttl_seconds\"=\"86400\");" || exit 1
# mysql -uroot -p123456 -e "use test;show tables;"
