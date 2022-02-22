systemctl restart mariadb
cd /usr/local/zookeeper/bin && ./zkServer.sh start && echo "zk starting..."
cd -
sleep 10
rm -rf /tmp/kafka-logs/*
cd /usr/local/kafka && ./bin/kafka-server-start.sh -daemon config/server.properties && echo "kafka starting..."
cd -
mysql -u root -ptest < pkg/db/init.sql