# pull and run docker image
#MySQL
docker pull mysql/mysql-server:latest
docker run -p 3307:3306 --name=mysql1 -d --restart unless-stopped mysql/mysql-server:latest

#MongoDB
docker run -d --name mongodb -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=admin mongo:latest
db.getCollectionInfos({ name: "Users" })[0].options.validator
#Redis
docker pull redis
# docker run --name redis -d --restart unless-stopped -p 6379:6379 redis redis-server --requirepass "789"
docker run -d --name redis -p 6380:6379 -v C:/Users/PC/Desktop/data-sync-pipeline/database/users.acl:/usr/local/etc/redis/users.acl redis redis-server --aclfile /usr/local/etc/redis/users.acl
#Set up user on redis
docker exec -it my-redis-alt /bin/bash
redis-cli -h 127.0.0.1 -a 789
AUTH redis 789
PING
ACL LIST
db.Users.find().pretty()