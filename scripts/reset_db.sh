#! /bin/bash

mysql -uroot -ppassword -e"DROP DATABASE IF EXISTS $1"
mysql -uroot -ppassword -e"CREATE DATABASE $1"

migrate -source file://services/db/migrations -database "mysql://root:password@tcp(127.0.0.1:3306)/$1" up
