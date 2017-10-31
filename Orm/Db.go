package Orm

import (
	"fmt"
	_ "code.google.com/p/odbc"
	// "strings"
)

type Orm interface{
	table(database string)Orm
	alias(as string)Orm
	field(row string)Orm
	where(condtion ...interface{})Orm
	limit(page int,num int)Orm
	order(row string,sort string)Orm
	join(condition ...string)Orm
	group(row string)Orm
	insert(add map[string]string)int64
	insertAll(addAll []map[string]string)int
	update(renew map[string]string)int64
	delete(del map[string]string)int64
	query(sql string)[]map[string]string
	find()map[string]string
	findAll()[]map[string]string
	fetchSql()string
	connect(driverName string,dataSourceName string)error
	construct()
}

func Connect(driverName string,dataSourceName string)Orm{
	var m Orm
	var err error
	switch driverName{
		case "mysql":
			m = new(mysql)			
		break

		case "obdc":
			m = new(sqlserver)
		break
		
	}
	err = m.connect(driverName,dataSourceName)
	defer SqlErr(err)
	return m
}

func SqlErr(err error){//输出错误
	if err != nil{
		fmt.Print(err)
	}
}

