package Orm



type Db interface{
	Table(database string)Db
	Alias(as string)Db
	Field(row string)Db
	Where(condtion ...interface{})Db
	Limit(page int,num int)Db
	Order(row string,sort string)Db
	Join(condition ...string)Db
	Group(row string)Db
	Insert(add map[string]string)int64
	InsertAll(addAll []map[string]string)int
	Update(renew map[string]string)int64
	Delete()int64
	Query(sql string)[]map[string]string
	Find()map[string]string
	Select()[]map[string]string
	Have(condition string)Db
	connect(driverName string,dataSourceName string)error
	construct()
}

func Connect(driverName string,dataSourceName string)Db{
	var m Db
	var err error
	switch driverName{
		case "mysql":
			m = new(mysql)
		break

		case "sqlserver":
			m = new(sqlserver)
		break
		
	}
	err = m.connect(driverName,dataSourceName)
	defer SqlErr(err)
	return m
}


func SqlErr(err error){//输出错误
	if err != nil{
		panic(err)
	}
}

