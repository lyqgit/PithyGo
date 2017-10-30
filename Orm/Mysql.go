package Orm

import (
	"database/sql"
	"strings"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

type mysql struct{
	db *sql.DB
	selectSql string
	insertSql string
	RowSql string
	ValSql string
	insertAllSql string
	updateSql string
	deleteSql string
	querysql string
	option map[string]string
}



func(m *mysql)connect(driverName string,dataSourceName string)error{
	var err error
	m.db,err = sql.Open(driverName,dataSourceName)
	return err
}

func(m *mysql)construct(){
	m.option = make(map[string]string)
	m.selectSql = "SELECT %FIELD% FROM %TABLE% %SUBQUERY% %FORCE% %JOIN% %WHERE% %GROUP% %HAVING% %ORDER% %LIMIT% %UNION% %LOCK% %COMMENT%"
	m.insertSql = "INSERT %TABLE% SET "+m.RowSql
	m.RowSql = ""
	m.ValSql = ""
	m.updateSql = "UPDATE %TABLE% SET "+m.RowSql+" %WHERE% "
	m.deleteSql = "DELETE FROM %TABLE% %WHERE%"
	m.option = make(map[string]string)
	m.querysql = ""
}

func(m *mysql)table(database string)Orm{
	m.construct()
	m.option["table"] = database
	return m
}

func(m *mysql)alias(as string)Orm{
	m.option["alias"] = as
	return m
}

func(m *mysql)field(row string)Orm{
	m.option["field"] = row
	return m
}

func(m *mysql)where(condition ...interface{})Orm{
	switch t := condition[0].(type){
		case map[string]map[string]string:
			for k,v := range t{
				m.option["where"] += k+" = "
				for kk,vv := range v{
					if vv == ""{
						m.option["where"] += kk+" and "
					}else{
						m.option["where"] += kk+" "+vv
					}
					
				}
			}
		break
		case string:
			if condition[2].(string) == ""{
				if m.option["where"] == ""{
					m.option["where"] += t+" = "+condition[1].(string)
				}else{
					m.option["where"] += " and "+t+" = "+condition[1].(string)
				}
				
			}else{
				if m.option["where"] == ""{
					m.option["where"] += t+" = "+condition[1].(string)+" "+condition[2].(string)
				}else{
					m.option["where"] += condition[2].(string)+" "+t+" = "+condition[1].(string)
				}		
			}
			
		break
	}
	return m
}

func(m *mysql)limit(page int,num int)Orm{
	m.option["limit"] = "limit "+strconv.Itoa(page)+","+strconv.Itoa(num)
	return m
}

func(m *mysql)order(row string,sort string)Orm{
	m.option["order"] = "order by "+row+" "+sort
	return m
}

func(m *mysql)join(condition ...string)Orm{
	if condition[2] == ""{
		m.option["join"] = "left join "+condition[0]+" on "+condition[1]
	}
	return m
}

func(m *mysql)insert(add map[string]string)int64{
	m.assemble(m.insertSql,add)
	return m.rowsAffected(m.insertSql)
}

func(m *mysql)insertGetId(add map[string]string)int64{
	m.assemble(m.insertSql,add)
	res := m.Prepare(m.insertSql,m.ValSql)
	num,err := res.LastInsertId()
	defer SqlErr(err)
	return num
}

func(m *mysql)Prepare(sql string,val string)sql.Result{
	stmt,err := m.db.Prepare(sql)
	defer SqlErr(err)
	res,err := stmt.Exec(val)
	defer SqlErr(err)
	return res
}

func(m *mysql)assemble(cate string,assem map[string]string){
	for k,v := range assem{
		m.RowSql += k+" = ?,"
		m.ValSql += v+","
	}
	m.RowSql = strings.TrimRight(m.RowSql,",")
	m.ValSql = strings.TrimRight(m.ValSql,",")
	cate = strings.Replace(cate,"%TABLE%",m.option["table"],1)
}



func(m *mysql)insertAll(addAll []map[string]string)int{
	var num []int64
	for _,v := range addAll{
		num = append(num,m.insertGetId(v))
	}
	return len(num)
}

func(m *mysql)update(renew map[string]string)int64{
	m.assemble(m.updateSql,renew)
	return m.rowsAffected(m.updateSql)
}

func(m *mysql)rowsAffected(cate string)int64{//返回影响行数
	res := m.Prepare(cate,m.ValSql)
	num,err := res.RowsAffected()
	defer SqlErr(err)
	return num
}

func(m *mysql)delete(del map[string]string)int64{
	m.assemble(m.deleteSql,del)
	return m.rowsAffected(m.deleteSql)
}

func(m *mysql)query(sql string)[]map[string]string{
	
}

func(m *mysql)find()[]map[string]string{

}

func(m *mysql)findAll()[]map[string]string{

}


