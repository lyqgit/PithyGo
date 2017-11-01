package Orm

import (
	"fmt"
	"database/sql"
	"strings"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

type mysql struct{
	db *sql.DB
	sql string
	selectSql string
	insertSql string
	RowSql string
	ValSql []interface{}
	whereValSql []interface{}
	insertAllSql string
	updateSql string
	whereRowSql string
	deleteSql string
	querySql string
	wherecondition string
	option map[string]string
	out []map[string]string
}



func(m *mysql)connect(driverName string,dataSourceName string)error{
	var err error
	m.db,err = sql.Open(driverName,dataSourceName)
	return err
}


func(m *mysql)construct(){
	m.option = make(map[string]string)
	m.selectSql = "SELECT %FIELD% FROM %TABLE% %SUBQUERY% %FORCE% %JOIN% %WHERE% %GROUP% %HAVING% %ORDER% %LIMIT% %UNION% %LOCK% %COMMENT%"
	m.insertSql = "INSERT INTO %TABLE% (%COLUMN%) VALUES (%ROWSQL%)"
	m.option["column"]= ""
	m.whereRowSql = ""
	m.RowSql = ""
	m.ValSql = nil
	m.whereValSql = nil
	m.updateSql = "UPDATE %TABLE% SET "
	m.deleteSql = "DELETE FROM %TABLE% "
	m.wherecondition = ""
	m.option = make(map[string]string)
	m.option["field"] = "*"
	m.option["table"] = ""
	m.option["subquery"] = ""
	m.option["force"] = ""
	m.option["join"] = ""
	m.option["where"] = ""
	m.option["group"] = ""
	m.option["have"] = ""
	m.option["order"] = ""
	m.option["limit"] = ""
	m.option["union"] = ""
	m.option["lock"] = ""
	m.option["comment"] = ""
	m.querySql = ""
}

func(m *mysql)Table(database string)Db{
	m.construct()
	m.option["table"] = database
	return m
}

func(m *mysql)Alias(as string)Db{
	m.option["alias"] = as
	return m
}

func(m *mysql)Field(row string)Db{
	m.option["field"] = row
	return m
}

func(m *mysql)Where(condition ...interface{})Db{
	switch t := condition[0].(type){
		case map[string]string:
			if m.option["where"] == " where "{
				for k,v := range t{
					
					m.option["where"] += k+" = "+v+" and "
					m.whereRowSql += k+" = ? and "
					m.whereValSql = append(m.whereValSql,v)
					m.wherecondition = ""
				}
			}else{
				m.option["where"] = " where "
				m.whereRowSql = " where "
				for k,v := range t{
					
					m.option["where"] += k+" = "
					m.whereRowSql += k+" = ? and "
					m.whereValSql = append(m.whereValSql,v)
					m.wherecondition = ""
				}
			}
			m.whereRowSql = strings.TrimRight(m.whereRowSql,"and ")
		break
		case string:
			if len(condition) < 3{
				if m.option["where"] == ""{
					m.option["where"] = " where "
					m.option["where"] += t+" = "+condition[1].(string)
					m.whereRowSql = " where "
					m.whereRowSql += t+" = ?"
					m.ValSql = append(m.ValSql,condition[1].(string))
					m.wherecondition = ""
				}else{
					if m.wherecondition == ""{
						m.option["where"] += " and "+t+" = "+condition[1].(string)
						m.whereRowSql += " and "+t+" = ?"
						m.ValSql = append(m.ValSql,condition[1].(string))
					}else{
						m.option["where"] += " "+m.wherecondition+" "+t+" = "+condition[1].(string)
						m.whereRowSql += " "+m.wherecondition+" "+t+" = ?"
						m.ValSql = append(m.ValSql,condition[1].(string))
					}
					
					m.wherecondition = ""
				}
				
			}else{
				if m.option["where"] == ""{
					m.option["where"] = " where "
					m.option["where"] += t+" = "+condition[1].(string)+" "+condition[2].(string)
					m.whereRowSql = " where "
					m.whereRowSql += t+" = ? "+condition[2].(string)
					m.ValSql = append(m.ValSql,condition[1].(string))
				}else{
					if m.wherecondition == ""{
						m.option["where"] += condition[2].(string)+" "+t+" = "+condition[1].(string)
						m.whereRowSql += condition[2].(string)+" "+t+" = ? "
						m.ValSql = append(m.ValSql,condition[1].(string))
					}else{
						m.option["where"] += m.wherecondition+" "+t+" = "+condition[1].(string)+" "+condition[2].(string)
						m.whereRowSql += m.wherecondition+" "+t+" = ? "+condition[2].(string)
						m.ValSql = append(m.ValSql,condition[1].(string))
					}
				}
				m.wherecondition = condition[2].(string)	
			}
			
		break
	}
	return m
}

func(m *mysql)Limit(page int,num int)Db{
	m.option["limit"] = "limit "+strconv.Itoa(page)+","+strconv.Itoa(num)
	return m
}

func(m *mysql)Order(row string,sort string)Db{
	m.option["order"] = "order by "+row+" "+sort
	return m
}

func(m *mysql)Join(condition ...string)Db{
	if condition[2] == ""{
		m.option["join"] = "left join "+condition[0]+" on "+condition[1]
	}
	return m
}

func(m *mysql)Group(row string)Db{
	m.option["group"] = " group by "+row
	return m
}

func(m *mysql)Have(condition string)Db{
	m.option["have"] = "having "+condition
	return m
}

func(m *mysql)Insert(add map[string]string)int64{
	m.assemble("insert",add)
	return m.rowsAffected(m.insertSql)
}

func(m *mysql)InsertGetId(add map[string]string)int64{
	m.assemble("insert",add)
	res := m.Prepare(m.insertSql)
	num,err := res.LastInsertId()
	defer SqlErr(err)
	m.ValSql = nil
	return num
}

func(m *mysql)Prepare(sql string)sql.Result{
	stmt,err := m.db.Prepare(sql)
	defer SqlErr(err)
	res,err := stmt.Exec(m.ValSql...)
	defer SqlErr(err)
	return res
}

func(m *mysql)assemble(cate string,assem map[string]string){
	for k,v := range assem{
		m.option["column"] += k+","
		m.RowSql += k+"=?,"
		m.ValSql = append(m.ValSql,v)
	}
	m.option["column"] = strings.TrimRight(m.option["column"],",")
	m.RowSql = strings.TrimRight(m.RowSql,",")
	switch cate{
		case "insert":
			m.insertSql = strings.Replace(m.insertSql,"%TABLE%",m.option["table"],1)
			m.insertSql = strings.Replace(m.insertSql,"%COLUMN%",m.option["column"],1)
			m.insertSql = strings.Replace(m.insertSql,"%ROWSQL%",m.RowSql,1)
		break
		case "update":
			m.updateSql = strings.Replace(m.updateSql,"%TABLE%",m.option["table"],1)
			m.ValSql = append(m.ValSql,m.whereValSql...)
		break
	}
}



func(m *mysql)InsertAll(addAll []map[string]string)int{
	var num []int64
	for _,v := range addAll{
		num = append(num,m.InsertGetId(v))
	}
	return len(num)
}

func(m *mysql)Update(renew map[string]string)int64{
	m.assemble("update",renew)
	fmt.Println(m.updateSql+m.RowSql+m.whereRowSql)
	return m.rowsAffected(m.updateSql+m.RowSql+m.whereRowSql)
}

func(m *mysql)rowsAffected(cate string)int64{//返回影响行数
	res := m.Prepare(cate)
	num,err := res.RowsAffected()
	defer SqlErr(err)
	return num
}

func(m *mysql)Delete()int64{
	m.deleteSql = strings.Replace(m.deleteSql,"%TABLE%",m.option["table"],1)
	stmt,err := m.db.Prepare(m.deleteSql+m.whereRowSql)
	defer SqlErr(err)
	res,err := stmt.Exec(m.whereValSql...)
	defer SqlErr(err)
	num,err := res.RowsAffected()
	defer SqlErr(err)
	return num
}

func(m *mysql)Query(sql string)[]map[string]string{
	
	m.querySql = sql
	rows,err := m.db.Query(m.querySql)
	defer SqlErr(err)
	columns,_ := rows.Columns()
	scanArgs := make([]interface{},len(columns))
	values := make([]interface{},len(columns))

	for i := range values{
		scanArgs[i] = &values[i]
	}

	for rows.Next(){
		record := make(map[string]string)
		rows.Scan(scanArgs...)
		for i,col := range values{
			record[columns[i]] = string(col.([]byte))
		}
		m.out = append(m.out,record)
		
	}
	return m.out
}

func(m *mysql)Find()map[string]string{
	m.option["limit"] = "limit 1"
	m.selectSql = strings.Replace(m.selectSql,"%TABLE%",m.option["table"],1)
	m.selectSql = strings.Replace(m.selectSql,"%FIELD%",m.option["field"],1)
	m.selectSql = strings.Replace(m.selectSql,"%SUBQUERY%",m.option["subquery"],1)
	m.selectSql = strings.Replace(m.selectSql,"%FORCE%",m.option["force"],1)
	m.selectSql = strings.Replace(m.selectSql,"%JOIN%",m.option["join"],1)
	m.selectSql = strings.Replace(m.selectSql,"%WHERE%",m.option["where"],1)
	m.selectSql = strings.Replace(m.selectSql,"%GROUP%",m.option["group"],1)
	m.selectSql = strings.Replace(m.selectSql,"%HAVING%",m.option["have"],1)
	m.selectSql = strings.Replace(m.selectSql,"%ORDER%",m.option["order"],1)
	m.selectSql = strings.Replace(m.selectSql,"%LIMIT%",m.option["limit"],1)
	m.selectSql = strings.Replace(m.selectSql,"%UNION%",m.option["union"],1)
	m.selectSql = strings.Replace(m.selectSql,"%LOCK%",m.option["lock"],1)
	m.selectSql = strings.Replace(m.selectSql,"%COMMENT%",m.option["comment"],1)
	m.sql = m.selectSql
	
	return m.Query(m.selectSql)[0]
}

func(m *mysql)Select()[]map[string]string{
	m.selectSql = strings.Replace(m.selectSql,"%TABLE%",m.option["table"],1)
	m.selectSql = strings.Replace(m.selectSql,"%FIELD%",m.option["field"],1)
	m.selectSql = strings.Replace(m.selectSql,"%SUBQUERY%",m.option["subquery"],1)
	m.selectSql = strings.Replace(m.selectSql,"%FORCE%",m.option["force"],1)
	m.selectSql = strings.Replace(m.selectSql,"%JOIN%",m.option["join"],1)
	m.selectSql = strings.Replace(m.selectSql,"%WHERE%",m.option["where"],1)
	m.selectSql = strings.Replace(m.selectSql,"%GROUP%",m.option["group"],1)
	m.selectSql = strings.Replace(m.selectSql,"%HAVING%",m.option["have"],1)
	m.selectSql = strings.Replace(m.selectSql,"%ORDER%",m.option["order"],1)
	m.selectSql = strings.Replace(m.selectSql,"%LIMIT%",m.option["limit"],1)
	m.selectSql = strings.Replace(m.selectSql,"%UNION%",m.option["union"],1)
	m.selectSql = strings.Replace(m.selectSql,"%LOCK%",m.option["lock"],1)
	m.selectSql = strings.Replace(m.selectSql,"%COMMENT%",m.option["comment"],1)
	m.sql = m.selectSql
	
	return m.Query(m.selectSql)
}


