package Orm

import (
	"fmt"
	"database/sql"
	"strings"
	_ "code.google.com/p/odbc"
	"strconv"
	"time"
)

type sqlserver struct{
	db *sql.DB
	sql string
	selectSql string
	selectPageSql string
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


func(m *sqlserver)connect(driverName string,dataSourceName string)error{
	var err error
	m.db,err = sql.Open("odbc",dataSourceName)
	return err
}



func(m *sqlserver)construct(){
	m.option = make(map[string]string)
	m.selectSql = "SELECT %LIMIT% %FIELD% FROM %TABLE% %SUBQUERY% %FORCE% %JOIN% %WHERE% %GROUP% %HAVING% %ORDER%  %UNION% %LOCK% %COMMENT%"
	m.selectPageSql = "SELECT pithy.* from (SELECT %FIELD%,ROW_NUMBER() OVER (ORDER BY rand()) as 'row_num' from [dbo].[Account] %JOIN% %GROUP%)pithy %WHERE% %LIMIT% "
	m.insertSql = "INSERT INTO %TABLE% (%COLUMN%) VALUES (%ROWSQL%)"
	m.option["column"]= ""
	m.RowSql = ""
	m.whereRowSql = ""
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

func(m *sqlserver)Table(database string)Db{
	m.construct()
	m.option["table"] = database
	return m
}

func(m *sqlserver)Alias(as string)Db{
	m.option["alias"] = as
	return m
}

func(m *sqlserver)Field(row string)Db{
	m.option["field"] = row
	return m
}

func(m *sqlserver)Where(condition ...interface{})Db{
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

func(m *sqlserver)Limit(page int,num int)Db{
	if m.option["where"] == ""{
		m.option["limit"] = "where row_num between "+strconv.Itoa(page*num+1)+" and "+strconv.Itoa((page+1)*num)
	}else{
		m.option["limit"] = "and row_num between "+strconv.Itoa(page*num+1)+" and "+strconv.Itoa((page+1)*num)
	}
	
	return m
}

func(m *sqlserver)Order(row string,sort string)Db{
	m.option["order"] = "order by "+row+" "+sort
	return m
}

func(m *sqlserver)Join(condition ...string)Db{
	if condition[2] == ""{
		m.option["join"] = "left join "+condition[0]+" on "+condition[1]
	}else{
		m.option["join"] = " "+condition[2]+" join "+condition[0]+" on "+condition[1]
	}
	return m
}

func(m *sqlserver)Group(row string)Db{
	m.option["group"] = " group by "+row
	return m
}

func(m *sqlserver)Have(condition string)Db{
	m.option["have"] = "having "+condition
	return m
}

func(m *sqlserver)Insert(add map[string]string)int64{
	m.assemble("insert",add)
	return m.rowsAffected(m.insertSql+m.RowSql)
}

func(m *sqlserver)InsertGetId(add map[string]string)int64{
	m.assemble("insert",add)
	res := m.Prepare(m.insertSql)
	num,err := res.LastInsertId()
	defer SqlErr(err)
	m.ValSql = nil
	return num
}

func(m *sqlserver)Prepare(sql string)sql.Result{
	stmt,err := m.db.Prepare(sql)
	defer SqlErr(err)
	res,err := stmt.Exec(m.ValSql...)
	defer SqlErr(err)
	return res
}

func(m *sqlserver)assemble(cate string,assem map[string]string){
	for k,v := range assem{
		m.option["column"] += k+","
		m.RowSql += k+" = ?,"
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
		break
		case "delete":
			m.deleteSql = strings.Replace(m.deleteSql,"%TABLE%",m.option["table"],1)
		break
	}
}



func(m *sqlserver)InsertAll(addAll []map[string]string)int{
	var num []int64
	for _,v := range addAll{
		num = append(num,m.InsertGetId(v))
	}
	return len(num)
}

func(m *sqlserver)Update(renew map[string]string)int64{
	m.assemble("update",renew)
	return m.rowsAffected(m.updateSql+m.RowSql+m.whereRowSql)
}

func(m *sqlserver)rowsAffected(cate string)int64{//返回影响行数
	res := m.Prepare(cate)
	num,err := res.RowsAffected()
	defer SqlErr(err)
	return num
}

func(m *sqlserver)Delete()int64{
	m.deleteSql = strings.Replace(m.deleteSql,"%TABLE%",m.option["table"],1)
	stmt,err := m.db.Prepare(m.deleteSql+m.whereRowSql)
	defer SqlErr(err)
	res,err := stmt.Exec(m.whereValSql...)
	defer SqlErr(err)
	num,err := res.RowsAffected()
	defer SqlErr(err)
	return num
}

func(m *sqlserver)Query(sql string)[]map[string]string{
	
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
			switch col.(type){
				case string:
					record[columns[i]] = col.(string)
				break
				case int64:
					record[columns[i]] = strconv.FormatInt(col.(int64),10)
				break
				case int32:
					record[columns[i]] = strconv.FormatInt(int64(col.(int32)),10)
				break
				case float64:
					record[columns[i]] = strconv.FormatFloat(col.(float64),'f',-1,64)
				break
				case time.Time:
					record[columns[i]] = col.(time.Time).String()[:19]
				break
				case bool:
					record[columns[i]] = strconv.FormatBool(col.(bool))
				break
				case nil:
					record[columns[i]] = "null"
				break
				default:
					record[columns[i]] = string(col.([]byte))
				break

			}
		}
		m.out = append(m.out,record)
		
	}
	return m.out
}

func(m *sqlserver)Find()map[string]string{
	m.option["limit"] = "between 0 and 1"
	m.selectPageSql = strings.Replace(m.selectPageSql,"%TABLE%",m.option["table"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%FIELD%",m.option["field"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%SUBQUERY%",m.option["subquery"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%FORCE%",m.option["force"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%JOIN%",m.option["join"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%WHERE%",m.option["where"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%GROUP%",m.option["group"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%HAVING%",m.option["have"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%ORDER%",m.option["order"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%LIMIT%",m.option["limit"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%UNION%",m.option["union"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%LOCK%",m.option["lock"],1)
	m.selectPageSql = strings.Replace(m.selectPageSql,"%COMMENT%",m.option["comment"],1)
	return m.Query(m.selectPageSql)[0]
}

func(m *sqlserver)Select()[]map[string]string{
	if m.option["limit"] == ""{
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
	}else{
		m.selectPageSql = strings.Replace(m.selectPageSql,"%TABLE%",m.option["table"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%FIELD%",m.option["field"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%SUBQUERY%",m.option["subquery"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%FORCE%",m.option["force"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%JOIN%",m.option["join"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%WHERE%",m.option["where"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%GROUP%",m.option["group"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%HAVING%",m.option["have"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%ORDER%",m.option["order"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%LIMIT%",m.option["limit"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%UNION%",m.option["union"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%LOCK%",m.option["lock"],1)
		m.selectPageSql = strings.Replace(m.selectPageSql,"%COMMENT%",m.option["comment"],1)
		m.sql = m.selectPageSql
	}
	fmt.Println(m.sql)
	return m.Query(m.sql)
}


