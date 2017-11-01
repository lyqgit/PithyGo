package Orm

import (
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
	ValSql string
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

func(m *sqlserver)fetchSql()string{
	return m.sql
}

func(m *sqlserver)construct(){
	m.option = make(map[string]string)
	m.selectSql = "SELECT %LIMIT% %FIELD% FROM %TABLE% %SUBQUERY% %FORCE% %JOIN% %WHERE% %GROUP% %HAVING% %ORDER%  %UNION% %LOCK% %COMMENT%"
	m.selectPageSql = "SELECT pithy.* from (SELECT %FIELD%,ROW_NUMBER() OVER (ORDER BY rand()) as 'row_num' from [dbo].[Account] %JOIN% %GROUP%)pithy %WHERE% %LIMIT% "
	m.insertSql = "INSERT %TABLE% SET "+m.RowSql
	m.RowSql = ""
	m.whereRowSql = ""
	m.ValSql = ""
	m.updateSql = "UPDATE %TABLE% SET "+m.RowSql+m.whereRowSql
	m.deleteSql = "DELETE FROM %TABLE% "+m.whereRowSql
	m.wherecondition = ""
	m.option = make(map[string]string)
	m.option["field"] = ""
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

func(m *sqlserver)table(database string)Orm{
	m.construct()
	m.option["table"] = database
	return m
}

func(m *sqlserver)alias(as string)Orm{
	m.option["alias"] = as
	return m
}

func(m *sqlserver)field(row string)Orm{
	m.option["field"] = row
	return m
}

func(m *sqlserver)where(condition ...interface{})Orm{
	switch t := condition[0].(type){
		case map[string]map[string]string:
			for k,v := range t{
				m.option["where"] = "where "
				m.option["where"] += k+" = "
				m.whereRowSql += k+" = ?"
				for kk,vv := range v{
					if vv == ""{
						m.option["where"] += kk+" and "
						m.whereRowSql += " and "
					}else{
						m.option["where"] += kk+" "+vv+" "
						m.whereRowSql += " "+vv+" "
					}
					m.ValSql += kk+","
				}
			}
		break
		case string:
			if condition[2].(string) == ""{
				if m.option["where"] == ""{
					m.option["where"] = "where "
					m.option["where"] += t+" = "+condition[1].(string)
					m.whereRowSql = "where "
					m.whereRowSql += t+" = ?"
					m.ValSql += condition[1].(string)+","
					m.wherecondition = ""
				}else{
					if m.wherecondition == ""{
						m.option["where"] += " and "+t+" = "+condition[1].(string)
						m.whereRowSql += " and "+t+" = ?"
						m.ValSql += condition[1].(string)+","
					}else{
						m.option["where"] += " "+m.wherecondition+" "+t+" = "+condition[1].(string)
						m.whereRowSql += " "+m.wherecondition+" "+t+" = ?"
						m.ValSql += condition[1].(string)+","
					}
					
					m.wherecondition = ""
				}
				
			}else{
				m.wherecondition = condition[2].(string)
				if m.option["where"] == ""{
					m.option["where"] = "where "
					m.option["where"] += t+" = "+condition[1].(string)+" "+condition[2].(string)
					m.whereRowSql = "where "
					m.whereRowSql += t+" = ? "+condition[2].(string)
					m.ValSql += condition[1].(string)+","
				}else{
					if m.wherecondition == ""{
						m.option["where"] += condition[2].(string)+" "+t+" = "+condition[1].(string)
						m.whereRowSql += condition[2].(string)+" "+t+" = ? "
						m.ValSql += condition[1].(string)+","
					}else{
						m.option["where"] += m.wherecondition+" "+t+" = "+condition[1].(string)+" "+condition[2].(string)
						m.whereRowSql += m.wherecondition+" "+t+" = ? "+condition[2].(string)
						m.ValSql += condition[1].(string)+","
					}
				}	
			}
			
		break
	}
	return m
}

func(m *sqlserver)limit(page int,num int)Orm{
	if m.option["where"] == ""{
		m.option["limit"] = "where row_num between "+strconv.Itoa(page*num+1)+" and "+strconv.Itoa((page+1)*num)
	}else{
		m.option["limit"] = "and row_num between "+strconv.Itoa(page*num+1)+" and "+strconv.Itoa((page+1)*num)
	}
	
	return m
}

func(m *sqlserver)order(row string,sort string)Orm{
	m.option["order"] = "order by "+row+" "+sort
	return m
}

func(m *sqlserver)join(condition ...string)Orm{
	if condition[2] == ""{
		m.option["join"] = "left join "+condition[0]+" on "+condition[1]
	}else{
		m.option["join"] = " "+condition[2]+" join "+condition[0]+" on "+condition[1]
	}
	return m
}

func(m *sqlserver)group(row string)Orm{
	m.option["group"] = " group by "+row
	return m
}

func(m *sqlserver)insert(add map[string]string)int64{
	m.assemble(m.insertSql,add)
	return m.rowsAffected(m.insertSql)
}

func(m *sqlserver)insertGetId(add map[string]string)int64{
	m.assemble(m.insertSql,add)
	res := m.Prepare(m.insertSql,m.ValSql)
	num,err := res.LastInsertId()
	defer SqlErr(err)
	return num
}

func(m *sqlserver)Prepare(sql string,val string)sql.Result{
	stmt,err := m.db.Prepare(sql)
	defer SqlErr(err)
	res,err := stmt.Exec(val)
	defer SqlErr(err)
	return res
}

func(m *sqlserver)assemble(cate string,assem map[string]string){
	for k,v := range assem{
		m.RowSql += k+" = ?,"
		m.ValSql += v+","
	}
	m.RowSql = strings.TrimRight(m.RowSql,",")
	m.ValSql = strings.TrimRight(m.ValSql,",")
	cate = strings.Replace(cate,"%TABLE%",m.option["table"],1)
}



func(m *sqlserver)insertAll(addAll []map[string]string)int{
	var num []int64
	for _,v := range addAll{
		num = append(num,m.insertGetId(v))
	}
	return len(num)
}

func(m *sqlserver)update(renew map[string]string)int64{
	m.assemble(m.updateSql,renew)
	return m.rowsAffected(m.updateSql)
}

func(m *sqlserver)rowsAffected(cate string)int64{//返回影响行数
	res := m.Prepare(cate,m.ValSql)
	num,err := res.RowsAffected()
	defer SqlErr(err)
	return num
}

func(m *sqlserver)delete(del map[string]string)int64{
	m.assemble(m.deleteSql,del)
	return m.rowsAffected(m.deleteSql)
}

func(m *sqlserver)query(sql string)[]map[string]string{
	
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

func(m *sqlserver)find()map[string]string{
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
	return m.query(m.selectPageSql)[0]
}

func(m *sqlserver)findAll()[]map[string]string{
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
	}
	
	return m.query(m.selectSql)
}


