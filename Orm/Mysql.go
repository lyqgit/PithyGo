package Orm

import (
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



func(m *mysql)connect(driverName string,dataSourceName string)error{
	var err error
	m.db,err = sql.Open(driverName,dataSourceName)
	return err
}

func(m *mysql)fetchSql()string{
	return m.sql
}

func(m *mysql)construct(){
	m.option = make(map[string]string)
	m.selectSql = "SELECT %FIELD% FROM %TABLE% %SUBQUERY% %FORCE% %JOIN% %WHERE% %GROUP% %HAVING% %ORDER% %LIMIT% %UNION% %LOCK% %COMMENT%"
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
				m.wherecondition = condition[2].(string)	
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

func(m *mysql)group(row string)Orm{
	m.option["group"] = " group by "+row
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
			record[columns[i]] = string(col.(string))
		}
		m.out = append(m.out,record)
		
	}
	return m.out
}

func(m *mysql)find()map[string]string{
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
	return m.query(m.selectSql)[0]
}

func(m *mysql)findAll()[]map[string]string{
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
	return m.query(m.selectSql)
}


