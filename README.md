# PithyGo
简便的go框架
=====

Orm库

使用Connect(driverName,driverSource)//方法创建连接

Table(database string)//传入表名称

Where()//查询条件：第一个参数为map[字段string]值string时不再接收以后的参数，当第一个参数为string时，第一个参数为字段，第二个为值，第三个为连接的符号（and 、 or）
