# mysql_db_tool
 **封装mysql常用操作**



**支持功能：**

- 创建数据表：create_table(self, table_name, field_info=[], primary_key=[], table_comment='None')
- 查询表的数据：query_db(self, table_name, field_name=None, query_sql=None)
- 插入数据：insert_db(self, df, table_name)
- 查询表的字段信息：query_db_field(self, table_name)

