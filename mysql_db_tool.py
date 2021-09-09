"""
代码贡献者：小洪
更新时间：2021/9/2
"""

from sqlalchemy import create_engine
import pymysql
import pandas as pd


class mysql_db_tool:
    def __init__(self, db_username, db_password, db_server, db_port, db_name):
        """
        :param db_username: 用户名
        :param db_password: 密码
        :param db_server: 数据库IP
        :param db_port: 数据库端口
        :param db_name: 数据库名
        """
        pymysql.install_as_MySQLdb()

        self.db_name = db_name
        self.engine = create_engine("mysql://%s:%s@%s:%s/%s?charset=utf8&use_unicode=1"
                                    % (db_username, db_password, db_server, db_port, self.db_name))

        try:
            self.engine.connect()
        except Exception as e:
            print("数据库连接失败")

    # 建表
    def create_table(self, table_name, field_info=[], primary_key=[], table_comment='None'):
        """
        :param table_name: 表名
        :param field_info: [list] 自定义字段信息
            [字段名，字段类型，是否可为空，字段注释]
            ex.
            field_info = [
                ['country', 'VARCHAR(20)', 'NOT NULL', '国家'],
                ['amount_mon', 'DECIMAL(20,6)', 'NULL', '当月同比']
            ]

        :param primary_key: [list] 自定义联合主键
            注：函数中已默认字段 UID 和 REPORT_DATE 为主键
            ex. [primary_key1, primary_key2, primary_key3]

        :param table_comment: 表的注释
        """
        delete_table_sql = 'DROP TABLE IF EXISTS %s' % table_name
        self.engine.execute(delete_table_sql)

        field_info_sql = ""
        for _i in field_info:
            field_str = "%s %s %s COMMENT '%s'," % (_i[0], _i[1], _i[2], _i[3])
            field_info_sql += field_str

        if primary_key:
            primary_key_sql = ',' + str(primary_key)[1:-1].replace("'", "")
        else:
            primary_key_sql = ''

        create_table_sql = """
            create table %s(
                UID BIGINT(20) AUTO_INCREMENT,
                REPORT_DATE VARCHAR(20) COMMENT '日期',
                %s
                HCREATETIME timestamp default CURRENT_TIMESTAMP COMMENT '创建时间', 
                HCREATE_BY VARCHAR(20) NULL COMMENT '创建人', 
                HUPDATETIME timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP COMMENT '更新时间',  
                HUPDATE_BY VARCHAR(20) NULL COMMENT '更新人', 
                HISVALID tinyint NULL COMMENT '是否有效，1为有效，0为无效',

            PRIMARY KEY(UID, REPORT_DATE %s)
            ) COMMENT '%s'
        """ % (table_name, field_info_sql, primary_key_sql, table_comment)

        self.engine.execute(create_table_sql)

    # 查询数据
    def query_db(self, table_name, field_name=None, query_sql=None):
        """
        :param table_name: 表名
        :param field_name: [list] 默认查询所有
        :param query_sql: 自定义查询语句
        :return: [DataFrame]
        """
        if field_name is None:
            field_name = '*'
        else:
            field_name = str(field_name)[1:-1].replace("'", "")

        if query_sql is None:
            query_sql = """
            SELECT %s FROM %s WHERE HISVALID = 1 
            """ % (field_name, table_name)

        df = pd.read_sql_query(query_sql, self.engine)
        return df

    # 插入数据
    def insert_db(self, df, table_name):
        """
        :param df: [dataframe] 需要插入的数据
        :param table_name: 表名
        """
        insert_sql = """
            INSERT INTO %s
            (%s)
            VALUES ({})
            ON DUPLICATE KEY UPDATE {}
        """ % (table_name, str(list(df.columns))[1:-1].replace("'", ""))

        for i in range(len(df)):
            values = ''
            update = ''
            for j in range(len(df.iloc[i])):
                if isinstance(df.iloc[i][j], str):
                    values += '\'' + str(df.iloc[i][j]) + '\','
                    update += df.columns[j] + '=\'' + str(df.iloc[i][j]) + '\','
                else:
                    values += str(df.iloc[i][j]) + ','
                    update += df.columns[j] + '=' + str(df.iloc[i][j]) + ','

            # DataFrame 空字段为 nan, 插入数据库替换为 null
            values = values[:-1].replace('nan', 'null').replace('None', 'null')
            update = update[:-1].replace('nan', 'null').replace('None', 'null')

            self.engine.execute(insert_sql.format(values, update))

    # 查询表的字段信息
    def query_db_field(self, table_name):
        """
        :param table_name: 表名
        :return: [DataFrame]  返回表的 COLUMN_NAME / COLUMN_COMMENT / DATA_TYPE
        """
        query_field_sql = """
            SELECT column_name, column_comment, data_type 
            FROM information_schema.columns 
            WHERE table_name='%s' and table_schema='%s'
        """ % (table_name, self.db_name)
        df = pd.read_sql_query(query_field_sql, self.engine)
        return df
