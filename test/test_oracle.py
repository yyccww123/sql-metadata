"""
Set of unit tests for handling of Apache Hive queries
"""
import sqlparse

from sql_metadata.parser import Parser


def test_from_to_table():
    sql = """
    merge into aa a
    using (select * from bb a
    left join cc b
    on a.q = b.e) b
    when matched then update set a.q = b.e
    when not matched then insert a values(b.e)
    """
    # assert ["aa"] == Parser(sql).to_tables
    # assert ["bb", 'cc'] == Parser(sql).from_tables

    sql = """
        create table aa
        as 
        select * from bb a
        left join (select * from cc) b
        on a.a = b.b
        """
    # assert ["aa"] == Parser(sql).to_tables
    # assert ["bb", "cc"] == Parser(sql).from_tables


    sql = """
    WITH a AS --this_day
(SELECT a.loan_no, --借据号
a.pact_no, --合同号
a.cust_no, --客户号
a.rpt_inst AS inst_no, --报表机构
nvl(a.gs_flag, '1') AS gs_flag, --公司条线标识
SUM(loan_bal) AS loan_bal  --贷款余额
FROM l_agr_com_lon_acc_inf a
WHERE a.curr_cd = 'CNY'
AND a.busi_flag = '2'
AND a.prd_no not in ('TG10')
AND a.busi_dt = '[&"par_date"]'
GROUP BY a.loan_no,
a.pact_no,
a.cust_no,
a.rpt_inst,
nvl(a.gs_flag, '1')),
b AS --last_day
(SELECT a.loan_no,
a.pact_no,
a.cust_no,
a.rpt_inst AS inst_no,
nvl(a.gs_flag, '1') AS gs_flag,
SUM(loan_bal) AS loan_bal_ld
FROM l_agr_com_lon_acc_inf a
WHERE a.curr_cd = 'CNY'
AND a.busi_flag = '2'
AND a.prd_no not in ('TG10')
AND a.busi_dt = to_char(to_date('[&"par_date"]', 'yyyy-mm-dd') - 1, 'yyyy-mm-dd')
GROUP BY a.loan_no,
a.pact_no,
a.cust_no,
a.rpt_inst,
nvl(a.gs_flag, '1'))
SELECT nvl(a.loan_no, b.loan_no) AS loan_no,
nvl(a.pact_no, b.pact_no) AS cont_no,
nvl(a.cust_no, b.cust_no) AS cust_no,
nvl(a.inst_no, b.inst_no) AS inst_no,
nvl(a.gs_flag, b.gs_flag) AS gs_flag,
nvl(a.loan_bal, 0) AS loan_bal,
nvl(b.loan_bal_ld, 0) AS loan_bal_ld
FROM a
FULL JOIN b
ON a.loan_no = b.loan_no
WHERE nvl(a.loan_bal, 0) <> nvl(b.loan_bal_ld, 0)

        """
    # assert ["a"] == Parser(sql).to_tables
    # assert ["b"] == Parser(sql).from_tables

    stmts = sqlparse.split(sql)
    for stmt in stmts:
        stmt = sqlparse.format(stmt, use_space_around_operators=True, strip_comments=True)
        # print("stmt: ", stmt)
        print("to_tables: ", Parser(stmt).to_tables)
        print("from_tables: ", Parser(stmt).from_tables)
        print("tables: ", Parser(stmt).tables)