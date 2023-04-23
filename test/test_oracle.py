"""
Set of unit tests for handling of Apache Hive queries
"""

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
    assert ["aa"] == Parser(sql).to_tables
    assert ["bb", 'cc'] == Parser(sql).from_tables

    sql = """
        create table aa
        as 
        select * from bb a
        left join (select * from cc) b
        on a.a = b.b
        """
    assert ["aa"] == Parser(sql).to_tables
    assert ["bb", "cc"] == Parser(sql).from_tables


    sql = """
        insert into aa
        (
        a
        )
        select a.a from bb a
        left join (select * from cc) b
        on a.a = b.b
        """
    assert ["aa"] == Parser(sql).to_tables
    assert ["bb", "cc"] == Parser(sql).from_tables