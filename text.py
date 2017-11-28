# -*- coding: utf-8 -*-
import pybdrt

if __name__ == '__main__':

    user = ""
    passwd = ""
    #url = "jdbc:bdrt://192.168.100.144:1234/db1"
    url = "http://172.16.210.161:8765"
    #url = "http://192.168.253.1:1234"
    db='zl'

    connect = pybdrt.connect(user, passwd, url, db)

    cursor = connect.cursor()

    connection = cursor.connection

    print connection

    execute = cursor.execute("show databases", "true")

    print(1)
