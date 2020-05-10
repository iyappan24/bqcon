

def count_query(project,dataset,tablename,condition):
    """
    Function to process query for count process
    """

    if isinstance(tablename, str):
        pass
    else:
        raise ValueError("Tablename should be a String")

    if isinstance(dataset, str):
        pass
    else:
        raise ValueError("Tablename should be a String")


    if condition == None or isinstance(condition, str):
        pass
    else:
        raise ValueError("Condition can only be either None or String")


    final_tablename = "["+project+":"+dataset+"."+tablename+"]"


    if condition == None:

        query = "select count(*) from " + final_tablename

    else:
        # building the query
        query = "select count(*) from " + final_tablename + " where " + condition

    return query




def fetchone_query(project, dataset,columns,tablename,condition):
    """"
    Function to process the query for fetch one process
    """

    if isinstance(columns, tuple) or isinstance(columns, list) or isinstance(columns, set):
        pass
    else:
        raise ValueError("Columns argument must be tuple of String s")

    if isinstance(tablename, str):
        pass
    else:
        raise ValueError("Table name should be a String")

    if condition == None or isinstance(condition, str):
        pass
    else:
        raise ValueError("Condition can only be either None or String")


    final_tablename = "[" + project + ":" + dataset + "." + tablename + "]"


    # parsing columns into a string for quering
    if (len(columns) > 1):
        cols = ""
        for i in columns:
            cols = cols + i + ","

        cols = cols[:len(cols) - 1]
    else:
        cols = str(columns[0])

    #creating query

    if condition == None:

        query = "select " + cols + " from " + final_tablename  + " limit 1"

    else:

        query = "select " + cols + " from " + final_tablename + " where " + condition + " limit 1 "

    return   query



def fetchmany_query(project,dataset,columns,tablename,condition,rows=-1):
    """
    Function to process the query for fetch many process
    """

    if isinstance(columns, tuple) or isinstance(columns, list) or isinstance(columns, set):
        pass
    else:
        raise ValueError("Columns argument must be iterator of Strings")

    if isinstance(columns[0], str):
        pass
    else:
        raise ValueError("Column names must be a String")

    if isinstance(tablename, str):
        pass
    else:
        raise ValueError("Tablename should be a String")

    if condition == None or isinstance(condition, str):
        pass
    else:
        raise ValueError("Condition can only be either None or String")

    if isinstance(rows, int) and (rows > 1 or rows == -1):
        pass
    else:
        raise ValueError("rows can only be a integer and not less than 1")


    #generating tablename

    final_tablename = "[" + project + ":" + dataset + "." + tablename + "]"

    # generating columns

    if (len(columns) > 1):
        cols = ""
        for i in columns:
            cols = cols + i + ","

        cols = cols[:len(cols) - 1]
    else:

        cols = str(columns[0])  # breakage -- check

    # 2 different queries : with and without where clause

    if condition == None:

        if rows == -1:
            # fetch all
            query = "Select " + cols + " from " + final_tablename
        else:
            # fetch given records
            query = "Select " + cols + " from " + final_tablename + " LIMIT " + str(rows)


    else:

        if rows == -1:
            # fetch all
            query = "Select " + cols + " from " + final_tablename + " where " + condition
        else:
            # fetch given records
            query = "Select " + cols + " from " + final_tablename + " where " + condition + " LIMIT " + str(rows)


    return query




def update_query(project,dataset,tablename,objects,condition):
    """
    Function to process the query for update process
    """

    if isinstance(tablename, str):
        pass
    else:
        raise ValueError("Tablename should be a String")

    if condition == None or isinstance(condition, str):
        pass
    else:
        raise ValueError("Condition can only be either None or String")

    if isinstance(objects, dict):
        pass
    else:
        raise ValueError("Object argument must be Dictionary in format {column name : Value}")


    final_tablename = "`"+project+"."+dataset+"."+tablename+"`"

    # processing columns
    cols = ""

    for i in objects.keys():
        if isinstance(objects[i],str): #prefixing and suffixing ' for string values
            substr = str(i) + " = '" + str(objects[i]) + "',"
        else:
            substr = str(i) + " = " + str(objects[i]) + ","
        cols = cols + substr

    cols = cols[:len(cols) - 1]

    # query creation

    if condition == None:

        query = "Update " + final_tablename + " set " + cols

    else:
        query = "Update " + final_tablename + " set " + cols + " where " + condition


    return query
