import pandas as pd
from google.cloud import bigquery
import sys
import os
import pandas_gbq

from .query_processor import  count_query,fetchmany_query,fetchone_query,update_query


class Bqsdk :

    def __int__(self):
        """
        Empty constructor
        """


    def setConnect(self,path):
        """
        Function to take in path of credentials json file for initialisation for the connection

        Input :

        path : String : Path of credentials json file

        """
        #setting the path variables

        self.cred_path = path
        self.datasets = list()
        self.tables = dict()
        self.connection = ""
        self.project_name = ""
        self.schemas = dict()

        #-------------------------------------------------------------------------------------------------------------------------------------

        if os.path.isfile(path):
            """
            if the file is present 
            """

            #setting the environment variable
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path

        else:

            raise ValueError("The file does not exist")


        #creating the connection and getting the details

        try :

            #intialising the connection
            self.connection = bigquery.Client()

            self.project_name = self.connection.project

            datasets = list(self.connection.list_datasets())



            #getting the datatsets

            if len(datasets) >0:

                #appending the list of all the datasets
                for dataset in datasets:

                    self.datasets.append(dataset.dataset_id)

                # getting all the tables in each dataset

                for value in self.datasets:

                    table_names = []

                    get_tables = list(self.connection.list_tables(value))

                    if get_tables:

                        for row in get_tables:

                            table_names.append(row.table_id.lower())

                        self.tables[value] = table_names

                    else:

                        self.tables[value] = list()


                #getting the column names of each table
                for dataset in self.datasets:

                    for table in self.tables[dataset]:

                        schemas = self.connection.get_table(self.project_name+"."+dataset+"."+table).schema

                        columns = [x.name for x in schemas]

                        self.schemas[table] = columns

            else:

                print("""No data-sets available. Create a dataset and tables for the best usage of the SDK \n""")

        except:

            raise  ValueError(sys.exc_info()[1])



    def count(self,dataset,tablename,condition=None):

        """
        Function to return the count for the given condition

        Input :

        dataset : String : Dataset name
        tablename : String : table name under the dataset
        condition: String : big query where clause

        """

        if dataset in self.datasets:
            if tablename in self.tables[dataset]:
                pass
            else:
                raise ValueError("Table not found in the dataset")
        else:
            raise  ValueError("Dataset not found")


        query = count_query(self.project_name,dataset,tablename,condition)

        try:
            count = pd.read_gbq(query, progress_bar_type='tqdm').iloc[0][0]
        except:
            raise ValueError(sys.exc_info()[1])

        return count



    def fetchone(self,dataset,tablename,columns,condition= None):
        """
        Function to return one record for the given condition

        Input:

        dataset: String : dataset name
        tablename: String : tablename
        columns : Iterator of Strings list or tuple or set of Strings (columns names) in the table you want to view
        condition: String : big query where clause

        returns Dataframe
        """

        if dataset in self.datasets:
            if tablename in self.tables[dataset]:
                pass
            else:
                raise ValueError("Table not found in the dataset")
        else:
            raise  ValueError("Dataset not found")


        query = fetchone_query(project=self.project_name,dataset=dataset,tablename=tablename,condition=condition,columns=columns)

        try:
            df = pd.read_gbq(query, progress_bar_type='tqdm')
        except:
            raise ValueError(sys.exc_info()[1])


        return df





    def fetchmany(self,dataset,tablename,columns,condition=None, rows = -1):
        """
        Function to return the  record for the given condition

        Input:

        dataset: String : dataset name
        tablename: String : tablename
        condition: String : big query where clause
        rows : Integer : -1 (Fetch all records) else specifies the given records

        returns Dataframe
        """

        if dataset in self.datasets:
            if tablename in self.tables[dataset]:
                pass
            else:
                raise ValueError("Table not found in the dataset")
        else:
            raise ValueError("Dataset not found")

        query = fetchmany_query(project=self.project_name,dataset=dataset,tablename=tablename,condition=condition,columns=columns,rows=rows)

        try:
            df = pd.read_gbq(query, progress_bar_type='tqdm')
        except:
            raise ValueError(sys.exc_info()[1])

        return df




    def customquery(self,query):
        """
        Function to input custom query to fetch data from GBQ

        Input

        query: String : google big query

        return: Dataframe
        """

        if isinstance(query, str):
            pass
        else:
            raise ValueError("query must be a string ")

        try:
            df = pd.read_gbq(query, progress_bar_type='tqdm')
        except:
            raise ValueError(sys.exc_info()[1])

        return df




    def insert(self,data,dataset,tablename,mode = 'append'):
        """
        Function to insert data into the Google Big-query table

        Input:

        data : Dataframe : data to insert in the table with same column names and attribute types
        dataset : String : Dataset name
        tablename : String : table name
        mode : String :  'append' adds rows into the table, 'replace' recreates the table, 'fail' raise exception if the table is existing

        return: True if successful insertion
        """

        if isinstance(data, pd.DataFrame):
            pass
        else:
            raise ValueError("data must be dataframe object")

        if isinstance(dataset,str):
            pass
        else:
            raise ValueError("dataset  argumnent must be a string")

        if isinstance(tablename,str):
            pass
        else:
            raise  ValueError("tablename arugment must be a string")

        if isinstance(mode,str):

            if mode in ['append','fail','replace']:
                pass
            else:
                raise ValueError("invalid mode type")
        else:
            raise  ValueError("Mode argument must  be a string ")





        if dataset in self.datasets:

            if tablename in self.tables[dataset]:
                "any mode will suffice"
                pass

            elif mode != 'fail':
                "recreation situation : either create a new table or append table"

                #updating object data members
                self.tables[dataset] = list()
                self.tables[dataset].append(tablename) #new table will be added in bigquery
                self.schemas[tablename] = list(data.columns) #new columns that are being added in the big query

        elif mode!='fail':

            "recreation situation : either create a new table and dataset or append table and dataset"

            #updating object data members
            self.tables[dataset] = list()
            self.tables[dataset].append(tablename)  # new table will be added in bigquery
            self.schemas[tablename] = list(data.columns)  # new columns that are being added in the big query
            self.datasets.append(dataset)




        try:
            table_id = dataset + "." + tablename
            pandas_gbq.to_gbq(data,table_id,project_id=self.project_name,if_exists=mode)

            return True
        except:

            raise ValueError(sys.exc_info()[1])



    def delete_table(self,dataset,tablename):
        """
        Function to delete a table


        return: True if successful deletion
        """
        if isinstance(dataset, str):
            pass
        else:
            raise ValueError("dataset  argumnent must be a string")

        if isinstance(tablename, str):
            pass
        else:
            raise ValueError("tablename arugment must be a string")


        if dataset in self.datasets:
            if tablename in self.tables[dataset]:
                pass
            else:
                raise ValueError("Table not found in the dataset")
        else:
            raise ValueError("Dataset not found")



        try:
            table_id = dataset+"."+tablename
            self.connection.delete_table(table_id,not_found_ok=False)


            self.tables[dataset].remove(tablename) #removing from the object memory
            del self.schemas[tablename] #removing the schemas of the deleted table

            return True
        except:

            raise ValueError(sys.exc_info()[1])




    def delete_dataset(self,dataset):
        """
        Function to delete a dataset and its internal tables

        Input :

        dataset : String : Dataset name

        return: True if successful deletion

        """

        if isinstance(dataset, str):
            pass
        else:
            raise ValueError("dataset  argumnent must be a string")


        if dataset in self.datasets:
            pass

        else:
            raise ValueError("Dataset not found")


        try:
            self.connection.delete_dataset(dataset,delete_contents=True)

            self.datasets.remove(dataset) #removing from object moemory

            for table in self.tables[dataset]:
                del self.schemas[table] #removing all the nested table schemas

            del self.tables[dataset] #removing the nested tables inside the datset from the object memory

            return True

        except:

            raise ValueError(sys.exc_info()[1])


    def update(self,dataset,tablename,updations,condition=None):
        """
        Input :

        dataset : String : Dataset name
        tablename: String : Tablename of the DB
        updations: Object : Dictionary : Format : {column:value}
        condition: String : Where condition to filter in the Table

        Output:

        returns: True if successful updation is done successfully

        """
        if isinstance(dataset, str):
            pass
        else:
            raise ValueError("dataset  argument must be a string")


        if dataset in self.datasets:
            if tablename in self.tables[dataset]:
                pass
            else:
                raise ValueError("Table not found in the dataset")
        else:
            raise ValueError("Dataset not found")


        for col in updations:

            if col in self.schemas[tablename]:
                pass
            else:
                raise ValueError("Column not found in table")


        query = update_query(project=self.project_name,dataset=dataset,condition=condition,tablename=tablename,objects=updations)

        try:
            self.connection.query(query)

            return True

        except:
            raise ValueError(sys.exc_info()[1])




