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


    def config(self,path):
        """
        Function to take in path of credentials json file for initialisation for the connection

        Input :

        path : String : Path of credentials json file
        """
        #setting the path variables

        self.cred_path = path
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

                    self.schemas[dataset.dataset_id] = dict()

                # getting all the tables in each dataset and appending the dataset on nested dictionary

                for dataset in self.schemas:

                    get_tables = list(self.connection.list_tables(dataset))

                    if get_tables:

                        for row in get_tables:

                            cols = self.connection.get_table(self.project_name+"."+dataset+"."+row.table_id).schema

                            columns= [ x.name for x in cols]

                            if columns:
                                self.schemas[dataset][row.table_id] = columns #getting the schemas of each table and appending as a list
                            else:
                                self.schemas[dataset][row.table_id] = list()



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

        if dataset in self.schemas:
            if tablename in self.schemas[dataset]:
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

        if dataset in self.schemas:
            if tablename in self.schemas[dataset]:
                pass
            else:
                raise ValueError("Table not found in the dataset")
        else:
            raise ValueError("Dataset not found")

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

        if dataset in self.schemas:
            if tablename in self.schemas[dataset]:
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
            raise ValueError("dataset  argument must be a string")

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


        try:
            table_id = dataset + "." + tablename
            pandas_gbq.to_gbq(data,table_id,project_id=self.project_name,if_exists=mode)

            #after successful insertion updation of current instance object

            if dataset in self.schemas:

                if tablename in self.schemas[dataset]:
                    "any mode will suffice"
                    pass

                elif mode != 'fail':
                    "recreation situation : either create a new table or append table"

                    # updating object data members
                    self.schemas[dataset][tablename] = list(data.columns)

            elif mode != 'fail':

                "recreation situation : either create a new table and dataset or append table and dataset"

                # updating object data members
                self.schemas[dataset] = dict()
                self.schemas[dataset][tablename] = list(data.columns)


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
            raise ValueError("data set  argument must be a string")

        if isinstance(tablename, str):
            pass
        else:
            raise ValueError("table name argument must be a string")


        if dataset in self.schemas:
            if tablename in self.schemas[dataset]:
                pass
            else:
                raise ValueError("Table not found in the data set")
        else:
            raise ValueError("Data set not found")



        try:
            table_id = dataset+"."+tablename

            self.connection.delete_table(table_id,not_found_ok=False)


            #removing table from object memory
            del self.schemas[dataset][tablename]  #removing the schemas of the deleted table

            return True
        except:

            raise ValueError(sys.exc_info()[1])




    def delete_dataset(self,dataset,delete_tables = False):
        """
        Function to delete a dataset and its internal tables

        Input :

        data set : String : Dataset name
        delete_tables : Boolean : Default False, if True it will delete the tables inside the dataset as well, else raise exception

        return: True if successful deletion

        """

        if isinstance(dataset, str):
            pass
        else:
            raise ValueError("dataset  argument must be a string")

        if isinstance(delete_tables,bool):
            pass
        else:
            raise ValueError("delete_tables argument must be a boolean")


        if dataset in self.schemas:
            pass

        else:
            raise ValueError("Dataset not found")


        try:
            self.connection.delete_dataset(dataset,delete_contents=delete_tables)

            #removing from object memory

            #removing nested tables first

            for tables in self.schemas[dataset]:
                del self.schemas[dataset][tables]

            #removing the dataset itself from the object memory
            del self.schemas[dataset]

            return True

        except:

            raise ValueError(sys.exc_info()[1])


    def update(self,dataset,tablename,updations,condition=None):
        """
        Input :

        dataset : String : Data set name
        tablename: String : Table name of the DB
        updations: Object : Dictionary : Format : {column:value}
        condition: String : Where condition to filter in the Table

        Output:

        returns: True if successful updation is done successfully

        """
        if isinstance(dataset, str):
            pass
        else:
            raise ValueError("data set  argument must be a string")

        if dataset in self.schemas:
            if tablename in self.schemas[dataset]:
                pass
            else:
                raise ValueError("Table not found in the dataset")
        else:
            raise ValueError("Data set not found")



        for col in updations:

            if col in self.schemas[dataset][tablename]:
                pass
            else:
                raise ValueError("Column not found in table")


        query = update_query(project=self.project_name,dataset=dataset,condition=condition,tablename=tablename,objects=updations)

        try:
            self.connection.query(query)

            return True

        except:
            raise ValueError(sys.exc_info()[1])




