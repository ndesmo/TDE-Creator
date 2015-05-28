import dataextract as tde
import ConfigParser
import logging
import pyodbc
import shutil
import time
import csv
import sys
import os
import re

import multiprocessing
from pyodbc import OperationalError
from optparse import OptionParser
from math import floor
from dataextract.Exceptions import TableauException

class NoCredentialsError(Exception):
    pass

class TDEFileWriteError(Exception):
    pass

class CSVFileWriteError(Exception):
    pass

def f(arg):
    b = BuildTDE(arg)
    b.run()
    return 0

class BuildTDE():
    
    
    """ BuildTDE class
    
        This script builds the Tableau Data Extract from an Output table in SQL Server.
        
        To activate help, run "python buildTDE.py -h".
        
        The following details are required by the user:
            - Table name
            - Database name
            - Server name ( can include Port at the end, separated by comma )
            - Port ( not needed if included in Server name )
        
        Users can specify connection details in either the "buildTDE.ini" file, or
        in the command line using the relevant arguments described in the help.
        
        To run the script, use:
        
        "python buildTDE.py username/password [+ optional extra -arguments]"
        
        Note: if the user supplies connection details on the command line, these
        override any settings included in the "buildTDE.ini" file.
        
        This class can take a dictionary of options as an input, so can be imported
        and initiated from within a Python script.
    
    """
    
    def __init__(self, args = {}):
        
        self.args = args
        
        self.setup_config()
        self.parse_arguments()
        self.setup_logging()
    
    def run(self):
        
        self.connect()
        
        if self.resume_csv:
            self.combine_TDE(True)
        
        # Process one part
        elif self.parts == 1 or ( self.parts > 1 and self.part_no ):
            if not self.csv:
                self.setup_TDE()
                self.set_table_layout()
                self.build_TDE()
            else:
                self.setup_csv()
                self.build_csv()
        
        # Parallel processing, one instance per part
        else:
            p = multiprocessing.Pool(self.parts)
            args = []
            for i in range(self.parts):
                a = dict((x,y) for x,y in self.args.iteritems())
                a['part_no'] = i+1
                a['csv'] = True
                args.append(a)
                del a

            logging.info('Running %d parts in parallel... Check log files of individual parts for updates' % (self.parts))
            p.map(f,args)
            logging.info('Complete!')
            if self.csv:
                self.combine_csv()
            else:
                self.combine_TDE()
            
        
    def setup_config(self):
    
        """ Get defaults from the config file if they are there """
        
        if not os.path.exists('buildTDE.ini'):
            return
        
        config = ConfigParser.ConfigParser()
        config.read('buildTDE.ini')
        
        self.server = None; self.port = None ; self.database = None ; self.table_name = None
        
        try:
            self.server = config.get('Settings', 'Server') #Staging server
        except ConfigParser.NoOptionError:
            pass
        
        try:
            self.database = config.get('Settings', 'Database')
        except ConfigParser.NoOptionError:
            pass
        
        try:
            self.port = config.get('Settings', 'Port')
        except ConfigParser.NoOptionError:
            try:
                if self.server: self.port = self.server.split(',')[1]
            except IndexError:
                pass
            if self.server: self.server = self.server.split(',')[0]
        
        try:
            self.table_name = config.get('Settings', 'Table_name')
        except ConfigParser.NoOptionError:
            pass
    
    def parse_arguments(self):
        
        """ Get any remaining details from command line """
        
        # Get any remaining details from the command line
        parser = OptionParser()
        parser.add_option('-c', '--credentials', action='store', dest='credentials', help='User/Password credentials')
        parser.add_option('-u', '--user', action='store', dest='user', help='Username')
        parser.add_option('-p', '--password', action='store', dest='password', help='Password')
        parser.add_option('-t', '--table', action='store', dest='table_name', help='Name of table to extract from')
        parser.add_option('-m', '--schema', action='store', dest='schema', default='dbo', help='Name of schema to use (optional)')
        parser.add_option('-d', '--database', action='store', dest='database', help='Name of database to use')
        parser.add_option('-s', '--server', action='store', dest='server', help='Name of server to use (can include port number with comma separator')
        parser.add_option('-o', '--port', action='store', dest='port', help='Port number of server')
        parser.add_option('-n', '--nonverbose', action='store_true', dest='nonverbose', default=False, help='Disable printing of regular updates of progress')
        parser.add_option('-b', '--batch_size', action='store', dest='batch_size', default='10000', help='Batch size of regular updates of progress')
        parser.add_option('-w', '--write_size', action='store', dest='write_size', default='1000000', help='Batch size of regular writes to file / memory dumping')
        parser.add_option('-l', '--removelogs', action='store_true', dest='removelogs', default=False, help='Delete all previous log files for this table')
        parser.add_option('-e', '--removetdes', action='store_true', dest='removetdes', default=False, help='Delete all previous TDE files for this table')
        parser.add_option('-i', '--fromID', action='store', dest='fromID', default=0, help='Last ID processed, continue processing from after this ID')
        parser.add_option('-k', '--from_backup', action='store', dest='from_backup', default=0, help='Resume from previous Backup file, from specified ID')
        parser.add_option('-v', '--csv', action='store_true', dest='csv', default=False, help='Output file to a csv instead of a TDE')
        parser.add_option('-a', '--parts', action='store', dest='parts', default=1, help='Split TDE into parts and build in parallel')
        parser.add_option('-r', '--resume_csv', action='store_true', dest='resume_csv', default=False, help='Resume from full csv')
        parser.add_option('-z', '--header', action='store_true', dest='header', default=False, help='Output a header row with column names, if outputting a csv file')
        
        (self.options, args) = parser.parse_args()
        
        if self.options.credentials:
            try:
                self.user = self.options.credentials.split('/')[0]
                self.password = self.options.credentials.split('/')[1]
            except IndexError:
                pass
        
        if self.options.user: self.user = self.options.user
        if self.options.password: self.password = self.options.password
        if self.options.server: self.server = self.options.server
        if self.options.port: self.port = self.options.port
        if self.options.schema: self.schema = self.options.schema
        if self.options.database: self.database = self.options.database
        if self.options.table_name: self.table_name = self.options.table_name
        
        self.batch_size = int(self.options.batch_size)
        self.write_size = int(self.options.write_size)
        self.nonverbose = self.options.nonverbose
        self.removelogs = self.options.removelogs
        self.removetdes = self.options.removetdes
        self.fromID = int(self.options.fromID)
        self.from_backup = int(self.options.from_backup)
        self.csv = self.options.csv
        self.parts = int(self.options.parts)
        self.resume_csv = self.options.resume_csv
        self.header = self.options.header
        
        # Override any command line arguments with arguments passed via Python        
        if 'user' in self.args: self.user = self.args['user']
        if 'password' in self.args: self.password = self.args['password']
        if 'server' in self.args: self.server = self.args['server']
        if 'port' in self.args: self.port = str(self.args['port'])
        if 'database' in self.args: self.database = self.args['database']
        if 'schema' in self.args: self.schema = self.args['schema']
        if 'table_name' in self.args: self.table_name = self.args['table_name']
        if 'batch_size' in self.args: self.batch_size = self.args['batch_size']
        if 'write_size' in self.args: self.write_size = self.args['write_size']
        if 'nonverbose' in self.args: self.nonverbose = self.args['nonverbose']
        if 'removelogs' in self.args: self.removelogs = self.args['removelogs']
        if 'removetdes' in self.args: self.removetdes = self.args['removetdes']
        if 'fromID' in self.args: self.fromID = self.args['fromID']
        if 'from_backup' in self.args: self.from_backup = self.args['from_backup']
        if 'csv' in self.args: self.csv = self.args['csv']
        if 'resume_csv' in self.args: self.resume_csv = self.args['resume_csv']   
        if 'header' in self.args: self.header = self.args['header']        
        
        if 'parts' in self.args:
            self.parts = self.args['parts']
        else:
            self.parts = 1
        
        if 'part_no' in self.args: 
            self.part_no = self.args['part_no']
        else:
            self.part_no = None
        
        try:
            self.port = self.server.split(',')[1]
        except IndexError:
            pass
        self.server = self.server.split(',')[0]
        
        self.combine_csv_file = None        
    
    def setup_logging(self):
        
        """ Log to 'log' folder within current directory.
            Log file is named after table name and timestamped.
            Print logs of level INFO and higher to console
        """
        
        # Remove logfiles created by DataExtract library
        if os.path.exists(os.path.join(os.getcwd(), 'DataExtract.log')):
            try:
                os.unlink(os.path.join(os.getcwd(), 'DataExtract.log'))
            except WindowsError:
                pass
        
        log_dir_path = os.path.join(os.getcwd(), 'log', self.database)
        if not os.path.exists(log_dir_path): os.makedirs(log_dir_path)
        
        if self.removelogs:
            for the_file in os.listdir(log_dir_path):
                file_path = os.path.join(log_dir_path, the_file)
                try:
                    if self.table_name in the_file and os.path.isfile(file_path) and os.path.splitext(file_path)[1] == '.log':
                        os.unlink(file_path)
                except Exception, e:
                    logging.error(e)
        
        addon = ''
        if self.part_no: addon = '_%d' % self.part_no            
        log_file_path = os.path.join(log_dir_path, '%s_%s_%s%s.log' % ( self.database, self.table_name,
                                                                      time.strftime('%Y%m%d_%H%M',
                                                                                    time.gmtime(time.time()) ),
                                                                       addon ))
        
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            filename=log_file_path,
                            filemode='w')
        
        # Master process print logging to screen
        if self.parts == 1 or ( self.parts > 1 and not self.part_no ):
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            
            formatter = logging.Formatter('%(asctime)s %(message)s')
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)

        
    def connect(self):
        
        """ Connect to the DB and get stats about the table to extract
            for reporting purposes.
        """
        
        logging.info('Server: %s,%s Database: %s' % ( self.server, self.port, self.database ))
        
        #self.conn = pymssql.connect(self.server, self.user, self.password, self.database, self.port)
        self.conn = pyodbc.connect('DRIVER={SQL Server};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s' % ( self.server,
                                                                                                    self.port,
                                                                                                    self.database,
                                                                                                    self.user,
                                                                                                    self.password ),
                                   readonly=True, unicode_results=True)
        
        self.cursor = self.conn.cursor()
        
        SQL = """ SELECT MAX(rows) AS rows
                    FROM %s.dbo.SYSOBJECTS so
                    JOIN %s.dbo.SYSINDEXES si
                      ON si.id = so.id
                   WHERE so.name = '%s'
              """ % ( self.database, self.database, self.table_name )
              
        try:                
            self.cursor.execute(SQL)
        except pyodbc.ProgrammingError:
            
            SQL = """ SELECT COUNT(*) AS rows
                        FROM %s.%s.%s
                  """ % ( self.database, self.schema, self.table_name )
            self.cursor.execute(SQL)
        
        self.rowcount = self.cursor.fetchone()[0]
        logging.info('Table name: %s; Rowcount: %d' % ( self.table_name, self.rowcount ) )
        
        # Use the PK column to keep track of the ID if the connection dies
        
        SQL = """ SELECT c.COLUMN_NAME, c.ORDINAL_POSITION
                    FROM %s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    JOIN %s.INFORMATION_SCHEMA.COLUMNS c
                      ON kcu.COLUMN_NAME = c.COLUMN_NAME
                     AND kcu.TABLE_NAME = c.TABLE_NAME
                   WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                     AND kcu.TABLE_NAME = '%s'
              """ % ( self.database, self.database, self.table_name )
              
        self.cursor.execute(SQL)
        results = self.cursor.fetchone()
        
        if results:
            self.PK_column = results[0]
            self.PK_column_position = results[1]
            logging.info('Primary Key column: %s (pos: %d). Resume from ID > %d' % ( self.PK_column, self.PK_column_position, self.fromID ) )
        else:
            self.PK_column = None
            self.PK_column_position = None
            logging.info('No Primary Key column. Re-starting')
                    
        
        
    def setup_TDE(self, combine=False):
        
        """ Initiate the TDE file, save it in /extracts/ folder.
            TDE file is named after table name and timestamped.
        """
        
        TDE_dir_path = os.path.join(os.getcwd(), 'extracts', self.database )
        if not os.path.exists(TDE_dir_path): os.makedirs(TDE_dir_path)
        
        if combine:
            prev_csv = ''
            csv_dir_path = os.path.join(os.getcwd(), 'csv', self.database )
            for the_file in os.listdir(csv_dir_path):
                m = re.match('^%s_%s_\d{8}_\d{4,6}.csv$' % (self.database, self.table_name), the_file)
                if m and m.group() > prev_csv: prev_csv = m.group()
            
            #self.TDE_file_path = os.path.join(TDE_dir_path, prev_csv[:-4] + '.tde' )
            self.TDE_file_path = os.path.join(TDE_dir_path, '%s_%s_%s.tde' % ( self.database, self.table_name,
                                                                                   time.strftime('%Y%m%d_%H%M',
                                                                                                 time.gmtime(time.time()) ) ))
        
        else:
                
            if self.from_backup and self.PK_column > 0:
                prev_TDE = ''
                for the_file in os.listdir(TDE_dir_path):
                    m = re.match('^%s_%s_\d{8}_\d{4,6}_backup.tde$' % (self.database, self.table_name), the_file)
                    if m and m.group() > prev_TDE: prev_TDE = m.group()
                    
                self.TDE_file_path = os.path.join(TDE_dir_path, prev_TDE )
                self.fromID = self.from_backup
                
                logging.info('Using previous TDE: %s' % self.TDE_file_path)
            
            elif self.fromID and self.PK_column > 0:
                prev_TDE = ''
                for the_file in os.listdir(TDE_dir_path):
                    m = re.match('^%s_%s_\d{8}_\d{4,6}.tde$' % (self.database, self.table_name), the_file)
                    if m and m.group() > prev_TDE: prev_TDE = m.group()
                    
                self.TDE_file_path = os.path.join(TDE_dir_path, prev_TDE )
                
                logging.info('Using previous TDE: %s' % self.TDE_file_path)
            
            else:
                if self.removetdes:
                    for the_file in os.listdir(TDE_dir_path):
                        file_path = os.path.join(TDE_dir_path, the_file)
                        try:
                            if self.table_name in the_file and os.path.isfile(file_path) and os.path.splitext(file_path)[1] == '.tde':
                                os.unlink(file_path)
                        except Exception, e:
                            logging.error(e)
                
                addon = ''
                if self.part_no: addon = '_%d' % self.part_no
                self.TDE_file_path = os.path.join(TDE_dir_path, '%s_%s_%s%s.tde' % ( self.database, self.table_name,
                                                                                   time.strftime('%Y%m%d_%H%M',
                                                                                                 time.gmtime(time.time()) ),
                                                                                    addon ))
        
        # Open TDE file
        self.tdefile = tde.Extract(self.TDE_file_path)
        
    def setup_csv(self, combine=False):
        
        """ Initiate the csv file if the user chose the csv option """
            
        csv_dir_path = os.path.join(os.getcwd(), 'csv', self.database )
        if not os.path.exists(csv_dir_path): os.makedirs(csv_dir_path)
        
        if combine:
            prev_csv = ''
            self.other_parts = []
            for the_file in os.listdir(csv_dir_path):
                m = re.match('^%s_%s_\d{8}_\d{4,6}_1.csv$' % (self.database, self.table_name), the_file)
                if m and m.group() > prev_csv: prev_csv = m.group()
            for the_file in os.listdir(csv_dir_path):
                file_root = prev_csv[:-6]
                n = re.match('^%s_\d+.csv' % file_root, the_file )
                if n and n.group()[-6:] != '_1.csv': self.other_parts.append(os.path.join(csv_dir_path, n.group()))
                
            self.csv_file_path = os.path.join(csv_dir_path, prev_csv )
            
            logging.info('Combining data files into csv: %s' % self.csv_file_path)
            
            self.csvfile = open(self.csv_file_path, 'a')
        
        else:
            
            if self.fromID and self.PK_column > 0:
                prev_csv = ''
                for the_file in os.listdir(csv_dir_path):
                    m = re.match('^%s_%s_\d{8}_\d{4,6}.csv$' % (self.database, self.table_name), the_file)
                    if m and m.group() > prev_csv: prev_csv = m.group()
                    
                self.csv_file_path = os.path.join(csv_dir_path, prev_csv )
                
                logging.info('Using previous csv: %s' % self.csv_file_path)
                
                self.csvfile = open(self.csv_file_path, 'wb')
            
            else:
                if self.removetdes:
                    for the_file in os.listdir(csv_dir_path):
                        file_path = os.path.join(csv_dir_path, the_file)
                        try:
                            if self.table_name in the_file and os.path.isfile(file_path) and os.path.splitext(file_path)[1] == '.csv':
                                os.unlink(file_path)
                        except Exception, e:
                            logging.error(e)
                
                addon = ''
                if self.part_no: addon = '_%d' % self.part_no
                self.csv_file_path = os.path.join(csv_dir_path, '%s_%s_%s%s.csv' % ( self.database, self.table_name,
                                                                                   time.strftime('%Y%m%d_%H%M',
                                                                                                 time.gmtime(time.time()) ),
                                                                                    addon ))
                self.csvfile = open(self.csv_file_path, 'ab')
            
            self.csvTable = csv.writer(self.csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            
        
    def set_table_layout(self, for_tde=True):
        
        self.tableDef = tde.TableDefinition()
        
        # Get Table Definition from INFORMATION_SCHEMA
        SQL = """ SELECT COLUMN_NAME
                       , DATA_TYPE
                    FROM %s.INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_NAME = '%s'
                   ORDER BY ORDINAL_POSITION
              """ % ( self.database, self.table_name )
        #print SQL
              
        self.cursor.execute(SQL)
        
        self.TABLE_DEF = [x for x in self.cursor.fetchall()]
        if not for_tde: return
        
        for column in self.TABLE_DEF:
            if column[1].lower() == 'float':
                self.tableDef.addColumn(column[0], tde.Type.DOUBLE)
            elif column[1].lower() == 'int':
                self.tableDef.addColumn(column[0], tde.Type.INTEGER)
            elif column[1].lower() == 'bigint':
                self.tableDef.addColumn(column[0], tde.Type.INTEGER)
            elif column[1].lower() == 'date':
                self.tableDef.addColumn(column[0], tde.Type.DATE)
            elif column[1].lower() == 'datetime':
                self.tableDef.addColumn(column[0], tde.Type.DATETIME)
            elif column[1].lower() == 'nvarchar':
                self.tableDef.addColumn(column[0], tde.Type.UNICODE_STRING)
            elif column[1].lower() == 'varchar':
                self.tableDef.addColumn(column[0], tde.Type.CHAR_STRING)
            else:
                self.tableDef.addColumn(column[0], tde.Type.UNICODE_STRING)
                
        if self.fromID <= 0:
            self.TDETable = self.tdefile.addTable('Extract', self.tableDef)
        else:
            self.TDETable = self.tdefile.openTable('Extract')
                        
    def build_TDE(self, cursor=None):
        
        if not cursor:
            
            parts_query_addon = ''
            if self.parts > 1:
                parts_query_addon = ' AND %s %% %d + 1 = %d' % ( self.PK_column, self.parts, self.part_no)
                self.rowcount = floor(self.rowcount) / self.parts
            
            if self.PK_column:
            
                SQL = """ SELECT *
                            FROM %s.%s.[%s]
                           WHERE %s > %d %s
                           ORDER BY %s
                      """ % ( self.database, self.schema, self.table_name, self.PK_column, self.fromID, parts_query_addon, self.PK_column )
                      
            else:
                
                SQL = """ SELECT *
                            FROM %s.%s.[%s]
                      """ % ( self.database, self.schema, self.table_name )
                  
            logging.debug(SQL)
                  
            self.cursor.execute(SQL)
        
            startct = self.fromID
            ct = self.fromID
        
        else:
            startct = 0
            ct = 0
            logging.info('Building final TDE file: %s' % self.TDE_file_path)
        
        t0 = time.time()
        
        try:
            filesize = os.path.getsize(self.TDE_file_path)
        except WindowsError:
            filesize = 0
        writerow = self.fromID
        
        while True:
            
            try:
                try:
                    if not cursor:
                        row = self.cursor.fetchone()
                    else:
                        row = cursor.next()
                        
                except OperationalError:
                    
                    if self.PK_column:
                        logging.error('Unable to fetch next record. Last record processed: %s = %d. Retrying...' % ( self.PK_column, self.fromID ))
                        
                        # Re-connect if there is an error
                        time.sleep(2)
                        self.connect()
                        
                        SQL = """  SELECT *
                                     FROM %s.%s.[%s]
                                    WHERE %s > %d
                                    ORDER BY %s
                              """ % ( self.database, self.schema, self.table_name, self.PK_column, self.fromID, self.PK_column )
                              
                        logging.debug(SQL)
                        
                        self.cursor.execute(SQL)
                        
                    else: raise
                
                except StopIteration:
                    break
                    
                if not row: break
                
                newrow = tde.Row(self.tableDef)
                    
                for i in range(len(row)):
                    if row[i] == None or row[i] == '':
                        newrow.setNull(i)
                    elif self.TABLE_DEF[i][1] == 'float':
                        newrow.setDouble(i, row[i])
                    elif self.TABLE_DEF[i][1] == 'int':
                        newrow.setInteger(i, int(row[i]))
                    elif self.TABLE_DEF[i][1] == 'bigint':
                        newrow.setInteger(i, int(row[i]))
                    elif self.TABLE_DEF[i][1] == 'date':
                        if isinstance(row[i], unicode) or isinstance(row[i], str):
                            if ' ' in row[i]:
                                d = time.strptime(row[i].split('.')[0], '%Y-%m-%d %H:%M:%S')
                            else:
                                d = time.strptime(row[i].split('.')[0], '%Y-%m-%d')
                            
                            newrow.setDate(i, d.tm_year, d.tm_mon, d.tm_mday)
                        else:
                            newrow.setDate(i, row[i].year, row[i].month, row[i].day)
                            
                    elif self.TABLE_DEF[i][1] == 'datetime':
                        if isinstance(row[i], unicode) or isinstance(row[i], str):
                            if ' ' in row[i]:
                                d = time.strptime(row[i].split('.')[0], '%Y-%m-%d %H:%M:%S')
                                newrow.setDateTime(i, d.tm_year, d.tm_mon, d.tm_mday, d.tm_hour, d.tm_min, d.tm_sec, 0)
                            else:
                                d = time.strptime(row[i].split('.')[0], '%H:%M:%S')
                                newrow.setDateTime(i, 0, 0, 0, d.tm_hour, d.tm_hour, d.tm_min, d.tm_sec, 0)
                                #newrow.setDateTime(i, d.tm_year, d.tm_mon, d.tm_day, d.tm_hour, d.tm_hour, d.tm_minute, d.tm_sec, 0)
                        else:
                            newrow.setDateTime(i, row[i].year, row[i].month, row[i].day, row[i].hour, row[i].minute, row[i].second, row[i].microsecond)
                            
                    elif self.TABLE_DEF[i][1] == 'nvarchar':
                        #print self.TABLE_DEF[i], row[i]
                        try:
                            newrow.setString(i, row[i].decode('utf-8'))
                        except UnicodeEncodeError:
                            newrow.setString(i, row[i])
                    elif self.TABLE_DEF[i][1] == 'varchar':
                        newrow.setCharString(i, str(row[i]))
                    else:
                        newrow.setString(i, str(row[i]))
                
                try:
                    self.TDETable.insert(newrow)
                    
                except KeyboardInterrupt:
                    self.TDETable.insert(newrow)
                    newrow.close()
                    ct += 1#; print ct
                    if self.PK_column: self.fromID = row[self.PK_column_position - 1]

                    raise
                
                except Exception, e:
                    raise TDEFileWriteError('Error while writing row to TDE. TDE file is now corrupt. Please resume from backup file or restart process: %s' % e)
                
                ct += 1#; print ct
                if self.PK_column: self.fromID = row[self.PK_column_position - 1]
                
                newrow.close()
                
                # Write to file and flush memory
                if ct % self.write_size == 0:
                    
                    logging.debug('Writing to file. Current filesize = %d bytes' % filesize)
                    
                    self.tdefile.close()
                    del self.tdefile
                    self.tdefile = tde.Extract(self.TDE_file_path)
                    self.TDETable = self.tdefile.openTable('Extract')
                    
                    newfilesize = os.path.getsize(self.TDE_file_path)
                    if newfilesize == filesize:
                        raise TDEFileWriteError('Error while writing %d row chunk to TDE file. Please resume from backup file or from ID = %d' % ( self.write_size,
                                                                                                                               writerow ))
                    else:
                        logging.info('Wrote %d bytes for %d rows. Filesize now %d. (current ID = %d)' % ( newfilesize - filesize,
                                                                                                          self.write_size,
                                                                                                          newfilesize,
                                                                                                          self.fromID ))
                        filesize = newfilesize
                        writerow = self.fromID
                        
                    # Create a backup file to resume from if all hell breaks loose (i.e. DASHB export)
                    self.TDE_backup_file = os.path.splitext(self.TDE_file_path)[0] + '_backup.tde'
                    if os.path.exists(self.TDE_backup_file):
                        os.unlink(self.TDE_backup_file)
                    shutil.copy2(self.TDE_file_path, self.TDE_backup_file)
                    logging.info('Copied to backup file %s' % self.TDE_backup_file)
                    
                    
                if ct % self.batch_size == 0:
                    
                    t1 = time.time()
                    time_taken = t1-t0
                    time_taken = '%02d:%02d:%02d' % ( floor(time_taken / 3600),
                                                 floor(time_taken / 60) - floor(time_taken / 3600) * 60,
                                                 floor(time_taken) - floor(time_taken / 60) * 60 )
                    
                    time_left = (self.rowcount - ct) * ( t1-t0 ) / ( ct - startct )
                    time_left = '%02d:%02d:%02d' % ( floor(time_left / 3600),
                                                floor(time_left / 60) - floor(time_left / 3600) * 60,
                                                floor(time_left) - floor(time_left / 60) * 60 )
                    rpm = int( (ct - startct) * 60 / ( t1-t0 ))
                    
                    if self.options.nonverbose:
                        logging.debug('%d/%d (%d rpm) ; Duration: %s ETA: %s' % ( ct, self.rowcount, rpm, time_taken, time_left ) )
                    else:
                        logging.info('%d/%d (%d rpm) ; Duration: %s ETA: %s' % ( ct, self.rowcount, rpm, time_taken, time_left ) )
            
            except KeyboardInterrupt:
                if self.PK_column:
                    logging.error('Stopping process. Last ID processed: %s = %d.' % ( self.PK_column, self.fromID ))
                else:
                    logging.error('Stopping process')
                self.tdefile.close()
                raise 

            except Exception, e:
                if self.PK_column:
                    logging.error('Stopping process. Last ID processed: %s = %d.' % ( self.PK_column, self.fromID ))
                else:
                    logging.error('Stopping process')
                self.tdefile.close()
                raise                
        
        self.tdefile.close()        
        logging.info('Complete! %d/%d records processed' % ( ct, self.rowcount ))
        
        new_filepath = os.path.splitext(self.TDE_file_path)[0][:-14] + '.tde'
        old_backup_filepath = os.path.splitext(self.TDE_file_path)[0] + '_backup.tde'
        
        try:
            os.unlink(new_filepath)
        except OSError:
            pass
    
        try:
            shutil.copy2(self.TDE_file_path, new_filepath)
            try:
                os.unlink(old_backup_filepath)
            except OSError:
                pass
        except:
            raise
        
    def build_csv(self):
        
        parts_query_addon = ''
        if self.parts > 1:
            parts_query_addon = ' AND %s %% %d + 1 = %d' % ( self.PK_column, self.parts, self.part_no)
            self.rowcount = floor(self.rowcount) / self.parts
            
        # Add header row to first csv if done in parallel, or to the only csv if done serially 
        if ( (self.parts > 1 and self.part_no == 1) or self.parts == 1 ) and self.header:
                self.set_table_layout(False)
                newrow = [x[0] for x in self.TABLE_DEF]
                self.csvTable.writerow(newrow)
        
        if self.PK_column:
        
            SQL = """ SELECT *
                        FROM %s.%s.[%s]
                       WHERE %s > %d %s
                       ORDER BY %s
                  """ % ( self.database, self.schema, self.table_name, self.PK_column, self.fromID, parts_query_addon, self.PK_column )
        
        #( self.database, self.table_name, self.PK_column, 14285104, self.PK_column )
        
        else:
            
            SQL = """ SELECT *
                        FROM %s.%s.[%s]
                  """ % ( self.database, self.schema, self.table_name )
              
        logging.debug(SQL)
              
        self.cursor.execute(SQL)
        
        startct = self.fromID
        ct = self.fromID
        
        t0 = time.time()
        
        try:
            filesize = os.path.getsize(self.csv_file_path)
        except WindowsError:
            filesize = 0
        
        while True:
            
            try:
                try:
                    row = self.cursor.fetchone()
                except OperationalError:
                    
                    if self.PK_column:
                        logging.error('Unable to fetch next record. Last record processed: %s = %d. Retrying...' % ( self.PK_column, self.fromID ))
                        
                        # Re-connect if there is an error
                        time.sleep(2)
                        self.connect()
                        
                        SQL = """  SELECT *
                                     FROM %s.%s.[%s]
                                    WHERE %s > %d
                                    ORDER BY %s
                              """ % ( self.database, self.schema, self.table_name, self.PK_column, self.fromID, self.PK_column )
                              
                        logging.debug(SQL)
                        
                        self.cursor.execute(SQL)
                        
                    else: raise
                    
                if not row: break
                
                newrow = []
                
                for item in row:
                    #print item
                    if isinstance(item, unicode):
                        newrow.append(item.encode('utf-8'))
                    else:
                        newrow.append(item)

                try:
                    self.csvTable.writerow(newrow)
                    
                except KeyboardInterrupt:
                    self.csvTable.writerow(newrow)
                    ct += 1#; print ct
                    if self.PK_column: self.fromID = row[self.PK_column_position - 1]

                    raise
                
                except Exception, e:
                    raise CSVFileWriteError('Error while writing row to CSV. CSV file is now corrupt. Please restart process: %s' % e)
                
                ct += 1#; print ct
                if self.PK_column: self.fromID = row[self.PK_column_position - 1]

                '''# Write to backup file
                if ct % self.write_size == 0:
                    
                    logging.debug('Writing to backup file. Current filesize = %d bytes' % filesize)
                        
                    # Create a backup file to resume from if all hell breaks loose (i.e. DASHB export)
                    self.csv_backup_file = os.path.splitext(self.csv_file_path)[0] + '_backup.csv'
                    if os.path.exists(self.csv_backup_file):
                        os.unlink(self.csv_backup_file)
                    shutil.copy2(self.csv_file_path, self.csv_backup_file)
                    logging.info('Copied to backup file %s. Filesize now %d. (current ID = %d)' % (self.csv_backup_file, 
                                                                                                   self.write_size,
                                                                                                   self.fromID) )
                    '''
                    
                if ct % self.batch_size == 0:
                    
                    t1 = time.time()
                    time_taken = t1-t0
                    time_taken = '%02d:%02d:%02d' % ( floor(time_taken / 3600),
                                                 floor(time_taken / 60) - floor(time_taken / 3600) * 60,
                                                 floor(time_taken) - floor(time_taken / 60) * 60 )
                    
                    time_left = (self.rowcount - ct) * ( t1-t0 ) / ( ct - startct )
                    time_left = '%02d:%02d:%02d' % ( floor(time_left / 3600),
                                                floor(time_left / 60) - floor(time_left / 3600) * 60,
                                                floor(time_left) - floor(time_left / 60) * 60 )
                    rpm = int( (ct - startct) * 60 / ( t1-t0 ))
                    
                    if self.options.nonverbose:
                        logging.debug('%d/%d (%d rpm) ; Duration: %s ETA: %s' % ( ct, self.rowcount, rpm, time_taken, time_left ) )
                    else:
                        logging.info('%d/%d (%d rpm) ; Duration: %s ETA: %s' % ( ct, self.rowcount, rpm, time_taken, time_left ) )
            
            except KeyboardInterrupt:
                if self.PK_column:
                    logging.error('Stopping process. Last ID processed: %s = %d.' % ( self.PK_column, self.fromID ))
                else:
                    logging.error('Stopping process')
                self.csvfile.close()
                raise 

            except Exception, e:
                if self.PK_column:
                    logging.error('Stopping process. Last ID processed: %s = %d.' % ( self.PK_column, self.fromID ))
                else:
                    logging.error('Stopping process')
                self.csvfile.close()
                raise                
        
        self.csvfile.close()
        logging.info('Complete! %d/%d records processed' % ( ct, self.rowcount ))
        
    def combine_csv(self):
        
        self.setup_csv(True)
        for part in self.other_parts:
            for line in open(part, 'r'):
                self.csvfile.write(line)
            os.unlink(part)
        self.csvfile.close()
        os.rename(self.csv_file_path, self.csv_file_path[:-6] + '.csv')
        self.csv_file_path = self.csv_file_path[:-6] + '.csv'
        
    def combine_TDE(self, resume=False):
        
        if resume:
            csv_dir_path = os.path.join(os.getcwd(), 'csv', self.database )
            
            prev_csv = ''
            for the_file in os.listdir(csv_dir_path):
                m = re.match('^%s_%s_\d{8}_\d{4,6}.csv$' % (self.database, self.table_name), the_file)
                if m and m.group() > prev_csv: prev_csv = m.group()
                
            self.csv_file_path = os.path.join(csv_dir_path, prev_csv )
            
            logging.info('Converting into TDE from previous csv: %s' % self.csv_file_path)
            
            #self.csvfile = open(self.csv_file_path, 'rb')
            
        else:
            self.combine_csv()
            
        self.setup_TDE(True)
        self.set_table_layout()
        
        self.csvfile = open(self.csv_file_path, 'rb')
        self.csvTable = csv.reader(self.csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        self.build_TDE(self.csvTable)
        
        
    
if __name__ == '__main__':
    
    args = {}
    args['user'] = 'username'
    args['password'] = 'password'
    args['server'] = 'server'
    args['database'] = 'database'
    args['table_name'] = 'table_name'
    args['removetdes'] = False
    args['removelogs'] = False
    args['write_size'] = 1000000
    args['csv'] = True
    args['parts'] = 1
    args['header'] = True
    
    t = BuildTDE(args)
    t.run()