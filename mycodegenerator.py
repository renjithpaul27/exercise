import pyodbc 
import os
from shutil import copyfile
import fileinput

server_name = input("Enter the server name: ") #IN2399861W1
db_name  = input("Enter the database name: ") #dev_raw_adfinitas

conn = pyodbc.connect('Driver={SQL Server};Server=%s;Database=%s;Trusted_Connection=yes;'%(server_name,db_name))

cursor = conn.cursor()

monn = pyodbc.connect('Driver={SQL Server};Server=%s;Database=%s;Trusted_Connection=yes;'%(server_name,db_name))

monncursor = monn.cursor()

schema_name = input("Enter the schema name: ")
BIML_name = input("Script needed for all tables in schema(Y/N): ")
if BIML_name.upper() == 'Y':
    pass
else:
    table_name_sql = input("Enter the table name/pattern: ")

if BIML_name.upper() == 'Y':    
    sql1="""SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{}' 
            """.format(schema_name)
else:
    sql1="""SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{}' 
            AND TABLE_NAME like '%{}%'""".format(schema_name,table_name_sql)

table_list = cursor.execute(sql1)
tablecountresult = cursor.fetchone()
if tablecountresult is None:
    print("*******Invalid table : {}. Please create the table********".format(table_name_sql)) 
    raise AssertionError
table_list = cursor.execute(sql1)
table_count = 0
seq_container_name = input("Enter the sequence number(if needed):")
if len(seq_container_name) == 0:
    seq_container_name=200
else:
    seq_container_name = int(seq_container_name)


#print("Double check the template file")
orginal_template_file_path = input("Enter the path of template file: ") 
#'C:\\Users\\Renjith.Paul\\Desktop\\TAL\\BIMLPOC\\original_template\\corro_template.xml'
generated_biml_file_path = input("Enter the path to where BIML file to be generated: ") 
#"C:\\Users\\Renjith.Paul\\Desktop\\TAL\\BIMLPOC\\generated_biml_scripts"
for onebyone in table_list:
    table_count = table_count + 1
    seq_container_name = seq_container_name + 1
    TABLE_NAME=onebyone[0]
    table_name_generated  = TABLE_NAME.split("_")
    #orginal_template_file_path='C:\\Users\\Renjith.Paul\\Desktop\\TAL\\BIMLPOC\\original_template\\corro_template.xml'
    filename = table_name_generated[2] + '_bimlscript.xml'
    #copyfile(orginal_template_file_path, os.path.join("C:\\Users\\Renjith.Paul\\Desktop\\TAL\\BIMLPOC\\generated_biml_scripts", filename))
    copyfile(orginal_template_file_path, os.path.join(generated_biml_file_path, filename))
    sql ="""select concat(a,b,c) as xml_string from
    (
    SELECT
    '<Column Name="' + COLUMN_NAME + '"' as a,
    CASE WHEN DATA_TYPE in ('char','varchar') then ' DataType="AnsiString"'
     WHEN DATA_TYPE in ('decimal') then ' DataType="Decimal"'
     WHEN DATA_TYPE in ('int') then ' DataType="Int32"' end as b,
    case 
    when DATA_TYPE in ('char','varchar') then
    ' Length="' + trim(str(CHARACTER_MAXIMUM_LENGTH)) + '" Delimiter="Comma" MaximumWidth="50" />'
    when DATA_TYPE = 'datetime2' then
    ' DataType="DateTime" Delimiter="Comma" />'
     when DATA_TYPE = 'datetime' then
    ' DataType="DateTime" Delimiter="Comma" />'
    when DATA_TYPE = 'time' then
    ' DataType="Time" Delimiter="Comma" />'
	when DATA_TYPE = 'date' then
    ' DataType="Date" Delimiter="Comma" />'
    when DATA_TYPE = 'float' then
    ' DataType="Double" Delimiter="Comma" />'
     when DATA_TYPE = 'decimal' and  COLUMN_NAME <> 'RELATIVE_RECORD_NO' then 
    ' Precision="' + trim(str(NUMERIC_PRECISION)) + '" Scale="' + trim(str(NUMERIC_SCALE)) + '" Delimiter="Comma" />'	
	 when DATA_TYPE = 'int' then 
    ' Precision="' + trim(str(NUMERIC_PRECISION)) + '" Scale="' + trim(str(NUMERIC_SCALE)) + '" Delimiter="Comma" />'
     when DATA_TYPE = 'bigint' then 
    ' Precision="' + trim(str(NUMERIC_PRECISION)) + '" Scale="' + trim(str(NUMERIC_SCALE)) + '" Delimiter="Comma" />'
    end as c
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{}'
    AND TABLE_NAME='{}') z;""".format(schema_name,TABLE_NAME)
    monncursor.execute(sql)
    b=''
    for a in monncursor:
        b=b+'\n'+a[0]
    sql2="""SELECT
    '<Column ErrorRowDisposition="RedirectRow" TruncationRowDisposition="RedirectRow" ColumnName="'+
    COLUMN_NAME+'" />'
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'RAW'
    AND TABLE_NAME='{}'""".format(TABLE_NAME)
    monncursor.execute(sql2)
    c=''
    for a in monncursor:
        c=c+'\n'+a[0]
    with fileinput.FileInput(os.path.join(generated_biml_file_path, filename), inplace=True) as file:
        for line in file:
            line = line.replace('TABLENAME','{}'.format(table_name_generated[2]))
            line = line.replace('seqcontname', str(seq_container_name))
            line = line.replace('xmlcolumntag', b)
            line = line.replace('xmlerrorcolumntag', c)
            print (line, end='')
    print("BIML Script generated for Table no {}:{}".format(seq_container_name,TABLE_NAME))
#print("Script location : C:\\Users\\Renjith.Paul\\Desktop\\TAL\\BIMLPOC\\generated_biml_scripts")
print("Script generation completed")



