import database
import re
from database import Database
db = Database('vsmdb', load=False)

# Creating the tables
db.create_table('classroom', ['building', 'room_number', 'capacity'], [str,int,int], primary_key='room_number')
# insert 5 rows
db.insert('classroom', ['Packard', '101', '500'])
db.insert('classroom', ['Painter', '514', '10'])
db.insert('classroom', ['Taylor', '3128', '70'])
db.insert('classroom', ['Watson', '100', '30'])
db.insert('classroom', ['Watson', '120', '50'])

db.create_table('restroom', ['building', 'gender'], [str,str])
# insert 5 rows
db.insert('restroom', ['Packard', 'male'])
db.insert('restroom', ['Painter', 'female'])
db.insert('restroom', ['Taylor', 'male'])
db.insert('restroom', ['Watson', 'male'])
db.insert('restroom', ['Watson', 'female'])

sql = ""
while sql != "exit":
	# Get sql command from user
	sql = input("\nWrite an sql command (low case letters only!)\nIf you want to terminate the program, write 'exit'\n\n")


	dispatcher={'str':str, 'int':int} # Convert string to function
	table = "" # The table's name
	sql_splitted = sql.split(" ") # Get the type of the command
	
	if sql_splitted[0] == "select": # Type of command is select
		select_split = sql.split("select") # select_split = ['','columns...from table ....']
		after_from = select_split[1].split("from") # after_from = ['columns','table ....']
		if "top" not in sql_splitted:
			if "into" not in sql_splitted: 
				columns = after_from[0].replace(" ","") # after_from[0] = 'columns'
				into = None
			else: # There is into in the command
				into_split = after_from[0].split("into") # into_split = ['columns','table name to save as']
				columns = into_split[0].replace(" ","") # into_split[0] = 'columns'
				into = into_split[1].replace(" ","") # into_split[1] = 'table name to save as'
			top = None
		else: # There is top in the command
			temp = after_from[0].split(" ")
			top = int(temp[2]) # Value of top (ex. top 3)
			temp1 = after_from[0].split(str(top)) # temp1 = ['top','columns']
			columns = temp1[1].replace(" ","") # temp1[1] = 'columns'
			if "into" in sql_splitted: # There is into in the command
				into_split = temp1[1].split("into") # into_split = ['columns','table name to save as']
				into = into_split[1].replace(" ","") # into_split[1] = 'table name to save as'
				columns = into_split[0].replace(" ","") # into_split[0] = 'columns'
			else:
				into = None
		if "inner join" in after_from[1]: # There is inner join in the command
			in_join = after_from[1].split("inner join") # in_join = ['left table', 'right table and condition']
			left_table = in_join[0].replace(" ","") # in_join[0] = 'left table'
			in_join_temp = in_join[1].split("on") # in_join_temp = ['right table', 'condition']
			right_table = in_join_temp[0].replace(" ","") # in_join_temp[0] = 'right table'
			in_join_condition = in_join_temp[1].replace(" ","") # in_join_temp[1] = 'condition'
			db.inner_join(left_table, right_table, in_join_condition, "testtable")
			table = "testtable" # the name of the table, result of inner join
		if "where" not in sql_splitted:
			if "order" not in sql_splitted and "by" not in sql_splitted:
				if table != "testtable": # there is no inner join
					table = after_from[1].replace(" ","") # after_from[1] = 'table'
				if columns == "*":
					db.select(table, columns, None, None, False, top, into)
					# Show the table that was saved as...
					if into != None:
						db.show_table(into)
				else: # There is order in the command
					db.select(table, columns.split(","), None, None, False, top, into)
					# Show the table that was saved as...
					if into != None:
						db.show_table(into)
			else: # There is order by in the command
				after_order_by = after_from[1].split("order by") # after_order_by = ['table', 'order by column']
				table = after_order_by[0].replace(" ","") # after_order_by[0] = 'table'
				if "asc" not in sql_splitted: # desc
					order_by_clm = (after_order_by[1].replace("desc","")).replace(" ","") # after_order_by[1] = 'column'
					asc = False
				else: # asc
					order_by_clm = (after_order_by[1].replace("asc","")).replace(" ","") # after_order_by[1] = 'column'
					asc = True
				if columns == "*":
					db.select(table, columns, None, order_by_clm, asc, top, into)
					# Show the table that was saved as...
					if into != None:
						db.show_table(into)
				else:
					db.select(table, columns.split(","), None, order_by_clm, asc, top, into)
					# Show the table that was saved as...
					if into != None:
						db.show_table(into)
		else: # There is where in the command
			if "order" not in sql_splitted and "by" not in sql_splitted:
				tmp = after_from[1].split("where") # tmp = ['table','condition']
				table = tmp[0].replace(" ","") # tmp[0] = 'table'
				where_condition = tmp[1].replace(" ","") # # tmp[1] = 'condition'
				if columns == "*":
					db.select(table, columns, where_condition, None, False, top, into)
					# Show the table that was saved as...
					if into != None:
						db.show_table(into)
				else: 
					db.select(table, columns.split(","), where_condition, None, False, top, into)
					# Show the table that was saved as...
					if into != None:
						db.show_table(into)
					
			else: # There is order by in the command
				tmp = after_from[1].split("where") # tmp = ['table','condition order by column']
				table = tmp[0].replace(" ","") # tmp[0] = 'table'
				tmp1 = tmp[1].split("order by") # tmp1 = ['condition','column']
				where_condition = tmp1[0].replace(" ","") # tmp1[0] = 'condition'
				if "asc" not in sql_splitted: # desc
					order_by_clm = (tmp1[1].replace("desc","")).replace(" ","") # tmp1[1] = 'column'
					asc = False
				else: # asc
					order_by_clm = (tmp1[1].replace("asc","")).replace(" ","") # tmp1[1] = 'column'
					asc = True
				if columns == "*":
					db.select(table, columns, where_condition, order_by_clm, asc, top, into)
					# Show the table that was saved as...
					if into != None:
						db.show_table(into)
				else:
					db.select(table, columns.split(","), where_condition, order_by_clm, asc, top, into)
					# Show the table that was saved as...
					if into != None:
						db.show_table(into)
		# Drop the table formed as a result of inner join
		if table == "testtable":
			db.drop_table("testtable")

	elif sql_splitted[0] == "update": # Type of command is update
		update_split = sql.split("update") # update_split = ['','table set.... where....']
		set_split = update_split[1].split("set") # set_split = ['table','....where....']
		table = set_split[0].replace(" ","") # set_split[0] = 'table'
		after_set = set_split[1].replace(" ","") # after_set = '....where....'
		temp = after_set.split("where") # temp = ['new value','condition']
		condition = temp[1].replace(" ","") # temp[1] = 'condition'
		temp1 = temp[0].split("==") # temp1 = ['column','value']
		column = temp1[0].replace(" ","") # temp1[0] = 'column'
		value = temp1[1].replace(" ","") # temp1[1] = 'value'
		db.update(table, value, column, condition)
		# Show the table
		db.show_table(table)	
		
	elif sql_splitted[0] == "insert" and sql_splitted[1] == "into": # Type of command is insert into
		insert_split = sql.split("insert into") # insert_split = ['','table....values....']
		values_split = insert_split[1].split("values") # values_split = ['table','(values)']
		table = values_split[0].replace(" ", "") # values_split[0] = 'table'
		values = str(values_split[1].replace(" ", ""))[1:-1] # values_split[1] = '(values)' removes ()
		row = values.split(",") # creates the right format of argument (list)
		db.insert(table, row)
		# Show the table
		db.show_table(table)
		
	elif sql_splitted[0] == "delete" and sql_splitted[1] == "from": # Type of command is delete from
		delete_split = sql.split("delete from") # delete_split = ['','table....where....']
		where_split = delete_split[1].split("where") # where_split = ['table','condition']
		table = where_split[0].replace(" ", "") # where_split[0] = 'table'
		condition = where_split[1].replace(" ", "") # where_split[1] = 'condition'
		db.delete(table, condition)
		# Show the table
		db.show_table(table)

	elif sql_splitted[0] == "create" and sql_splitted[1] == "table": # Type of command is create table
		create_split = sql.split("create table") # create_split = ['','table (columns)']
		after_create = create_split[1].split("(") # after_create = ['table','columns)']
		table = after_create[0].replace(" ", "") # after_create[0] = 'table'
		if "primary key" in sql: # There is a private key
			primary_key = after_create[2].replace("))", "") # after_create[2] = 'primary key' removes ))
			temp_list = after_create[1].split(",") # temp_list = ['columns types']
			temp_list.pop() # removes , primary key
			temp_list1 = []
			temp_list2 = []
			# Removes spaces and makes right format
			for i in range(0,len(temp_list)):
				temp_list1.append(temp_list[i].split(" "))
			for i in temp_list1:
				for j in range(0,len(i)):
					if i[j] != "":
						temp_list2.append(i[j])
			rows = []
			types = []
			for i in range(0,len(temp_list2)):
				# Creates two lists, one with rows and the other one with types
				if i % 2 == 0:
					rows.append(temp_list2[i])
				else:
					types.append(temp_list2[i])
			for i in range(0,len(types)):
				if types[i] == "str":
					types[i] = dispatcher["str"] # Convert string to function using the dispatcher
				if types[i] == "int":
					types[i] = dispatcher["int"] # Convert string to function using the dispatcher
			db.create_table(table, rows, types, primary_key)
		else: # There is not a private key
			primary_key = None
			temp_list = after_create[1].split(")") # temp_list = ['columns and types','']
			temp_list.pop() # removes the ''
			temp_list1 = temp_list[0].split(",")
			temp_list2 = []
			temp_list3 = []
			# Removes spaces and makes right format
			for i in range(0,len(temp_list1)):
				temp_list2.append(temp_list1[i].split(" "))
			for i in temp_list2:
				for j in range(0,len(i)):
					if i[j] != "":
						temp_list3.append(i[j])
			rows = []
			types = []
			for i in range(0,len(temp_list3)):
				# Creates two lists, one with rows and the other one with types
				if i % 2 == 0:
					rows.append(temp_list3[i])
				else:
					types.append(temp_list3[i])
			for i in range(0,len(types)):
				if types[i] == "str":
					types[i] = dispatcher["str"] # Convert string to function using the dispatcher
				if types[i] == "int":
					types[i] = dispatcher["int"] # Convert string to function using the dispatcher
			db.create_table(table, rows, types, primary_key)
			
	elif sql_splitted[0] == "drop": # Type of command is drop
		drop_split = sql.split("drop") # drop_split = ['','table']
		table = drop_split[1].replace(" ","") # drop_split[1] = 'table'
		db.drop_table(table)
	
	elif sql_splitted[0] == "create" and sql_splitted[1] == "database": # Type of command is create database
		table = sql_splitted[2] # the new database name
		db = Database(table, load=False)		

	elif sql_splitted[0] == "create" and sql_splitted[1] == "index": # Type of command is create index
		index_split = sql.split("create index") # index_split = ['','name of index on table name']
		after_on = index_split[1].split("on") # after_on = ['name of index','table name']
		index_name = after_on[0].replace(" ","") # after_on[0] = 'name of index'
		table = after_on[1].replace(" ","") # after_on[1] = 'table name'
		db.create_index(table, index_name)
		# Show index
		idx = db._load_idx(index_name)
		idx.plot()
		
	else:
		if sql != "exit": # Wrong user input
			print("\nWrong syntax!")

# Drop the restroom table
db.drop_table("restroom")