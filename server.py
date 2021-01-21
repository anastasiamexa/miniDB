import re
import socket
from database import Database


host = ''        # Symbolic name meaning all available interfaces
port = 12345     # Arbitrary non-privileged port
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((host, port))
sql=""
print ("Please run client")
s.listen(1)
conn, addr = s.accept()
print('Connected by', addr)

try:
    data = conn.recv(1024)
    sql=str(data)[2:-1]

    print ("Client Says: "+sql)
    db = Database('vsmdb', load=False)


    # Creating the tables
    db.create_table('classroom', ['building', 'room_number', 'capacity'], [str, int, int],
                    primary_key='room_number')
    # Insert 5 rows
    db.insert('classroom', ['Packard', '101', '500'])
    db.insert('classroom', ['Painter', '514', '10'])
    db.insert('classroom', ['Taylor', '3128', '70'])
    db.insert('classroom', ['Watson', '100', '30'])
    db.insert('classroom', ['Watson', '120', '50'])

    dispatcher = {'str': str, 'int': int}
    table = ""
    sql_splitted = sql.split(" ")  # Get the type of the command


    if sql_splitted[0] == "drop":
        drop_split = sql.split("drop")
        table = drop_split[1].replace(" ", "")
        db.drop_table(table)
        conn.sendall(re.sub(r'\s+', '', "Done").encode())
    else:
        if sql != "exit":
            print("\nWrong syntax!")


    s.close()

except socket.error:
    print ("Error Occured.")




conn.close()