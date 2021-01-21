import socket


host = socket.gethostname()
port = 12345                   # The same port as used by the server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))
sql=input("Write query\n")
s.sendall(sql.encode())

# Received encoded message
encoded=str(s.recv(1024))
s.close()

print('Received', repr(str(encoded))[2:-1])