import socket
import threading

# Function to handle a client connection
def handle_client(client_socket):
    while True:
        message = client_socket.recv(1024).decode("utf-8")
        if not message:
            break
        print(f"Received message: {message}")
    client_socket.close()

# Create a server socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the server socket to localhost and a specific port
server_address = ("127.0.0.1", 6000)
server_socket.bind(server_address)

# Listen for incoming connections
server_socket.listen(5)

print("Server is waiting for connections...")

while True:
    client_socket, client_address = server_socket.accept()
    print(f"Accepted connection from: {client_address}")

    # Create a new thread to handle the client
    client_thread = threading.Thread(target=handle_client, args=(client_socket,))
    client_thread.start()
