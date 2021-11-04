import socket,getpass,threading

def s_send(sock, data):

    lngth = str(len(data))
    lngth = '0'*(9-len(lngth)) + lngth
    data = (lngth+data).encode()
    sock.send(data)

def s_recv(sock):
    lngth = sock.recv(9).decode()
    if not lngth:
        return '', -1
    lngth = int(lngth)
    data = sock.recv(lngth).decode()
    pswd = data.find('$$$~')
    login = data.find('@$$~')
    answ = data.find('@$@~')

    if pswd != -1:
        return data[4:], 0

    elif login != -1:
        return data[4:], 1
        
    elif answ != -1:
        return data[4:], 2

    else:
        return data, 3
    
socket.socket.s_send = s_send
socket.socket.s_recv = s_recv

def listening(sock):
    while True:
        data = sock.s_recv()
        print(data[0])


def main(sock):
    try:
        while True:
            data = sock.s_recv()
            if data[1] == -1:
                return
            elif data[1] == 1:
                print(data[0])
                sock.s_send(input())
            elif data[1] == 0:
                print(data[0])
                sock.s_send(getpass.getpass())
            elif data[1] == 2:
                print(data[0])
                break
            elif data[1] == 3:
                print(data[0])

        print(r'$~: For exit enter "/exit"!')

        threading.Thread(target = listening, args = [sock], daemon = True).start()

        while True:
            for_server = input()
            if for_server == r'/exit':
                break
            sock.s_send(for_server)
    except (ConnectionAbortedError, ConnectionResetError) as err:
        print(err)



sock = socket.socket()
sock.setblocking(True)
ip_addr,con_port='192.168.1.39',13131
sock.connect((ip_addr, con_port))

main(sock)

sock.close()