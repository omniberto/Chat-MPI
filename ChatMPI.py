import mpi4py
mpi4py.rc.initialize = False

from mpi4py import MPI
from time import sleep
from random import randint
from datetime import datetime
from GATES import *
from threading import Thread, Lock

# Varíavel para finalizar execução
terminate = False
provided = MPI.Init_thread(MPI.THREAD_MULTIPLE)


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Lock para garantir que os MPIs não prejudiquem o funcionamento de outro rank
mpi_lock = Lock()

# Definindo timers de pausa dos processos
timer_server = int(randint(2, 8))
timer_sender = int(randint(2, 8))
timer_receiver = int(randint(2, 8))

# Thread do Servidor
def server_thread():
    message_buffer: list[str] = [] # Mensagens a serem enviadas
    delivered: list[str] = [] # Mensagens entregues
    timeout_receiver = 0 # Contador de timeout do processo de recepção de mensagens

    with mpi_lock:
        comm.send(True, dest=SENDER, tag=UNLOCK_SENDER) # Liberar o input() do processo de envio
        request_sender = comm.irecv(source = SENDER, tag = MESSAGE_TAG) # Canal de mensagens do Sender
        request_receiver = comm.irecv(source = RECEIVER, tag = RECEIVER_RECEIVED) # Canal de feedback de recebido do Receiver
        request_read = comm.irecv(source = RECEIVER, tag = RECEIVER_READ) # Canal de feedback de lido do Receiver
    
    # Laço de repetição
    while True:
        with mpi_lock:
            message_from_sender, message = request_sender.test() # Verifica se tem nova mensagem
            
        if message_from_sender and message:
            message_buffer.append(message)

            with open("serverlog.txt", "a+") as server_log:
                server_log.write(f'[{datetime.now().strftime("%d/%m/%Y - %Hh%M")}]: "{message}" foi enviada\n')

            with mpi_lock:
                if len(message_buffer) == 1:
                    comm.send(message_buffer[0], dest = RECEIVER, tag = MESSAGE_TAG_RECEIVER)

                request_sender = comm.irecv(source = SENDER, tag = MESSAGE_TAG)
                comm.send(True, dest = SENDER, tag = UNLOCK_SENDER)

        with mpi_lock:
            receiver_received, feedback_received = request_receiver.test()
            receiver_read, feedback_read = request_read.test()

        if receiver_received and feedback_received: 
            timeout_receiver = 0

            with open("serverlog.txt", "a+") as server_log:
                server_log.write(f'[{datetime.now().strftime("%d/%m/%Y - %Hh%M")}]: "{feedback_received}" foi recebida\n')

            if message_buffer:
                delivered.append(message_buffer.pop(0))

            with mpi_lock:
                comm.send(feedback_received, dest = SENDER, tag = RECEIVER_RECEIVED)
                request_read = comm.irecv(source = RECEIVER, tag = RECEIVER_RECEIVED) 
            
                if len(message_buffer) > 0:
                    comm.send(message_buffer[0], dest = RECEIVER, tag = MESSAGE_TAG_RECEIVER)    

        elif len(message_buffer) > 0:
            timeout_receiver += 1

        if receiver_read and feedback_read:

            with open("serverlog.txt", "a+") as server_log:
                server_log.write(f'[{feedback_read[0]}]: "{feedback_read[1]}" foi lida\n')

            with mpi_lock:
                comm.send(feedback_read, dest = SENDER, tag = RECEIVER_READ)
                request_read = comm.irecv(source = RECEIVER, tag = RECEIVER_READ) 

        if timeout_receiver >= (TIMEOUT_CONTER + timer_receiver):
            timeout_receiver = 0

            if len(message_buffer) > 0:
                with mpi_lock:
                    comm.send(message_buffer[0], dest = RECEIVER, tag = MESSAGE_TAG_RECEIVER)

        sleep(timer_server)

def sender_thread():
    data: str = "" 
    timeout_server = 0 
    
    with mpi_lock:
        request_server= comm.irecv(source = SERVER, tag = UNLOCK_SENDER)
        request_receiver_server = comm.irecv(source = SERVER, tag = RECEIVER_RECEIVED)
        request_read_server = comm.irecv(source = SERVER, tag = RECEIVER_READ)
        
    while True:
        with mpi_lock:
            received_server, unlocked = request_server.test()
            receiver_received_server, feedback_received_server = request_receiver_server.test()
            receiver_read_server, feedback_read_server = request_read_server.test()
            
        if received_server and unlocked:
            timeout_server = 0
            data = input("Digite uma mensagem (end0 para finalizar) >> ").strip()

            if data.lower() == 'end0':
                global terminate
                terminate = True

            with mpi_lock:
                comm.send(data, dest = SERVER, tag = MESSAGE_TAG)
                request_server = comm.irecv(source = SERVER, tag = UNLOCK_SENDER)
        
        if data:
            timeout_server += 1

            if timeout_server >= (TIMEOUT_CONTER + timer_server):
                timeout_server = 0

                with mpi_lock:
                    comm.send(data, dest = SERVER, tag = MESSAGE_TAG)

        if receiver_received_server and feedback_received_server:
            print(f'"{feedback_received_server}" foi recebida!', flush = True)

            with mpi_lock:
                request_receiver_server = comm.irecv(source = SERVER, tag = RECEIVER_RECEIVED) 
        
        if receiver_read_server and feedback_read_server:
            print(f'{feedback_read_server[1]} foi lida em {feedback_read_server[0]}\n', flush = True)

            with mpi_lock:
                receiver_read_server = comm.irecv(source = SERVER, tag = RECEIVER_READ) 

        sleep(timer_sender)

def receiver_thread():
    with mpi_lock:
        request_sender_server = comm.irecv(source = SERVER, tag = MESSAGE_TAG_RECEIVER)

    while True:
        with mpi_lock:
            message_from_server, message_from_sender = request_sender_server.test()

        if message_from_server and message_from_sender:
            date = datetime.now().strftime("%d/%m/%Y - %Hh%M")

            with mpi_lock:
                comm.send(message_from_sender, dest = SERVER, tag = RECEIVER_RECEIVED)

            with open("messages.txt", 'a+') as data_file:
                data_file.write(f'[{date}]: {message_from_sender}\n')

            with mpi_lock:
                comm.send([date, message_from_sender], dest = SERVER, tag = RECEIVER_READ)
                request_sender_server = comm.irecv(source = SERVER, tag = MESSAGE_TAG_RECEIVER) 
            
        sleep(timer_receiver)

if size != 3:
    if rank == 0:
        print(f"Numero de processos nao condizem! Executado com {size}, esperado 3.", flush = True)
        
else:
    if rank == SERVER:
        server = Thread(target = server_thread, daemon = True)
        server.start()

    elif rank == SENDER:
        sender = Thread(target = sender_thread, daemon = True)
        sender.start()

    elif rank == RECEIVER:
        receiver = Thread(target = receiver_thread, daemon = True)
        receiver.start()
        
    try:
        with open("messages.txt", 'w+') as msg:
            msg.write("Inicio das Mensagens:\n")

        with open("serverlog.txt", 'w+') as server_log:
            server_log.write("Inicio do servidor:\n")

        while True:
            if terminate:
                MPI.Finalize()
                break
            sleep(1)

    except KeyboardInterrupt:
        if rank == 0:
            MPI.Finalize()
            print("\nTerminando execução...", flush = True)