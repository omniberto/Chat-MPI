import mpi4py
mpi4py.rc.initialize = False

from mpi4py import MPI
from time import sleep
from random import randint
from datetime import datetime
from GATES import *
from threading import Thread, Lock

# Varíavel para finalizar execução
global terminate
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
    received_feedbacks: list[str] = [] # Feedbacks de Recebimento
    read_feedbacks: list[tuple[str, str]] = []
    timeout_receiver = 0 # Contador de timeout do processo de recepção de mensagens

    with mpi_lock:
        comm.send(True, dest=SENDER, tag=UNLOCK_SENDER) # Liberar o input() do processo de envio
        request_sender = comm.irecv(source = SENDER, tag = MESSAGE_TAG) # Canal de mensagens do Sender
        request_receiver = comm.irecv(source = RECEIVER, tag = RECEIVER_RECEIVED) # Canal de feedback de recebido do Receiver
        request_read = comm.irecv(source = RECEIVER, tag = RECEIVER_READ) # Canal de feedback de lido do Receiver
        request_sender_received_feedback = comm.irecv(source = SENDER, tag = SENDER_RECEIVED_RECEIVED_FEEDBACK) # Canal de feedback de recebido do Sender
        request_sender_read_feedback = comm.irecv(source = SENDER, tag = SENDER_RECEIVED_READ_FEEDBACK) # Canal de feedback de lido do Sender
    
    # Laço de repetição
    while True:
        with mpi_lock:
            message_from_sender, message = request_sender.test() # Verifica se tem nova mensagem
            
        if message_from_sender and message:

            message_buffer.append(message)

            with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                server_log.write(f"[{datetime.now().strftime("%d/%m/%Y - %Hh%M")}]: '{message.decode('utf-8')}' foi enviada ao Servidor pelo Sender\n")

            with mpi_lock:
                if len(message_buffer) == 1:
                    comm.send(message_buffer[0], dest = RECEIVER, tag = MESSAGE_TAG_RECEIVER)
                    with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                        server_log.write(f'[{datetime.now().strftime("%d/%m/%Y - %Hh%M")}]: "{message.decode('utf-8')}" foi enviada ao Receiver pelo Servidor\n')

                request_sender = comm.irecv(source = SENDER, tag = MESSAGE_TAG)
                comm.send(True, dest = SENDER, tag = UNLOCK_SENDER)

        with mpi_lock:
            receiver_received, feedback_received = request_receiver.test()
            receiver_read, feedback_read = request_read.test()
            sender_received, feedback_sender_received = request_sender_received_feedback.test()
            sender_read, feedback_sender_read = request_sender_read_feedback.test()

        if receiver_received and feedback_received: 
            timeout_receiver = 0

            with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                server_log.write(f'[{datetime.now().strftime("%d/%m/%Y - %Hh%M")}]: "{feedback_received.decode('utf-8')}" foi recebida pelo Receiver\n')

            if message_buffer:
                delivered.append(message_buffer.pop(0))

            received_feedbacks.append(feedback_received)

            with mpi_lock:
                if sender_received:
                    with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                        server_log.write(f'[{datetime.now().strftime("%d/%m/%Y - %Hh%M")}]: Confirmação de recebimento da mensagem "{feedback_received.decode('utf-8')}" foi enviada ao Sender pelo Servidor\n')
                    comm.send(received_feedbacks.pop(0), dest = SENDER, tag = RECEIVER_RECEIVED)
                    request_sender_received_feedback = comm.irecv(source = SENDER, tag = SENDER_RECEIVED_RECEIVED_FEEDBACK)

                request_receiver = comm.irecv(source = RECEIVER, tag = RECEIVER_RECEIVED) 
            
                if len(message_buffer) > 0:
                    with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                        server_log.write(f'[{datetime.now().strftime("%d/%m/%Y - %Hh%M")}]: "{message_buffer[0].decode('utf-8')}" foi enviada ao Receiver pelo Servidor\n')
                    comm.send(message_buffer[0], dest = RECEIVER, tag = MESSAGE_TAG_RECEIVER)    

        elif len(message_buffer) > 0:
            timeout_receiver += timer_server

        if receiver_read and feedback_read:

            with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                server_log.write(f'[{feedback_read[0]}]: "{feedback_read[1].decode('utf-8')}" foi lida pelo Receiver\n')
            
            read_feedbacks.append(feedback_read)

            with mpi_lock:
                if sender_read:
                    with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                         server_log.write(f'[{datetime.now().strftime("%d/%m/%Y - %Hh%M")}]: Confirmação de leitura da mensagem "{feedback_read[1].decode('utf-8')}" foi enviada ao Sender pelo Servidor\n')
                    comm.send(read_feedbacks.pop(0), dest = SENDER, tag = RECEIVER_READ)
                    request_sender_read_feedback = comm.irecv(source = SENDER, tag = SENDER_RECEIVED_READ_FEEDBACK)
                
                request_read = comm.irecv(source = RECEIVER, tag = RECEIVER_READ) 

        if timeout_receiver >= (TIMEOUT_CONTER + timer_receiver):
            timeout_receiver = 0

            if len(message_buffer) > 0:
                with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                    server_log.write(f'[{datetime.now().strftime("%d/%m/%Y - %Hh%M")}]: Timeout no Receiver "{message_buffer[0].decode('utf-8')}" foi reenviada ao Receiver pelo Servidor\n')
                with mpi_lock:
                    comm.send(message_buffer[0], dest = RECEIVER, tag = MESSAGE_TAG_RECEIVER)

        sleep(timer_server)

def sender_thread():
    data: str = "" 
    timeout_server = 0 
    
    with mpi_lock:
        comm.send(obj = True, dest = SERVER, tag = SENDER_RECEIVED_RECEIVED_FEEDBACK)
        comm.send(obj = True, dest = SERVER, tag = SENDER_RECEIVED_READ_FEEDBACK)
        request_server = comm.irecv(source = SERVER, tag = UNLOCK_SENDER)
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

            data = data.encode('cp1252', errors='replace')

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
            print(f'"{feedback_received_server.decode('cp1252')}" foi recebida!', flush = True)

            with mpi_lock:
                comm.send(obj = True, dest = SERVER, tag = SENDER_RECEIVED_RECEIVED_FEEDBACK)
                request_receiver_server = comm.irecv(source = SERVER, tag = RECEIVER_RECEIVED) 
        
        if receiver_read_server and feedback_read_server:
            print(f'"{feedback_read_server[1].decode('cp1252')}" foi lida em {feedback_read_server[0]}\n', flush = True)

            with mpi_lock:
                comm.send(obj = True, dest = SERVER, tag = SENDER_RECEIVED_READ_FEEDBACK)
                request_read_server = comm.irecv(source = SERVER, tag = RECEIVER_READ) 

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

            with open("messages.txt", 'a', encoding='utf-8') as data_file:
                data_file.write(f'[{date}] Sender: {message_from_sender.decode('utf-8')}\n')

            with mpi_lock:
                comm.send((date, message_from_sender), dest = SERVER, tag = RECEIVER_READ)
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
        with open("messages.txt", 'w+',encoding='utf-8') as msg:
            msg.write("Inicio das Mensagens:\n")

        with open("serverlog.txt", 'w+',encoding='utf-8') as server_log:
            server_log.write("Inicio do servidor:\n")

        while True:
            if terminate:
                break
            sleep(1)

    except KeyboardInterrupt:
        print("\nTerminando execução...", flush = True)
    finally:
        if rank == 0:
            comm.Abort(0)
            MPI.Finalize()