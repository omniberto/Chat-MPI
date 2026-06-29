import mpi4py
# Permite a inicializaçao manual do MPI
mpi4py.rc.initialize = False 

from mpi4py import MPI
from time import sleep
from random import randint, random
from datetime import datetime
from values import *
from threading import Thread, Lock

# Variável que indica que está em processo de finalização da execução
global terminating
terminating = False

# Variável que indica a terminação da execução
global terminate
terminate = False

# Permite que o MPI rode com threads simultaneas
provided = MPI.Init_thread(MPI.THREAD_MULTIPLE) 

# Inicialização do MPI, receber o rank e a quantidade de processos
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Lock para garantir que uma thread não prejudiquem o funcionamento de outra
mpi_lock = Lock()

# Definindo timers de pausa dos processos
timer_server:float = random() * randint(2, 8)
timer_sender:float =  random() * randint(2, 8)
timer_receiver:float =  random() * randint(2, 8)

# Função para formatação do texto
def format(text: str, invert = False) -> str:
    if invert:
        return text.encode('cp850', errors='strict').decode('cp1252', errors='strict')
    return text.encode('cp1252', errors='strict').decode('cp850', errors='strict')

# Função para pegar o tempo atual já formatado
def get_time() -> str:
    return datetime.now().strftime("%d/%m/%Y - %Hh%M:%S")

# Thread do Servidor
def server():
    global terminating # Variável de Terminação
    message_queue: list[str] = [] # Mensagens a serem enviadas para o Receiver
    delivered: list[str] = [] # Mensagens entregues ao Receiver
    received_feedbacks_queue: list[str] = [] # Feedbacks de Recebimento a serem enviados para o Sender
    read_feedbacks_queue: list[tuple[str, str]] = [] # Feedbacks de Leitura a serem enviados para o Sender
    timeout_receiver = 0 # Contador de timeout do processo de recepção de mensagens

    with mpi_lock:
        comm.send(True, dest=SENDER, tag=UNLOCK_SENDER) # Liberar o input() do processo de envio
        request_sender = comm.irecv(source = SENDER, tag = MESSAGE_TAG) # Canal de mensagens do Sender
        request_receiver = comm.irecv(source = RECEIVER, tag = RECEIVER_RECEIVED) # Canal de feedback de recebido do Receiver
        request_read = comm.irecv(source = RECEIVER, tag = RECEIVER_READ) # Canal de feedback de lido do Receiver
        request_sender_received_feedback = comm.irecv(source = SENDER, tag = SENDER_RECEIVED_RECEIVED_FEEDBACK) # Canal de feedback de recebido do Sender
        request_sender_read_feedback = comm.irecv(source = SENDER, tag = SENDER_RECEIVED_READ_FEEDBACK) # Canal de feedback de lido do Sender
    
    while True:
        with mpi_lock:
            message_from_sender, message = request_sender.test() # Verifica se tem nova mensagem do Sender
            
        if message_from_sender and message:
            # Se a mensagem for a de finalização
            if message.lower() == 'end0':
                terminating = True
                with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                    server_log.write(f"[{get_time()}]: Sender pediu finalização da execução ao Servidor\n")

                # Feedback de terminação de execução
                print(f'{C_ENDING}{format("Terminação do programa solicitada. Esvaziando filas...", invert = True)}{C_REGULAR}', flush = True)

            else: # Se não
                with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                    server_log.write(f'[{get_time()}]: "{message}" foi enviada ao Servidor pelo Sender\n')
                
            message_queue.append(message)
            
            # Se não estiver finalizando a execução
            if not terminating:
                with mpi_lock:
                    comm.send(True, dest = SENDER, tag = UNLOCK_SENDER) # Libera o Sender para enviar mais alguma mensagem
                    request_sender = comm.irecv(source = SENDER, tag = MESSAGE_TAG) # Atualiza o canal de mensagens do Sender

                with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                    server_log.write(f'[{get_time()}]: Servidor desbloqueou canal de mensagens do Sender\n')
            
            with mpi_lock:
                if len(message_queue) == 1: # Verifica se aquela é a única mensagem no buffer
                    comm.send(message_queue[0], dest = RECEIVER, tag = MESSAGE_TAG_RECEIVER) # Envia ao Receiver se for

                    # Processamento condicional para a missão
                    if message_queue[0] == 'end0':
                        script = f'[{get_time()}]: Requisição de finalização enviada ao Receiver pelo Server\n'
                    else:
                        script = f'[{get_time()}]: "{message}" foi enviada ao Receiver pelo Server\n'

                    with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                        server_log.write(script)

        with mpi_lock: # Testa os outros canais
            receiver_received, feedback_received = request_receiver.test()
            receiver_read, feedback_read = request_read.test()
            sender_received, feedback_sender_received = request_sender_received_feedback.test()
            sender_read, feedback_sender_read = request_sender_read_feedback.test()
            
        if receiver_received and feedback_received: # Se o Receiver acusou que uma mensagem foi recebida
            timeout_receiver = 0 # Reseta o contador de timeout

            # Processamento condicional para a missão
            with open("serverlog.txt", "a", encoding = 'utf-8') as server_log:
                if feedback_received == 'end0':
                    script = f'[{get_time()}]: Requisição de finalização recebida pelo Receiver\n'
                else:
                    script = f'[{get_time()}]: "{feedback_received}" foi recebida pelo Receiver\n'
                server_log.write(script)

            if message_queue and feedback_received == message_queue[0]: # Verifica se tem mensagens no buffer para remover
                delivered.append(message_queue.pop(0))

            received_feedbacks_queue.append(feedback_received) # Adiciona a fila de recebidas
            if feedback_received != 'end0':
                with mpi_lock:
                    request_receiver = comm.irecv(source = RECEIVER, tag = RECEIVER_RECEIVED) # Atualiza o canal de feedback de recebidos do Receiver
                    if len(message_queue) > 0: # Verifica se tem mensagens na fila
                        with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                            if message_queue[0] == 'end0': # Processamento condicional para a missão
                                script = f'[{get_time()}]: Requisição de finalização enviada ao Receiver pelo Server\n'
                            else:
                                script = f'[{get_time()}]: "{message_queue[0]}" foi enviada ao Receiver pelo Servidor\n'
                            server_log.write(script)
                            
                        comm.send(message_queue[0], dest = RECEIVER, tag = MESSAGE_TAG_RECEIVER) # Envia ao Receiver

        elif len(message_queue) > 0 and not receiver_received: # Se ainda tiver mensagens no buffer e o Receiver não acusou ter recebido
            timeout_receiver += timer_server # Aumenta a contagem de timeout

        if sender_received: # Se o Sender recebeu a confirmação de recebido do Receiver
            if isinstance(feedback_sender_received, str): # Se o feedback recebido foi uma string
                with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                    if feedback_sender_received == 'end0': # Processamento condicional para a missão
                        script = f'[{get_time()}]: Requisição de finalização recebida pelo Sender\n'
                    else:
                        script = f'[{get_time()}]: Confirmação de recebimento de recebimento da mensagem "{feedback_sender_received}" foi enviada ao Servidor pelo Sender\n'
                    server_log.write(script)

            if received_feedbacks_queue and feedback_sender_received == received_feedbacks_queue[0]: # Se ainda tem feedbacks na fila e o feedback recebido condiz com o topo da fila
                received_feedbacks_queue.pop(0) # Remove da fila
            
            if received_feedbacks_queue: # Se tem feedbacks pra serem enviados
                comm.send(received_feedbacks_queue[0], dest = SENDER, tag = RECEIVER_RECEIVED) # Envia para o Sender
                with mpi_lock:
                    request_sender_received_feedback = comm.irecv(source = SENDER, tag = SENDER_RECEIVED_RECEIVED_FEEDBACK) # Atualiza o canal de feedback de recebido do Sender

                with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                    if received_feedbacks_queue[0] == 'end0': # Processamento condicional para a missão
                        script = f'[{get_time()}]: Requisição de finalização enviada ao Sender pelo Server\n'
                    else:
                        script = f'[{get_time()}]: Confirmação de recebimento da mensagem "{received_feedbacks_queue[0]}" foi enviada ao Sender pelo Servidor\n'
                    server_log.write(script)

        if receiver_read and feedback_read: # Se o Receiver acusou que a mensagem foi lida

            with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                server_log.write(f'[{feedback_read[0]}]: "{feedback_read[1]}" foi lida pelo Receiver\n')
            
            read_feedbacks_queue.append(feedback_read) # Adiciona na fila de feedbacks de leitura

            with mpi_lock:
                request_read = comm.irecv(source = RECEIVER, tag = RECEIVER_READ) # Atualiza o canal de feedback de lido do Receiver

        if sender_read: # Se o Sender recebeu a confimação de leitura do Receiver
            if isinstance(feedback_sender_read, tuple): # Verifica se recebeu uma tupla de feedback
                if isinstance(feedback_sender_read[1], str): # Verifica se o item '1' da tupla é uma string
                    with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                        server_log.write(f'[{get_time()}]: Confirmação de recebimento de leitura da mensagem "{feedback_sender_read[1]}" foi enviada ao Servidor pelo Sender\n')

            if read_feedbacks_queue and feedback_sender_read == read_feedbacks_queue[0]: # Se ainda tem feedbacks de leitura no fila e se o item 0 da lista for igual ao feedback do Sender
                read_feedbacks_queue.pop(0) # Tira ele da fila
            
            if read_feedbacks_queue: # Se ainda tem itens na fila de feedbacks de leitura
                comm.send(read_feedbacks_queue[0], dest = SENDER, tag = RECEIVER_READ) # Envia o feedback de leitura para o Sender
                        
                with mpi_lock:
                    request_sender_read_feedback = comm.irecv(source = SENDER, tag = SENDER_RECEIVED_READ_FEEDBACK) # Atualiza o canal de feedback de lido do Sender

                with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                        server_log.write(f'[{get_time()}]: Confirmação de leitura da mensagem "{read_feedbacks_queue[0][1]}" foi enviada ao Sender pelo Servidor\n') 

        if timeout_receiver >= (TIMEOUT_CONTER + timer_receiver): # Se o Receiver deu timeout
            timeout_receiver = 0 # Reseta o contador de timeout

            if len(message_queue) > 0: # Verifica que tem item na fila de mensagens
                with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                    server_log.write(f'[{get_time()}]: Timeout no Receiver "{message_queue[0]}" foi reenviada ao Receiver pelo Servidor\n')

                with mpi_lock:
                    comm.send(message_queue[0], dest = RECEIVER, tag = MESSAGE_TAG_RECEIVER) # Reenvia mensagem no topo da fila
                    request_receiver = comm.irecv(source = RECEIVER, tag = RECEIVER_RECEIVED) # Reinicia o canal de feedback de recebido do Receiver
                    request_read = comm.irecv(source = RECEIVER, tag = RECEIVER_READ) # Reinicia o canal de feedback de lido do Receiver
                    
        # Se o programa está em modo de finalização e as filas estão zeradas
        if terminating and len(message_queue) == 0 and len(received_feedbacks_queue) == 0 and len(read_feedbacks_queue) == 0:
            print(f'{C_END}{format("O programa será finalizado!", True)}{C_REGULAR}', flush = True) # Feedback de encerramento

            with open("serverlog.txt", "a", encoding='utf-8') as server_log:
                server_log.write(f'[{get_time()}]: O programa foi finalizado')

            global terminate # Mudando a varíavel de finalização para True
            terminate = True
            break

        sleep(timer_server) # Tempo de pausa do Server

# Thread do Sender
def sender():
    data: str = "" # Variável de controle de mensagens
    timeout_server = 0 # Tempo de timeout do servidor, para reenvio de mensagens
    global terminating # Variável global de finalização
    
    with mpi_lock:
        comm.send(True, dest = SERVER, tag = SENDER_RECEIVED_RECEIVED_FEEDBACK) # Libera canal de feedback de recebimento do Receiver
        comm.send(True, dest = SERVER, tag = SENDER_RECEIVED_READ_FEEDBACK) # Libera canal de feedback de lido do Receiver
        request_server = comm.irecv(source = SERVER, tag = UNLOCK_SENDER) # Canal de requerimento do Server
        request_receiver_server = comm.irecv(source = SERVER, tag = RECEIVER_RECEIVED) # Canal de recebimento do Receiver
        request_read_server = comm.irecv(source = SERVER, tag = RECEIVER_READ) # Canal de lido do Receiver
        
    while True:
        with mpi_lock:
            if not terminating: # Se não está em processo de finalização
                received_server, unlocked = request_server.test() # Verifica o canal resposta do servidor

            receiver_received_server, feedback_received_server = request_receiver_server.test() # Verifica o canal de recebimento do Receiver
            receiver_read_server, feedback_read_server = request_read_server.test() # Verifica o canal de lido do Receiver
        
        if received_server and unlocked and not terminating: # Se o Server recebeu a última mensagem, desbloqueou o enviou e o programa não está em modo de finalização
            timeout_server = 0 # Reseta timeout do Servidor

            data = format(input("\rDigite uma mensagem (end0 para finalizar) >> ")).strip() # Pega mensagem via terminal

            if data.lower() == 'end0': # Verifica se a mensagem não é a de terminação 'end0'
                print(f'{C_ENDING}{format('Você não poderá mandar mais mensagens!', True)}\n{C_REGULAR}', flush = True)
                terminating = True # Se for, inicia processo de finalização

            with mpi_lock:
                comm.send(data, dest = SERVER, tag = MESSAGE_TAG) # Envia mensagem ao Server
                request_server = comm.irecv(source = SERVER, tag = UNLOCK_SENDER) # Atualiza canal de requisição do Server

        if data and not terminating: # Se existe uma mensagem, e não está em processo de finalização
            timeout_server += timer_sender # Aumenta o contador de timeout do Server

            if timeout_server >= (TIMEOUT_CONTER + timer_server): # Se o timeout do Server ultrapssou o limite
                timeout_server = 0 # Reseta o contador de timeout

                with mpi_lock:
                    comm.send(data, dest = SERVER, tag = MESSAGE_TAG) # Reenvia a mensagem
                    request_server = comm.irecv(source = SERVER, tag = UNLOCK_SENDER) # Reinicia canal de requisição do Server

        if receiver_received_server and feedback_received_server: # Se recebeu uma confirmação de recebimento do Receiver
            if format(feedback_received_server, True) == 'end0': # Se recebeu um feedback de finalização
                global terminate
                terminate = True
                with mpi_lock:
                    comm.send(feedback_received_server, dest = SERVER, tag = SENDER_RECEIVED_RECEIVED_FEEDBACK) # Envia a confirmação e o feedback de terminação segura para o Server
                break # Sai do loop

            else:
                print(f'\n{C_RECEIVED}> "{format(feedback_received_server, True)}" foi recebida!{C_REGULAR}', flush = True)

                with mpi_lock:
                    comm.send(feedback_received_server, dest = SERVER, tag = SENDER_RECEIVED_RECEIVED_FEEDBACK) # Envia a confirmação e o feedback de validação para o Server
                    request_receiver_server = comm.irecv(source = SERVER, tag = RECEIVER_RECEIVED) # Atualiza o canal de feedback de recebimento do Receiver
        
        if receiver_read_server and feedback_read_server: # Se recebeu uma confirmação de leitura do Receiver
            print(f'{C_READ}>> "{format(feedback_read_server[1], True)}" foi lida em {feedback_read_server[0]}\n{C_REGULAR}', flush = True)

            with mpi_lock:
                comm.send(feedback_read_server, dest = SERVER, tag = SENDER_RECEIVED_READ_FEEDBACK) # Envia a confirmação e o feedback de validação para o Server
                request_read_server = comm.irecv(source = SERVER, tag = RECEIVER_READ) # Atualiza o canal de feedback de leitura do Receiver

        sleep(timer_sender) # Tempo de pausa do Sender

# Thread do Receiver
def receiver():
    with mpi_lock:
        request_sender_server = comm.irecv(source = SERVER, tag = MESSAGE_TAG_RECEIVER) # Canal de recebimento de mensagens do Sender

    while True:
        with mpi_lock:
            message_from_server, message_from_sender = request_sender_server.test() # Verifica se tem mensagens

        if message_from_server and message_from_sender: # Se recebeu nova mensagem
            date = get_time() # Pega o tempo atual formatado

            if message_from_sender == 'end0': # Se recebeu a mensagem de finalização
                global terminate
                terminate = True
                with mpi_lock:
                    comm.send(message_from_sender, dest = SERVER, tag = RECEIVER_RECEIVED) # Envia o feedback de recebimento ao Server
                break

            else:
                with mpi_lock:
                    comm.send(message_from_sender, dest = SERVER, tag = RECEIVER_RECEIVED) # Envia o feedback de recebimento ao Server

                with open("messages.txt", 'a', encoding='utf-8') as data_file:
                    data_file.write(f'[{date}] Sender: {message_from_sender}\n') # Adiciona a mensagem no arquivo de mensagens

                with mpi_lock:
                    comm.send((date, message_from_sender), dest = SERVER, tag = RECEIVER_READ) # Envia o feedback de leitura ao Server
                    request_sender_server = comm.irecv(source = SERVER, tag = MESSAGE_TAG_RECEIVER) # Atualiza o canal de recebimento de mensagens do Sender

        sleep(timer_receiver) # Tempo de pausa do Receiver

if size != 3: # Verifica se a quantidade de processos condiz com a esperada
    if rank == 0: # Mostra na tela apenas de for o rank 0 (rank garantido de existir e também impede múltiplos feedbacks)
        print(f"{format("Numero de processos nao condizem! Executado com {size}, esperado 3.", True)}", flush = True)
        
else: # Se for a quantidaded correta

    if rank == SENDER: # Se for o processo do Sender
        sender_thread = Thread(target = sender, daemon = True) # Cria uma thread com a função de Sender
        sender_thread.start() # Inicia a thread

    try: # Roda enquanto não há a finalização direta do usuário (ctrl-c)
        with open("messages.txt", 'w+',encoding='utf-8') as msg: # Reinicia o arquivo de mensagens
            msg.write("Inicio das Mensagens:\n")

        with open("serverlog.txt", 'w+',encoding='utf-8') as server_log: # Reinicia o log do Server
            server_log.write(f"Tempos:\nServer: {round(timer_server, 2)}\nSender: {round(timer_sender, 2)}\nReceiver: {round(timer_receiver, 2)}\nInicio do servidor:\n")
        
        if rank == SERVER: # Se for o rank do Server, rode o Server
            server() 

        elif rank == RECEIVER: # Se for o rank do Receiver, rode o Receiver
            receiver()

        while not terminate: # Mantém o programa rodando enquanto não há necessidade de terminação (Necessário para a thread)
            sleep(1)

    except KeyboardInterrupt: # Se for interrompido
        print("\nTerminando execução...", flush = True)

    finally: # Ao final, finaliza os processos
        MPI.Finalize()
