'''
COMP9334 Project 18s1
Server setup in data centres
Author: Tianpeng Chen z5176343

Update: Seed set for replications
'''
import sys
import time
import operator
from queue import Queue
from numpy import random

next_event = None
event_queue = None
master_clock = None
root = './test/output/'

# TODO: Implement Wrapper.py! Calculate mtr for simulation! Determine Tc!


class SimulationFailure(Exception):

    def __init__(self):
        super().__init__(self)
        self.msg = 'Simulation Failed'

    def __str__(self):
        return self.msg


class Event:

    def __init__(self, e_type, e_time, item, tag=True):
        self.type = e_type    # arrival, departure (job done), setup (accomplishment), off (accomplishment)
        self.time = e_time
        self.item = item
        self.tag = tag

    def __str__(self):
        return f'Event:\t type:{self.type}\t time:{self.time}\t item:{self.item}'

    def __dict__(self):
        return {'type': self.type,
                'time': self.time,
                'item': self.item}

    def cancel_event(self):
        self.tag = False


class EventQueue(list):

    def __init__(self):
        super().__init__(self)

    def get_next_event(self, sort=True):
        if sort:
            try:
                self.sort(key=operator.attrgetter('time'))
                return self.pop(0)
            except IndexError:
                # print('No Events!')
                return Event('End', master_clock.get_time(), None)
        else:
            return self.pop(0)


class MyTimer:

    def __init__(self):
        self.value = 0
        self.finished = False
        # self.t = Timer(0, self.__run__)
        # self.t.start()

    def step(self, step=1):
        self.value += step

    def start(self):
        # self.t.start()
        pass

    def __run__(self):
        while not self.finished:
            self.value += 0.1
            # time.sleep(MY_CLOCK_SPEED * 0.1)

    def reset(self):
        # self.t = Timer(0, self.__run__)
        self.value = 0

    def get_time(self):
        return float(f'{self.value : .1f}')

    def set_time(self, value):
        self.value = value

    def __str__(self):
        return str(self.get_time())


class Job:

    def __init__(self, id, status, arrival_time, service_time, done=False):
        self.id = id
        self.status = status # MARKED or UNMARKED
        self.arrival_time = arrival_time
        self.service_time = service_time
        self.depart_time = 999
        self.done = done

    def set_depart_time(self, d_time):
        self.depart_time = d_time

    def __str__(self):
        return f'id: {self.id}\t status: {self.status}\t arrival_time: {self.arrival_time}\t ' \
               f'service_time: {self.service_time}\t depart_time: {self.depart_time}\t job_done: {self.done}'


class Dispatcher:
    '''
    The dispatcher has sufficent memory (infinite waiting slots)
    '''

    def __init__(self, servers, master_clock):
        self.queue = Queue()
        self.servers = servers
        self.master_clock = master_clock
        # self.running_servers = []

    def __str__(self):

        res = ''
        # running_servers = self.find_servers_by_mode(mode='SETUP')
        # running_servers += self.find_servers_by_mode(mode='BUSY')
        # running_servers.append += self.find_servers_by_mode(mode='DELAYEDOFF')

        for job in self.queue.queue:    #UNSAFE!
            tmp = f'({job.arrival_time:.3f}, {job.service_time:.3f}, {job.status})\n'
            res += tmp

        return res

    def find_servers_by_mode(self, mode='DELAYEDOFF'):

        res = []

        for server in self.servers:
            if server.mode == mode:
                res.append(server)

        return res

    @staticmethod
    def find_server_with_highest_value(servers, mode='DELAYEDOFF'):

        res = servers[0]

        if mode == 'DELAYEDOFF':
            for server in servers:
                if server.get_timer_value() > res.get_timer_value():
                    res = server

        if mode == 'SETUP':
            for server in servers:
                if server.get_remaining_setup_time() > res.get_remaining_setup_time():
                    res = server

        return res

    def arrive(self, job):
        '''
        enqueue a job
        :param job: a Job object
        :return: None
        '''

        # try to find an available server
        available_servers = self.find_servers_by_mode(mode='DELAYEDOFF')
        if len(available_servers) != 0:
            # get server with the highest countdown timer value
            server = self.find_server_with_highest_value(available_servers)
            # send job to server directly
            # self.queue.put_nowait(job)
            self.send_to_server(job, server)
        else:
            # try to find an OFF server
            try:
                target_server = self.find_servers_by_mode(mode='OFF')[0]
            except IndexError:
                # no servers available
                self.enqueue(job, mark='UNMARKED')
                return
            # mark job
            self.enqueue(job, mark='MARKED')
            # turn on target server
            target_server.power_on()    # Be careful!! DEAD LOCK!

    def enqueue(self, job, mark):
        job.status = mark
        self.queue.put_nowait(job)

    @staticmethod
    def send_to_server(job, server):
        '''
        send a job (send to server)
        :param job: a Job object
        :param server: a Server object
        :return: None
        '''
        # item = self.queue.get_nowait()
        server.put_job(job)

    def get_jobs_by_mark(self, mark):

        res = []

        # UNSAFE!
        for job in self.queue.queue:
            if job.status == mark:
                res.append(job)

        return res

    def _get_new_job(self, server):
        # TODO: Debug!!! DONE
        # print('Dispatcher Log: getting new job')
        job = self.dequeue()
        # print(f'Job found:\t {job}')

        # if job is UNMARKED
        if job.status == 'UNMARKED':
            self.send_to_server(job, server)
            # return
        # if job is MARKED
        else:
            unmarked_jobs = self.get_jobs_by_mark('UNMARKED')
            # if there are unmarked jobs
            if len(unmarked_jobs) != 0:
                # print('mark 1')
                chosen_job = unmarked_jobs[0]
                chosen_job.status = 'MARKED'
                self.send_to_server(job, server)
                # recover MARKED job to the queue (UNSAFE!) NO NEED!
                # self.queue.queue.appendleft(job)
                # return
            # if there are no unmarked jobs
            else:
                # print('mark 2')
                setup_servers = self.find_servers_by_mode(mode='SETUP')
                try:
                    target_server = self.find_server_with_highest_value(setup_servers, mode='SETUP')
                    target_server.power_off()
                except IndexError:
                    print('No server is setting up\n')
                finally:
                    self.send_to_server(job, server)
                # return

    def get_job_after_setup(self, server):

        if server.mode != 'SETUP':
            return

        marked_jobs = self.get_jobs_by_mark('MARKED')

        try:
            job = marked_jobs[0]
            self.queue.queue.remove(job)
            self.send_to_server(job, server)
        except IndexError:
            print('No marked jobs found')
            print(f'Called by server: {server}')

    def dequeue(self):

        # assert not self.empty()
        if self.empty():
            print('Queue empty!\n')

        return self.queue.get_nowait()

    def empty(self):

        return self.queue.empty()


class Server:

    def __init__(self, id, setup_time, delayedoff_time, master_clock):
        # _mode_list = ['OFF', 'SETUP', 'BUSY', 'DELAYEDOFF']

        self.id = id
        self.mode = 'OFF'
        self.setup_time = setup_time
        self.setup_counter = setup_time
        self.delayedoff_time = delayedoff_time
        self.master_clock = master_clock
        self.available = False
        self.processing = False
        # self.timer = Timer(delayedoff_time * MY_CLOCK_SPEED, self.power_off)
        self.start_time = None
        self.off_counter = None
        self.dispatcher = None
        self.job = None
        self.job_arrival_time = None
        self.my_clock = MyTimer()
        self.delayedoff_event = None

    def set_dispatcher(self, dispatcher):
        self.dispatcher = dispatcher

    def power_on(self):

        # assert self.mode == 'OFF'

        # setting up server
        self.start_time = master_clock.get_time()
        self.mode = 'SETUP'
        # new event
        new_event = Event('SETUP', next_event.time + self.setup_time, self)
        event_queue.append(new_event)
        return
        # while self.setup_counter > 0:
        #     time.sleep(MY_CLOCK_SPEED)
        #     self.setup_counter -= 1
        # self.my_clock.start()
        # while self.my_clock.get_time() < self.setup_time:
        #     # self.my_clock.step()
        #     # time.sleep(MY_CLOCK_SPEED)
        #     continue

        # while self.master_clock.get_time() != self.start_time + self.setup_time:
        #     # print('Setting up server...')
        #     continue
        # if self.mode != 'SETUP':
        #     # sys.exit(0)
        #     return

    def setup_finished(self):
        self.available = True
        # self.delayed_off()
        self.dispatcher.get_job_after_setup(server=self)

    def power_off(self):

        # assert self.mode != 'OFF'

        self.mode = 'OFF'
        # sys.exit(0)

    def delayed_off(self):

        # assert self.mode in ['SETUP', 'BUSY']

        self.mode = 'DELAYEDOFF'
        self.off_counter = master_clock.get_time()
        new_event = Event('DELAYEDOFF', master_clock.get_time() + self.delayedoff_time, self, tag='Countdown')
        # self.delayed_off()
        self.delayedoff_event = new_event
        event_queue.append(new_event)
        # t = Timer(delayedoff_time, self.power_off())
        # self.timer.start()
        # self.off_counter = self.master_clock.get_time()

    def wake_up(self):

        # assert self.mode == 'DELAYEDOFF'

        # self.timer.cancel()
        self.delayedoff_event.tag = False
        pass

    def server_available(self):
        return self.available

    def server_processing(self):
        return self.processing

    def process_job(self, job):

        # assert self.server_available()

        self.available = False
        self.processing = True
        self.mode = 'BUSY'

        job_done_time = next_event.time + job.service_time
        # new event
        new_event = Event('Depart', job_done_time, self)
        job.set_depart_time(job_done_time)
        # job.done = True
        event_queue.append(new_event)
        # time.sleep(job.service_time * MY_CLOCK_SPEED)
        # while self.master_clock.get_time() != self.job_arrival_time + self.job.service_time:
        #     # print(f'Processing job: {self.job}')
        #     # print(f'expected to finish at t = {self.job_arrival_time + self.job.service_time}')
        #     continue
        # self.my_clock.reset()
        # self.my_clock.start()
        # while self.my_clock.get_time() < self.job.service_time:
        #     # self.my_clock.step()
        #     # time.sleep(MY_CLOCK_SPEED)
        #     continue

        # self.job = None
        # self.depart()

    def put_job(self, job):

        # assert self.server_available()
        if self.mode == 'DELAYEDOFF':
            self.wake_up()
        self.job = job
        self.job_arrival_time = self.master_clock.get_time()
        self.process_job(job)

    def get_timer_value(self):

        return self.master_clock.get_time() - self.off_counter

    def get_remaining_setup_time(self):

        return self.setup_counter

    def depart(self):
        '''
        With current job finished, try to fetch a new job from dispatcher
        '''

        self.processing = False
        self.available = True
        # assert self.mode == 'BUSY'
        _dispatcher = self.dispatcher
        if not self.job:
            return
        self.job.done = True
        print(f'Job Done: {self.job} by {self.id}\n')
        self.job = None
        # TEST ONLY!
        # self.mode = 'DONE'
        print(f'{self.id} is getting new job')

        # if the dispatcher queue is empty, start to delayed off
        if _dispatcher.empty():
            # TODO: This clause is never REACHED!!!
            print(f'No jobs in queue, {self.id} is shutting down')
            self.delayed_off()

        else:
            # print(f'trying...')
            _dispatcher._get_new_job(server=self)

    def __str__(self):
        res = f'Server {self.id}\t Mode: {self.mode}'

        if self.mode == 'SETUP':
            res += f'(complete at t = {self.start_time + self.setup_time})'

        if self.mode == 'BUSY' and self.processing:
            res += f'({self.job.arrival_time}, {self.job.service_time})'

        if self.mode == 'DELAYEDOFF':
            res += f'(expires t = {self.off_counter + self.delayedoff_time})'

        return res


# init master clock
master_clock = MyTimer()


def show_stats(master_clock, dispatcher, servers):

    print(master_clock)
    print(dispatcher)
    if servers:
        print('\t'.join([str(e) for e in servers]))
    # print('\n\n')


def calc_mrt_and_output(job_list, output_filename=None):
    res = 0

    for job in job_list:
        res += job.depart_time - job.arrival_time

    try:
        res /= len(job_list)
    except ZeroDivisionError:
        print('Simulation Failed')
        raise SimulationFailure

    if output_filename:
        with open(root + output_filename, 'w') as f:
            f.write(f'{res:.3f}')

    return res


def output_departure(job_list, output_filename):
    f = open(root + output_filename, 'w')
    job_list.sort(key=operator.attrgetter('depart_time'))
    f.write('\n'.join([f'{job.arrival_time:.3f}\t{job.depart_time:.3f}' for job in job_list]))
    f.write('\n')


def simulate(testid, mode, arrival, service, m, setup_time, delayedoff_time, time_end, seed=0, output=True):
    '''
    main simulation function
    :param testid: for wrapper.py
    :param mode: 'random' or 'trace'
    :param arrival: supplying arrival info, depending on mode
    :param service: supplying service time info, depending on mode
    :param m: #servers, positive integer
    :param setup_time: setup time, positive float
    :param delayedoff_time: the initial value of the countdown timer, positive float
    :param time_end: Only works if mode == 'random', stops the simulation if the master clock exceeds this value
    :param seed: seed for generating random numbers
    :param output: if true, output departure_*.txt and mrt_*.txt
    :return: multiple returns
    '''

    # declare global variables
    global master_clock, event_queue, next_event

    # init servers
    servers = []
    for i in range(m):
        servers.append(Server(i + 1, setup_time, delayedoff_time, master_clock))

    # init dispatcher
    dispatcher = Dispatcher(servers, master_clock)

    # set dispatcher
    for server in servers:
        server.set_dispatcher(dispatcher)

    # def get_time(t):
    #     return t - master_clock

    if mode == 'trace':

        # start master clock
        master_clock.start()

        # trace_number = args[0]

        # arrival_filename = './sample_1/arrival_' + str(arrival) + '.txt'
        # service_filename = './sample_1/service_' + str(service) + '.txt'
        #
        # with open(arrival_filename, 'r') as f_1:
        #     arrival_times = [float(line) for line in f_1]
        # with open(service_filename, 'r') as f_2:
        #     service_times = [float(line) for line in f_2]
        #
        # assert len(arrival_times) == len(service_times)
        arrival_times = arrival
        service_times = service
        nb_of_jobs = len(arrival_times)

        # init jobs
        job_list = []
        for i in range(nb_of_jobs):
            job_list.append(Job(i + 1, '', arrival_times[i], service_times[i]))

        # reset master clock
        # master_clock = MyTimer()

        # run simulation
        count = 0

        # run monitor thread
        # monitor_thread = Thread(target=show_stats, args=(master_clock, dispatcher, servers, time_end))
        # monitor_thread.start()

        # server threads
        server_threads = []

        # while master_clock.get_time() < time_end and count < nb_of_jobs:
        #     # master_clock.step()
        #     next_arrival = arrival_times[count]
        #     if master_clock.get_time() > time_end:
        #         sys.exit()
        #     if master_clock.get_time() == next_arrival:
        #         print('ready to go!\n')
        #         current_job = job_list[count]
        #         server_threads.append(Thread(target=dispatcher.arrive, args=(current_job, )))
        #         server_threads[count].start()
        #         count += 1
            # t = Timer(1, show_stats, [master_clock, dispatcher])
            # t.start()
        event_queue = EventQueue()
        # event_queue.append(init)
        for i in range(nb_of_jobs):
            event_queue.append(Event('Job', arrival_times[i], job_list[i]))

        # start simulation
        while 1:
            next_event = event_queue.get_next_event()
            master_clock.set_time(next_event.time)
            if next_event.type == 'Job':
                dispatcher.arrive(next_event.item)
            elif next_event.type == 'SETUP':
                server = next_event.item
                server.setup_finished()
            elif next_event.type == 'Depart':
                server = next_event.item
                server.depart()
            elif next_event.type == 'DELAYEDOFF':
                server = next_event.item
                server.power_off()
            show_stats(master_clock, dispatcher, servers)
            print('Current event:')
            print(next_event)
            if next_event.type == 'End':
                # sys.exit(0)
                break

        # output = []
        # for job in job_list:
        #     output.append(job.arrival_time, job.depart_time)
        #
        # with open('test_log_3.txt', 'w') as f:
        #     for job in job_list:
        #         f.write(f'{job.arrival_time:.3f}\t {job.depart_time:.3f}\n')

        # departures = [job.depart_time for job in job_list]
        # output_departure(job_list, 'test_departure_3.txt')
        # calc_mrt_and_output(job_list, 'test_mrt_3.txt')
        if output:
            calc_mrt_and_output(job_list, f'mrt_{testid}.txt')
            output_departure(job_list, f'departure_{testid}.txt')

    elif mode == 'random':

        inter_arrival_times = []
        service_times = []
        # indices = []

        def generate_service_time(mu, seed):
            random.seed(seed)
            # random_numbers = []
            for _ in range(nb_of_jobs * 10):
                service_times.append(random.exponential(scale=1/mu))

        def generate_inter_arrival_time(_lambda, seed):
            random.seed(seed)

            for _ in range(nb_of_jobs):
                inter_arrival_times.append(random.exponential(scale=1/_lambda))

        def get_service_time(k):
            indices = []
            service_time = 0
            for i in range(1, 4):
                index = int(str(i) + str(k))
                indices.append(index)
            for index in indices:
                service_time += service_times[index]

            return service_time

        # init jobs
        job_list = []
        last_arrival = 0
        _lambda, _mu = arrival, service
        nb_of_jobs = round(_lambda * time_end)
        SEED = seed
        # init random numbers
        generate_service_time(_mu, SEED)
        generate_inter_arrival_time(_lambda, SEED)
        for i in range(nb_of_jobs):
            _arrival = last_arrival + inter_arrival_times[i]
            _job = Job(id=i + 1, status='', arrival_time=_arrival, service_time=get_service_time(i + 1))
            job_list.append(_job)
            last_arrival = _arrival

        event_queue = EventQueue()
        event_queue.append(Event('init', 0, None))
        next_event = event_queue.pop(0)
        for i in range(nb_of_jobs):
            event_queue.append(Event('Job', job_list[i].arrival_time, job_list[i]))

        # reset master clock
        master_clock.reset()

        # start simulation
        while master_clock.get_time() < time_end:
            next_event = event_queue.get_next_event()
            master_clock.set_time(next_event.time)
            if next_event.type == 'Job':
                dispatcher.arrive(next_event.item)
            elif next_event.type == 'SETUP':
                server = next_event.item
                server.setup_finished()
            elif next_event.type == 'Depart':
                server = next_event.item
                server.depart()
            elif next_event.type == 'DELAYEDOFF':
                server = next_event.item
                if next_event.tag:
                    server.power_off()
            show_stats(master_clock, dispatcher, servers)
            print('Current event:')
            print(next_event)
            if next_event.type == 'End':
                # sys.exit(0)
                break

        job_done_list = []
        for job in job_list:
            if job.done:
                job_done_list.append(job)

        # with open('test_log.txt', 'w') as f:
        #     for job in job_done_list:
        #         f.write(f'{job.arrival_time:.3f}\t {job.depart_time:.3f}\n')

        arrivals = [job.arrival_time for job in job_done_list]
        departures = [job.depart_time for job in job_done_list]

        if output:
            mrt = calc_mrt_and_output(job_done_list, f'mrt_{testid}.txt')
            output_departure(job_done_list, f'departure_{testid}.txt')
        else:
            mrt = calc_mrt_and_output(job_done_list)

        service_times_phase = [job.service_time for job in job_done_list]

        return mrt, inter_arrival_times, service_times, service_times_phase


# params mode, arrival, service, m, setup_time, tc, time_end
# sys.stdout = open('./test_log', 'w')
# test 1
# first_event = Event('init', 0, None)
# simulate(0, 'trace', 1, 1, 3, 50, 100, 1000)
# simulate('trace', 3, 3, 3, 50, 100, 170, init=first_event)

# test 2
# first_event = Event('init', 0, None)
# simulate('trace', 2, 2, 3, 5, 10, 999, init=first_event)

# test 9
# simulate(9, 'random', 0.35, 1, 5, 5, 0.1, 100, seed=0)

# debug test
# print(simulate(99, 'random', 0.35, 1, 5, 5, 29, 1000, seed=0, output=False)[0])
